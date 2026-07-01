//! VTuber platform adapter.
//!
//! OpenAI-compatible POST /v1/chat/completions (SSE) streams agent replies as
//! `chat.completion.chunk` deltas.

use crate::schema::*;
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use futures_util::stream::Stream;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Tool-status suppression
// ---------------------------------------------------------------------------

fn parse_pure_tool_status(text: &str) -> Option<(String, &'static str)> {
    let normalized = text
        .trim()
        .trim_matches('`')
        .trim()
        .trim_start_matches('✅')
        .trim_start_matches('✔')
        .trim_start_matches('✓')
        .trim()
        .trim_matches('`')
        .trim();

    if normalized.is_empty() || normalized.len() > 64 {
        return None;
    }

    let mut chars = normalized.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ' ') {
        return None;
    }

    let lower = normalized.to_ascii_lowercase();
    if lower == "toolsearch"
        || lower == "tool search"
        || lower.ends_with("search")
        || lower.ends_with("tool")
    {
        return Some((normalized.to_string(), "done"));
    }

    None
}

// ---------------------------------------------------------------------------
// OpenAI-compatible /v1/chat/completions (SSE)
// ---------------------------------------------------------------------------

pub enum ReplyChunk {
    Snapshot(String),
    Done,
}

pub type ReplyRegistry = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<ReplyChunk>>>>;

const REPLY_FIRST_TIMEOUT: Duration = Duration::from_secs(180);
const DEFAULT_REPLY_TAIL_IDLE: Duration = Duration::from_millis(1500);

fn reply_tail_idle_timeout() -> Duration {
    std::env::var("VTUBER_REPLY_TAIL_IDLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_REPLY_TAIL_IDLE)
}

pub struct VtuberConfig {
    pub auth_key: Option<String>,
    pub default_model: String,
}

impl VtuberConfig {
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("VTUBER_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        if !enabled {
            return None;
        }
        let auth_key = std::env::var("VTUBER_AUTH_KEY").ok();
        if auth_key.is_none() {
            warn!("VTUBER_AUTH_KEY not set — /v1/chat/completions is UNAUTHENTICATED");
        }
        let default_model =
            std::env::var("VTUBER_DEFAULT_MODEL").unwrap_or_else(|_| "openab".into());
        Some(Self {
            auth_key,
            default_model,
        })
    }
}

#[derive(Deserialize)]
pub struct ChatMessage {
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: String,
}

#[derive(Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: Option<bool>,
}

fn flatten_messages(messages: &[ChatMessage]) -> String {
    let mut out = String::new();
    for m in messages {
        if m.content.trim().is_empty() {
            continue;
        }
        let label = match m.role.as_str() {
            "system" => "System",
            "assistant" => "Assistant",
            "user" | "" => "User",
            other => other,
        };
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(label);
        out.push_str(": ");
        out.push_str(&m.content);
    }
    out
}

fn delta_suffix(full: &str, sent_len: usize) -> (String, usize) {
    match full.get(sent_len..) {
        Some(suffix) => (suffix.to_string(), full.len()),
        None => (full.to_string(), full.len()),
    }
}

pub async fn chat_completions(
    State(state): State<Arc<crate::AppState>>,
    headers: HeaderMap,
    Json(req): Json<ChatRequest>,
) -> axum::response::Response {
    let Some(ref cfg) = state.vtuber else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "vtuber adapter not configured",
        )
            .into_response();
    };

    if let Some(expected) = cfg.auth_key.as_ref() {
        let provided = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        if provided != Some(expected.as_str()) {
            return (StatusCode::UNAUTHORIZED, "invalid api key").into_response();
        }
    }

    if req.stream != Some(true) {
        return (
            StatusCode::BAD_REQUEST,
            "only streaming mode is supported; set stream: true",
        )
            .into_response();
    }

    let prompt = flatten_messages(&req.messages);
    if prompt.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "messages must contain non-empty content",
        )
            .into_response();
    }
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| cfg.default_model.clone());

    let channel_id = format!("vtb_{}", Uuid::new_v4());
    let (tx, rx) = mpsc::unbounded_channel::<ReplyChunk>();
    state
        .vtuber_pending
        .lock()
        .await
        .insert(channel_id.clone(), tx);

    let event = GatewayEvent::new(
        "vtuber",
        ChannelInfo {
            id: channel_id.clone(),
            channel_type: "dm".into(),
            thread_id: None,
        },
        SenderInfo {
            id: "vtuber".into(),
            name: "vtuber".into(),
            display_name: "VTuber".into(),
            is_bot: false,
        },
        &prompt,
        &format!("vtbmsg_{}", Uuid::new_v4()),
        Vec::new(),
    );
    match serde_json::to_string(&event) {
        Ok(json) => {
            let _ = state.event_tx.send(json);
        }
        Err(e) => {
            state.vtuber_pending.lock().await.remove(&channel_id);
            warn!("vtuber: failed to serialize event: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    }
    info!(channel = %channel_id, "vtuber: chat request dispatched");

    let stream = reply_stream(
        rx,
        model,
        channel_id,
        state.vtuber_pending.clone(),
        reply_tail_idle_timeout(),
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn chunk_event(
    id: &str,
    created: i64,
    model: &str,
    delta: serde_json::Value,
    finish: Option<&str>,
) -> SseEvent {
    let payload = json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
    });
    SseEvent::default().data(payload.to_string())
}

struct StreamState {
    rx: mpsc::UnboundedReceiver<ReplyChunk>,
    sent_len: usize,
    phase: u8,
    id: String,
    created: i64,
    model: String,
    channel_id: String,
    registry: ReplyRegistry,
    seen_snapshot: bool,
    tail_idle: Duration,
}

fn reply_stream(
    rx: mpsc::UnboundedReceiver<ReplyChunk>,
    model: String,
    channel_id: String,
    registry: ReplyRegistry,
    tail_idle: Duration,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    let init = StreamState {
        rx,
        sent_len: 0,
        phase: 0,
        id: format!("chatcmpl-{}", Uuid::new_v4()),
        created: chrono::Utc::now().timestamp(),
        model,
        channel_id,
        registry,
        seen_snapshot: false,
        tail_idle,
    };
    futures_util::stream::unfold(init, |mut s| async move {
        loop {
            match s.phase {
                0 => {
                    s.phase = 1;
                    let ev = chunk_event(
                        &s.id,
                        s.created,
                        &s.model,
                        json!({"role":"assistant"}),
                        None,
                    );
                    return Some((Ok(ev), s));
                }
                1 => match tokio::time::timeout(
                    if s.seen_snapshot {
                        s.tail_idle
                    } else {
                        REPLY_FIRST_TIMEOUT
                    },
                    s.rx.recv(),
                )
                .await
                {
                    Ok(Some(ReplyChunk::Snapshot(full))) => {
                        let (delta, new_len) = delta_suffix(&full, s.sent_len);
                        if delta.is_empty() {
                            continue;
                        }
                        s.sent_len = new_len;
                        s.seen_snapshot = true;
                        let ev = chunk_event(
                            &s.id,
                            s.created,
                            &s.model,
                            json!({"content": delta}),
                            None,
                        );
                        return Some((Ok(ev), s));
                    }
                    Ok(Some(ReplyChunk::Done)) | Ok(None) => {
                        s.phase = 2;
                        continue;
                    }
                    Err(_) => {
                        if s.seen_snapshot {
                            info!(channel = %s.channel_id, "vtuber: reply stream idle, closing");
                        } else {
                            warn!(channel = %s.channel_id, "vtuber: reply timed out");
                        }
                        s.phase = 2;
                        continue;
                    }
                },
                2 => {
                    s.phase = 3;
                    let ev = chunk_event(&s.id, s.created, &s.model, json!({}), Some("stop"));
                    return Some((Ok(ev), s));
                }
                3 => {
                    s.phase = 4;
                    s.registry.lock().await.remove(&s.channel_id);
                    return Some((Ok(SseEvent::default().data("[DONE]")), s));
                }
                _ => return None,
            }
        }
    })
}

pub async fn handle_reply(reply: &GatewayReply, registry: &ReplyRegistry) {
    let key = reply.channel.id.as_str();
    let full = reply.content.text.clone();
    if full == "…" || full == "draft" {
        return;
    }
    let is_pure_tool_status = parse_pure_tool_status(&full).is_some();

    let mut map = registry.lock().await;
    let Some(tx) = map.get(key) else {
        return;
    };

    match reply.command.as_deref() {
        Some("edit_message") => {
            if is_pure_tool_status {
                return;
            }
            if tx.send(ReplyChunk::Snapshot(full)).is_err() {
                map.remove(key);
            }
        }
        None => {
            if !is_pure_tool_status {
                let _ = tx.send(ReplyChunk::Snapshot(full));
            }
            let _ = tx.send(ReplyChunk::Done);
            map.remove(key);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    // -----------------------------------------------------------------------
    // Helper: spin up a minimal gateway with the VTuber chat route.
    // -----------------------------------------------------------------------
    async fn start_gateway() -> String {
        let (event_tx, _) = tokio::sync::broadcast::channel::<String>(256);
        let state = Arc::new(crate::AppState {
            telegram_bot_token: None,
            telegram_secret_token: None,
            telegram_rich_messages: true,
            line_channel_secret: None,
            line_access_token: None,
            teams: None,
            teams_service_urls: Mutex::new(HashMap::new()),
            feishu: None,
            google_chat: None,
            wecom: None,
            telegram_trusted_source_only: false,
            vtuber: Some(VtuberConfig {
                auth_key: Some("test-key".into()),
                default_model: "openab".into(),
            }),
            vtuber_pending: Arc::new(Mutex::new(HashMap::new())),
            ws_token: Some("oab-token".into()),
            event_tx,
            reply_token_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            line_webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            client: reqwest::Client::new(),
        });

        let app = axum::Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(chat_completions),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        addr
    }

    // -----------------------------------------------------------------------
    // Integration: Tier-1 explicitly requires streaming mode.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chat_completions_rejects_non_streaming_requests() {
        let addr = start_gateway().await;
        let url = format!("http://{}/v1/chat/completions", addr);
        let client = reqwest::Client::new();
        let bodies = [
            serde_json::json!({
                "model": "openab",
                "messages": [{"role": "user", "content": "hello"}]
            }),
            serde_json::json!({
                "model": "openab",
                "stream": false,
                "messages": [{"role": "user", "content": "hello"}]
            }),
        ];

        for body in bodies {
            let resp = client
                .post(&url)
                .bearer_auth("test-key")
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let text = resp.text().await.unwrap();
            assert!(text.contains("only streaming mode is supported"));
        }
    }

    // -----------------------------------------------------------------------
    // Unit tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn reply_stream_finishes_after_snapshot_idle() {
        let (tx, rx) = mpsc::unbounded_channel::<ReplyChunk>();
        let registry: ReplyRegistry = Arc::new(Mutex::new(HashMap::new()));
        registry.lock().await.insert("ch_idle".into(), tx.clone());

        let mut stream = Box::pin(reply_stream(
            rx,
            "openab".into(),
            "ch_idle".into(),
            registry.clone(),
            Duration::from_millis(10),
        ));

        assert!(
            stream.next().await.is_some(),
            "role chunk should be emitted first"
        );
        tx.send(ReplyChunk::Snapshot("hello".into())).unwrap();
        assert!(
            stream.next().await.is_some(),
            "content chunk should be emitted"
        );

        let finish = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
        assert!(finish.is_ok(), "finish chunk should arrive after tail idle");
        assert!(finish.unwrap().is_some());

        let done = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
        assert!(done.is_ok(), "[DONE] should arrive after finish chunk");
        assert!(done.unwrap().is_some());

        assert!(
            !registry.lock().await.contains_key("ch_idle"),
            "stream completion should remove pending registry entry"
        );
    }
}
