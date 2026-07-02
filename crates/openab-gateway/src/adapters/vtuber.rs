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

/// Fixed channel ID for VTuber session reuse.
/// All /v1/chat/completions requests share this channel so the OAB
/// session pool (`[pool]`) reuses the same warm agent process.
const VTUBER_PERSISTENT_CHANNEL: &str = "vtb_persistent";

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
    /// When true (default), the adapter auto-resumes after a tool-call result
    /// so the user sees the final answer, not the intermediate tool output.
    pub auto_tool_loop: bool,
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
            auto_tool_loop: std::env::var("VTUBER_AUTO_TOOL_LOOP")
                .map(|v| v != "false" && v != "0")
                .unwrap_or(true),
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

    // Serialise requests — one agent turn at a time
    let Ok(_guard) = state.vtuber_request_lock.try_lock() else {
        let mut resp = axum::response::Response::new(
            "agent is busy, retry in a moment".into(),
        );
        *resp.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        resp.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            "3".parse().unwrap(),
        );
        return resp;
    };

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

    // Persistent channel: reuse same session across all vtuber requests
    let channel_id = VTUBER_PERSISTENT_CHANNEL.to_string();
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
        cfg.auto_tool_loop,
        state.event_tx.clone(),
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
    /// Accumulated reply text for tool-result detection.
    accumulated: String,
    /// Whether auto-tool-loop is enabled.
    auto_tool_loop: bool,
    /// Event sender for re-dispatching tool-continue prompts.
    event_tx: tokio::sync::broadcast::Sender<String>,
    /// How many tool-loop iterations we've done so far.
    tool_loops: u8,
}

const MAX_TOOL_LOOPS: u8 = 4;

/// Heuristic: does the accumulated reply look like a pure tool result
/// (no meaningful final answer for the user)?
fn is_tool_result_reply(text: &str) -> bool {
    let t = text.trim();
    if t.is_empty() || t.len() > 300 {
        return false;
    }
    // Common patterns from tool-call results (fetch, search, etc.)
    let lower = t.to_lowercase();
    if lower.starts_with("✅") || lower.starts_with("✔") || lower.starts_with("✓") {
        return true;
    }
    if lower.contains("(text/html") && lower.contains("charset") {
        return true; // raw fetch result header
    }
    false
}

/// Send a "continue" prompt through the event bus so the persistent
/// session picks up where it left off after a tool call.
fn send_continue(evt: &tokio::sync::broadcast::Sender<String>, channel_id: &str, label: &str) {
    let event = GatewayEvent::new(
        "vtuber",
        ChannelInfo {
            id: channel_id.to_string(),
            channel_type: "dm".into(),
            thread_id: None,
        },
        SenderInfo {
            id: "vtuber".into(),
            name: "vtuber".into(),
            display_name: "VTuber".into(),
            is_bot: false,
        },
        label,
        &format!("vtbmsg_{}", Uuid::new_v4()),
        Vec::new(),
    );
    if let Ok(json) = serde_json::to_string(&event) {
        let _ = evt.send(json);
    }
}

fn reply_stream(
    rx: mpsc::UnboundedReceiver<ReplyChunk>,
    model: String,
    channel_id: String,
    registry: ReplyRegistry,
    tail_idle: Duration,
    auto_tool_loop: bool,
    event_tx: tokio::sync::broadcast::Sender<String>,
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
        accumulated: String::new(),
        auto_tool_loop,
        event_tx,
        tool_loops: 0,
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
                        s.accumulated = full.clone();
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
                            warn!(channel = %s.channel_id, "vtuber: no reply — session may be dead; next request triggers respawn");
                        }
                        s.phase = 2;
                        continue;
                    }
                },
                2 => {
                    // Auto tool-loop: if the reply looks like a bare tool result
                    // (e.g. "✅ URL"), re-send a continue prompt and re-stream.
                    if s.auto_tool_loop
                        && s.tool_loops < MAX_TOOL_LOOPS
                        && is_tool_result_reply(&s.accumulated)
                    {
                        s.tool_loops += 1;
                        info!(
                            channel = %s.channel_id,
                            loops = s.tool_loops,
                            reply_len = s.accumulated.len(),
                            "vtuber: tool-loop continue"
                        );
                        s.sent_len = 0;
                        s.accumulated = String::new();
                        s.seen_snapshot = false;
                        s.phase = 0;
                        send_continue(&s.event_tx, &s.channel_id, "continue");
                        continue;
                    }
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
            if key != VTUBER_PERSISTENT_CHANNEL {
                map.remove(key);
            }
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
            telegram_streaming: None,
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
                auto_tool_loop: false, // tests don't need tool-loop
            }),
            vtuber_pending: Arc::new(Mutex::new(HashMap::new())),
            vtuber_request_lock: Arc::new(tokio::sync::Mutex::new(())),
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

        let (evt_tx, _) = tokio::sync::broadcast::channel::<String>(1);
        let mut stream = Box::pin(reply_stream(
            rx,
            "openab".into(),
            "ch_idle".into(),
            registry.clone(),
            Duration::from_millis(10),
            false, // auto_tool_loop
            evt_tx,
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
