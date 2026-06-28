//! VTuber adapter — an OpenAI-compatible `/v1/chat/completions` (SSE) front door
//! backed by an OAB ACP agent.
//!
//! Any "skin" that already speaks OpenAI chat completions (AniCompanion,
//! Open-LLM-VTuber, …) points at this endpoint and gets a real agent with zero
//! client changes. Inline `[emotion]` tags emitted by the agent pass through the
//! stream untouched; the skin parses + maps them to its own motion system.
//!
//! The gateway has no embedded agent — it relays events to an OAB process over
//! `/ws` and receives replies asynchronously on a different task. So an in-flight
//! HTTP request parks an `mpsc` sender in [`ReplyRegistry`] keyed by a per-request
//! `channel.id`; [`handle_reply`] (called from the `/ws` recv loop) feeds reply
//! snapshots back into it, and the SSE stream re-emits them as OpenAI deltas.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::stream::Stream;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

use crate::schema::{ChannelInfo, GatewayEvent, GatewayReply, SenderInfo};

/// A piece of a streamed reply, handed from the `/ws` recv task to the SSE response.
///
/// The agent streams *full accumulated snapshots* (not deltas) every ~1.5s, plus a
/// final snapshot; the SSE side diffs them into OpenAI content deltas.
pub enum ReplyChunk {
    Snapshot(String),
    Done,
}

/// Per-request `channel.id` → the SSE response awaiting that request's reply.
pub type ReplyRegistry = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<ReplyChunk>>>>;

/// Max wait for the next reply snapshot before closing the stream. Guards the case
/// where no OAB agent is connected to `/ws` (otherwise the request would hang).
const REPLY_IDLE_TIMEOUT: Duration = Duration::from_secs(180);

// --- Config ---

pub struct VtuberConfig {
    /// Bearer key required on inbound requests. `None` = unauthenticated (warned).
    pub auth_key: Option<String>,
    /// Model name echoed back in chunks when the request omits one.
    pub default_model: String,
}

impl VtuberConfig {
    pub fn from_env() -> Option<Self> {
        Self::from_reader(|k| std::env::var(k).ok())
    }

    /// Build from an arbitrary reader so tests avoid `env::set_var` races under
    /// cargo's parallel runner (same pattern as the other adapters).
    fn from_reader<F: Fn(&str) -> Option<String>>(read: F) -> Option<Self> {
        let enabled = read("VTUBER_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        if !enabled {
            return None;
        }
        let auth_key = read("VTUBER_AUTH_KEY");
        if auth_key.is_none() {
            warn!("VTUBER_AUTH_KEY not set — /v1/chat/completions is UNAUTHENTICATED (insecure)");
        }
        let default_model = read("VTUBER_DEFAULT_MODEL").unwrap_or_else(|| "openab".into());
        info!(
            default_model = %default_model,
            authenticated = auth_key.is_some(),
            "vtuber adapter configured"
        );
        Some(Self {
            auth_key,
            default_model,
        })
    }
}

// --- OpenAI request DTOs (subset we honor) ---

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

/// Flatten OpenAI `messages[]` (incl. the skin's own persona/system prompt) into a
/// single prompt string. Each request mints a fresh session, so the full history
/// carried in `messages` is the agent's only context.
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

/// Newly-appended suffix of `full` beyond `sent_len`, and the new sent length.
/// Snapshots grow monotonically; if the prefix ever changed (shouldn't happen) we
/// resend the whole string rather than panic on a non-char-boundary slice.
fn delta_suffix(full: &str, sent_len: usize) -> (String, usize) {
    match full.get(sent_len..) {
        Some(suffix) => (suffix.to_string(), full.len()),
        None => (full.to_string(), full.len()),
    }
}

// --- HTTP handler ---

pub async fn chat_completions(
    State(state): State<Arc<crate::AppState>>,
    headers: HeaderMap,
    Json(req): Json<ChatRequest>,
) -> Response {
    let Some(cfg) = state.vtuber.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "vtuber adapter not configured",
        )
            .into_response();
    };

    // Bearer auth
    if let Some(expected) = cfg.auth_key.as_ref() {
        let provided = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        if provided != Some(expected.as_str()) {
            return (StatusCode::UNAUTHORIZED, "invalid api key").into_response();
        }
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

    // Per-request session id. The reply's `channel.id` echoes this, routing the
    // agent's reply chunks back to this exact request.
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

    let stream = reply_stream(rx, model, channel_id, state.vtuber_pending.clone());
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// OpenAI `chat.completion.chunk` SSE event.
fn chunk_event(
    id: &str,
    created: i64,
    model: &str,
    delta: serde_json::Value,
    finish: Option<&str>,
) -> Event {
    let payload = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
    });
    Event::default().data(payload.to_string())
}

struct StreamState {
    rx: mpsc::UnboundedReceiver<ReplyChunk>,
    sent_len: usize,
    phase: u8, // 0=role, 1=stream snapshots, 2=finish, 3=[DONE], 4=end
    id: String,
    created: i64,
    model: String,
    channel_id: String,
    registry: ReplyRegistry,
}

/// Turn the reply-chunk receiver into an OpenAI SSE stream:
/// `role` chunk → content deltas → `finish_reason:"stop"` → `data: [DONE]`.
fn reply_stream(
    rx: mpsc::UnboundedReceiver<ReplyChunk>,
    model: String,
    channel_id: String,
    registry: ReplyRegistry,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let init = StreamState {
        rx,
        sent_len: 0,
        phase: 0,
        id: format!("chatcmpl-{}", Uuid::new_v4()),
        created: chrono::Utc::now().timestamp(),
        model,
        channel_id,
        registry,
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
                        json!({ "role": "assistant" }),
                        None,
                    );
                    return Some((Ok(ev), s));
                }
                1 => match tokio::time::timeout(REPLY_IDLE_TIMEOUT, s.rx.recv()).await {
                    Ok(Some(ReplyChunk::Snapshot(full))) => {
                        let (delta, new_len) = delta_suffix(&full, s.sent_len);
                        if delta.is_empty() {
                            continue;
                        }
                        s.sent_len = new_len;
                        let ev = chunk_event(
                            &s.id,
                            s.created,
                            &s.model,
                            json!({ "content": delta }),
                            None,
                        );
                        return Some((Ok(ev), s));
                    }
                    Ok(Some(ReplyChunk::Done)) | Ok(None) => {
                        s.phase = 2;
                        continue;
                    }
                    Err(_) => {
                        warn!(channel = %s.channel_id, "vtuber: reply timed out (no agent connected?)");
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
                    return Some((Ok(Event::default().data("[DONE]")), s));
                }
                _ => return None,
            }
        }
    })
}

// --- Reply ingestion (called from the `/ws` recv loop) ---

/// Route a `GatewayReply` (platform = "vtuber") back to its waiting SSE response.
pub async fn handle_reply(reply: &GatewayReply, registry: &ReplyRegistry) {
    let key = reply.channel.id.as_str();
    let full = reply.content.text.clone();
    // Streaming-placeholder bodies are not real content.
    if full == "…" || full == "draft" {
        return;
    }

    let mut map = registry.lock().await;
    let Some(tx) = map.get(key) else {
        return;
    };

    match reply.command.as_deref() {
        // Partial: an in-progress edit carrying the full accumulated text.
        Some("edit_message") => {
            if tx.send(ReplyChunk::Snapshot(full)).is_err() {
                map.remove(key); // SSE side hung up
            }
        }
        // Final: the turn's last message (command = None).
        None => {
            let _ = tx.send(ReplyChunk::Snapshot(full));
            let _ = tx.send(ReplyChunk::Done);
            map.remove(key);
        }
        // Other commands (reactions, create_topic, …) are irrelevant to the chat stream.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_disabled_by_default() {
        assert!(VtuberConfig::from_reader(|_| None).is_none());
    }

    #[test]
    fn config_enabled_with_defaults() {
        let cfg = VtuberConfig::from_reader(|k| match k {
            "VTUBER_ENABLED" => Some("true".into()),
            "VTUBER_AUTH_KEY" => Some("secret".into()),
            _ => None,
        })
        .expect("enabled");
        assert_eq!(cfg.auth_key.as_deref(), Some("secret"));
        assert_eq!(cfg.default_model, "openab");
    }

    #[test]
    fn flatten_labels_roles_and_skips_empty() {
        let msgs = vec![
            ChatMessage {
                role: "system".into(),
                content: "be 小光".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "   ".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "hi".into(),
            },
        ];
        assert_eq!(flatten_messages(&msgs), "System: be 小光\n\nUser: hi");
    }

    #[test]
    fn delta_suffix_emits_only_new_text() {
        let (d1, n1) = delta_suffix("Hello", 0);
        assert_eq!((d1.as_str(), n1), ("Hello", 5));
        let (d2, n2) = delta_suffix("Hello world", n1);
        assert_eq!((d2.as_str(), n2), (" world", 11));
        let (d3, _) = delta_suffix("Hello world", n2);
        assert_eq!(d3, "");
    }

    #[test]
    fn delta_suffix_handles_multibyte() {
        let (d1, n1) = delta_suffix("小光", 0);
        assert_eq!(d1, "小光");
        let (d2, _) = delta_suffix("小光你好", n1);
        assert_eq!(d2, "你好");
    }
}
