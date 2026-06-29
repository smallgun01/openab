//! VTuber platform adapter.
//!
//! Tier-1: OpenAI-compatible POST /v1/chat/completions (SSE) — streams agent
//!         replies as `chat.completion.chunk` deltas.
//! Tier-2: WebSocket /v1/vtuber/ws — pushes agent_state, tool_status, emotion,
//!         and notification events derived from GatewayReply commands.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
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
const REPLY_AGENT_WAIT: Duration = Duration::from_secs(10);
const VTUBER_MAX_INFLIGHT: usize = 32;

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

    if state.vtuber_pending.lock().await.len() >= VTUBER_MAX_INFLIGHT {
        return (StatusCode::TOO_MANY_REQUESTS, "too many in-flight requests").into_response();
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
    warned: bool,
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
        warned: false,
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
                1 => {
                    let wait = if s.warned { REPLY_IDLE_TIMEOUT } else { REPLY_AGENT_WAIT };
                    match tokio::time::timeout(wait, s.rx.recv()).await {
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
                        Err(_) if !s.warned => {
                            s.warned = true;
                            return Some((Ok(Event::default().comment("waiting for agent")), s));
                        }
                        Err(_) => {
                            warn!(channel = %s.channel_id, "vtuber: reply timed out (no agent connected?)");
                            s.phase = 2;
                            continue;
                        }
                    }
                }
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
        Some("edit_message") if tx.send(ReplyChunk::Snapshot(full.clone())).is_err() => {
            map.remove(key);
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

// ---------------------------------------------------------------------------
// Tier-2 WS event types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
pub enum WsEvent {
    #[serde(rename = "agent_state")]
    AgentState {
        ts: i64,
        state: &'static str,
        session_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<AgentStateDetail>,
    },
    #[serde(rename = "emotion")]
    Emotion {
        ts: i64,
        session_id: String,
        tag: String,
        intensity: f32,
    },
    #[serde(rename = "notification")]
    Notification {
        ts: i64,
        text: String,
        urgency: &'static str,
    },
    #[serde(rename = "pong")]
    Pong { ts: i64 },
}

#[derive(Clone, Debug, Serialize)]
pub struct AgentStateDetail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subagent_count: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum WsCommand {
    #[serde(rename = "subscribe")]
    Subscribe { events: Vec<String> },
    #[serde(rename = "ping")]
    Ping,
}

// ---------------------------------------------------------------------------
// Connected WS clients registry
// ---------------------------------------------------------------------------

pub struct WsClient {
    pub tx: mpsc::UnboundedSender<String>,
    pub subscribed: Option<HashSet<String>>,
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

pub type WsClients = Arc<Mutex<HashMap<u64, WsClient>>>;

pub fn new_ws_clients() -> WsClients {
    Arc::new(Mutex::new(HashMap::new()))
}

// ---------------------------------------------------------------------------
// Reaction emoji → agent_state mapping
// ---------------------------------------------------------------------------

struct ReactionMapping {
    state: &'static str,
    tool_class: Option<&'static str>,
}

fn reaction_to_state(emoji: &str) -> Option<ReactionMapping> {
    let first = emoji.chars().next()?;
    Some(match first {
        '👀' => ReactionMapping { state: "thinking", tool_class: None },
        '🤔' => ReactionMapping { state: "thinking", tool_class: None },
        '🔥' => ReactionMapping { state: "working", tool_class: Some("tool") },
        '👨' => ReactionMapping { state: "working", tool_class: Some("coding") },
        '⚡' => ReactionMapping { state: "working", tool_class: Some("web") },
        '🆗' => ReactionMapping { state: "attention", tool_class: None },
        '😱' => ReactionMapping { state: "error", tool_class: None },
        '🥱' => ReactionMapping { state: "error", tool_class: None },
        '😨' => ReactionMapping { state: "error", tool_class: None },
        _ => return None,
    })
}

fn extract_emotion_tags(text: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('[') {
        if let Some(end) = rest[start..].find(']') {
            let tag = &rest[start + 1..start + end];
            if !tag.is_empty() && tag.len() < 30 && tag.chars().all(|c| c.is_alphanumeric() || c == '_') {
                tags.push(tag.to_string());
            }
            rest = &rest[start + end + 1..];
        } else {
            break;
        }
    }
    tags
}

// ---------------------------------------------------------------------------
// Derive Tier-2 WS events from a GatewayReply
// ---------------------------------------------------------------------------

pub fn derive_events(reply: &GatewayReply) -> Vec<WsEvent> {
    let ts = chrono::Utc::now().timestamp();
    let session_id = reply.channel.id.clone();
    let mut events = Vec::new();

    match reply.command.as_deref() {
        Some("add_reaction") => {
            if let Some(mapping) = reaction_to_state(&reply.content.text) {
                events.push(WsEvent::AgentState {
                    ts,
                    state: mapping.state,
                    session_id,
                    detail: mapping.tool_class.map(|tc| AgentStateDetail {
                        tool_class: Some(tc),
                        subagent_count: None,
                    }),
                });
            }
        }
        Some("remove_reaction") => {}
        Some("edit_message") => {
            for tag in extract_emotion_tags(&reply.content.text) {
                events.push(WsEvent::Emotion {
                    ts,
                    session_id: session_id.clone(),
                    tag,
                    intensity: 1.0,
                });
            }
        }
        None | Some("send_message") => {
            for tag in extract_emotion_tags(&reply.content.text) {
                events.push(WsEvent::Emotion {
                    ts,
                    session_id: session_id.clone(),
                    tag,
                    intensity: 1.0,
                });
            }
            events.push(WsEvent::AgentState {
                ts,
                state: "idle",
                session_id,
                detail: None,
            });
        }
        _ => {}
    }

    events
}

// ---------------------------------------------------------------------------
// Broadcast events to connected WS clients
// ---------------------------------------------------------------------------

pub async fn broadcast(clients: &WsClients, events: &[WsEvent]) {
    if events.is_empty() {
        return;
    }
    let mut dead = Vec::new();
    let mut guard = clients.lock().await;
    for (&id, client) in guard.iter() {
        for event in events {
            let event_type = match event {
                WsEvent::AgentState { .. } => "agent_state",
                WsEvent::Emotion { .. } => "emotion",
                WsEvent::Notification { .. } => "notification",
                WsEvent::Pong { .. } => continue,
            };
            if let Some(ref subs) = client.subscribed {
                if !subs.contains(event_type) {
                    continue;
                }
            }
            if let Ok(json) = serde_json::to_string(event) {
                if client.tx.send(json).is_err() {
                    dead.push(id);
                    break;
                }
            }
        }
    }
    for id in dead {
        guard.remove(&id);
    }
}

// ---------------------------------------------------------------------------
// WS upgrade handler: GET /v1/vtuber/ws
// ---------------------------------------------------------------------------

pub async fn ws_upgrade(
    State(state): State<Arc<crate::AppState>>,
    query: Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| query.get("token").map(|s| s.as_str()));

    let expected = state.vtuber.as_ref().and_then(|c| c.auth_key.as_ref());
    if let Some(expected) = expected {
        if token != Some(expected.as_str()) {
            warn!("vtuber WS rejected: invalid or missing token");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_ws(state, socket))
}

async fn handle_ws(state: Arc<crate::AppState>, socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    info!(client_id, "vtuber WS client connected");

    let clients = match &state.vtuber_ws_clients {
        Some(c) => c.clone(),
        None => return,
    };
    clients.lock().await.insert(client_id, WsClient { tx, subscribed: None });

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_tx.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let clients_for_recv = clients.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            if let Message::Text(text) = msg {
                match serde_json::from_str::<WsCommand>(&text) {
                    Ok(WsCommand::Subscribe { events }) => {
                        let mut guard = clients_for_recv.lock().await;
                        if let Some(client) = guard.get_mut(&client_id) {
                            client.subscribed = Some(events.into_iter().collect());
                            info!(events = ?client.subscribed, "vtuber WS subscribe updated");
                        }
                    }
                    Ok(WsCommand::Ping) => {
                        let pong = WsEvent::Pong {
                            ts: chrono::Utc::now().timestamp(),
                        };
                        if let Ok(json) = serde_json::to_string(&pong) {
                            let guard = clients_for_recv.lock().await;
                            if let Some(client) = guard.get(&client_id) {
                                let _ = client.tx.send(json);
                            }
                        }
                    }
                    Err(_) => {
                        warn!(raw = %text, "vtuber WS unknown command");
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    clients.lock().await.remove(&client_id);
    info!(client_id, "vtuber WS client disconnected");
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

    // --- Tier-2 unit tests ---

    #[test]
    fn reaction_mapping_covers_all_oab_emojis() {
        assert_eq!(reaction_to_state("👀").unwrap().state, "thinking");
        assert_eq!(reaction_to_state("🤔").unwrap().state, "thinking");
        assert_eq!(reaction_to_state("🔥").unwrap().state, "working");
        assert_eq!(reaction_to_state("👨\u{200d}💻").unwrap().state, "working");
        assert_eq!(reaction_to_state("👨\u{200d}💻").unwrap().tool_class, Some("coding"));
        assert_eq!(reaction_to_state("⚡").unwrap().state, "working");
        assert_eq!(reaction_to_state("⚡").unwrap().tool_class, Some("web"));
        assert_eq!(reaction_to_state("🆗").unwrap().state, "attention");
        assert_eq!(reaction_to_state("😱").unwrap().state, "error");
        assert_eq!(reaction_to_state("🥱").unwrap().state, "error");
        assert_eq!(reaction_to_state("😨").unwrap().state, "error");
        assert!(reaction_to_state("😊").is_none());
    }

    #[test]
    fn extract_emotion_tags_basic() {
        assert_eq!(extract_emotion_tags("Hello [happy] world"), vec!["happy"]);
        assert_eq!(
            extract_emotion_tags("[excited] Hi [joy] there"),
            vec!["excited", "joy"]
        );
        assert!(extract_emotion_tags("no tags here").is_empty());
        assert!(extract_emotion_tags("[with spaces]").is_empty());
        assert!(extract_emotion_tags("[]").is_empty());
    }

    #[test]
    fn derive_events_add_reaction() {
        use crate::schema::{Content, ReplyChannel};
        let reply = GatewayReply {
            schema: "openab.gateway.reply.v1".into(),
            reply_to: "evt_1".into(),
            platform: "vtuber".into(),
            channel: ReplyChannel { id: "ch_1".into(), thread_id: None },
            content: Content { content_type: "text".into(), text: "🤔".into(), attachments: vec![] },
            command: Some("add_reaction".into()),
            request_id: None,
            quote_message_id: None,
        };
        let events = derive_events(&reply);
        assert_eq!(events.len(), 1);
        match &events[0] {
            WsEvent::AgentState { state, session_id, .. } => {
                assert_eq!(*state, "thinking");
                assert_eq!(session_id, "ch_1");
            }
            _ => panic!("expected AgentState"),
        }
    }

    #[test]
    fn derive_events_edit_message_with_emotion() {
        use crate::schema::{Content, ReplyChannel};
        let reply = GatewayReply {
            schema: "openab.gateway.reply.v1".into(),
            reply_to: "evt_1".into(),
            platform: "vtuber".into(),
            channel: ReplyChannel { id: "ch_1".into(), thread_id: None },
            content: Content { content_type: "text".into(), text: "[excited] Hello!".into(), attachments: vec![] },
            command: Some("edit_message".into()),
            request_id: Some("req_1".into()),
            quote_message_id: None,
        };
        let events = derive_events(&reply);
        assert_eq!(events.len(), 1);
        match &events[0] {
            WsEvent::Emotion { tag, intensity, .. } => {
                assert_eq!(tag, "excited");
                assert_eq!(*intensity, 1.0);
            }
            _ => panic!("expected Emotion"),
        }
    }

    #[test]
    fn derive_events_send_message_idle() {
        use crate::schema::{Content, ReplyChannel};
        let reply = GatewayReply {
            schema: "openab.gateway.reply.v1".into(),
            reply_to: "evt_1".into(),
            platform: "vtuber".into(),
            channel: ReplyChannel { id: "ch_1".into(), thread_id: None },
            content: Content { content_type: "text".into(), text: "Done!".into(), attachments: vec![] },
            command: None,
            request_id: None,
            quote_message_id: None,
        };
        let events = derive_events(&reply);
        assert_eq!(events.len(), 1);
        match &events[0] {
            WsEvent::AgentState { state, .. } => assert_eq!(*state, "idle"),
            _ => panic!("expected AgentState idle"),
        }
    }
}
