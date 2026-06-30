//! VTuber platform adapter.
//!
//! Tier-1: OpenAI-compatible POST /v1/chat/completions (SSE) — streams agent
//!         replies as `chat.completion.chunk` deltas.
//! Tier-2: WebSocket /v1/vtuber/ws — pushes agent_state, tool_status, emotion,
//!         and notification events derived from GatewayReply commands.

use crate::schema::*;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use futures_util::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

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
    #[serde(rename = "tool_status")]
    ToolStatus {
        ts: i64,
        session_id: String,
        tool_name: String,
        status: &'static str,
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

// ---------------------------------------------------------------------------
// Client → Server commands
// ---------------------------------------------------------------------------

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
// Ambient notification loop
// ---------------------------------------------------------------------------

pub struct AmbientConfig {
    interval: Duration,
    prompt: String,
    urgency: &'static str,
}

impl AmbientConfig {
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("VTUBER_AMBIENT_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        if !enabled {
            return None;
        }

        let interval_secs = std::env::var("VTUBER_AMBIENT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v >= 60)
            .unwrap_or(1800);
        let prompt = std::env::var("VTUBER_AMBIENT_PROMPT").unwrap_or_else(|_| {
            "Ambient check-in: say one brief, natural message as 小光. If there is no urgent context, keep it warm and non-intrusive.".into()
        });
        let urgency = match std::env::var("VTUBER_AMBIENT_URGENCY")
            .unwrap_or_else(|_| "normal".into())
            .to_lowercase()
            .as_str()
        {
            "low" => "low",
            "high" => "high",
            _ => "normal",
        };

        Some(Self {
            interval: Duration::from_secs(interval_secs),
            prompt,
            urgency,
        })
    }
}

pub fn spawn_ambient_task(clients: WsClients) {
    let Some(config) = AmbientConfig::from_env() else {
        return;
    };

    info!(
        interval_secs = config.interval.as_secs(),
        urgency = config.urgency,
        "vtuber ambient mode enabled"
    );

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(config.interval).await;

            let client_count = clients.lock().await.len();
            if client_count == 0 {
                continue;
            }

            let event = WsEvent::Notification {
                ts: chrono::Utc::now().timestamp(),
                text: config.prompt.clone(),
                urgency: config.urgency,
            };
            broadcast(&clients, &[event]).await;
            info!(client_count, "vtuber ambient notification broadcast");
        }
    });
}

// ---------------------------------------------------------------------------
// Reaction emoji → agent_state mapping (OAB core reactions.rs)
// ---------------------------------------------------------------------------

struct ReactionMapping {
    state: &'static str,
    tool_class: Option<&'static str>,
}

fn reaction_to_state(emoji: &str) -> Option<ReactionMapping> {
    // ponytail: match on first char for multi-codepoint emojis (👨‍💻 = U+1F468 ZWJ U+1F4BB)
    let first = emoji.chars().next()?;
    Some(match first {
        '👀' => ReactionMapping {
            state: "thinking",
            tool_class: None,
        },
        '🤔' => ReactionMapping {
            state: "thinking",
            tool_class: None,
        },
        '🔥' => ReactionMapping {
            state: "working",
            tool_class: Some("tool"),
        },
        '👨' => ReactionMapping {
            state: "working",
            tool_class: Some("coding"),
        }, // 👨‍💻
        '⚡' => ReactionMapping {
            state: "working",
            tool_class: Some("web"),
        },
        '🆗' => ReactionMapping {
            state: "attention",
            tool_class: None,
        },
        '😱' => ReactionMapping {
            state: "error",
            tool_class: None,
        },
        '🥱' => ReactionMapping {
            state: "error",
            tool_class: None,
        },
        '😨' => ReactionMapping {
            state: "error",
            tool_class: None,
        },
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Extract [emotion] tags from streamed text
// ---------------------------------------------------------------------------

fn extract_emotion_tags(text: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('[') {
        if let Some(end) = rest[start..].find(']') {
            let tag = &rest[start + 1..start + end];
            if !tag.is_empty()
                && tag.len() < 30
                && tag.chars().all(|c| c.is_alphanumeric() || c == '_')
            {
                tags.push(tag.to_string());
            }
            rest = &rest[start + end + 1..];
        } else {
            break;
        }
    }
    tags
}

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
        Some("remove_reaction") => {
            // Reaction cleared — no state change pushed; next add_reaction or
            // send_message will update the state.
        }
        Some("edit_message") => {
            if let Some((tool_name, status)) = parse_pure_tool_status(&reply.content.text) {
                events.push(WsEvent::ToolStatus {
                    ts,
                    session_id,
                    tool_name,
                    status,
                });
                return events;
            }

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
            // Final message — extract any trailing emotions, then go idle.
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
    for (&client_id, client) in guard.iter() {
        for event in events {
            let event_type = match event {
                WsEvent::AgentState { .. } => "agent_state",
                WsEvent::Emotion { .. } => "emotion",
                WsEvent::Notification { .. } => "notification",
                WsEvent::ToolStatus { .. } => "tool_status",
                WsEvent::Pong { .. } => continue,
            };
            if let Some(ref subs) = client.subscribed {
                if !subs.contains(event_type) {
                    continue;
                }
            }
            if let Ok(json) = serde_json::to_string(event) {
                if client.tx.send(json).is_err() {
                    dead.push(client_id);
                    break;
                }
            }
        }
    }
    for client_id in dead {
        guard.remove(&client_id);
    }
}

// ---------------------------------------------------------------------------
// WS upgrade handler: GET /v1/vtuber/ws
// ---------------------------------------------------------------------------

pub async fn ws_upgrade(
    State(state): State<Arc<crate::AppState>>,
    query: Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    // Auth: Bearer token from Authorization header or ?token= query param
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| query.get("token").map(|s| s.as_str()));

    let expected = state.vtuber.as_ref().and_then(|c| c.auth_key.as_ref());
    if let Some(expected) = expected {
        if token != Some(expected.as_str()) {
            warn!("vtuber WS rejected: invalid or missing token");
            return axum::http::StatusCode::UNAUTHORIZED.into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_ws(state, socket))
}

async fn handle_ws(state: Arc<crate::AppState>, socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);

    info!(client_id, "vtuber WS client connected");

    // Register client
    let clients = match &state.vtuber_ws_clients {
        Some(c) => c.clone(),
        None => return,
    };
    clients.lock().await.insert(
        client_id,
        WsClient {
            tx,
            subscribed: None,
        },
    );

    // Forward events → client
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_tx.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    // Receive commands from client
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

    // Cleanup
    clients.lock().await.remove(&client_id);
    info!(client_id, "vtuber WS client disconnected");
}

// ---------------------------------------------------------------------------
// Tier-1: OpenAI-compatible /v1/chat/completions (SSE)
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

    // Serialise requests — one agent turn at a time
    let Ok(_guard) = state.vtuber_request_lock.try_lock() else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "agent is busy, retry in a moment",
        )
            .into_response();
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
    let channel_id = "vtb_persistent".to_string();
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
                            warn!(channel = %s.channel_id, "vtuber: no reply — session may be dead; next request triggers respawn");
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
            if !key.starts_with("vtb_persistent") {
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

    // -----------------------------------------------------------------------
    // Helper: spin up gateway, return (addr, oab_ws_url, vtuber_ws_url)
    // -----------------------------------------------------------------------
    async fn start_gateway() -> (String, String, String) {
        let (event_tx, _) = tokio::sync::broadcast::channel::<String>(256);
        let ws_clients = new_ws_clients();
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
            vtuber_ws_clients: Some(ws_clients),
            ws_token: Some("oab-token".into()),
            event_tx,
            reply_token_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            line_webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            client: reqwest::Client::new(),
        });

        let app = axum::Router::new()
            .route("/ws", axum::routing::get(crate::ws_handler))
            .route("/v1/vtuber/ws", axum::routing::get(ws_upgrade))
            .route(
                "/v1/chat/completions",
                axum::routing::post(chat_completions),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let oab_url = format!("ws://{}/ws?token=oab-token", addr);
        let vtb_url = format!("ws://{}/v1/vtuber/ws?token=test-key", addr);
        (addr, oab_url, vtb_url)
    }

    // -----------------------------------------------------------------------
    // Integration: OAB sends add_reaction → vtuber WS receives agent_state
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_reaction_to_agent_state() {
        let (_addr, oab_url, vtb_url) = start_gateway().await;

        // Connect vtuber WS client
        let (mut vtb_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();

        // Connect OAB client and send a vtuber add_reaction reply
        let (mut oab_ws, _) = tokio_tungstenite::connect_async(&oab_url).await.unwrap();
        let reply = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1",
            "platform": "vtuber",
            "channel": {"id": "ch_test"},
            "content": {"type": "text", "text": "🤔"},
            "command": "add_reaction"
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                reply.to_string(),
            ))
            .await
            .unwrap();

        // Receive event on vtuber WS
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vtb_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
        assert_eq!(event["type"], "agent_state");
        assert_eq!(event["state"], "thinking");
        assert_eq!(event["session_id"], "ch_test");
    }

    // -----------------------------------------------------------------------
    // Integration: OAB sends edit_message with [emotion] → vtuber WS gets it
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_emotion_from_edit_message() {
        let (_addr, oab_url, vtb_url) = start_gateway().await;

        let (mut vtb_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        let (mut oab_ws, _) = tokio_tungstenite::connect_async(&oab_url).await.unwrap();

        let reply = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1",
            "platform": "vtuber",
            "channel": {"id": "ch_test"},
            "content": {"type": "text", "text": "[happy] Hello world!"},
            "command": "edit_message",
            "request_id": "req_1"
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                reply.to_string(),
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vtb_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
        assert_eq!(event["type"], "emotion");
        assert_eq!(event["tag"], "happy");
        assert_eq!(event["intensity"], 1.0);
    }

    // -----------------------------------------------------------------------
    // Integration: full agent lifecycle (queued → thinking → working → done → idle)
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_full_lifecycle() {
        let (_addr, oab_url, vtb_url) = start_gateway().await;

        let (mut vtb_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        let (mut oab_ws, _) = tokio_tungstenite::connect_async(&oab_url).await.unwrap();

        // Lifecycle: 👀 → 🤔 → 👨‍💻 → 🆗 → send_message
        let sequence = [
            ("👀", "add_reaction", "thinking"),
            ("🤔", "add_reaction", "thinking"),
            ("👨\u{200d}💻", "add_reaction", "working"),
            ("🆗", "add_reaction", "attention"),
        ];

        for (emoji, cmd, expected_state) in &sequence {
            let reply = serde_json::json!({
                "schema": "openab.gateway.reply.v1",
                "reply_to": "evt_1",
                "platform": "vtuber",
                "channel": {"id": "ch_lifecycle"},
                "content": {"type": "text", "text": emoji},
                "command": cmd
            });
            oab_ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    reply.to_string(),
                ))
                .await
                .unwrap();

            let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vtb_ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
            assert_eq!(event["type"], "agent_state", "for emoji {emoji}");
            assert_eq!(event["state"], *expected_state, "for emoji {emoji}");
        }

        // Final send_message → idle
        let final_reply = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1",
            "platform": "vtuber",
            "channel": {"id": "ch_lifecycle"},
            "content": {"type": "text", "text": "All done!"}
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                final_reply.to_string(),
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vtb_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
        assert_eq!(event["type"], "agent_state");
        assert_eq!(event["state"], "idle");
    }

    // -----------------------------------------------------------------------
    // Integration: subscribe filtering
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_subscribe_filters_events() {
        let (_addr, oab_url, vtb_url) = start_gateway().await;

        let (mut vtb_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        let (mut oab_ws, _) = tokio_tungstenite::connect_async(&oab_url).await.unwrap();

        // Subscribe only to emotion events
        vtb_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                r#"{"type":"subscribe","events":["emotion"]}"#.into(),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send add_reaction (agent_state) — should be filtered out
        let reaction = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1", "platform": "vtuber",
            "channel": {"id": "ch_test"},
            "content": {"type": "text", "text": "🤔"},
            "command": "add_reaction"
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                reaction.to_string(),
            ))
            .await
            .unwrap();

        // Send edit_message with [emotion] — should pass through
        let edit = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1", "platform": "vtuber",
            "channel": {"id": "ch_test"},
            "content": {"type": "text", "text": "[joy] yay"},
            "command": "edit_message", "request_id": "req_1"
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                edit.to_string(),
            ))
            .await
            .unwrap();

        // Should receive emotion, NOT agent_state
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vtb_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
        assert_eq!(event["type"], "emotion");
        assert_eq!(event["tag"], "joy");
    }

    // -----------------------------------------------------------------------
    // Regression: subscription updates stay attached to the same client after
    // another client disconnects.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_subscribe_after_disconnect_uses_same_client() {
        let (_addr, oab_url, vtb_url) = start_gateway().await;

        let (mut first_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        let (_middle_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        let (mut target_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        let (mut oab_ws, _) = tokio_tungstenite::connect_async(&oab_url).await.unwrap();

        first_ws.close(None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        target_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                r#"{"type":"subscribe","events":["emotion"]}"#.into(),
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reaction = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1", "platform": "vtuber",
            "channel": {"id": "ch_test"},
            "content": {"type": "text", "text": "🤔"},
            "command": "add_reaction"
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                reaction.to_string(),
            ))
            .await
            .unwrap();

        let filtered =
            tokio::time::timeout(std::time::Duration::from_millis(300), target_ws.next()).await;
        assert!(
            filtered.is_err(),
            "target client received agent_state despite emotion-only subscription"
        );

        let edit = serde_json::json!({
            "schema": "openab.gateway.reply.v1",
            "reply_to": "evt_1", "platform": "vtuber",
            "channel": {"id": "ch_test"},
            "content": {"type": "text", "text": "[joy] yay"},
            "command": "edit_message", "request_id": "req_1"
        });
        oab_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                edit.to_string(),
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), target_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
        assert_eq!(event["type"], "emotion");
        assert_eq!(event["tag"], "joy");
    }

    // -----------------------------------------------------------------------
    // Integration: ping → pong
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_ping_pong() {
        let (_addr, _oab_url, vtb_url) = start_gateway().await;

        let (mut vtb_ws, _) = tokio_tungstenite::connect_async(&vtb_url).await.unwrap();
        vtb_ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                r#"{"type":"ping"}"#.into(),
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vtb_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
        assert_eq!(event["type"], "pong");
        assert!(event["ts"].is_number());
    }

    // -----------------------------------------------------------------------
    // Integration: bad auth → 401
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn ws_e2e_bad_auth_rejected() {
        let (addr, _, _) = start_gateway().await;
        let bad_url = format!("ws://{}/v1/vtuber/ws?token=wrong", addr);
        let result = tokio_tungstenite::connect_async(&bad_url).await;
        assert!(
            result.is_err() || {
                let (_, resp) = result.unwrap();
                resp.status() == reqwest::StatusCode::UNAUTHORIZED
            }
        );
    }

    // -----------------------------------------------------------------------
    // Integration: Tier-1 explicitly requires streaming mode.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chat_completions_rejects_non_streaming_requests() {
        let (addr, _, _) = start_gateway().await;
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

    #[test]
    fn reaction_mapping_covers_all_oab_emojis() {
        assert_eq!(reaction_to_state("👀").unwrap().state, "thinking");
        assert_eq!(reaction_to_state("🤔").unwrap().state, "thinking");
        assert_eq!(reaction_to_state("🔥").unwrap().state, "working");
        assert_eq!(reaction_to_state("👨\u{200d}💻").unwrap().state, "working");
        assert_eq!(
            reaction_to_state("👨\u{200d}💻").unwrap().tool_class,
            Some("coding")
        );
        assert_eq!(reaction_to_state("⚡").unwrap().state, "working");
        assert_eq!(reaction_to_state("⚡").unwrap().tool_class, Some("web"));
        assert_eq!(reaction_to_state("🆗").unwrap().state, "attention");
        assert_eq!(reaction_to_state("😱").unwrap().state, "error");
        assert_eq!(reaction_to_state("🥱").unwrap().state, "error");
        assert_eq!(reaction_to_state("😨").unwrap().state, "error");
        assert!(reaction_to_state("😊").is_none()); // mood face, not a state
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
        let reply = GatewayReply {
            schema: "openab.gateway.reply.v1".into(),
            reply_to: "evt_1".into(),
            platform: "vtuber".into(),
            channel: ReplyChannel {
                id: "ch_1".into(),
                thread_id: None,
            },
            content: Content {
                content_type: "text".into(),
                text: "🤔".into(),
                attachments: vec![],
            },
            command: Some("add_reaction".into()),
            request_id: None,
            quote_message_id: None,
        };
        let events = derive_events(&reply);
        assert_eq!(events.len(), 1);
        match &events[0] {
            WsEvent::AgentState {
                state, session_id, ..
            } => {
                assert_eq!(*state, "thinking");
                assert_eq!(session_id, "ch_1");
            }
            _ => panic!("expected AgentState"),
        }
    }

    #[test]
    fn derive_events_edit_message_with_emotion() {
        let reply = GatewayReply {
            schema: "openab.gateway.reply.v1".into(),
            reply_to: "evt_1".into(),
            platform: "vtuber".into(),
            channel: ReplyChannel {
                id: "ch_1".into(),
                thread_id: None,
            },
            content: Content {
                content_type: "text".into(),
                text: "[excited] Hello!".into(),
                attachments: vec![],
            },
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
        let reply = GatewayReply {
            schema: "openab.gateway.reply.v1".into(),
            reply_to: "evt_1".into(),
            platform: "vtuber".into(),
            channel: ReplyChannel {
                id: "ch_1".into(),
                thread_id: None,
            },
            content: Content {
                content_type: "text".into(),
                text: "Done!".into(),
                attachments: vec![],
            },
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
