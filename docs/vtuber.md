# VTuber (OpenAI-compatible) Setup

Expose an **OpenAI-compatible `/v1/chat/completions` (SSE)** endpoint from the
unified OpenAB binary, so any character "skin" that already speaks OpenAI chat
completions (AniCompanion, Open-LLM-VTuber, ChatVRM, ...) gets a real agent:
tool use, code, MCP, memory, and the same configured ACP backend.

```
Skin ──POST /v1/chat/completions (SSE)──▶ OpenAB unified binary ──▶ ACP agent
Skin ◀─GET  /v1/vtuber/ws (optional WS)──┘
```

Unlike chat-platform adapters (LINE, Telegram, ...), this is **not a webhook**:
the skin opens an HTTP request and the reply streams back on that same
connection. The optional Tier-2 WebSocket is only for side-channel UI events
such as agent state, emotions, tool status, and ambient notifications.

## Prerequisites

- An OpenAB image/binary built with the `unified` feature.
- An ACP agent configured in the same OpenAB process.
- A public URL, tunnel, or localhost endpoint reachable by the skin.
- A skin that supports an OpenAI-compatible backend, for example
  AniCompanion -> Settings -> Agent backend -> OpenAI-compatible.

## 1. Enable the VTuber Adapter

Set these environment variables on the OpenAB process:

```bash
VTUBER_ENABLED=true
VTUBER_AUTH_KEY="$(openssl rand -hex 32)"
VTUBER_DEFAULT_MODEL=openab
GATEWAY_LISTEN=0.0.0.0:8080
```

Example Docker run with a Kiro-backed OpenAB image:

```bash
docker run -d --name openab-vtuber \
  -e VTUBER_ENABLED=true \
  -e VTUBER_AUTH_KEY="$VTUBER_AUTH_KEY" \
  -e VTUBER_DEFAULT_MODEL=openab \
  -e KIRO_API_KEY="$KIRO_API_KEY" \
  -p 8080:8080 \
  ghcr.io/openabdev/openab:beta-kiro
```

For Kubernetes or Zeabur, put the same environment variables on the OpenAB
service. No companion container or adapter config block is needed in unified
mode.

## 2. Configure the Agent

Use the normal OpenAB agent configuration. The VTuber adapter submits incoming
skin messages directly to OpenAB's in-process dispatcher.

```toml
[agent]
command = "kiro-cli"
args = ["acp", "--trust-all-tools"]
working_dir = "/home/agent"
```

Streaming is handled by the VTuber adapter's SSE endpoint; it does not require
separate streaming settings.

## 3. Point the Skin at OpenAB

In the skin's OpenAI-compatible backend settings:

- **Endpoint / Base URL**: `https://your-openab-host` (the adapter serves
  `/v1/chat/completions`)
- **API Key**: the `VTUBER_AUTH_KEY` value (sent as `Authorization: Bearer <key>`)
- **Model**: anything; OpenAB routes to the configured ACP agent and echoes the
  model name back in OpenAI-compatible chunks

If the skin supports the Tier-2 side channel, connect it to the same base URL
at `/v1/vtuber/ws` with the same bearer key.

## 4. Test

```bash
curl -N https://your-openab-host/v1/chat/completions \
  -H "Authorization: Bearer $VTUBER_AUTH_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"openab","stream":true,
       "messages":[{"role":"user","content":"hi 小光"}]}'
```

You should see `data: {...chat.completion.chunk...}` lines ending with
`data: [DONE]`.

## Tier-2 WebSocket

`GET /v1/vtuber/ws` is optional. It pushes structured UI events:

- `agent_state`
- `emotion`
- `tool_status`
- `notification`

Clients can send `{"type":"subscribe","events":[...]}` to filter event types
and `{"type":"ping"}` for keepalive.

Ambient notifications are opt-in:

```bash
VTUBER_AMBIENT_ENABLED=true
VTUBER_AMBIENT_INTERVAL_SECS=1800
VTUBER_AMBIENT_URGENCY=normal
VTUBER_AMBIENT_PROMPT="Ambient check-in prompt..."
```

## Emotion Tags

Inline `[emotion]` tags (for example AniCompanion's `[happy]`, `[sad]`,
`[curious]`, ...) are the skin's own convention. The skin's persona/system
prompt instructs the agent to emit them, and the skin parses and strips them
before TTS. Tier-1 passes them through verbatim; Tier-2 also extracts recognized
tags as `emotion` events for clients that subscribe to the side channel.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `VTUBER_ENABLED` | Yes | `true`/`1` to enable the adapter in the unified binary |
| `VTUBER_AUTH_KEY` | Recommended | Bearer key required on Tier-1 and Tier-2 requests. If unset, the endpoint is unauthenticated and logs a warning |
| `VTUBER_DEFAULT_MODEL` | No | Model name echoed back when the request omits one (default `openab`) |
| `VTUBER_PATH` | No | Tier-1 route path (default `/v1/chat/completions`) |
| `VTUBER_REPLY_TAIL_IDLE_MS` | No | Tail-idle close delay after the first content snapshot (default `1500`) |
| `VTUBER_AMBIENT_ENABLED` | No | `true`/`1` to enable Tier-2 ambient notifications |
| `VTUBER_AMBIENT_INTERVAL_SECS` | No | Ambient notification interval, minimum 60 seconds (default `1800`) |
| `VTUBER_AMBIENT_URGENCY` | No | `low`, `normal`, or `high` (default `normal`) |
| `VTUBER_AMBIENT_PROMPT` | No | Prompt text sent to Tier-2 clients as a `notification` event |
| `GATEWAY_LISTEN` | No | Bind address for the unified HTTP listener (default `0.0.0.0:8080`) |

## Notes & Limitations

- **One session per request.** Each call mints a fresh agent session; the full
  conversation history must be carried in `messages[]`, which OpenAI clients
  already do.
- **No agent output => no chat output.** If the configured ACP agent cannot
  answer, the SSE request eventually closes after the reply timeout.
- **Tier-2 is optional.** Skins that only support OpenAI chat completions still
  get full Tier-1 chat. Tier-2 adds richer UI state when the skin supports it.
- **Tags are not motion.** Mapping `[emotion]` or Tier-2 `emotion` events to VRM
  expressions, Live2D parameters, or VTube Studio actions is the skin's job.

## Troubleshooting

**No response / stream hangs then closes:**
- Confirm `VTUBER_ENABLED=true` is set on the OpenAB process.
- Confirm the ACP agent is configured and authenticated.
- Check OpenAB logs for `unified: vtuber adapter enabled`.

**`401 invalid api key`:**
- The `Authorization: Bearer <key>` value must match `VTUBER_AUTH_KEY`.

**Reply arrives all at once instead of streaming:**
- Confirm the skin is calling `/v1/chat/completions` with `stream: true`.
- Confirm no proxy in front of OpenAB buffers SSE responses.

**Tier-2 events do not arrive:**
- Confirm the client connects to `/v1/vtuber/ws` on the same OpenAB host.
- Confirm it uses the same bearer key as Tier-1.
- Check OpenAB logs for `vtuber WS client connected`.

## References

- [RFC: VTuber adapter](https://github.com/openabdev/openab/issues/1233)
