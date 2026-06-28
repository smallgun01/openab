# VTuber (OpenAI-compatible) Setup

Expose an **OpenAI-compatible `/v1/chat/completions` (SSE)** endpoint backed by your
OAB agent, so any character "skin" that already speaks OpenAI chat completions
(AniCompanion, Open-LLM-VTuber, …) gets a real agent — tool use, code, MCP, memory —
with **zero client changes**.

```
Skin ──POST /v1/chat/completions (SSE)──▶ Gateway (:8080) ◀──WebSocket── OAB Pod
       choices[].delta.content (incl. inline [emotion] tags)   (OAB connects out)
```

Unlike the chat-platform adapters (LINE, Telegram, …) this is **not a webhook**: the
skin holds an HTTP request open and the reply streams back on the same connection.

## Prerequisites

- A running OAB instance (with kiro-cli or any ACP agent authenticated), **connected
  to the gateway** — the gateway has no embedded model; without a connected agent the
  endpoint returns nothing.
- The Custom Gateway deployed ([gateway/README.md](../gateway/README.md)).
- A skin that supports an OpenAI-compatible backend (e.g. AniCompanion → Settings →
  Agent backend → OpenAI-compatible).

## 1. Configure the Gateway

```bash
# Docker
docker run -d --name openab-gateway \
  -e VTUBER_ENABLED="true" \
  -e VTUBER_AUTH_KEY="$(openssl rand -hex 32)" \
  -e VTUBER_DEFAULT_MODEL="openab" \
  -p 8080:8080 \
  ghcr.io/openabdev/openab-gateway:latest

# Kubernetes
kubectl set env deployment/openab-gateway \
  VTUBER_ENABLED=true \
  VTUBER_AUTH_KEY=<your-key> \
  VTUBER_DEFAULT_MODEL=openab
```

## 2. Configure OAB

Point an OAB gateway connection at the `vtuber` platform. Streaming must use the
**draft** path (no thinking-placeholder) so the gateway can tell a partial edit from
the final message:

```toml
[gateway]
url = "ws://openab-gateway:8080/ws"
platform = "vtuber"
streaming = true
streaming_placeholder = false   # required: avoids the "…" placeholder ambiguity

[agent]
command = "kiro-cli"
args = ["acp", "--trust-all-tools"]
working_dir = "/home/agent"
```

`streaming = false` also works — the whole reply arrives as one chunk + `[DONE]`.

## 3. Point the Skin at the Gateway

In the skin's OpenAI-compatible backend settings:

- **Endpoint / Base URL**: `https://gw.yourdomain.com` (the adapter serves
  `/v1/chat/completions`)
- **API Key**: the `VTUBER_AUTH_KEY` value (sent as `Authorization: Bearer <key>`)
- **Model**: anything — the gateway routes to the connected agent regardless and echoes
  the name back

## 4. Test

```bash
curl -N https://gw.yourdomain.com/v1/chat/completions \
  -H "Authorization: Bearer $VTUBER_AUTH_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"openab","stream":true,
       "messages":[{"role":"user","content":"hi 小光"}]}'
```

You should see `data: {...chat.completion.chunk...}` lines ending with `data: [DONE]`.

## Emotion tags

Inline `[emotion]` tags (e.g. AniCompanion's 16: `[happy] [sad] [curious] …`) are the
skin's own convention — the skin's persona/system prompt instructs the agent to emit
them, and the skin parses + strips them before TTS. **The adapter passes them through
the stream verbatim** and does no emotion handling itself.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `VTUBER_ENABLED` | Yes | `true`/`1` to enable the adapter |
| `VTUBER_AUTH_KEY` | Recommended | Bearer key required on requests. If unset, the endpoint is **unauthenticated** (logged as insecure) |
| `VTUBER_DEFAULT_MODEL` | No | Model name echoed back when the request omits one (default `openab`) |
| `VTUBER_PATH` | No | Route path (default `/v1/chat/completions`) |

## Notes & Limitations

- **One session per request.** Each call mints a fresh agent session; the full
  conversation history must be carried in `messages[]` (which OpenAI clients already do).
- **No agent connected ⇒ no output.** The request closes after a 180s idle timeout if no
  reply arrives. Check that an OAB agent is connected to `/ws`.
- **Pull-only.** OpenAI chat completions cannot push agent-state animation cues or
  proactive/ambient messages. Those are a planned Tier-2 WebSocket side-channel
  (see the RFC), not part of this adapter.
- **Tags are not motion.** Mapping `[emotion]` → VRM expression / Live2D / VTube Studio
  is the skin's job; the adapter stays motion-system-agnostic.

## Troubleshooting

**No response / stream hangs then closes:**
- Confirm an OAB agent is connected: check gateway logs for the `/ws` connection.
- Confirm `[gateway] platform = "vtuber"` in OAB config.

**`401 invalid api key`:**
- The `Authorization: Bearer <key>` value must match `VTUBER_AUTH_KEY`.

**Reply arrives all at once instead of streaming:**
- Set `streaming = true` and `streaming_placeholder = false` in OAB's `[gateway]` block.

**Duplicated/garbled streaming text:**
- Ensure only one OAB agent is connected for the `vtuber` platform; multiple agents
  reply on the same request id.

## References

- [ADR: Custom Gateway](adr/custom-gateway.md)
- [RFC: VTuber adapter](https://github.com/openabdev/openab/issues/1233)
