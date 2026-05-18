# xAI Proxy (SuperGrok Sidecar)

xai-proxy is a lightweight Rust sidecar that lets any OpenAI-compatible agent use your **SuperGrok subscription** instead of per-token API credits. It authenticates via OAuth and proxies requests to `api.x.ai/v1`.

## Architecture

```
┌─ Kubernetes Pod ──────────────────────────────────────────────┐
│                                                               │
│  openab → opencode acp                                        │
│               │  POST /v1/chat/completions                    │
│               ▼                                               │
│         xai-proxy :9090                                       │
│           • Injects OAuth Bearer token                        │
│           • Auto-refreshes 120s before expiry                 │
│               │                                               │
│  PVC: /home/agent/.openab/xai-proxy/tokens.json               │
└───────────────┼───────────────────────────────────────────────┘
                ▼
        https://api.x.ai/v1  (SuperGrok)
```

## Prerequisites

- Active SuperGrok subscription (any tier)
- A machine with browser access (or SSH tunnel) for initial login

## Helm Install

```bash
# 1. Login locally to get tokens
xai-proxy login-device

# 2. Create K8s secret from token file
kubectl create secret generic xai-proxy-tokens \
  --from-file=tokens.json=$HOME/.xai-proxy/tokens.json

# 3. Deploy with opencode + xai-proxy sidecar
helm install openab openab/openab \
  --set agents.kiro.enabled=false \
  --set agents.mybot.discord.botToken="$DISCORD_BOT_TOKEN" \
  --set agents.mybot.discord.allowAllChannels=true \
  --set agents.mybot.command=opencode \
  --set-json 'agents.mybot.args=["acp"]' \
  --set agents.mybot.image=ghcr.io/openabdev/openab-opencode \
  --set-json 'agents.mybot.extraVolumes=[{"name":"xai-tokens-src","secret":{"secretName":"xai-proxy-tokens"}},{"name":"opencode-config","configMap":{"name":"opencode-xai-config"}},{"name":"opencode-auth","configMap":{"name":"opencode-xai-auth"}}]' \
  --set-json 'agents.mybot.extraVolumeMounts=[{"name":"opencode-config","mountPath":"/home/agent/opencode.json","subPath":"opencode.json"},{"name":"opencode-config","mountPath":"/home/agent/.config/opencode/opencode.json","subPath":"opencode.json"},{"name":"opencode-auth","mountPath":"/home/agent/.local/share/opencode/auth.json","subPath":"auth.json"}]' \
  --set-json 'agents.mybot.extraInitContainers=[{"name":"copy-tokens","image":"busybox","command":["sh","-c","if [ ! -f /dest/.openab/xai-proxy/tokens.json ]; then mkdir -p /dest/.openab/xai-proxy && cp /src/tokens.json /dest/.openab/xai-proxy/tokens.json; fi"],"volumeMounts":[{"name":"xai-tokens-src","mountPath":"/src","readOnly":true},{"name":"data","mountPath":"/dest"}]}]' \
  --set-json 'agents.mybot.extraContainers=[{"name":"xai-proxy","image":"xai-proxy:latest","args":["serve","--bind","0.0.0.0"],"env":[{"name":"XAI_PROXY_TOKEN_PATH","value":"/home/agent/.openab/xai-proxy/tokens.json"}],"ports":[{"containerPort":9090}],"volumeMounts":[{"name":"data","mountPath":"/home/agent"}]}]'
```

## OpenCode Configuration

Create a ConfigMap for the opencode provider config:

```bash
kubectl create configmap opencode-xai-config --from-file=opencode.json=- <<'EOF'
{
  "$schema": "https://opencode.ai/config.json",
  "model": "xai/grok-4.3",
  "provider": {
    "xai": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "xAI (SuperGrok)",
      "options": {
        "baseURL": "http://localhost:9090/v1",
        "apiKey": "dummy"
      },
      "models": {
        "grok-4.3": { "name": "Grok 4.3" }
      }
    }
  }
}
EOF

kubectl create configmap opencode-xai-auth --from-file=auth.json=- <<'EOF'
{ "xai": "dummy" }
EOF
```

## Authentication

### Device-code flow (recommended for headless)

```bash
xai-proxy login-device
```

Prints a URL and code. Open the URL in any browser, enter the code, and authorize.

### Browser OAuth (local machine)

```bash
xai-proxy login
```

Opens your browser to `auth.x.ai`. Sign in and authorize. Callback is received on `127.0.0.1:56121`.

### Token refresh

xai-proxy auto-refreshes the OAuth token 120 seconds before expiry. The refreshed token is written back to the token file (persisted on PVC across pod restarts).

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `XAI_PROXY_TOKEN_PATH` | `~/.xai-proxy/tokens.json` | Custom token file path |
| `RUST_LOG` | `xai_proxy=info` | Log level |

## Token Persistence

The init container seeds the token from the K8s secret on first boot only. After that, xai-proxy reads and writes the token directly on the PVC. This means:

- Token refreshes survive pod restarts
- The K8s secret is only needed for initial bootstrap
- To force a token reset, delete `/home/agent/.openab/xai-proxy/tokens.json` from the PVC

## Standalone Usage (no K8s)

```bash
# Build
cargo build --release

# Login
./target/release/xai-proxy login-device

# Serve
./target/release/xai-proxy serve --port 9090

# Use with any OpenAI-compatible client
export OPENAI_BASE_URL=http://127.0.0.1:9090/v1
export OPENAI_API_KEY=dummy
opencode
```

## Limitations

- **codex-acp** and **claude-agent-acp** require their own proprietary auth and won't use `OPENAI_BASE_URL` — use opencode or hermes-acp instead
- Browser OAuth (`xai-proxy login`) requires Cloudflare to not block your IP — use `login-device` if blocked
