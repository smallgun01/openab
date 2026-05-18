# xai-proxy

Lightweight Rust sidecar that authenticates with xAI via OAuth PKCE (SuperGrok subscription) and proxies OpenAI-compatible requests to `api.x.ai/v1`.

## Why

Use your SuperGrok subscription quota (instead of API credits) with any OpenAI-compatible coding agent — OpenCode, Hermes, etc.

## How it works

```
┌──────────────────────────────────────────────────────────────┐
│ Pod / Host                                                   │
│                                                              │
│  ┌────────────────────┐    POST /v1/chat/completions         │
│  │  coding agent      │──────────────────────┐               │
│  │  (OpenCode, etc.)  │                      ▼               │
│  └────────────────────┘    ┌─────────────────────────────┐   │
│                            │  xai-proxy  :9090            │   │
│                            │                             │   │
│                            │  • Injects Bearer token     │   │
│                            │  • Auto-refreshes < 120s    │   │
│                            └──────────────┬──────────────┘   │
└───────────────────────────────────────────┼──────────────────┘
                                            │
                                            ▼
                            ┌─────────────────────────────┐
                            │  https://api.x.ai/v1        │
                            │  (SuperGrok subscription)   │
                            └─────────────────────────────┘
```

## Build

```bash
cargo build --release
```

## Docker

```bash
docker build -t xai-proxy .
docker run --rm -v ~/.xai-proxy:/root/.xai-proxy xai-proxy serve --bind 0.0.0.0
```

## Usage

### 1. Login (one-time)

```bash
# Browser OAuth (local machine)
./target/release/xai-proxy login

# Device-code flow (headless / K8s / ECS)
./target/release/xai-proxy login-device
```

Token is saved to `~/.xai-proxy/tokens.json` (or custom path via `XAI_PROXY_TOKEN_PATH`).

### 2. Start proxy

```bash
./target/release/xai-proxy serve --port 9090
```

### 3. Point your client

```bash
export OPENAI_BASE_URL=http://127.0.0.1:9090/v1
export OPENAI_API_KEY=dummy
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `XAI_PROXY_TOKEN_PATH` | `~/.xai-proxy/tokens.json` | Custom token file location |
| `RUST_LOG` | `xai_proxy=info` | Log level |

## Kubernetes Sidecar

Deploy alongside openab as a sidecar container with PVC persistence:

```yaml
extraInitContainers:
  - name: copy-tokens
    image: busybox
    command: ["sh", "-c", "if [ ! -f /dest/.openab/xai-proxy/tokens.json ]; then mkdir -p /dest/.openab/xai-proxy && cp /src/tokens.json /dest/.openab/xai-proxy/tokens.json; fi"]
    volumeMounts:
      - name: xai-tokens-src
        mountPath: /src
        readOnly: true
      - name: data
        mountPath: /dest

extraContainers:
  - name: xai-proxy
    image: xai-proxy:latest
    args: ["serve", "--bind", "0.0.0.0"]
    env:
      - name: XAI_PROXY_TOKEN_PATH
        value: /home/agent/.openab/xai-proxy/tokens.json
    volumeMounts:
      - name: data
        mountPath: /home/agent

extraVolumes:
  - name: xai-tokens-src
    secret:
      secretName: xai-proxy-tokens
```

## OAuth details

| Item | Value |
|------|-------|
| Auth server | `https://auth.x.ai` |
| Client ID | Grok CLI public client |
| Flow | OAuth 2.0 PKCE (loopback) or device-code |
| Scope | `openid profile email offline_access grok-cli:access api:access` |
| Token storage | `~/.xai-proxy/tokens.json` (chmod 600) |
| Auto-refresh | Yes, 120s before expiry |

## Requirements

- Active SuperGrok subscription (any tier)
- Rust 1.86+ (build)
- Browser or device-code access for initial login

## Headless / SSH login

```bash
# Option A: device-code (recommended)
xai-proxy login-device

# Option B: SSH port-forward
ssh -N -L 56121:127.0.0.1:56121 user@remote-host
xai-proxy login  # open the URL in your local browser
```

## License

MIT
