# Docker Image Tagging Convention

## Unified Image Repository (`ghcr.io/openabdev/openab`)

All agent variants are published under a single image repository using tag-based variants:

```
ghcr.io/openabdev/openab:<version>-<agent>   # per-agent variant
ghcr.io/openabdev/openab:<version>           # default (kiro)
```

### Default (kiro) tags

| Tag | Points to | Updated when |
|-----|-----------|--------------|
| `0.9.0-beta.1` | Exact pre-release build | Pre-release tag pushed |
| `beta` | Latest pre-release | Every pre-release build |
| `0.9.0` | Promoted stable build | Stable tag pushed |
| `0.9` | Latest patch in minor | Stable promotion |
| `stable` | Latest stable | Stable promotion |
| `latest` | Latest stable (= `stable`) | Stable promotion |

### Per-agent variant tags

Agent variants use the format `<version>-<agent>`:

| Tag | Example | Points to |
|-----|---------|-----------|
| `<version>-<agent>` | `0.9.0-beta.1-claude` | Exact pre-release build for claude |
| `beta-<agent>` | `beta-codex` | Latest pre-release for codex |
| `<version>-<agent>` | `0.9.0-gemini` | Promoted stable for gemini |
| `stable-<agent>` | `stable-grok` | Latest stable for grok |

Available agents: `kiro`, `claude`, `codex`, `copilot`, `cursor`, `gemini`, `grok`, `hermes`, `mimocode`, `opencode`, `antigravity`, `pi`, `native`, `agentcore`

### Migration from per-repo images (deprecated)

Previously, each agent had its own image repository (`ghcr.io/openabdev/openab-codex:beta`).
These are now replaced by the unified tag format (`ghcr.io/openabdev/openab:beta-codex`).

| Old (deprecated) | New |
|------------------|-----|
| `ghcr.io/openabdev/openab-claude:beta` | `ghcr.io/openabdev/openab:beta-claude` |
| `ghcr.io/openabdev/openab-codex:0.8.5-beta.13` | `ghcr.io/openabdev/openab:0.8.5-beta.13-codex` |
| `ghcr.io/openabdev/openab:beta` | `ghcr.io/openabdev/openab:beta` (unchanged, kiro is default) |

## Gateway (`ghcr.io/openabdev/openab-gateway`)

| Tag | Points to | Updated when |
|-----|-----------|--------------|
| `0.5.1` | Exact release | `gateway-v*` tag pushed |
| `v0.5.1` | Same as above (v-prefixed alias) | Same |
| `latest` | Latest release | Every release |

## Which tag to use

| Use case | Recommended tag |
|----------|----------------|
| Production (pinned) | Exact version (`0.9.0-beta.1-claude`) |
| Helm chart default | `stable` or `beta` (channel-based) — chart auto-appends `-<agent>` |
| Local dev / quick test | `beta` or `beta-<agent>` |
| CI | Exact version or SHA |

## Release flow

```
release PR merged → tag-on-merge → v0.9.0-beta.1
                                         │
                                         ▼
                                  build-operator.yml
                                         │
                              ┌──────────┴──────────┐
                              │ is_prerelease=true   │
                              ▼                      │
                    openab:0.9.0-beta.1              │
                    openab:0.9.0-beta.1-claude       │
                    openab:0.9.0-beta.1-codex        │
                    openab:beta                      │
                    openab:beta-claude               │
                    openab:beta-codex                │
                    ... (all agents)                 │
                                                    │
                              ┌──────────────────────┘
                              │ is_prerelease=false (stable)
                              ▼
                    promote latest beta images →
                    openab:0.9.0
                    openab:0.9.0-claude
                    openab:0.9
                    openab:0.9-claude
                    openab:stable
                    openab:stable-claude
                    openab:latest
                    ... (all agents)
```
