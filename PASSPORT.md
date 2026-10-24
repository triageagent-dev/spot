# Application Passport — tria-spot

## 1. Identity

| Field | Value |
| --- | --- |
| Component | tria-spot |
| Description | Always-on agent for one workspace of the Triage core: a supervisor wakes its own Go agent loop in turns, the loop calls a model (Anthropic or Gemini) and runs tools taken from MCP servers (the core, ego, connectors) |
| Repository | github.com/triageagent-dev/tria-spot (public) |
| Image registry | ghcr.io/triageagent-dev/spot |
| Chart registry | oci://ghcr.io/triageagent-dev/charts/tria-spot |
| Version | commit 7fac342 (no release tag yet); chart 0.1.0, appVersion 0.1.0 |
| Owner | Repository maintainer (CODEOWNERS) |
| License | Apache-2.0 |
| Date | 2026-10-24 |
| Audit status | No CRITICAL/HIGH CVEs; 1 unknown-severity advisory on an unused module path |
| Code review | Findings open, none High or Critical (2026-10-24) |

## 2. Classification

| Field | Value |
| --- | --- |
| Class | Kubernetes workload (Deployment, 1 replica, Recreate), long-running |
| Type | LLM agent (uses models) with a model-free supervisor |
| Capabilities | Reads the workspace (issues, situation, memory, rooms, files) from the core over MCP; writes to the core (issue comments, memory items, room messages, status line, heartbeat); calls connector MCP tools by exact name; exports OTLP spans |
| Model role | Tool-calling agent loop; one empty conversation per wake |
| Runtime | Go 1.27 static binary on a distroless image, Kubernetes pod |
| Public accessibility | None: no listening port, NetworkPolicy ingress empty |
| Prod data access | Read + write on one core workspace (tenant); clusters only through connectors, with the connector's identity |
| PII | Room message text, sender display names and messenger ids pass through prompts; with content telemetry on they also reach the OTLP receiver |
| Sensitivity | Operational data of one workspace; model and core credentials as mounted files |

## 3. Artifact

| Field | Value |
| --- | --- |
| Form | Container image + Helm chart (`charts/tria-spot`, renders nothing unless `spot.enabled=true`) |
| Language | Go 1.27 (toolchain go1.27.3) |
| Build tool | ko (no Dockerfile, no Docker); CGO disabled, trimpath, stripped |
| Base image | gcr.io/distroless/static@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 |
| Platforms | linux/amd64 (local build); linux/amd64 + linux/arm64 (release) |
| Shell / package manager in image | None |
| Runtime user | uid 10001, gid 10001, fsGroup 10001, runAsNonRoot |
| Container hardening | readOnlyRootFilesystem, allowPrivilegeEscalation false, capabilities drop ALL, seccomp RuntimeDefault |
| Writable paths | `/state` (PVC, 256Mi default), `/tmp` (emptyDir, 256Mi) |
| Resources | requests 100m / 128Mi, limits 500m / 512Mi |
| Release | Git tag `v*` → GitHub Actions builds image + chart, publishes to ghcr.io and a GitHub release |
| CI | go build, go vet, gofmt, go test -race, golangci-lint, helm lint + helm template assertions; actions pinned by commit SHA; Dependabot (gomod, github-actions, weekly) |
| Image signing / attestation | None |

## 4. AI / LLM Supply Chain

| Field | Value |
| --- | --- |
| Providers | Anthropic (default); Google Gemini; either on Google Vertex AI |
| Default models | `claude-opus-5-5` (anthropic), `gemini-3-flash-preview` (gemini) |
| SDKs | github.com/anthropics/anthropic-sdk-go v1.84.0; google.golang.org/genai v1.72.0 |
| LLM auth modes | `key`: API key read from a mounted Secret file on every request; `federation`: Anthropic Workload Identity Federation with a projected ServiceAccount token (audience `https://api.anthropic.com`, 600 s); `vertex`: Application Default Credentials via the pod's Workload Identity |
| Credentials in env/args | None; federation mode sets `ANTHROPIC_FEDERATION_RULE_ID`, `ANTHROPIC_ORGANIZATION_ID`, `ANTHROPIC_SERVICE_ACCOUNT_ID`, `ANTHROPIC_IDENTITY_TOKEN_FILE`, `ANTHROPIC_WORKSPACE_ID` (ids and a path) in its own process |
| Per-call limits | `max_tokens` 16000; effort `medium` (default); SDK retries 2 (Anthropic) / 3 attempts (Gemini) |
| Per-turn limits | 40 model steps; 20 m wall clock; 60 s per tool call; 32 KiB per tool result |
| Global budget | 2,000,000 tokens per UTC day (input + output + cache writes; cache reads tracked, not counted), held in memory and written through to `/state`; core-side per-workspace budget received over the trigger stream, blocks wakes when exceeded |
| Wake controls | min/max wake 2 m / 2 h; edge cooldown 1 m; chat batch 10 m; 12 early wakes per hour |
| Tool sources | Core MCP server (curated allow list of 31 names, deny list of 22 names applied in every mode); ego MCP (11 read-only names); connectors (exact names from the operator's list); built-ins `schedule_next`, `finish` |
| Model-side tools absent | Shell, filesystem, HTTP fetch, subagents |
| System prompt | Embedded template, rendered once per process (name, tenant, wake bounds, connector hints); cached prefix with a 1 h breakpoint |
| Untrusted text in prompts | Issue summaries and room messages quoted as Go string literals, capped (80/160 runes for issue fields, 2500 runes per room message), labelled untrusted |
| Guardrails | Name allow/deny lists; prompt instructions; no input or output classifier |
| Telemetry | OTLP/HTTP spans (root per wake, one per model request, one per tool call); content attributes (prompts, answers, tool arguments and results) on by default when telemetry is enabled, 64 KiB cap per attribute |

| Egress destination | Purpose | Auth | Enabled when |
| --- | --- | --- | --- |
| Anthropic API, 160.79.104.0/23 and 2607:6bc0::/48, TCP 443 | Model requests | API key file or federation access token | provider anthropic, auth key or federation |
| Google APIs, operator-named CIDRs, TCP 443 | Gemini API / Vertex AI | API key file or ADC token | provider gemini, or auth vertex |
| Node metadata server 169.254.169.252/32, 169.254.169.254/32, TCP 988 and 80 | Workload Identity token for ADC | None | auth vertex |
| Core MCP endpoint and HTTP API (namespace/pod selector or CIDR, TCP 443 default) | Tools, polling, heartbeat, trigger WebSocket `/v2/messenger/spot/ws` | Bearer: projected ServiceAccount token (audience `triage-core`) or Secret token | always |
| Connector MCP endpoints (selector or CIDR per connector) | Connector tools | None sent; the connector holds its own credentials | connectors configured |
| OTLP endpoint and ego MCP (selector or CIDR, TCP 4318 default) | Spans; ego read tools | Bearer token from a Secret file | telemetry enabled |
| Cluster DNS (kube-system namespace, UDP/TCP 53) | Name resolution | None | always |

## 5. Privileges

| Field | Value |
| --- | --- |
| Kubernetes API access | None: no ClusterRole/ClusterRoleBinding, no Role on Kubernetes API resources, `automountServiceAccountToken: false` |
| Core grant (identity `serviceAccount`, default) | Role + RoleBinding `<spot name>-core` in the release namespace: apiGroup `triageagent.dev`, resource `tenants`, resourceName `<tenant>`, verbs get, list, create, update (configurable); projected token audience `triage-core`, 3600 s, mode 0440 |
| Core grant (identity `secret`) | Bearer token from a Secret, mounted at `/secrets/core`, mode 0440 |
| Model credential | `/secrets/anthropic/<key>` or `/secrets/gemini/<key>` Secret file (key mode); `/var/run/secrets/anthropic.com/token` projected token (federation); ServiceAccount annotation for Workload Identity (vertex) |
| Telemetry credential | `/secrets/otel/<key>` Secret file, mode 0440 |
| Connector credentials | None held by the spot |
| Connector list | ConfigMap at `/etc/tria-spot/connectors/connectors.json`: names, URLs, tool names, hints |
| Pod environment variables | None rendered by the chart |
| Filesystem scope | `/state/supervisor` (usage counter, room cursors, session flag, status.json), `/tmp` |
| Google Cloud | Vertex AI caller role on the bound Google service account (vertex mode, operator-provided) |
| Anthropic | Federation rule, organization, service account and optional workspace ids (federation mode, operator-provided) |

## 6. Network Surface

| Field | Value |
| --- | --- |
| Listening ports | None |
| Ingress | NetworkPolicy `policyTypes: [Ingress, Egress]`, `ingress: []` |
| Egress | Allow list only: DNS, model API, core, connectors, telemetry (see §4 table); NetworkPolicy enabled by default |
| Outbound protocols | HTTPS to model APIs; MCP streamable HTTP (http or https as configured); WebSocket (ws/wss) to the core; OTLP/HTTP |
| Replicas | 1 |

## 7. Dependency Vulnerabilities

| Ecosystem | Manifest | Components (SBOM) | CRITICAL | HIGH | MEDIUM | LOW | UNKNOWN | Note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Go modules | go.mod | 57 | 0 | 0 | 0 | 0 | 1 | GO-2026-5932, golang.org/x/crypto v0.57.0 (openpgp package unmaintained); govulncheck: not called by this code |

| Scan | Result |
| --- | --- |
| SBOM | syft, CycloneDX 1.6 |
| Vulnerabilities | trivy fs (go.mod), govulncheck (symbol level: 0 reachable) |
| Secrets in history | gitleaks: 84 commits, no leaks |
| Static analysis | staticcheck 0, golangci-lint 0 |
