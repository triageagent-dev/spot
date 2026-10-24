# Application Passport — tria-spot

## 1. Identity

| Field | Value |
| --- | --- |
| Component | tria-spot |
| Description | A Go supervisor that keeps one Claude Code CLI process working as an always-on agent for one workspace of a triage system, in one Kubernetes pod; it wakes the agent on events pushed by the core and reports token statistics back to the core |
| Repo | Local git repository, no remote configured |
| Revision described | 710a959 (committed tree; uncommitted edits and ignored files in the working copy are not covered) |
| Registry | ghcr.io/triageagent-dev/spot (default in chart values; Makefile `REGISTRY` default ghcr.io/triageagent-dev) |
| Version | Chart 0.1.0, appVersion 0.1.0; no git tags (`git describe`: 710a959) |
| Owner | Not recorded in the repository |
| License | No license file in the repository |
| Date | 2026-10-24 |
| Audit status | HIGH findings open |
| Code review | Findings open (2026-10-24) |

## 2. Classification

| Field | Value |
| --- | --- |
| Class | Service (single-replica Deployment plus supervisor process) |
| Type | Agent (LLM-driven, tool-using) |
| Capabilities | Schedules and runs CLI turns; receives change notices from the core over a websocket (polling as fallback); reads and posts workspace chat and issue comments through the core's MCP tools; calls the tools of operator-configured connectors; passes a person's room message to the running CLI process; keeps a daily token ledger and an audit log of shell commands; reports turn and status counts to the core |
| Model role | Decision-making agent: investigates and reports; acts on a cluster only through connectors |
| Runtime | Kubernetes pod, Deployment with 1 replica, strategy Recreate |
| Public accessibility | No listener; NetworkPolicy ingress is empty |
| Production data access | Read: whatever the core's Role grants on one tenant and whatever each configured connector exposes; Kubernetes read through the Read-stage RBAC (default on). Write: core tools create/update on one tenant (comments, memory items, chat, status); Kubernetes writes only in namespaces listed in `actNamespaces` (default none) |
| PII | Not classified in the repository; chat text, issue text and tool output pass through the agent and the model provider |
| Sensitivity | Holds an Anthropic credential (API key file or projected identity token) and a core bearer token; reads workspace chat and issue data |
| Enabled by default | No (`spot.enabled=false`) |

## 3. Artifact

| Field | Value |
| --- | --- |
| Form | OCI image built with ko, plus a Helm chart (`charts/tria-spot`) |
| Language | Go 1.27 (module go directive), CGO disabled |
| Build tool | ko (no Dockerfile); `make kodata` fetches bundled binaries |
| Base image | docker.io/library/buildpack-deps@sha256:5f8a65a7e3b419450aebd73c24e3ce8b25810354eccd7ff6f673b0c387479131 (Debian 12, bash, coreutils, curl, wget, CA certificates), linux/amd64 |
| Bundled binary: Claude Code CLI | 2.1.304, from npm package `@anthropic-ai/claude-code-linux-x64`, sha512 pinned in Makefile |
| Bundled binary: jq | 1.7.1, sha256 pinned in Makefile |
| Bundled binary: kubectl | None; `make kodata` no longer fetches it |
| Entrypoint | /ko-app/tria-spot |
| Subcommands | `run` (default), `hook pretool`, `version` |
| Runtime user | uid/gid 10001, runAsNonRoot, fsGroup 10001 |
| Pod hardening | readOnlyRootFilesystem, allowPrivilegeEscalation false, capabilities drop ALL, seccomp RuntimeDefault |
| Writable paths | /state (PVC, 2Gi default), /run/tria-spot (memory emptyDir, 16Mi), /tmp (emptyDir, 256Mi) |
| Chart image tag | appVersion unless `image.tag` is set; pullPolicy IfNotPresent |
| Pod environment variables | None in the chart; all credentials are mounted file paths |

## 4. AI / LLM Supply Chain

| Field | Value |
| --- | --- |
| Models | CLI default unless `--model` / `agent.model` is set; not pinned in the repository |
| Model client | Claude Code CLI run with stream-json output; one process per turn (default) or one process kept alive between turns with stream-json input (`--persistent`, `agent.persistent`, default false; process replaced after 12 h, on a changed argument list or MCP file, or on a session change) |
| LLM auth, mode `key` (default) | Secret mounted as a file, read through the CLI `apiKeyHelper` script |
| LLM auth, mode `federation` | Projected ServiceAccount token (audience https://api.anthropic.com, 600 s) exchanged by the CLI through an OIDC federation profile; ids passed as flags (rule, organization, service account, optional workspace) |
| Core auth, `core.identity: serviceAccount` (default) | Projected ServiceAccount token, audience `triage-core`, 3600 s; the chart renders a Role and RoleBinding in the release namespace for one tenant with verbs get, list, create, update |
| Core auth, `core.identity: secret` | Bearer token from a Secret mounted as a file |
| Core token use | Written into the CLI MCP file (mode 0400, memory-backed directory) before every turn; re-read per request by the supervisor's MCP client and per connection by the trigger stream |
| Egress: Anthropic API | api.anthropic.com over 443; NetworkPolicy allows CIDRs 160.79.104.0/23 and 2607:6bc0::/48 |
| Egress: core | Core `/mcp` endpoint (streamable HTTP) and, when `core.apiUrl` is set, the core's websocket `/v2/messenger/spot/ws`; selected by namespace/pod selector or CIDR, ports from `networkPolicy.core.ports` (default 443) |
| Egress: connectors | One rule per connector from its `egress` block (selectors or CIDRs, ports required) |
| Egress: telemetry | OTLP/HTTP endpoint (default port 4318) when `telemetry.enabled`; the same token reads ego's MCP endpoint when `telemetry.mcpUrl` is set |
| Egress: Kubernetes API | API server CIDRs set by the operator, ports 443 and 6443 |
| Egress: cluster DNS | Namespace `kube-system` (configurable), UDP/TCP 53 |
| MCP servers | `core` (always); `ego` (when `--ego-mcp-url` is set); one per connector. `--strict-mcp-config` |
| Connectors | JSON file (`--connectors-file`, rendered from `connectors` values into a ConfigMap): name, URL, exact tool names, hint; the spot sends no credential to a connector; name must match `^[a-z][a-z0-9-]{0,29}$` and not be `core` or `ego`; tool names match `^[A-Za-z0-9_]+$`; default none |
| Core tools allowed without prompt (`--core-tools curated`, default) | 31 named tools: get_triage_instructions, get_tenant_situation, get_tenant_health, get_tenant_goals, list_open_issues, list_issues, get_issue_detail, get_issue_graph, get_recent_events, search_memory, search_corpus, get_memory, get_memory_context, get_memory_items, get_memory_cell, fetch_memory, get_world_context, get_service_analytics, get_service_prediction, inspect_service, list_services, list_applications, analyze_issue, verify_hypothesis, comment_on_issue, upsert_memory_item, note_observation, say_in_workspace, read_workspace_room, read_workspace_file, set_spot_status |
| Core tools allowed with `--core-tools all` | `mcp__core__*` |
| Ego tools allowed (when ego is configured) | list_metrics, list_sessions, summarize_session, explain_session, list_runs, get_run_results, get_usage, get_quality, explain_regression, get_findings, list_corrections |
| Other tools allowed without prompt | Read, Glob, Grep; Edit and Write under /state/notes/ and on /state/next.json; `Bash(jq *)`, `Bash(ls *)`, `Bash(head *)`, `Bash(tail *)`, `Bash(grep *)`, `Bash(wc *)`, `Bash(date *)`; each connector's named tools; rules from `--allowed-tools` / `agent.extraAllowedTools` (default none) |
| Tools disallowed | WebFetch, WebSearch, Task, Agent; in curated mode also 22 core tools by name (resolve_issue, forget_issue, update_issue_severity, link_issues, set_tenant_goals, run_rsi, emit_event, classify_event, decide_event, guardrail_event, test_notify, test_push, approve_pattern, link_pattern, recognize_pattern, expire_memory_item, refute_memory_item, supersede_memory_item, consolidate_memory, build_memory, build_knowledge, build_rca); rules from `--disallowed-tools` (no chart value) |
| Permission mode | Chart default `auto`, fallback `dontAsk` when the CLI does not report `auto`; `--permission-prompts none` |
| Setting sources | Not restricted by a flag; the CLI working directory is /state/work, the CLI config directory is /state/claude |
| PreToolUse hook | `tria-spot hook pretool` on the Bash tool: appends the command (up to 4000 bytes) with verdict "allowed" to /state/supervisor/audit.log (rotated at 5 MiB); makes no decision; fails open |
| Per-turn limits | `--max-turns` 40; turn timeout 20m |
| Token budget | Daily ceiling 2,000,000 billable tokens per UTC day (input + output + cache creation; cache reads tracked, not counted); 0 disables; optional dollar ceiling (default 0, off); both checked before a wake starts; the core also holds a cap per spot, delivered over the stream, that holds wakes back while it says blocked |
| Wake limits | Scheduled wake clamp 2m to 2h; edge cooldown 1m; live gap 5s for a person's message; early wakes capped at 12 per hour; a person's message that does not name the spot waits up to 10m; poll interval 30s |
| Mid-turn messages | With `--persistent` and the trigger stream, a person's room message that names the spot (or any person's message when chat batching is 0) is written to the running process's stdin as a user message; machine messages are held for the next wake; machine messages with hop count 2 or more are dropped |
| Environment passed to the CLI | Parent environment without names containing KEY, TOKEN, SECRET, PASSWORD, CREDENTIAL or starting ANTHROPIC_, CLAUDE_CODE_OAUTH, CLAUDE_CODE_USE_; HOME and CLAUDE_CONFIG_DIR under /state; PATH with the kodata directory first; federation adds ANTHROPIC_CONFIG_DIR and ANTHROPIC_PROFILE; telemetry adds OTEL_* variables |
| Telemetry | Off by default; with `--otel-endpoint`, CLI traces (spans with names, timings, token counts; tool parameters only with `--otel-tool-details`); resource attributes: tenant label under `--otel-tenant-attr` (default triage.tenant), ego.tenant.name, gen_ai.agent.name, ego.thread; sender token read from a file by a rendered `otelHeadersHelper` script |
| Prompt | System prompt embedded in the binary and appended with `--append-system-prompt-file`; room messages, issue ids and summaries and names enter the turn prompt as quoted strings (Go `%q`) cut at 60 to 2500 characters; the agent's own `why` text and the core-given display name enter unquoted; connector hints enter the system prompt unquoted |
| Logging | Counts, kinds, ids and timings; message, issue and model text only with `--log-content` at debug level; failed-turn errors and up to 300 bytes of CLI stderr are logged at error level |

## 5. Privileges

| Scope | Grant |
| --- | --- |
| ServiceAccount | One per release; token automounted (`automountServiceAccountToken: true`) |
| Core grant (`core.identity: serviceAccount`) | Role and RoleBinding `<name>-core` in the release namespace: apiGroup triageagent.dev, resource tenants, resourceNames the one tenant, verbs from `core.verbs` (default get, list, create, update) |
| Read stage (default `rbac.read.enabled=true`, `rbac.read.namespaces=[]`) | ClusterRole and ClusterRoleBinding: get/list/watch on pods, services, endpoints, events, nodes, namespaces, persistentvolumeclaims, resourcequotas, limitranges, replicationcontrollers, deployments, statefulsets, daemonsets, replicasets, jobs, cronjobs, horizontalpodautoscalers, ingresses, networkpolicies, events.k8s.io events; get/list on pods/log, metrics pods and nodes |
| Read stage with `rbac.read.namespaces` set | ClusterRole limited to nodes, namespaces (get/list/watch) and node metrics; one Role and RoleBinding per listed namespace with the rules above |
| Secrets | No rule in the chart names them |
| ConfigMaps | Not in the Read stage; get/list/create/update/delete/patch (all verbs) in each Act namespace when `act.scope: namespace` |
| Act stage, `act.scope: limited` (default; `actNamespaces`, default empty) | One Role and RoleBinding per listed namespace: delete pods; patch and update deployments/scale and statefulsets/scale |
| Act stage, `act.scope: namespace` | One Role and RoleBinding per listed namespace: all verbs on pods, pods/log, pods/exec, pods/portforward, pods/eviction, services, endpoints, configmaps, persistentvolumeclaims, resourcequotas, limitranges, events, replicationcontrollers (and scale), deployments (and scale), statefulsets (and scale), daemonsets, replicasets, jobs, cronjobs, horizontalpodautoscalers, ingresses, networkpolicies, poddisruptionbudgets; no Secrets, ServiceAccounts, Roles or RoleBindings |
| Consumers of Kubernetes API access in the pod | No binary in the image calls the API server with the ServiceAccount token; the supervisor makes no Kubernetes API call |
| Mounted credentials | Core token at /secrets/core/token (projected) or /secrets/core/<key> (Secret); key mode: API key Secret at /secrets/anthropic; federation mode: projected token at /var/run/secrets/anthropic.com; optional telemetry token Secret at /secrets/otel; ServiceAccount token at the default path |
| Credential file modes | Secret and projected volumes defaultMode 0440; rendered CLI files 0400 (helper scripts 0500) in a 0700 directory on a memory-backed emptyDir |
| CLI permission deny rules | Read denied on the run directory, the core token file, the telemetry token file (when set), and the key file (key mode) or identity token file (federation mode) |
| Process identity | Supervisor, CLI and the shell commands the CLI runs share one uid |
| Persistent state | /state/notes and /state/next.json (agent area); /state/supervisor (session.json, usage.json 0600, status.json 0644, rooms.json, audit.log; directory 0700); /state/claude (CLI settings and session files); /state/home; /state/work (CLI working directory) |
| Daily usage counter | Held in memory, written through to usage.json, read from the file once at start |

## 6. Network Surface

| Item | Value |
| --- | --- |
| Listening ports | None; no container ports, no Service, no Ingress |
| Outbound connections made by the supervisor | Core MCP endpoint; core websocket (when `core.apiUrl` is set) |
| NetworkPolicy | Created by default (`networkPolicy.enabled=true`); policyTypes Ingress and Egress; ingress empty |
| Egress allowed | Cluster DNS (namespace selector only, no pod selector); Anthropic CIDRs on 443; the core; each connector; the telemetry endpoint when enabled; API server CIDRs on 443 and 6443 (required by the chart when the policy is on) |
| Outbound auth | Anthropic: API key or federated access token; core: bearer token; connectors: none sent by the spot; telemetry: bearer token from file |
| Core URL scheme | `--core-mcp-url` accepts any value; `--core-api-url`, `--otel-endpoint`, `--ego-mcp-url` and connector URLs accept http:// or https://; chart examples use https for the MCP URL and http for the core API, telemetry and connectors |
| Inbound auth | Not applicable |

## 7. Dependency Vulnerabilities

| Ecosystem / target | Tool | Critical | High | Medium | Low | Note |
| --- | --- | --- | --- | --- | --- | --- |
| Go modules (go.mod, 9 components in SBOM) | trivy fs | 0 | 2 | 0 | 0 | golang.org/x/text v0.14.0 (CVE-2026-56851 fixed in 0.41.0, CVE-2026-56852 fixed in 0.39.0) |
| Go modules and standard library | govulncheck | - | - | - | - | 9 standard-library findings reachable (toolchain go1.27.1, fixed in go1.27.2 and go1.27.3); 5 findings in imported standard-library packages and 2 in golang.org/x/text, not called |
| Base image buildpack-deps (Debian 12.15) | trivy image | 2 | 74 | 157 | 187 | Counts from the 2026-10-22 scan of the same digest; not rescanned on 2026-10-24; 2 unknown; no fixed version listed for the Critical and High entries |
| Helm chart, rendered (key mode, two limited Act namespaces) | trivy config | 0 | 0 | 3 | 2 | KSV0125, KSV048 (x2); KSV011, KSV110 |
| Helm chart, rendered (federation mode) | trivy config | 0 | 0 | 1 | 2 | KSV0125; KSV011, KSV110 |
| Helm chart, rendered (connectors values) | trivy config | 0 | 0 | 1 | 2 | KSV0125; KSV011, KSV110 |
| Helm chart, rendered (key mode, one Act namespace with `act.scope: namespace`) | trivy config | 3 | 3 | 6 | 2 | KSV045 (x3); KSV053, KSV056 (x2); KSV0125, KSV042, KSV048 (x3), KSV049; KSV011, KSV110 |
| Bundled CLI and jq binaries | not scanned | - | - | - | - | Fetched at build time; not present in the source tree |
