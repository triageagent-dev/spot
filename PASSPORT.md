# Application Passport — tria-spot

## 1. Identity

| Field | Value |
| --- | --- |
| Component | tria-spot |
| Description | A Go supervisor that runs the Claude Code CLI as an always-on agent for one workspace of a triage system, in one Kubernetes pod |
| Repo | Local git repository, no remote configured |
| Revision described | 4a92abb (committed tree; uncommitted edits in the working copy are not covered) |
| Registry | ghcr.io/<org>/spot (default in chart values and Makefile) |
| Version | Chart 0.1.0, appVersion 0.1.0; no git tags (`git describe`: 4a92abb) |
| Owner | Not recorded in the repository |
| License | No license file in the repository |
| Date | 2026-10-22 |
| Audit status | HIGH findings open |
| Code review | Findings open (2026-10-22) |

## 2. Classification

| Field | Value |
| --- | --- |
| Class | Service (single-replica Deployment plus supervisor process) |
| Type | Agent (LLM-driven, tool-using) |
| Capabilities | Schedules and runs CLI turns; polls the core for issue and chat changes; reads and posts workspace chat through the core's MCP tools; runs `kubectl` under a Kubernetes Role; keeps a daily token ledger and an audit log |
| Model role | Decision-making agent: investigates, proposes and (in the Act stage) changes cluster workloads |
| Runtime | Kubernetes pod, Deployment with 1 replica, strategy Recreate |
| Public accessibility | No listener; NetworkPolicy ingress is empty |
| Production data access | Read: pods, logs, events, workloads, nodes, namespaces and related objects through the Read stage. Write: pod delete and workload patch/scale only in namespaces listed in `actNamespaces` (default none) |
| PII | Not classified in the repository; chat text, issue text and pod logs pass through the agent and the model provider |
| Sensitivity | Holds an Anthropic credential (API key file or projected identity token) and a core bearer token; reads cluster workload data |
| Enabled by default | No (`spot.enabled=false`) |

## 3. Artifact

| Field | Value |
| --- | --- |
| Form | OCI image built with ko, plus a Helm chart (`charts/tria-spot`) |
| Language | Go 1.27 (module go directive), CGO disabled |
| Build tool | ko (no Dockerfile); `make kodata` fetches bundled binaries |
| Base image | docker.io/library/buildpack-deps@sha256:5f8a65a7e3b419450aebd73c24e3ce8b25810354eccd7ff6f673b0c387479131 (Debian 12, bash, coreutils, curl, wget, CA certificates), linux/amd64 |
| Bundled binary: Claude Code CLI | 2.1.304, from npm package `@anthropic-ai/claude-code-linux-x64`, sha512 pinned in Makefile |
| Bundled binary: kubectl | v1.37.1, sha256 pinned in Makefile |
| Bundled binary: jq | 1.7.1, sha256 pinned in Makefile |
| Entrypoint | /ko-app/tria-spot |
| Runtime user | uid/gid 10001, runAsNonRoot, fsGroup 10001 |
| Pod hardening | readOnlyRootFilesystem, allowPrivilegeEscalation false, capabilities drop ALL, seccomp RuntimeDefault |
| Writable paths | /state (PVC, 2Gi default), /run/tria-spot (memory emptyDir, 16Mi), /tmp (emptyDir, 256Mi) |
| Chart image tag | appVersion unless `image.tag` is set; pullPolicy IfNotPresent |

## 4. AI / LLM Supply Chain

| Field | Value |
| --- | --- |
| Models | Chosen by the CLI default unless `--model` / `agent.model` is set; not pinned in the repository |
| Model client | Claude Code CLI run as `claude -p` with stream-json output, one process per turn, conversation resumed by session id |
| LLM auth, mode `key` (default) | Secret mounted as a file, read through the CLI `apiKeyHelper` script |
| LLM auth, mode `federation` | Projected ServiceAccount token (audience https://api.anthropic.com, 600 s) exchanged by the CLI through an OIDC federation profile |
| Core auth | Bearer token from a Secret mounted as a file; written into the CLI MCP file (mode 0400, memory-backed directory) and re-read per request by the supervisor |
| Egress: Anthropic API | api.anthropic.com over 443; NetworkPolicy allows CIDRs 160.79.104.0/23 and 2607:6bc0::/48 |
| Egress: core | Core `/mcp` endpoint (streamable HTTP), selected by namespace/pod selector or CIDR, port 443 |
| Egress: Kubernetes API | API server CIDRs set by the operator, ports 443 and 6443 |
| Egress: cluster DNS | Namespace `kube-system` (configurable), UDP/TCP 53 |
| MCP servers | One, `core`, all of its tools allowed (`mcp__core__*`); `--strict-mcp-config` |
| Tools allowed without prompt | Read, Glob, Grep; Edit under /state; `Bash(kubectl *)`, `jq`, `ls`, `head`, `tail`, `grep`, `wc`, `sort`, `uniq`, `date`, `echo`, `mkdir` |
| Tools disallowed | WebFetch, WebSearch |
| Permission mode | `auto` (default), fallback `dontAsk`; `--permission-prompts none` |
| PreToolUse hook | `tria-spot hook pretool` on the Bash tool: appends commands to /state/audit.log; counts mutating `kubectl` verbs against a cap of 20 per hour; fails open |
| Per-turn limits | `--max-turns` 40; turn timeout 20m |
| Token budget | Daily ceiling 2,000,000 billable tokens per UTC day (input + output + cache creation; cache reads tracked, not counted); 0 disables; checked before a wake starts |
| Wake clamp | 2m to 2h; edge cooldown 1m; poll interval 30s |
| Environment passed to the CLI | Parent environment without names containing KEY, TOKEN, SECRET, PASSWORD, CREDENTIAL or starting ANTHROPIC_, CLAUDE_CODE_OAUTH, CLAUDE_CODE_USE_; HOME and CLAUDE_CONFIG_DIR under /state |
| Prompt | System prompt embedded in the binary and appended with `--append-system-prompt-file`; chat messages enter the turn prompt quoted and labelled untrusted; issue summaries enter unquoted |
| Logging | Counts, kinds, ids and timings; message, issue and model text only with `--log-content` at debug level |

## 5. Privileges

| Scope | Grant |
| --- | --- |
| ServiceAccount | One per release; token automounted |
| Read stage (default `rbac.read.enabled=true`, `rbac.read.namespaces=[]`) | ClusterRole and ClusterRoleBinding: get/list/watch on pods, services, endpoints, events, nodes, namespaces, persistentvolumeclaims, resourcequotas, limitranges, replicationcontrollers, deployments, statefulsets, daemonsets, replicasets, jobs, cronjobs, horizontalpodautoscalers, ingresses, networkpolicies, events.k8s.io events; get/list on pods/log, metrics pods and nodes |
| Read stage with `rbac.read.namespaces` set | ClusterRole limited to nodes, namespaces (get/list/watch) and node metrics; one Role and RoleBinding per listed namespace with the rules above |
| Secrets, ConfigMaps | No rule names them in any role |
| Act stage (`actNamespaces`, default empty) | One Role and RoleBinding per listed namespace: delete pods; patch deployments, statefulsets, daemonsets; patch and update deployments/scale and statefulsets/scale |
| Core token scope | Per chart documentation: read and write on one tenant |
| Mounted credentials | Core token Secret at /secrets/core; key mode: API key Secret at /secrets/anthropic; federation mode: projected token at /var/run/secrets/anthropic.com; ServiceAccount token at the default path |
| Credential file modes | Secret volumes defaultMode 0440; rendered CLI files 0400 in a 0700 directory on a memory-backed emptyDir |
| CLI permission deny rules | Read denied on the run directory, the core token file, and the key file (key mode) or identity token file (federation mode) |
| Process identity | Supervisor and CLI run as the same uid |
| Persistent state | /state: notes, session id, CLI session files and settings directory, next.json, usage.json, status.json (mode 0644), audit.log, hook-writes.json |

## 6. Network Surface

| Item | Value |
| --- | --- |
| Listening ports | None; no container ports, no Service, no Ingress |
| NetworkPolicy | Created by default (`networkPolicy.enabled=true`); policyTypes Ingress and Egress; ingress empty |
| Egress allowed | Cluster DNS (namespace selector only); Anthropic CIDRs on 443; the core on 443; API server CIDRs on 443 and 6443 |
| Outbound auth | Anthropic: API key or federated access token; core: bearer token; API server: ServiceAccount token |
| Core URL scheme | Any URL accepted by `--core-mcp-url`; chart example uses https |
| Inbound auth | Not applicable |

## 7. Dependency Vulnerabilities

| Ecosystem / target | Tool | Critical | High | Medium | Low | Note |
| --- | --- | --- | --- | --- | --- | --- |
| Go modules (go.mod, 7 modules in SBOM) | trivy fs | 0 | 2 | 0 | 0 | golang.org/x/text v0.14.0 (CVE-2026-56851, CVE-2026-56852); fixed in 0.41.0 |
| Go modules and standard library | govulncheck | - | - | - | - | 9 standard-library findings reachable (toolchain go1.27.1, fixed in go1.27.2 and go1.27.3); 2 findings in golang.org/x/text and 5 in imported standard-library packages, not called |
| Base image buildpack-deps (Debian 12.15) | trivy image | 2 | 74 | 157 | 187 | 2 unknown; no fixed version listed for the Critical and High entries |
| Helm chart, rendered (key mode) | trivy config | 0 | 0 | 5 | 2 | KSV048 (x4), KSV0125; KSV011, KSV110 |
| Helm chart, rendered (federation mode) | trivy config | 0 | 0 | 1 | 2 | KSV0125; KSV011, KSV110 |
| Bundled CLI, kubectl, jq binaries | not scanned | - | - | - | - | Fetched at build time; not present in the source tree |
