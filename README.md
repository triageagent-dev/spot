# tria-spot

The Tria spot for one tenant (ADR-095). It is Claude Code in a pod: an agent
that watches the tenant's cluster through the core's MCP tools and `kubectl`,
fixes what its ServiceAccount lets it fix, and tells the team what it saw and
did. The component is a small Go supervisor that wakes the agent in turns; the
agent owns the loop and does the triage. The core gets no Kubernetes client.

This repo is phases 1 and 2 of the ADR: the pod and read-only loop, and the
agent's own schedule (`next.json`, clamps, edge polling, heartbeat). Speech
(`say_in_workspace`, `read_workspace_room`) is phase 3 and lives in the core;
the agent's prompt uses those tools when the core offers them, and the
supervisor works without them.

```
                       Anthropic API
                            ^
   +------- pod -----------|---------------------------------+
   |  tria-spot (supervisor, entrypoint)                      |
   |    renders /run/tria-spot/{mcp.json,settings.json,...}   |
   |    polls the core (MCP, model-free) for edges            |
   |    claude -p --resume ... (one turn per wake)  ----+     |
   |                                                    v     |
   |  Claude Code CLI -- MCP --> core /mcp   kubectl --> API  |
   |        ^  PreToolUse hook: tria-spot hook pretool        |
   |  /state: session, notes/, next.json, usage.json,         |
   |          status.json, audit.log                          |
   +----------------------------------------------------------+
```

## The loop

1. **Startup.** The supervisor renders the CLI's configuration (below), then
   runs a `startup` turn that resumes the saved conversation.
2. **A turn** is `claude -p` with the prompt on stdin: why it woke (`startup`,
   `tick`, `edge`, `human`), the facts behind it, the token budget left, and
   the path to write `next.json`. The agent looks with the core's tools first,
   then `kubectl` reads, verifies its hypothesis before any write, records
   what it did on the issue, and ends by writing
   `/state/next.json`: `{"after":"15m","why":"check the rollout"}`.
3. **The schedule** is clamped to `[--min-wake, --max-wake]` (2m to 2h). A
   missing or unreadable file means `--min-wake`; the supervisor logs why. The
   file is consumed, so a stale one is never read twice.
4. **Between turns** the supervisor polls the core every `--poll-interval`
   with `list_open_issues` for the tenant (no model involved) and wakes the
   agent early on a new issue, a reopened issue, a severity change or a new
   comment. It re-reads the issue list after each turn so the agent's own
   comments are not edges. If the core offers `read_workspace_room`, messages
   from anyone but the spot also wake it (`human`); the first read only sets a
   cursor. Edges inside `--edge-cooldown` of the last turn are held, not lost.
5. **The heartbeat** (`upsert_memory_item`, kind `note`, `stale_after` of twice
   the next sleep, at least 5m) is refreshed after every turn that succeeded.
   A failed turn does not refresh it: the agent is dark, so reflection should
   resume (docs/todo/system2-loop-contract.md in the core).
6. **Spend.** Each turn's tokens (input + output + cache creation, from the
   CLI's result; cache reads are tracked but not counted) go into a UTC-day
   counter in `/state/usage.json`. Once it reaches `--daily-token-ceiling` no
   wake starts, edges are held, and the loop sleeps to UTC midnight. A turn in
   flight can overshoot by up to one turn (bounded by `--max-turns` and
   `--turn-timeout`). The spend limit on the Anthropic workspace that owns the
   key is the real stop.
7. **Failure.** A failed turn (non-zero exit, error result, no result, timeout)
   backs the next wake off from `--min-wake`, doubling to `--max-wake`. Four
   consecutive 401 retries stop the turn at once (`authFailed` in the status).
   A saved session the CLI can no longer find is dropped and a new one starts.
8. **SIGTERM** stops the running CLI (SIGTERM to its process group, kill after
   30 s), counts the tokens seen so far, marks the session as cut off, writes
   `status.json` and exits. The next start tells the agent its last turn was
   cut off. A killed pod resumes the same conversation from `/state`.

## Flags

Flags only. No credential is a flag value or an environment variable.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--tenant` | required | tenant this spot works for |
| `--name` | `<tenant>-spot` | spot name (messenger name, machine identity) |
| `--core-mcp-url` | required | the core's `/mcp` (streamable HTTP) |
| `--core-token-file` | required | file holding the core bearer token |
| `--anthropic-key-file` | required | file holding the Anthropic API key |
| `--state-dir` | `/state` | persistent: notes, session, schedule, ledger, audit log |
| `--run-dir` | `/run/tria-spot` | memory-backed: rendered CLI config |
| `--claude-bin` | `claude` | the CLI |
| `--model` | CLI default | `--model` for the CLI |
| `--min-wake` / `--max-wake` | `2m` / `2h` | clamp for the agent's schedule |
| `--max-turns` | `40` | CLI `--max-turns` per wake |
| `--turn-timeout` | `20m` | wall-clock cap per wake |
| `--daily-token-ceiling` | `2000000` | tokens per UTC day; `0` = none |
| `--poll-interval` | `30s` | edge polling between turns |
| `--edge-cooldown` | `1m` | minimum gap between a turn's end and an edge wake |
| `--permission-mode` | `auto` | `auto`, `dontAsk` or `acceptEdits` |
| `--fallback-permission-mode` | `dontAsk` | used if the CLI does not start in auto |
| `--allowed-tools` | | extra comma-separated `--allowedTools` rules |
| `--heartbeat-id` | `system2-heartbeat-<tenant>` | memory cell id of the heartbeat |
| `--kubectl-writes-per-hour` | `20` | PreToolUse hook cap |
| `--hook-bin` | this executable | binary the hook setting runs |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

`tria-spot hook pretool [--state-dir D] [--kubectl-writes-per-hour N]` is the
hook, `tria-spot version` prints the build.

## How credentials flow

- **Anthropic key.** A Secret mounted as a file. The supervisor renders
  `settings.json` with `apiKeyHelper` set to `api-key-helper.sh`, a two-line
  script that `exec`s `tr -d '\r\n' < <key file>`. The CLI runs it and sends
  its output as the API credential. The key is never in an environment
  variable or an argument. This was exercised against the real CLI (see
  Verification): the helper ran and the API received and rejected a dummy key.
- **Core token.** A Secret mounted as a file. The supervisor writes it into
  the CLI's MCP file (`mcp.json`, `Authorization: Bearer ...`) at start, mode
  0400 in a 0700 directory on a memory-backed emptyDir. Its own MCP client
  re-reads the token file on every request.
- **ServiceAccount token.** The cluster's projected token, used by `kubectl`.
- **The CLI's environment** is the supervisor's, minus every variable whose
  name contains KEY, TOKEN, SECRET, PASSWORD or CREDENTIAL or starts with
  `ANTHROPIC_`, plus `HOME` and `CLAUDE_CONFIG_DIR` under `/state`,
  `DISABLE_AUTOUPDATER=1`, `DISABLE_ERROR_REPORTING=1` and
  `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1`. These are not credentials.
- `settings.json` denies the Read tool on the run directory and the two
  secret files.

**Limit, stated plainly.** The agent's shell runs as the same user as the
supervisor, so a command it runs (`cat /run/tria-spot/mcp.json`) can read the
rendered files and the mounted Secrets. The deny rules and the prompt only
stop the obvious path. What bounds the damage is: the Anthropic key has a
spend limit on its workspace, the core token has `r` and `w` on one tenant and
nothing else, NetworkPolicy lets egress reach only the Anthropic API, the core
and the API server, and the Read stage grants no Secrets.

## Stages and RBAC

The cluster boundary is the ServiceAccount, not the prompt and not the hook.

| Stage | Chart | The agent can |
| --- | --- | --- |
| Read | ClusterRole + binding (`rbac.read.enabled`, default on) | `get/list/watch` pods, services, endpoints, events, nodes, namespaces, PVCs, Deployments, StatefulSets, DaemonSets, ReplicaSets, Jobs, CronJobs, HPAs, Ingresses, NetworkPolicies, metrics; `pods/log`. No Secrets, no ConfigMaps. |
| Act | one `Role` + `RoleBinding` per name in `actNamespaces` (default `[]`) | plus `delete` pods; `patch` Deployments, StatefulSets, DaemonSets; `patch`/`update` the scale subresource of Deployments and StatefulSets |

Back out by emptying `actNamespaces` or deleting the Role. Inside the RBAC the
supervisor narrows further: an explicit permission mode (auto where the CLI
offers it), `--allowedTools` for the tools the loop needs, WebFetch and
WebSearch disallowed, and the PreToolUse hook.

### The hook is not the boundary

`tria-spot hook pretool` appends every Bash command to `/state/audit.log` (one
JSON line each, rotated at 5 MiB) and refuses a mutating `kubectl` command
(apply, patch, delete, scale, rollout restart/undo, annotate, label, edit,
replace, cordon, uncordon, drain, taint, create, set, run, expose, autoscale)
once `--kubectl-writes-per-hour` have run in the last hour, counting every
write in a compound command. `--dry-run` other than `=none`/`=false` is not
counted. It scans words, so it also sees `bash -c "kubectl ..."` and
`xargs kubectl ...`. A shell can route around any text check (a script, an
interpreter, a client library), so treat the hook as a speed bump and an
audit trail. It fails open: a broken hook must not stop an investigation.

## Kill switches

Any one is enough: `spot.enabled=false`; delete the Deployment; delete the Act
`Role` (reads only); revoke the core token (it can neither read nor speak). The
Anthropic workspace's spend limit stops the model regardless of the pod.

## Status

`/state/status.json` (world-readable, no secrets): state (`starting`, `running`,
`idle`, `paused-ceiling`, `stopping`, `stopped`), session id, last turn
(reason, seconds, tokens, result subtype, timed out, auth failed), next wake
(time, why, source: `agent`, `agent-clamped`, `default-missing`,
`default-garbled`, `ceiling`), tokens today against the ceiling, the permission
mode asked for, reported and used, the last error and the heartbeat time.
Writing it into a core status cell (ADR-095 section 7) is a later slice.

## Permission mode and auto mode

The ADR left open whether auto mode runs under `-p`. The CLI reference says
`--permission-mode` accepts `auto`, and the permission-modes page says auto
mode works with `claude -p` on a direct Anthropic API connection from Claude
Code 2.1.281. Running 2.1.305 with `--permission-mode auto -p` and an
`apiKeyHelper`, the init message reported `"permissionMode":"auto"`. What was
not exercised is the classifier itself deciding a tool call: that needs a real
key. The docs also say that in `-p` a denied call is not retried and, after
repeated blocks, "the action doesn't run and Claude keeps working".

The supervisor reads `permissionMode` from every init message. If you asked
for `auto` and the CLI reports anything else, it logs an error, records it in
`status.json` (`fellBack`) and uses `--fallback-permission-mode` (`dontAsk`) for
all later turns. In `dontAsk`, only `--allowedTools` run, which is why that
list is kept (reads, edits under `/state`, `mcp__core__*`, `kubectl`, `jq`,
and a few text tools; no bare `Bash`, no `cat`), and the hook and RBAC carry
the rest. `--permission-prompts none` is always passed so nothing waits for an
answer.

## Image

`make image` builds the Dockerfile (multi-stage: the Go binary; then Node LTS
slim with the Claude Code CLI, `kubectl` and `jq`; non-root uid 10001; a
read-only root filesystem works because every write goes to `/state`,
`/run/tria-spot` or `/tmp`). It does not push. Versions are ARGs:

| Component | Version | Where it came from |
| --- | --- | --- |
| Claude Code CLI | `2.1.304` | `npm view @anthropic-ai/claude-code dist-tags` on 2026-10-21: `stable` = 2.1.304, `latest` = 2.1.305. The pinned one is `stable`. The build checks `npm view ...@2.1.304 dist.integrity` equals the `sha512-oAD2...WWQ==` value recorded in the Dockerfile and fails otherwise. |
| kubectl | `v1.37.1` | `https://dl.k8s.io/release/stable.txt` on 2026-10-21; the amd64 and arm64 sha256 files from `dl.k8s.io` are in the Dockerfile and checked with `sha256sum -c`. |
| Node | `24.22.0` (bookworm-slim) | newest LTS (Krypton) in `https://nodejs.org/dist/index.json`; the tag exists on Docker Hub (`library/node`). |
| Go | `1.27.3` (bookworm) | the tag exists on Docker Hub (`library/golang`). The module says `go 1.27`. |

Not done: no `docker build` was run, so the Dockerfile is untested. The
Claude Code package installs a native binary through optional per-platform
dependencies; `npm install -g` is expected to pull the linux one, but that
was not exercised.

## Chart

`charts/tria-spot`: renders nothing unless `spot.enabled=true`. Deployment (1
replica, `Recreate`, read-only root, non-root, all capabilities dropped), a
ServiceAccount, a PVC for `/state`, a memory-backed emptyDir for
`/run/tria-spot`, an emptyDir for `/tmp`, the two Secrets referenced by name
and mounted as files (mode 0440 with `fsGroup`), the Read ClusterRole and
binding, one Act Role per `actNamespaces` entry, and a NetworkPolicy (egress
to cluster DNS, the Anthropic API CIDRs, the core, and the API server;
nothing in). NetworkPolicy has no host names, so the Anthropic CIDRs are the
ones Anthropic publishes (`160.79.104.0/23`, `2607:6bc0::/48`,
platform.claude.com/docs/en/api/ip-addresses), the core is selected by
namespace and pod selector or CIDR, and `networkPolicy.apiServer.cidrs` is yours
to set. The chart fails to render when a required value is missing.
`make chart-lint` runs `helm lint` and `helm template` with the flag off (must
render nothing) and on (`ci/enabled-values.yaml`, placeholders only), and
checks that the pod has no `env` and no rule names Secrets.

## Run it against a local kubeconfig

The supervisor needs the CLI, a core you can reach and your own credentials in
files. Nothing below is a real name.

```sh
make build
mkdir -p /tmp/spot/state && umask 077
printf '%s' "$CORE_TOKEN" > /tmp/spot/core-token          # a token for a tenant identity
printf '%s' "$ANTHROPIC_KEY" > /tmp/spot/anthropic-key
./bin/tria-spot --tenant <tenant> \
  --core-mcp-url https://<core-host>/mcp \
  --core-token-file /tmp/spot/core-token --anthropic-key-file /tmp/spot/anthropic-key \
  --state-dir /tmp/spot/state --run-dir "$(mktemp -d)" \
  --min-wake 2m --daily-token-ceiling 200000 --log-level debug
```

`kubectl` uses your `KUBECONFIG`, which passes through the environment scrub,
so the agent has your own access: point it at a read-only context. `Ctrl-C`
stops it gracefully. Check `/tmp/spot/state/status.json` and
`audit.log`. Without a `claude` login on the machine the CLI uses only the
key helper.

## Layout

```
cmd/tria-spot            entrypoint: supervisor, `hook pretool`, `version`
internal/config          flags
internal/render          CLI config into the run dir (modes, helper script)
internal/runner          one `claude -p` turn: args, env scrub, stream parse, timeouts
internal/stream          stream-json reader (session, permission mode, usage, result)
internal/schedule        next.json parse and clamp
internal/corecli         the supervisor's MCP client (list_open_issues, heartbeat, rooms)
internal/edges           edge detection between issue snapshots
internal/ledger          /state files: session, daily usage, status
internal/hook            the PreToolUse hook
internal/prompt          the system prompt (instruction.md, embedded) and turn prompts
internal/supervisor      the loop; tests drive it with a fake `claude` script
charts/tria-spot         Helm chart
```

## Verification

Verified against the current docs (code.claude.com, fetched 2026-10-21) and
the CLI itself (2.1.305, offline `--help` and a run with a dummy key and an
unreachable core):

| Used | Source |
| --- | --- |
| `-p`, `--output-format stream-json`, `--verbose`, `--permission-mode` (`auto`, `dontAsk`, `acceptEdits`), `--permission-prompts none` (2.1.259+), `--allowedTools`, `--disallowedTools`, `--mcp-config`, `--strict-mcp-config`, `--settings`, `--append-system-prompt-file`, `--max-turns`, `--model`, `--resume` | https://code.claude.com/docs/en/cli-reference |
| prompt on stdin, `--resume <session_id>`, stream-json needs `--verbose`, SIGTERM exits 143 and a resume continues from the next prompt | https://code.claude.com/docs/en/headless |
| `apiKeyHelper` (run through `/bin/sh`, output sent as the API credential, cached 5 minutes) | https://code.claude.com/docs/en/settings-reference#apikeyhelper |
| PreToolUse input (`tool_name`, `tool_input.command`, `session_id`, `tool_use_id`), deny by `hookSpecificOutput.permissionDecision`, hook `args` exec form, settings shape | https://code.claude.com/docs/en/hooks |
| MCP file: `mcpServers.<name>` with `type: "http"`, `url`, `headers` | https://code.claude.com/docs/en/mcp |
| Permission rule syntax (`mcp__core__*`, `Edit(//abs/path/**)`, `Bash(kubectl *)`) | https://code.claude.com/docs/en/permissions |
| stream-json fields: `system/init` (`session_id`, `permissionMode`, `mcp_servers`), `assistant.message.usage`, `result` (`subtype`, `is_error`, `usage`, `total_cost_usd`, `terminal_reason`, `errors`), `system/api_retry` (`error: authentication_failed`) | https://code.claude.com/docs/en/agent-sdk/typescript and the live run |

Seen live: the flags are accepted; `settings.json` loads without a warning
(`claude doctor --settings`); the init message reports `permissionMode: auto`
and the `core` MCP server as `failed` when the URL is unreachable (the turn
still runs); the key helper is executed and its output sent (401
`authentication_failed`); SIGTERM yields exit 143.

Not verified, and why:

- A full turn against the real API: no key was used. So: the classifier under
  `-p`, the exact `result` usage numbers on a resumed conversation, whether
  `usage` includes subagent calls (the docs say it does not, so the ceiling can
  undercount a turn that spawns subagents), and `-p` reading a prompt from
  stdin when no argument is given (documented for piped input; the live run
  started a turn, but it never reached the model).
- Whether the hook `args` exec form and `timeout` are honoured: documented,
  and `settings.json` loaded without a warning, but no Bash call ran.
- The core side of phase 3: `say_in_workspace` and `read_workspace_room` do not
  exist yet. The reply shape of `read_workspace_room` is read loosely (an
  object with `messages`, or an array; `id`, `sender|from|author`,
  `text|body|message`, `ts|time`). Re-check it when the tool lands.
- The heartbeat against the core: the cell id. The ADR says
  `system2-heartbeat-<tenant>`; the core today looks only for the global id
  `system2-heartbeat` (`System2HeartbeatCellID`), so until the core reads the
  per-tenant id, this heartbeat does not change reflection. `--heartbeat-id`
  sets either. The tool and arguments (`upsert_memory_item`, kind `note`,
  scope `global`, `stale_after`, `tenant`) were read from the core's source.
- `list_open_issues` carries a comment count but not who wrote the comments (all
  MCP comments are attributed to `mcp-client`), so every new comment is a
  `human` wake and the agent reads the issue to see who. Machine and spot
  comments made during a turn are absorbed by the post-turn baseline.
- Hop counts and caps are enforced by the core (phase 3); the prompt only tells
  the agent to pass and respect them.
- The Dockerfile build, `helm install`, kubeconform, and `golangci-lint` (not
  installed here; `make lint` runs it when present).
