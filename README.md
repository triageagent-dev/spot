# tria-spot

The Tria spot for one tenant (ADR-095). It is Claude Code in a pod: an agent
that watches the tenant's cluster through the core's MCP tools and its
connectors (MCP servers that hold their own credentials), fixes what they let
it fix, and tells the team what it saw and
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
   |    claude -p (an empty conversation per wake)  ----+     |
   |                                                    v     |
   |  Claude Code CLI -- MCP --> core /mcp, connectors        |
   |  /state: notes/, next.json (the agent's)                 |
   |          supervisor/: session, usage, status, cursors    |
   +----------------------------------------------------------+
```

## The loop

1. **Startup.** The supervisor renders the CLI's configuration (below), then
   runs a `startup` turn. No wake resumes a conversation (see below).
2. **A turn** is `claude -p` with the prompt on stdin: why it woke (`startup`,
   `tick`, `edge`, `human`), the facts behind it, the token budget left, and
   the path to write `next.json`. The agent looks with the core's tools first,
   then the connectors' reads, verifies its hypothesis before any write, records
   what it did on the issue, and ends by writing
   `/state/next.json`: `{"after":"15m","why":"check the rollout"}`.
3. **The schedule** is clamped to `[--min-wake, --max-wake]` (2m to 2h). A
   missing or unreadable file means `--min-wake`; the supervisor logs why. The
   file is consumed, so a stale one is never read twice.
4. **Between turns** the supervisor watches the core without a model. With
   `--core-api-url` the core tells it: a websocket (`/v2/messenger/spot/ws`)
   carries content-free frames (issues changed, a room has a message), and the
   supervisor reads the core only then, after a 1.5 s pause so a burst is read
   once. While the stream is down, or without the flag, it polls the core every
   `--poll-interval` instead; `status.json` `triggers` says which (`stream` or
   `poll`). Either way it reads `list_open_issues` for the tenant (no model
   involved) and wakes the agent early on a new issue, a reopened issue, a severity change or a new
   comment. After each turn it compares the issue list with the one from
   before the turn: new issues, reopens and severity changes that landed
   during the turn are held like any edge; comment counts are re-baselined,
   because the agent comments itself. If the core offers
   `read_workspace_room`, messages from anyone but the spot are edges too.
   A message that names the spot (`@<name>` or the name) wakes it after
   `--edge-cooldown`; a person's message that does not waits up to
   `--chat-batch` (10m) and then wakes; a machine's message (Tria, another
   spot) never wakes on its own and is only a fact in the next prompt, and
   one with a hop count of 2 or more is dropped. Early wakes are capped at
   `--max-edge-wakes-per-hour` (12); past the cap, and inside the failure
   backoff, edges wait for the scheduled wake. Room cursors are saved under
   `/state/supervisor`, so a message sent while the pod was down is news on
   the next start; only the very first read of a room sets the cursor without
   waking. At most 20 edges are held; past that, machine messages are dropped
   first, then the oldest.
5. **The heartbeat** (`upsert_memory_item`, kind `note`, `stale_after` of twice
   the next sleep, at least 5m) is refreshed after every turn that succeeded.
   A failed turn does not refresh it: the agent is dark, so reflection should
   resume (docs/todo/system2-loop-contract.md in the core). A turn whose init
   message reports the `core` MCP server as anything but connected is a
   failed turn (`coreDown` in the status): the agent ran without its tools,
   and a stale core token must not look like a healthy spot.
6. **Spend.** Each turn's tokens (input + output + cache creation, from the
   CLI's result; cache reads are tracked but not counted) go into a UTC-day
   counter held in memory and written through to
   `/state/supervisor/usage.json`; the file is read once, at start, so the
   agent rewriting it changes nothing. Once it reaches `--daily-token-ceiling` no
   wake starts, edges are held, and the loop sleeps to UTC midnight. A turn in
   flight can overshoot by up to one turn (bounded by `--max-turns` and
   `--turn-timeout`). The spend limit on the Anthropic workspace that owns the
   key is the real stop.
7. **Failure.** A failed turn (non-zero exit, error result, no result, timeout,
   core not connected) backs the next wake off from `--min-wake`, doubling to
   `--max-wake`; edges wait for the backoff too. Running out of `--max-turns`
   (`error_max_turns`) is not a failure. Four consecutive 401 retries stop the
   turn at once (`authFailed` in the status). A saved session is dropped only
   when the CLI says it cannot find it; an outage keeps the conversation. A
   timed-out turn is reported as cut off on the next wake.
8. **SIGTERM** stops the running CLI (SIGTERM to its process group, kill after
   30 s), counts the tokens seen so far, marks the session as cut off, writes
   `status.json` and exits. The next start tells the agent its last turn was
   cut off. A killed pod keeps its notes, ledger and schedule in `/state`.

## Every wake is an empty conversation

The spot manages its own context. Nothing is resumed: each wake starts a new
conversation (no `--resume`), so a model call never pays again for the tool
output of earlier wakes. Continuity is what the spot reads at the start of the
wake: the room's last few messages (`read_workspace_room` with a `limit`), the
open issues (`list_open_issues` with a `limit`), its notes under `/state/notes`
and the `why` it left in `next.json`, and the core's memory. There is no
hand-over note and no compaction: the notes the agent keeps are its memory.

## One process between turns

By default each wake starts `claude -p` and it exits. With
`--persistent` (`agent.persistent`) the supervisor starts one process with
`--input-format stream-json` and writes every turn to its stdin as a user
message (`{"type":"user","message":{"role":"user","content":...},"parent_tool_use_id":null,"uuid":...}`),
reading the stream until that turn's `result`. The supervisor itself is
unchanged: it still decides when a turn runs, what it says and what a turn
may spend.

What was measured against the CLI (2.1.309), and what the code relies on:

- `system/init` is written at the start of every turn, not once; the
  permission mode and MCP server status checks work per turn as before.
- `result.usage` is that turn's own usage; `total_cost_usd` is cumulative.
- An interrupt (`{"type":"control_request","request_id":...,"request":{"subtype":"interrupt"}}`)
  is answered with a `control_response` and then a `result` with subtype
  `error_during_execution` and every count zero although the model had
  answered; the runner then counts the assistant messages' usage instead.

The process is replaced at every wake (a fresh conversation is asked for), and
also when the arguments
change (the permission-mode fallback), when the MCP file changes (the core
token rotated: it is read once at start), when the process exited or crashed,
and after 12 hours. A turn past its timeout is interrupted and the process is
kept; one that does not stop within the grace, and one that keeps failing
authentication, is killed. Shutdown closes stdin and waits for it to exit.

### A message while a turn runs

With the stream up and `--persistent`, a person's message in a room that
arrives during a turn is written to the running process as one more user
message instead of waiting for the turn to end. The turn runs in its own
goroutine, so the supervisor keeps reading the stream and everything it owns
stays on one goroutine. A machine's message, or a person's that does not name
the spot while chat batching is on, is held for the next wake as before; the
issues are not read mid-turn (what changed during a turn is held when it ends).

What was measured against the CLI (2.1.309): a message written while a tool
runs is taken at the next tool boundary and answered in the same turn, with a
single `result`; one written after the last boundary runs as a turn of its own
with a second `result`. Each message reports `command_lifecycle` (`queued` when
read, `started` when taken, `completed` after the result). A turn is over when
a result arrives and no message passed in is still `queued`; the usage of every
result is summed into the turn. A CLI that reports no lifecycle is given two
seconds after a result to begin the next turn (its `init`). A turn past its
timeout is interrupted and what was passed in and has not begun is given up
with it. Without `--persistent` nothing changes: a message during a turn waits.

## Names

Two names meet. `--name` is the deployment's technical name (Deployment,
volume, ServiceAccount, note tags, ego thread); people never see it. The name
people see and write to in the messenger belongs to the workspace: its owner
sets `spotName` on the workspace in the core (empty means
`<workspace name>-spot`), and the spot reads it with `get_spot_identity`
at start, when the trigger stream reconnects, at most every five minutes while
it works, and when a person writes an `@` it does not recognise. Every wake
prompt says "In the messenger you appear as ...", and a message naming that
name, the technical name or `--chat-name` (the fallback for a core that cannot
say) wakes the spot at once. A rename needs no restart.

## Live conversation

The messenger is a conversation, so the spot answers first and works after.

- **Typing.** While a turn runs, the supervisor shows the spot's "typing…"
  indicator through the core's `set_workspace_typing` (model-free, renewed
  every 20 seconds, cleared when the turn ends). A turn started by a person
  shows it in the team room; other turns show it in the agents room. A core
  without the tool, or a failed call, costs only the indicator.
- **Answer first.** The system prompt tells the spot to reply to a person
  before it investigates, to slice long work (report what it found, note the
  rest, wake itself in two minutes) so the next message reaches it, and to
  report when done.
- **Fast wakes.** A message that wakes the spot (it names the spot, or
  `--chat-batch` has passed; `0s` wakes for every message) waits only a few
  seconds after the last turn, not the edge cooldown. Lower `--poll-interval`
  to notice messages sooner.
- **Files.** A message that carries a file is marked in the wake, and the spot
  looks at it with `read_workspace_file` (an image comes back as an image).

## Flags

Flags only. No credential is a flag value or an environment variable.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--tenant` | required | tenant this spot works for |
| `--name` | `<tenant>-spot` | spot name (messenger name, machine identity) |
| `--core-mcp-url` | required | the core's `/mcp` (streamable HTTP) |
| `--core-token-file` | required | file holding the core bearer token |
| `--connectors-file` | none | JSON list of connectors, more MCP servers with the tools the spot may call (see Connectors) |
| `--persistent` | off | keep one `claude` process alive between turns (see One process between turns) |
| `--anthropic-auth` | `key` | `key` or `federation` (see How credentials flow) |
| `--anthropic-key-file` | required with `key` | file holding the Anthropic API key; rejected with `federation` |
| `--anthropic-identity-token-file` | required with `federation` | the projected identity token (a JWT), re-read on every exchange |
| `--anthropic-federation-rule-id` | required with `federation` | federation rule id (`fdrl_...`) |
| `--anthropic-org-id` | required with `federation` | Anthropic organization id |
| `--anthropic-service-account-id` | required with `federation` | Anthropic service account id (`svac_...`) |
| `--anthropic-workspace-id` | | workspace id (`wrkspc_...`); optional when the rule spans one workspace |
| `--state-dir` | `/state` | persistent: notes, session, schedule, ledger |
| `--run-dir` | `/run/tria-spot` | memory-backed: rendered CLI config |
| `--claude-bin` | `claude` | the CLI |
| `--model` | CLI default | `--model` for the CLI |
| `--min-wake` / `--max-wake` | `2m` / `2h` | clamp for the agent's schedule |
| `--max-turns` | `40` | CLI `--max-turns` per wake |
| `--turn-timeout` | `20m` | wall-clock cap per wake |
| `--daily-token-ceiling` | `2000000` | tokens per UTC day; `0` = none |
| `--core-api-url` | empty | the core's HTTP API; with it the core pushes triggers over a websocket and nothing polls while it is up |
| `--chat-name` | empty | fallback for the name the messenger shows, used until the core says (the workspace's owner sets it in the core) |
| `--poll-interval` | `30s` | edge polling between turns, or only while the trigger stream is down |
| `--edge-cooldown` | `1m` | minimum gap between a turn's end and an edge wake |
| `--permission-mode` | `auto` | `auto`, `dontAsk` or `acceptEdits` |
| `--fallback-permission-mode` | `dontAsk` | used if the CLI does not start in auto |
| `--allowed-tools` | | extra comma-separated `--allowedTools` rules |
| `--disallowed-tools` | | extra comma-separated `--disallowedTools` rules |
| `--chat-batch` | `10m` | how long a room message that does not name the spot waits before it wakes a turn; `0` wakes at once |
| `--max-edge-wakes-per-hour` | `12` | cap on early wakes per hour; `0` = no cap |
| `--heartbeat-id` | `system2-heartbeat-<tenant>` | memory cell id of the heartbeat |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

`tria-spot version` prints the build.

## How credentials flow

Two Anthropic auth modes, chosen with `--anthropic-auth`. The ids federation
needs are not secrets, but they are flags, not environment variables.

- **Anthropic key (`key`, the default).** A Secret mounted as a file. The
  supervisor renders `settings.json` with `apiKeyHelper` set to
  `api-key-helper.sh`, a two-line script that `exec`s `tr -d '\r\n' < <key
  file>`. The CLI runs it and sends its output as the API credential. The key
  is never in an environment variable or an argument.
- **Workload Identity Federation (`federation`).** No Anthropic secret at all.
  The pod gets a Kubernetes projected ServiceAccount token (audience
  `https://api.anthropic.com`, mounted at `/var/run/secrets/anthropic.com/token`)
  and the CLI exchanges it for a short-lived access token. The supervisor
  writes a profile, `<run-dir>/anthropic/configs/spot.json` (type
  `oidc_federation`, rule id, service account id, organization, optional
  workspace, `identity_token` from that file), and starts the CLI with only
  `ANTHROPIC_CONFIG_DIR=<run-dir>/anthropic` and `ANTHROPIC_PROFILE=spot`. The
  SDK caches the access token under `<run-dir>/anthropic/credentials/`, which
  is memory-backed, never the state volume. The profile holds no secret.
  In this mode the supervisor renders **no** `apiKeyHelper` and no helper script
  (and removes a stale one), refuses an `--anthropic-key-file`, and scrubs every
  `ANTHROPIC_*` variable, `CLAUDE_CODE_OAUTH_*` and `CLAUDE_CODE_USE_*` from the
  CLI's environment, because `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`,
  `apiKeyHelper` and `CLAUDE_CODE_OAUTH_TOKEN` all outrank federation in the
  CLI's credential order and would silently shadow it. The CLI is never started
  with `--bare`, which ignores profiles and federation variables.
- **Core token.** A file: by default the pod's own projected ServiceAccount
  token with audience `triage-core`, which the kubelet rotates (chart
  `core.identity: serviceAccount`); or a Secret (`core.identity: secret`).
  The supervisor writes it into the CLI's MCP file (`mcp.json`,
  `Authorization: Bearer ...`) before every turn, mode 0400 in a 0700
  directory on a memory-backed emptyDir, so a rotated token reaches the CLI
  without a restart. Its own MCP client re-reads the token file on every
  request.
- **ServiceAccount token.** The pod's default ServiceAccount token is not
  mounted (`automountServiceAccountToken: false`); nothing in the pod talks to
  the API server. The projected tokens above are separate volumes.
- **The CLI's environment** is the supervisor's, minus every variable whose
  name contains KEY, TOKEN, SECRET, PASSWORD or CREDENTIAL or starts with
  `ANTHROPIC_`, `CLAUDE_CODE_OAUTH` or `CLAUDE_CODE_USE_`, plus `HOME` and
  `CLAUDE_CONFIG_DIR` under `/state`, `DISABLE_AUTOUPDATER=1`,
  `DISABLE_ERROR_REPORTING=1` and `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1`. In
  federation mode `ANTHROPIC_CONFIG_DIR` and `ANTHROPIC_PROFILE` are added
  back, and nothing else with that prefix. None of these is a credential.
- `settings.json` denies the Read tool on the run directory, on
  `/var/run/secrets/**` and on the directory of every credential file (the
  core token, the OTLP token, and the key file in key mode or the identity
  token in federation mode). It is by directory because a Secret or projected
  volume serves the file through a `..data` symlink. It denies Edit and Write
  on `/state/supervisor`, `/state/claude` (the CLI's config home),
  `/state/home`, `/state/work` (the CLI's working directory, an empty
  directory of its own) and the run directory. The agent has no shell:
  `Bash` is in `--disallowedTools`, so the Read deny rules are the whole story
  for the CLI's file tools. The agent's writable files are `/state/notes/**`
  and `/state/next.json`, through Edit and Write.

### The identity token must be fresh at every exchange

The projected token carries a `jti` claim, and Anthropic rejects a `jti` it has
already seen (reason `jti_reused` in the Console's authentication history). So
the file must hold a token that has not been exchanged before each time the
CLI exchanges. The kubelet rewrites the file when about 80% of
`tokenExpirationSeconds` has passed (600 s by default, the minimum), and the
SDK re-reads the file on every exchange, so a second exchange inside the same
token's life, from a retry or from a new `claude -p` process whose cached
access token has expired, can send a token that was already used. Whether the
cached access token lasts long enough for that not to happen has **not** been
tested against the real API. This repo does not work around it. When a turn
fails and looks like an authentication failure in federation mode, the
supervisor logs and records in `status.json` (`lastError`):

> Anthropic authentication failed in federation mode; check the authentication
> history in the Claude Console (jti_reused / match_*)

The opaque 401 (`Authentication failed`) gives no reason; the reason is only in
the Console history: `jti_reused`, `match_subject_prefix` and the other
`match_*` rules, `workspace_id_required`. Whether to switch off `check_jti` on a
dedicated issuer is a decision for whoever runs the test.

**Limit, stated plainly.** There is no shell, so the only readers of the
rendered files and mounted tokens are the CLI's own file tools, which the deny
rules bind. What bounds the rest is: the Anthropic key (or the federated
service account's workspace) has a spend limit, the core identity has `r` and
`w` on one tenant and nothing else, the connector's own identity is whatever
its operator gave it, and the NetworkPolicy lets egress reach only the
Anthropic API, the core, the connectors and the OTLP endpoint. The
supervisor's counters are held in memory.

## Boundary

The spot holds no cluster RBAC. A cluster is reached through a connector,
whose own identity is the boundary. The supervisor narrows further with the
permission mode (auto where the CLI offers it), `--allowedTools` (the core's
tools by name, `runner.CoreTools`, and each connector's tools by exact name,
never `mcp__core__*`) and `--disallowedTools` (`Bash` unless `--sandbox`, WebFetch, WebSearch,
subagents, whose usage is not in the result the ledger counts, and the core
tools the spot must never call, `runner.CoreToolsDenied`: `resolve_issue`,
`forget_issue`, `update_issue_severity`, `set_tenant_goals`, `run_rsi`, the
event, pattern, memory-editing and test tools).

### A shell in a sandbox

By default the agent has no shell: `Bash` is disallowed. A shell run as the
agent's own user would read every mounted token, which no Read rule reaches.
With `--sandbox` (chart `sandbox.enabled: true`) the agent gets a `Bash` tool
that runs only inside the CLI's sandbox (bubblewrap). The rendered settings
(`internal/render`) set:

- `sandbox.enabled` with `failIfUnavailable`: the CLI exits at start when the
  sandbox cannot run, instead of running commands unsandboxed;
- `allowUnsandboxedCommands: false` and no `excludedCommands`: no command and
  no retry runs outside it;
- no allowed domain: a sandboxed command has no network (no cluster, no
  exfiltration), and the shell is for cutting down output (`jq`, `grep`) and
  `sleep`, not for the cluster;
- `denyRead` on the credential directories (run directory, `/secrets`,
  `/var/run/secrets`) and `denyWrite` on the supervisor's, the CLI's and the
  run directory, on top of the Read and Edit deny rules the CLI merges in;
- writes only under the notes and the temporary directory; the working
  directory is denied, so a command cannot leave a `CLAUDE.md` for the next
  conversation to load.

The sandbox runs in its own PID namespace with a fresh `/proc`, so a sandboxed
command sees none of the supervisor's or the CLI's processes (their memory
holds the tokens). That needs user namespaces and a fresh `/proc`, which the
runtime's default pod profile blocks. Measured on GKE (Container-Optimized
OS, kernel 6.18), each of these was needed:

| Pod setting | Without it |
|---|---|
| `hostUsers: false` (the pod's own user namespace) | `procMount: Unmasked` is refused, and bubblewrap cannot mount a fresh `/proc` |
| `procMount: Unmasked` | `Can't mount proc on /newroot/proc` |
| seccomp `Unconfined` | `unshare` is refused (`No permissions to create new namespace`) |
| AppArmor `Unconfined` | `Failed to make / slave: Permission denied` |

So `sandbox.enabled` takes the runtime's seccomp and AppArmor profiles off for
the pod. The container still runs non-root with every capability dropped, no
privilege escalation and a read-only root filesystem, and `hostUsers: false`
means root in the pod is not root on the node. `bubblewrap` and `socat` (and
the one library `socat` needs) are Debian bookworm packages fetched from a
fixed archive snapshot by `make kodata`, pinned by digest, and travel as
kodata next to the CLI. `enableWeakerNestedSandbox` is not used: it would
expose the container's `/proc`, and with it the other processes of the same
user.

### Identity towards the core

By default (`core.identity: serviceAccount`) the spot authenticates to the
core with its own ServiceAccount: the chart mounts a projected token with
audience `triage-core` (no core Secret), the kubelet rotates it, and the core
validates it by TokenReview. The chart renders a Role and RoleBinding named
`<spot name>-core`, labelled `triageagent.dev/managed: "true"`, that grant
the ServiceAccount `core.verbs` (default get, list, create, update) on the one
tenant; set `core.rbac.create: false` if the grant is made elsewhere. The core
only accepts ServiceAccounts from its own namespace and reads the grants from
that namespace, so in this mode the chart must be installed in the core's
namespace. `core.identity: secret` keeps the earlier behaviour: a bearer token
read from the Secret named in `core.tokenSecret`, usable from any namespace.

## Kill switches

Any one is enough: `spot.enabled=false`; delete the Deployment; delete the
connector's egress or the connector itself (it can read and write nothing in
the cluster); revoke the core token (it can neither read nor speak). The
Anthropic workspace's spend limit stops the model regardless of the pod.

## Status

`/state/supervisor/status.json` (world-readable, no secrets): state (`starting`, `running`,
`idle`, `paused-ceiling`, `stopping`, `stopped`), session id, last turn
(reason, seconds, tokens, result subtype, timed out, auth failed), next wake
(time, why, source: `agent`, `agent-clamped`, `default-missing`,
`default-garbled`, `ceiling`), tokens today against the ceiling, the permission
mode asked for, reported and used, the last error, the heartbeat time, the
edges held for the next wake and the early wakes in the last hour.
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
list is kept (reads, edits under `/state/notes` and `next.json`, the core
tools by name, the connectors' tools by name; there is no Bash at all), and
the connector's own limits carry the rest. `--permission-prompts none` is always passed so nothing waits for an
answer.

## Image

The image is built with ko only; there is no Dockerfile and no Docker. `make
kodata` downloads the Claude Code CLI and `jq` for linux/amd64 into
`cmd/tria-spot/kodata`, each checked against a pinned digest, and removes
anything else it finds in the directory so a stale binary cannot ship. ko adds them
to the image under `/var/run/ko`. The supervisor puts that directory first on
the CLI's `PATH` (from `KO_DATA_PATH`) and runs the kodata copy of the CLI.
`make image` writes an OCI layout to `bin/image` and pushes nothing; `make
push IMAGE=<registry>/<repo>` pushes. The pod runs as uid 10001 with a
read-only root filesystem; every write goes to `/state`, `/run/tria-spot` or
`/tmp`.

| Component | Version | Where it came from |
| --- | --- | --- |
| Claude Code CLI | `2.1.304` | The Claude Code package ships a native binary per platform. `npm view @anthropic-ai/claude-code dist-tags` on 2026-10-21: `stable` = 2.1.304. `make kodata` runs `npm pack @anthropic-ai/claude-code-linux-x64@2.1.304`, computes the tarball's sha512 and fails unless it equals the value in the Makefile (the registry's `dist.integrity` for that package). No Node is needed at run time. |
| jq | `1.7.1` | the `jq-linux-amd64` asset of the jqlang release; the sha256 is from the release's `sha256sum.txt` and is checked the same way. |
| Base image | `buildpack-deps:bookworm-curl` | pinned by digest in `.ko.yaml`: it has bash, coreutils, curl and CA certificates; the shell is only used by the CLI for `apiKeyHelper`/`otelHeadersHelper`. linux/amd64 only. |

Seen: ko builds it (`make image`), the kodata files land in the image as mode
`0555`, and the entrypoint is `/ko-app/tria-spot`.

## Traces

With `telemetry.enabled` (flag `--otel-endpoint`) the CLI exports its own
OpenTelemetry traces (beta in the CLI: `CLAUDE_CODE_ENABLE_TELEMETRY` and
`CLAUDE_CODE_ENHANCED_TELEMETRY_BETA`, protocol http/protobuf). The supervisor
sets these in the CLI's environment, so the pod still carries no environment
variable. Resource attributes are the tenant label under the attribute ego reads it from
(`triage.tenant=<label>`: `telemetry.tenant`, flag `--otel-tenant`, default the
spot's name, so each spot is its own tenant in ego; `telemetry.tenantAttr`, flag
`--otel-tenant-attr`, is ego's `--usage-tenant-attr`), its display name
(`ego.tenant.name=<name>`: `telemetry.tenantName`, flag `--otel-tenant-name`,
default the label; ego keeps the first name it sees for a tenant and drops an
invalid one silently, so the spot refuses a name over 80 characters, with
control characters, commas or equals signs) and `gen_ai.agent.name=<spot name>`;
`service.name` is the spot name. The tenant in ego is `<sender>/<label>`, the
sender coming from the token; a token an ego admin minted bound to a tenant
decides the tenant itself and these attributes only repeat it. Spans:
`claude_code.interaction` per wake, with `llm_request` (model, input, output,
cache-read and cache-creation tokens, stop reason, errors) and `tool` spans
under it. The sender's bearer token is a Secret mounted as a file
(`telemetry.tokenSecret`); a rendered helper script prints the header
(`otelHeadersHelper`), so no variable or argument carries it, and the agent's
Read rules deny the file. Prompts, responses and tool output are not added;
`--otel-tool-details` adds tool parameters such as Bash commands and file
paths. Logs and metrics exporters are off.

Claude Code does not set `gen_ai.usage.*` on its spans (it exports
`input_tokens` without the cached ones), so the receiver has to map them; ego
does. The traces carry no spend control: that is the workspace spend limit, the
supervisor's `--daily-token-ceiling` and `--daily-cost-ceiling-usd`.

## Spend

- `--daily-token-ceiling`: input + output + cache-creation tokens per UTC day.
  Cache reads are not counted, so it under-reports a long conversation.
- `--daily-cost-ceiling-usd`: the CLI's own dollar estimate per UTC day, which
  includes cache reads. Either ceiling pauses all wakes until UTC midnight.
- **No conversation grows.** Every wake starts empty, so the context of a turn
  is what that wake reads. `status.json` shows each turn's `contextTokens`.
- **The core's cap.** The supervisor's ceilings are the spot's own. The core
  keeps one it enforces (policy `budgets.spotTenantDailyTokens` over
  `budgets.spotDailyDefaultTokens`, tokens per workspace per UTC day, new input
  plus output). After every turn the supervisor sends the core a `turn` frame on
  the trigger stream (model, input with cache reads and writes included, cached,
  output, duration, failed): the core books it in its usage ring as kind `spot`,
  so the TUI usage panel shows it, and answers with the budget. Over the cap the
  core refuses `say_in_workspace` and `set_spot_status`, and the supervisor
  pauses all wakes until the budget lifts or UTC midnight. A turn sent while the
  stream is down waits for the next connection (the last 100 are kept).
- **Stats.** A `stats` frame (state, turns and wakes by reason today, failures in
  a row, context size, rotations, own token and cost counts, next wake) goes to
  the core after each turn, on a change of trigger mode and every two minutes,
  for the TUI's workspace panel. Counts and labels only, never content.

## Logs

Logs carry what happened, never what was said. At every level the supervisor
logs counts, kinds, ids, token numbers and timings: an `edges` line says
`kinds="room-message=1" human=true`, not the message. The text of a person's
message or an issue, the model's words and the raw CLI output are content, and
reach the log only with `--log-content` (chart: `agent.logContent`) at debug
level. An error the API or the CLI produced (`API Error: ...`) is operational
and is logged, because it is what diagnoses a failed login. `--log-level` is
`debug`, `info` (default), `warn` or `error`.

Two things on `/state` are not logs and do hold content: the notes and
`status.json` `nextWake.why`, which the agent wrote itself. They stay in the pod's
volume. The supervisor is PID 1 in the container and reaps children the CLI
leaves behind.

## Chart

`charts/tria-spot`: renders nothing unless `spot.enabled=true`. Deployment (1
replica, `Recreate`, read-only root, non-root, all capabilities dropped), a
ServiceAccount, a PVC for `/state`, a memory-backed emptyDir for
`/run/tria-spot`, an emptyDir for `/tmp`, the core identity (a projected
ServiceAccount token with audience `triage-core` plus the `<name>-core` Role
and RoleBinding, or a Secret mounted as a file, mode 0440 with `fsGroup`; see
Identity towards the core), and per `anthropic.auth`
either the key Secret (`key`, default) or a projected `serviceAccountToken`
volume at `/var/run/secrets/anthropic.com` (`federation`: audience
`https://api.anthropic.com`, `tokenExpirationSeconds` 600; no Secret mounted;
`anthropic.federation.ruleId`, `organizationId` and `serviceAccountId` are
required, `workspaceId` is optional), and a NetworkPolicy (egress
to cluster DNS, the Anthropic API CIDRs, the core, the connectors and, with
`telemetry.enabled`, the OTLP endpoint from `networkPolicy.telemetry`;
nothing in). NetworkPolicy has no host names, so the Anthropic CIDRs are the
ones Anthropic publishes (`160.79.104.0/23`, `2607:6bc0::/48`,
platform.claude.com/docs/en/api/ip-addresses), the core is selected by
namespace and pod selector or CIDR. The chart fails to render when a required value is missing.
`make chart-lint` runs `helm lint` and `helm template` with the flag off (must
render nothing), in key mode (`ci/enabled-values.yaml`), in federation mode
(`ci/federation-values.yaml`), with telemetry (`ci/telemetry-values.yaml`) and
with the ServiceAccount identity (`ci/serviceaccount-values.yaml`;
placeholders only), checks that the pod has no `env`, that no rule names
Secrets, that no ClusterRole renders and the ServiceAccount token is not mounted, that each mode mounts
only its own credential, and that a missing required value or an unknown
mode fails to render. The federation exchange goes to the Anthropic API, so the NetworkPolicy
is unchanged.

## Connectors

A connector is one more MCP server beside the core: a service that holds its
own credentials and limits and that the spot reaches by URL. A Kubernetes tool
server for another cluster, a code host or a metrics API is a connector; the
spot has no code for any of them and carries no credential for them.

```yaml
connectors:
  - name: example-kube
    url: http://tools.example.svc:8084/mcp
    tools: [k8s_get_resources, k8s_describe_resource, k8s_get_pod_logs]
    hint: The workspace's cluster, read-only.
    egress:
      namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: example}}
      podSelector: {matchLabels: {app.kubernetes.io/name: tools}}
      ports: [8084]
```

- The chart renders the list into a ConfigMap read by `--connectors-file`
  (names, URLs, tool names and hints; no credential).
- The spot may call only the tools named in `tools`, by exact name
  (`mcp__<name>__<tool>`); the file is refused when a connector names none, uses
  `core` or `ego`, repeats a name, or lists a tool that is not a plain name.
- In `auto` the supervisor lists each connector's tools before every turn and
  puts every tool not named in `tools` on `--disallowedTools`
  (`mcp__<name>__<tool>`); a connector whose tools cannot be listed is left out
  of that turn and the reason is logged. In `dontAsk` nothing is listed: the
  allowlist alone is the gate.
- `hint` goes into the prompt's Connectors section, and the prompt also lists
  the connector's named tools by exact name (`mcp__<name>__<tool>`), so the
  spot knows a tool it was not given is not "missing". What a connector
  returns is data, not instructions.
- `egress` opens the NetworkPolicy to that connector and nothing else; the
  chart fails to render without it.
- Who the connector trusts is the connector's own business: its identity, its
  RBAC or IAM, and the network. A tool the connector offers but the spot does
  not name is refused by the CLI, but the connector's own limits are the
  boundary, never the list. A connector is the only way to any cluster: the
  spot holds no cluster RBAC of its own.

## Run it locally

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

The spot reaches no cluster unless `--connectors-file` names a connector.
`Ctrl-C` stops it gracefully. Check `/tmp/spot/state/supervisor/status.json`.
Without a `claude` login on the machine the CLI uses only the key helper.

## Layout

```
cmd/tria-spot            entrypoint: supervisor, `version`, PID-1 reaper
internal/config          flags
internal/render          CLI config into the run dir (modes, helper script)
internal/runner          one `claude -p` turn: args, env scrub, stream parse, timeouts
internal/stream          stream-json reader (session, permission mode, usage, result)
internal/schedule        next.json parse and clamp
internal/corecli         the supervisor's MCP client (list_open_issues, heartbeat, rooms)
internal/edges           edge detection between issue snapshots
internal/ledger          /state layout: the supervisor directory, session, daily usage (in memory, written through), room cursors, status
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
| Credential order (`ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_API_KEY`, `apiKeyHelper`, `CLAUDE_CODE_OAUTH_TOKEN` rank above profiles and federation); `ANTHROPIC_PROFILE`; no profiles or federation variables in `--bare` | https://code.claude.com/docs/en/authentication#anthropic-profiles-and-federation-credentials |
| Federation variables, the all-four activation rule, `ANTHROPIC_CONFIG_DIR`, `ANTHROPIC_PROFILE`, profile file layout (`configs/<name>.json`, `credentials/<name>.json` mode 0600), profile JSON (`authentication.type: oidc_federation`, `identity_token {source: file, path}`), `jti_reused` and the opaque 401 | https://platform.claude.com/docs/en/manage-claude/wif-reference |
| MCP file: `mcpServers.<name>` with `type: "http"`, `url`, `headers` | https://code.claude.com/docs/en/mcp |
| Permission rule syntax (`mcp__core__*`, `Edit(//abs/path/**)`, `Read(//abs/path/**)`) | https://code.claude.com/docs/en/permissions |
| stream-json fields: `system/init` (`session_id`, `permissionMode`, `mcp_servers`), `assistant.message.usage`, `result` (`subtype`, `is_error`, `usage`, `total_cost_usd`, `terminal_reason`, `errors`), `system/api_retry` (`error: authentication_failed`) | https://code.claude.com/docs/en/agent-sdk/typescript and the live run |

Seen live in federation mode (2026-10-22, CLI 2.1.305, dummy identity token file, a rule id the API rejects as malformed): the CLI read the rendered profile from `ANTHROPIC_CONFIG_DIR`/`ANTHROPIC_PROFILE` and attempted a real exchange against `api.anthropic.com`. The result text was `API Error: Token exchange failed with status 400 ... federation_rule_id is not a well-formed fdrl_ tagged ID`, so the rule id, service account, organization and token path came from the profile (none was in the environment). The init message reported `apiKeySource: none` with no `apiKeyHelper` in `settings.json`. The SDK created `<run-dir>/anthropic/credentials/federation/`. `claude doctor --settings` raised no settings warning. That failure arrives as a `result` with `subtype: success`, `is_error: true` and no `api_error_status`, not as `api_retry` events.

Seen live: the flags are accepted; `settings.json` loads without a warning
(`claude doctor --settings`); the init message reports `permissionMode: auto`
and the `core` MCP server as `failed` when the URL is unreachable (the turn
still runs); the key helper is executed and its output sent (401
`authentication_failed`); SIGTERM yields exit 143.

Not verified, and why:

- **The federation exchange has not been run against the real API** with a
  valid rule, service account and identity token. Only the rejection of a
  malformed rule id (HTTP 400) was seen. So also unconfirmed: that the exchange
  succeeds from a pod, how long the exchanged access token lives and whether a
  new `claude -p` process reuses the cached one (this decides whether
  `jti_reused` shows up), what a 401 exchange failure looks like in the CLI's
  output (the supervisor's hint matches `Authentication failed`,
  `authentication_error`, `jti_reused` and `Token exchange failed with status
  401/403`; a 401 string was not seen), and that `ANTHROPIC_CONFIG_DIR` on a
  memory-backed directory is writable for the credentials cache in the pod.

- A full turn against the real API: no key was used. So: the classifier under
  `-p`, the exact `result` usage numbers on a resumed conversation, whether
  `usage` includes subagent calls (the docs say it does not, so the ceiling can
  undercount a turn that spawns subagents), and `-p` reading a prompt from
  stdin when no argument is given (documented for piped input; the live run
  started a turn, but it never reached the model).
- Whether the CLI applies a `Read(...)` deny rule to `Grep` and `Glob` as the
  permissions doc says: not exercised live.
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
- Running the image, `helm install`, kubeconform, and `golangci-lint` (not
  installed here; `make lint` runs it when present).
