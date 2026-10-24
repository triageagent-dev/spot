# tria-spot

The Tria spot for one workspace (the core calls it a tenant): an always-on
teammate that watches the workspace's cluster through the core's MCP tools and
its connectors (MCP servers that hold their own credentials), fixes what they
let it fix, and tells the team what it saw and did (ADR-095, ADR-098).

The binary is two things in one process. A **supervisor** wakes the agent in
turns, watches the core between turns without a model, and keeps the schedule,
the budget and the heartbeat. The **agent** is the spot's own loop in Go: it
sends the wake prompt to Claude through the official Anthropic Go SDK, runs the
tools Claude asks for, and sends the results back until Claude is done. The
spot has no shell, no filesystem tools and no cluster RBAC. The core gets no
Kubernetes client.

```
                    model API (Anthropic, or Vertex AI)
                            ^
   +------- pod ------------|--------------------------------+
   |  tria-spot                                              |
   |    supervisor  - polls or is told by the core (no model)|
   |       |            schedule, ceilings, heartbeat, status|
   |       v                                                 |
   |    agent loop  - one empty conversation per wake        |
   |       |              model request -> tool_use -> ...   |
   |       v                                                 |
   |    tools  ---- MCP ---> core /mcp, ego, connectors      |
   |    spans  ---- OTLP --> ego                             |
   |  /state/supervisor: usage, cursors, status              |
   +---------------------------------------------------------+
```

## The loop

1. **A wake** is why the supervisor starts a turn: `startup`, `tick` (the agent
   scheduled it), `edge` (the core changed) or `human` (someone wrote to the
   spot). It builds the wake prompt: the reason, the facts behind it, the
   token budget left. That is the first user message.
2. **A turn** is the agent loop (`internal/agent`): list the tools, send the
   system prompt, the tools and the wake prompt, and while the answer asks for
   tools, run them and send all the results of one answer back in one user
   message. The turn ends when the model ends its answer (`end_turn`) or calls
   `finish`, at `--max-steps` model requests (40), or at `--turn-timeout`
   (20m). The model looks with the core's tools first, then the connectors'
   reads, verifies its hypothesis before any write, and records what it did on
   the issue.
3. **Every wake is an empty conversation.** Nothing is resumed, so a model call
   never pays again for the tool output of earlier wakes. Continuity is what
   the spot reads at the start of a wake (the room's last messages, the open
   issues with a `limit`) and the core's memory: the spot's own notes are
   memory items (`upsert_memory_item`, `search_memory`). Nothing lives on a
   disk for the model, so a pod that starts with an empty `/state` starts
   fresh and loses nothing but the day's token count and room cursors.
4. **The schedule** is the agent's `schedule_next` call, last in the turn:
   `{"after": "15m", "why": "check the rollout"}`. It is clamped to
   `[--min-wake, --max-wake]` (2m to 2h). If the model never calls it, or the
   duration is not one, the next wake is `--min-wake` and the supervisor logs
   why. `finish` is the other built-in: `{"summary": "<one line>"}` ends the
   turn and the line shows in the status file. It is optional.
5. **Between turns** the supervisor watches the core without a model. With
   `--core-api-url` the core tells it: a websocket (`/v2/messenger/spot/ws`)
   carries content-free frames (issues changed, a room has a message), and the
   supervisor reads the core only then, after a 1.5 s pause so a burst is read
   once. While the stream is down, or without the flag, it polls the core every
   `--poll-interval` instead; `status.json` `triggers` says which (`stream` or
   `poll`). Either way it reads `list_open_issues` for the tenant and wakes the
   agent early on a new issue, a reopened issue, a severity change or a new
   comment. After each turn it compares the issue list with the one from before
   the turn: new issues, reopens and severity changes that landed during the
   turn are held like any edge; comment counts are re-baselined, because the
   agent comments itself. If the core offers `read_workspace_room`, messages
   from anyone but the spot are edges too. A message that names the spot
   (`@<name>` or the name) wakes it after `--edge-cooldown`; a person's message
   that does not waits up to `--chat-batch` (10m) and then wakes; a machine's
   message (Tria, another spot) never wakes on its own and is only a fact in
   the next prompt, and one with a hop count of 2 or more is dropped. Early
   wakes are capped at `--max-edge-wakes-per-hour` (12); past the cap, and
   inside the failure backoff, edges wait for the scheduled wake. Room cursors
   are saved under `/state/supervisor`, so a message sent while the pod was
   down is news on the next start; only the very first read of a room sets the
   cursor without waking. At most 20 edges are held; past that, machine
   messages are dropped first, then the oldest.
6. **The heartbeat** (`upsert_memory_item`, kind `note`, `stale_after` of twice
   the next sleep, at least 5m) is refreshed after every turn that succeeded.
   A failed turn does not refresh it: the agent is dark, so reflection should
   resume (docs/todo/system2-loop-contract.md in the core). A turn that could
   not connect to the core's MCP server or list its tools is a failed turn
   (`coreDown` in the status): the model would have run without its tools, so
   the loop does not start the model at all, and a stale core token must not
   look like a healthy spot.
7. **Spend.** Each turn's tokens (input + output + cache writes, from the API's
   usage; cache reads are tracked but not counted) go into a UTC-day counter
   held in memory and written through to `/state/supervisor/usage.json`; the
   file is read once, at start. Once it reaches `--daily-token-ceiling` no
   wake starts, edges are held, and the loop sleeps to UTC midnight. A turn in
   flight can overshoot by up to one turn (bounded by `--max-steps` and
   `--turn-timeout`). The spend limit on the Anthropic workspace (or the Google
   project) that serves the model is the real stop.
8. **Failure.** A failed turn (an API error after the SDK's two retries, a
   refusal, an answer cut at the output cap twice, a timeout, no core tools)
   backs the next wake off from `--min-wake`, doubling to `--max-wake`; edges
   wait for the backoff too. Running out of `--max-steps` is not a failure
   (`stepsExhausted` in the status). A 401 or 403 from the model API sets
   `authFailed` and says which credential to look at. A tool that errors or
   times out is not a failed turn: the model gets the error as the tool's
   result. A timed-out turn is reported as cut off on the next wake.
9. **SIGTERM** cancels the running turn, counts the tokens seen so far, marks
   it as cut off, flushes the spans, writes `status.json` and exits. The next
   start tells the agent its last turn was cut off.

### A message while a turn runs

With the stream up, a person's message in a room that arrives during a turn is
handed to the running turn: the model sees it with the next tool results, or
before the turn would have ended, and answers in the same turn. The turn runs
in its own goroutine, so the supervisor keeps reading the stream and
everything it owns stays on one goroutine. A machine's message, or a person's
that does not name the spot while chat batching is on, is held for the next
wake; the issues are not read mid-turn. If the turn ends without having shown a
handed-over message to the model (it failed, or the step cap came first), the
supervisor holds it and the next wake starts with it.

## The model

All model traffic goes through `internal/llm`. The request is built for a
prefix cache: the tools (sorted by name, with each server's input schema
unchanged) and the system prompt come first, and the system prompt's last
block carries a one-hour `cache_control` breakpoint, so the same prefix is read
again by wakes up to an hour apart (the schedule is 2m to 2h). Nothing volatile
is in the prefix: the time, the wake facts, the budget and the step cap are in
the first user message. A second, short breakpoint on the last block of the
latest message lets the steps of one turn read the conversation so far.
Cache reads and writes are recorded in the usage and in the spans.

- `--model` (`claude-opus-5-5`; on Vertex AI the same id, no prefix) and
  `--effort` (`low`, `medium`, the default, `high`, `xhigh`, `max`, sent as
  `output_config.effort`).
- Thinking is not configured: the model thinks adaptively by itself, and its
  thinking blocks go back with the tool results unchanged. No `tool_choice` is
  sent (auto). `max_tokens` is 16000 per request, not streamed.
- A `max_tokens` stop is answered once with "continue" (a tool call cut in the
  middle is dropped); a second one fails the turn. A `refusal` ends the turn as
  failed with the policy category in the error. A `pause_turn` is resent once.
- 429 and 5xx are retried twice by the SDK, then the turn fails.

## Tools

The model is offered the tools of the core's MCP server, of ego when
`--ego-mcp-url` is set, and of each connector, named `<server>__<tool>`
(`core__list_open_issues`, `<connector>__get_pods`), sorted, with the server's
JSON input schema passed through as the server wrote it. The core's token is
read from `--core-token-file` on every request, so a rotated Secret needs no
restart. Two tools are the spot's own and have no prefix: `schedule_next` and
`finish`.

- **Core.** By default the core's tools are allowed by exact name
  (`tools.CoreTools`) and the destructive ones are refused by name
  (`tools.CoreToolsDenied`: `resolve_issue`, `forget_issue`,
  `update_issue_severity`, `set_tenant_goals`, `run_rsi`, the event, pattern,
  memory-editing and test tools); a denied name stays denied even if it is also
  in the allow list. `--core-tools all` offers every tool the core offers: an
  operator's choice for an install, with the core's Role (`core.verbs`) as the
  bound.
- **Ego** (`tools.EgoTools`): the read-only ones that show sessions, usage,
  quality and findings.
- **Connectors**: only the tools named in the connector's `tools`, by exact
  name.
- A tool call has `--tool-timeout` (60s). Its result is the text blocks joined;
  an image or a resource becomes a one-line placeholder; a result past
  `--tool-result-max-bytes` (32768) is cut at a character boundary and ends
  with a line saying how many bytes were dropped. A tool error (the server's
  `isError`, a transport error, a timeout, an unknown tool) goes back to the
  model as an error result and never aborts the turn.
- The connection is opened at the start of each turn. A core that cannot be
  reached fails the turn (`coreDown`). A connector or ego that cannot be
  reached is logged and left out of that turn, and the wake prompt names it.

## Names

Two names meet. `--name` is the deployment's technical name (Deployment,
volume, ServiceAccount, memory-note tags, ego thread); people never see it. The
name people see and write to in the messenger belongs to the workspace: its
owner sets `spotName` on the workspace in the core (empty means
`<workspace name>-spot`), and the spot reads it with `get_spot_identity` at
start, when the trigger stream reconnects, at most every five minutes while it
works, and when a person writes an `@` it does not recognise. Every wake prompt
says "In the messenger you appear as ...", and a message naming that name, the
technical name or `--chat-name` (the fallback for a core that cannot say) wakes
the spot at once. A rename needs no restart.

## Live conversation

The messenger is a conversation, so the spot answers first and works after.

- **Typing.** While a turn runs, the supervisor shows the spot's "typing..."
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
| `--chat-name` | empty | fallback for the name the messenger shows, used until the core says |
| `--core-mcp-url` | required | the core's `/mcp` (streamable HTTP) |
| `--core-api-url` | empty | the core's HTTP API; with it the core pushes triggers over a websocket and nothing polls while it is up |
| `--core-token-file` | required | file holding the core bearer token |
| `--core-tools` | `curated` | `curated` or `all` (see Tools) |
| `--connectors-file` | none | JSON list of connectors, more MCP servers with the tools the spot may call (see Connectors) |
| `--ego-mcp-url` | empty | ego's MCP endpoint, read with the `--otel-token-file` token |
| `--llm-auth` | `key` | `key`, `federation` or `vertex` (see How credentials flow) |
| `--anthropic-key-file` | required with `key` | file holding the Anthropic API key; rejected with the others |
| `--anthropic-identity-token-file` | required with `federation` | the projected identity token (a JWT), re-read on every exchange |
| `--anthropic-federation-rule-id` | required with `federation` | federation rule id (`fdrl_...`) |
| `--anthropic-org-id` | required with `federation` | Anthropic organization id |
| `--anthropic-service-account-id` | required with `federation` | Anthropic service account id (`svac_...`) |
| `--anthropic-workspace-id` | | workspace id (`wrkspc_...`); optional when the rule spans one workspace |
| `--vertex-project` | required with `vertex` | the Google Cloud project that serves Claude |
| `--vertex-region` | `global` | the Vertex AI region, or `global` |
| `--model` | `claude-opus-5-5` | the model id |
| `--effort` | `medium` | `low`, `medium`, `high`, `xhigh` or `max` |
| `--max-steps` | `40` | model requests per wake |
| `--turn-timeout` | `20m` | wall-clock cap per wake |
| `--tool-timeout` | `60s` | cap on one tool call |
| `--tool-result-max-bytes` | `32768` | cap on one tool result the model sees |
| `--state-dir` | `/state` | the supervisor's files: usage, cursors, status |
| `--min-wake` / `--max-wake` | `2m` / `2h` | clamp for the agent's schedule |
| `--daily-token-ceiling` | `2000000` | tokens per UTC day; `0` = none |
| `--poll-interval` | `30s` | edge polling between turns, or only while the trigger stream is down |
| `--edge-cooldown` | `1m` | minimum gap between a turn's end and an edge wake |
| `--chat-batch` | `10m` | how long a room message that does not name the spot waits before it wakes a turn; `0` wakes at once |
| `--max-edge-wakes-per-hour` | `12` | cap on early wakes per hour; `0` = no cap |
| `--heartbeat-id` | `system2-heartbeat-<tenant>` | memory cell id of the heartbeat |
| `--otel-endpoint` | empty | OTLP/HTTP endpoint for the spans; empty = telemetry off |
| `--otel-token-file` | empty | file holding the sender's bearer token for the endpoint |
| `--otel-tenant` | the spot's name | tenant label of the spans in ego |
| `--otel-tenant-attr` | `triage.tenant` | the attribute ego reads the label from |
| `--otel-tenant-name` | the label | display name ego shows for the tenant |
| `--otel-tool-details` | off | add tool arguments and results to the spans |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `--log-content` | off | let debug logs carry message, issue, tool input and model text, and the spans carry the first prompt and the final text |

`tria-spot version` prints the build.

## How credentials flow

Three model auth modes, chosen with `--llm-auth`. The ids federation needs are
not secrets, but they are flags.

- **Anthropic key (`key`, the default).** A Secret mounted as a file, read with
  `--anthropic-key-file` and sent as the API key. The file is read again on
  every request, so a rotated Secret needs no restart. The key is never in an
  environment variable or an argument, and the SDK is built without its
  environment defaults, so a stray `ANTHROPIC_*` variable cannot replace it.
- **Workload Identity Federation (`federation`).** No Anthropic secret at all.
  The pod gets a Kubernetes projected ServiceAccount token (audience
  `https://api.anthropic.com`, mounted at `/var/run/secrets/anthropic.com/token`)
  and the SDK exchanges it for a short-lived access token. The SDK finds
  federation in the process environment: `ANTHROPIC_FEDERATION_RULE_ID`,
  `ANTHROPIC_ORGANIZATION_ID`, `ANTHROPIC_SERVICE_ACCOUNT_ID`,
  `ANTHROPIC_IDENTITY_TOKEN_FILE` and, if set, `ANTHROPIC_WORKSPACE_ID`. The
  process sets these for itself from its own flags just before it builds the
  client. They are ids and the path of the token file, never a credential, and
  there is no child process to inherit them. `ANTHROPIC_API_KEY`,
  `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_IDENTITY_TOKEN` and `ANTHROPIC_PROFILE`
  are unset in this mode, because each of them outranks federation and would
  silently replace it. An `--anthropic-key-file` is refused.
- **Vertex AI (`vertex`).** Claude on Google Vertex AI through Application
  Default Credentials: the pod's Workload Identity. Annotate the ServiceAccount
  (`serviceAccount.annotations`, `iam.gke.io/gcp-service-account`) with a Google
  service account that may call Vertex AI in `--vertex-project`; no Secret is
  mounted. The model id takes no prefix.
- **Core token.** A file: by default the pod's own projected ServiceAccount
  token with audience `triage-core`, which the kubelet rotates (chart
  `core.identity: serviceAccount`); or a Secret (`core.identity: secret`). It
  is read from the file on every request to the core.
- **ServiceAccount token.** The pod's default ServiceAccount token is not
  mounted (`automountServiceAccountToken: false`); nothing in the pod talks to
  the API server. The projected tokens above are separate volumes.

### The identity token must be fresh at every exchange

The projected token carries a `jti` claim, and Anthropic rejects a `jti` it has
already seen (reason `jti_reused` in the Console's authentication history). The
kubelet rewrites the file when about 80% of `tokenExpirationSeconds` has passed
(600 s by default, the minimum), and the SDK reads the file on every exchange,
so a second exchange inside the same token's life can send a token that was
already used. Whether the SDK's cached access token lasts long enough for that
not to happen has **not** been tested against the real API, and this repo does
not work around it. When a turn fails and looks like an authentication failure
in federation mode, the supervisor logs and records in `status.json`
(`lastError`):

> Anthropic authentication failed in federation mode; check the authentication
> history in the Claude Console (jti_reused / match_*)

The opaque 401 (`Authentication failed`) gives no reason; the reason is only in
the Console history: `jti_reused`, `match_subject_prefix` and the other
`match_*` rules, `workspace_id_required`.

**Limit, stated plainly.** There is no shell and no file tool, so the model
cannot read a mounted token. What bounds the rest is: the model credential has
a spend limit, the core identity has `r` and `w` on one tenant and nothing
else, the connector's own identity is whatever its operator gave it, and the
NetworkPolicy lets egress reach only the model API, the core, the connectors
and the OTLP endpoint. The supervisor's counters are held in memory.

## Boundary

The spot holds no cluster RBAC. A cluster is reached through a connector, whose
own identity is the boundary. The tool layer narrows further: only the tools in
the lists above are offered, and a call to a name that was not offered is
answered with an error and never sent. The model has no web tool, no subagents,
no shell and no file tool, because none exists in `internal/tools`. The prompt
is best effort; the lists and the connector are what hold.

### Identity towards the core

By default (`core.identity: serviceAccount`) the spot authenticates to the core
with its own ServiceAccount: the chart mounts a projected token with audience
`triage-core` (no core Secret), the kubelet rotates it, and the core validates
it by TokenReview. The chart renders a Role and RoleBinding named
`<spot name>-core`, labelled `triageagent.dev/managed: "true"`, that grant the
ServiceAccount `core.verbs` (default get, list, create, update) on the one
tenant; set `core.rbac.create: false` if the grant is made elsewhere. The core
only accepts ServiceAccounts from its own namespace and reads the grants from
that namespace, so in this mode the chart must be installed in the core's
namespace. `core.identity: secret` reads a bearer token from the Secret named
in `core.tokenSecret`, usable from any namespace. A per-spot minted token is
the next step (ADR-098).

## Kill switches

Any one is enough: `spot.enabled=false`; delete the Deployment; delete the
connector's egress or the connector itself (it can read and write nothing in
the cluster); revoke the core token (it can neither read nor speak). The spend
limit on the model credential stops the model regardless of the pod.

## Status

`/state/supervisor/status.json` (world-readable, no secrets) shows:

- `state`: `starting`, `running`, `idle`, `paused-ceiling`, `stopping`,
  `stopped`; when it started and was last updated;
- `lastTurn`: when, why it woke, seconds, billable tokens, model requests
  (`steps`), `toolCalls`, the `model` that answered, the `summary` line the
  model gave to `finish`, `contextTokens` (the conversation size at the last
  request), and the flags `isError`, `stepsExhausted`, `timedOut`,
  `authFailed`, `coreDown`;
- `nextWake`: time, why, source (`agent`, `agent-clamped`, `default-missing`,
  `default-garbled`, `ceiling`) and a note when it was changed;
- `tokensToday` against the ceiling, with cache reads and the turn count;
- `lastError` and `consecutiveFailures`, `coreError`;
- the heartbeat time and id, the edges held for the next wake, the early wakes
  in the last hour, and how the core is watched (`stream` or `poll`).

## Image

The image is built with ko only; there is no Dockerfile and no Docker. It is the
Go binary on `gcr.io/distroless/static`, pinned by digest in `.ko.yaml`: CA
certificates, no shell, no package manager, nothing else. `make image` writes
an OCI layout to `bin/image` and pushes nothing; `make push
REGISTRY=<registry>` pushes. The pod runs as uid 10001 with a read-only root
filesystem and the runtime's default seccomp profile; every write goes to
`/state` or `/tmp`.

## Traces

With `telemetry.enabled` (flag `--otel-endpoint`) the spot exports its own
OpenTelemetry spans over OTLP/HTTP, in batches, flushed at the end of each turn
and at shutdown. The sender's bearer token is a Secret mounted as a file
(`--otel-token-file`), read on every export.

Resource attributes, which ego reads: `service.name`, `gen_ai.agent.name` and
`ego.thread` are the spot's name (every wake is a segment of one thread); the
tenant label is under the attribute `--otel-tenant-attr` (`triage.tenant`, ego's
`--usage-tenant-attr`) with the value `--otel-tenant` (default the spot's name,
so each spot is its own tenant in ego); `ego.tenant.name` is
`--otel-tenant-name` (default the label; ego keeps the first name it sees for a
tenant and drops an invalid one silently, so the spot refuses a name over 80
characters, with control characters, commas or equals signs). The tenant in ego
is `<sender>/<label>`, the sender coming from the token.

Spans, per turn:

- `invoke_agent <name>`: the root, with `gen_ai.operation.name`,
  `gen_ai.conversation.id` (a fresh UUID per turn), `gen_ai.agent.name` and
  `tria.wake.reason`;
- `chat <model>` for each model request: `gen_ai.provider.name=anthropic`,
  the request and response model, `gen_ai.usage.input_tokens` (the uncached
  input), `output_tokens`, `cache_read.input_tokens`,
  `cache_creation.input_tokens`, `gen_ai.response.finish_reasons`;
- `execute_tool <name>` for each tool call: `gen_ai.tool.name`,
  `gen_ai.tool.call.id`, an error status when the tool errored.

Prompts, answers and tool parameters are not added by default.
`--otel-tool-details` adds `gen_ai.tool.call.arguments` and
`gen_ai.tool.call.result`; `--log-content` adds the first prompt as
`gen_ai.input.messages` and the final text as `gen_ai.output.messages` on the
root. The traces carry no spend control: that is the spend limit on the model
credential and the supervisor's `--daily-token-ceiling`.

## Spend

- `--daily-token-ceiling`: input + output + cache-write tokens per UTC day.
  Cache reads are not counted. It pauses all wakes until UTC midnight.
- **No conversation grows.** Every wake starts empty, so the context of a turn
  is what that wake reads. `status.json` shows each turn's `contextTokens`.
- **The core's cap.** The supervisor's ceiling is the spot's own. The core
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
  a row, context size, own token counts, next wake) goes to the core after each
  turn, on a change of trigger mode and every two minutes, for the TUI's
  workspace panel. Counts and labels only, never content.
- There is no dollar ceiling: the spot has no price table. Use the spend limit
  of the model credential.

## Logs

Logs carry what happened, never what was said. At every level the supervisor
and the agent log counts, kinds, ids, tool names, token numbers and timings: an
`edges` line says `kinds="room-message=1" human=true`, not the message. The
text of a person's message or an issue, tool input and the model's words are
content, and reach the log only with `--log-content` (chart: `agent.logContent`)
at debug level. An error the model API or a tool produced is operational and
is logged, because it is what diagnoses a failed login. `--log-level` is
`debug`, `info` (default), `warn` or `error`.

`status.json` `nextWake.why` and `lastTurn.summary` are text the model wrote
itself. They stay in the pod's volume.

## Chart

`charts/tria-spot`: renders nothing unless `spot.enabled=true`. Deployment (1
replica, `Recreate`, read-only root, non-root, all capabilities dropped, the
runtime's seccomp profile), a ServiceAccount, a PVC for `/state` (small: it
only holds the counter, the cursors and the status), an emptyDir for `/tmp`,
the core identity (a projected ServiceAccount token with audience `triage-core`
plus the `<name>-core` Role and RoleBinding, or a Secret mounted as a file, mode
0440 with `fsGroup`; see Identity towards the core), and per `llm.auth`:

- `key` (default): the key Secret from `llm.keySecret`, mounted as a file;
- `federation`: a projected `serviceAccountToken` volume at
  `/var/run/secrets/anthropic.com` (audience `https://api.anthropic.com`,
  `tokenExpirationSeconds` 600; no Secret mounted); `llm.federation.ruleId`,
  `orgId` and `serviceAccountId` are required, `workspaceId` is optional;
- `vertex`: nothing mounted; `llm.vertex.project` is required, `region` defaults
  to `global`; the Workload Identity binding goes in `serviceAccount.annotations`.

`llm.model`, `llm.effort` and `llm.maxSteps` set the model, the effort and the
step cap; `agent.toolTimeout` and `agent.toolResultMaxBytes` set the tool
limits. Resources default to 100m/128Mi requested and 500m/512Mi as the limit.

The NetworkPolicy allows egress to cluster DNS, the model API, the core, the
connectors and, with `telemetry.enabled`, the OTLP endpoint from
`networkPolicy.telemetry`; nothing in. NetworkPolicy has no host names, so for
`key` and `federation` the model API addresses are the CIDRs Anthropic publishes
(`160.79.104.0/23`, `2607:6bc0::/48`, platform.claude.com/docs/en/api/ip-addresses).
For `vertex` they are the Google API ranges you name in
`networkPolicy.vertex.cidrs` (required then; for example the Private Google
Access range), plus the node's metadata server for the Workload Identity token
(`networkPolicy.vertex.metadata`, GKE's addresses by default). The core is
selected by namespace and pod selector or CIDR. The chart fails to render when a
required value is missing.

`make chart-lint` runs `helm lint` and `helm template` with the flag off (must
render nothing) and with every values file in `charts/tria-spot/ci` (placeholders
only): key mode (`enabled-values.yaml`, `key-values.yaml`), federation, Vertex
AI, telemetry, the ServiceAccount identity and connectors. It checks that the pod
has no `env`, that no rule names Secrets, that no ClusterRole renders, that the
ServiceAccount token is not mounted, that nothing in the pod is loosened for a
shell, that each mode mounts and opens only its own, and that a missing required
value or an unknown mode fails to render.

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
- The model is offered only the tools named in `tools`, by exact name, as
  `<name>__<tool>`; the file is refused when a connector names none, uses
  `core` or `ego`, repeats a name, or lists a tool that is not a plain name. A
  tool the connector offers and `tools` does not name is not in the model's
  list, and a call to it is refused.
- `hint` goes into the prompt's Connectors section, and the prompt also lists
  the connector's named tools by exact name, so the spot knows a tool it was
  not given is not "missing". What a connector returns is data, not
  instructions.
- `egress` opens the NetworkPolicy to that connector and nothing else; the
  chart fails to render without it.
- Who the connector trusts is the connector's own business: its identity, its
  RBAC or IAM, and the network. The list is never the boundary: the connector's
  own limits are. A connector is the only way to any cluster: the spot holds no
  cluster RBAC of its own.

## Run it locally

You need a core you can reach and your own credentials in files. Nothing below
is a real name.

```sh
make build
mkdir -p /tmp/spot/state && umask 077
printf '%s' "$CORE_TOKEN" > /tmp/spot/core-token          # a token for a tenant identity
printf '%s' "$ANTHROPIC_KEY" > /tmp/spot/anthropic-key
./bin/tria-spot --tenant <tenant> \
  --core-mcp-url https://<core-host>/mcp \
  --core-token-file /tmp/spot/core-token --anthropic-key-file /tmp/spot/anthropic-key \
  --state-dir /tmp/spot/state \
  --min-wake 2m --daily-token-ceiling 200000 --log-level debug
```

The spot reaches no cluster unless `--connectors-file` names a connector.
`Ctrl-C` stops it gracefully. Check `/tmp/spot/state/supervisor/status.json`.

## Layout

```
cmd/tria-spot            entrypoint: flags, wiring, `version`
internal/config          flags, validation, connectors file
internal/llm             the model client (Anthropic SDK: key, federation, Vertex), a scripted fake
internal/tools           the model's tools: core, ego, connectors over MCP, schedule_next, finish
internal/mcptools        the MCP session the tools use (raw tools/list and tools/call)
internal/agent           one turn: the loop, steps, injection, results
internal/telemetry       spans to ego over OTLP/HTTP
internal/prompt          the system prompt (instruction.md, embedded) and turn prompts
internal/supervisor      the wake loop, edges, schedule, ceilings, heartbeat, status
internal/schedule        the schedule_next request, clamped
internal/triggers        the core's websocket trigger stream
internal/corecli         the supervisor's own MCP client (list_open_issues, heartbeat, rooms)
internal/edges           edge detection between issue snapshots
internal/ledger          /state/supervisor: daily usage (in memory, written through), room cursors, status
charts/tria-spot         Helm chart
docs/adr                 the decision records of this repo
```

## Not verified

No model API credential was used while this runtime was written, so the whole
path from the SDK to a real model is unexercised; the SDK calls are tested
against a local HTTP server and the loop against a scripted fake.

- A full turn against the real API: that adaptive thinking blocks round-trip
  with tool results, how often the one-hour prefix cache is hit across wakes,
  and what the usage numbers look like on a real answer.
- The federation exchange from a pod with a valid rule, service account and
  identity token, how long the exchanged access token lives and whether it is
  reused between requests (this decides whether `jti_reused` shows up), and what
  a rejected exchange looks like in the SDK's error. The supervisor's hint
  matches `Authentication failed`, `authentication_error`, `jti_reused`,
  `token exchange` and `federation` in the error text.
- Vertex AI: the Workload Identity binding, the egress ranges and the model id
  on a real project.
- How ego maps the span attributes; the GenAI conventions are followed, and the
  resource attributes are the ones the receiver is documented to read.
- The core side of phase 3: the reply shape of `read_workspace_room` is read
  loosely (an object with `messages`, or an array; `id`, `sender|from|author`,
  `text|body|message`, `ts|time`). Re-check it when the tool changes.
- The heartbeat against the core: the ADR says `system2-heartbeat-<tenant>`;
  the core today looks only for the global id `system2-heartbeat`
  (`System2HeartbeatCellID`), so until the core reads the per-tenant id, this
  heartbeat does not change reflection. `--heartbeat-id` sets either.
- `list_open_issues` carries a comment count but not who wrote the comments (all
  MCP comments are attributed to `mcp-client`), so every new comment is a
  `human` wake and the agent reads the issue to see who. Machine and spot
  comments made during a turn are absorbed by the post-turn baseline.
- Hop counts and caps are enforced by the core; the prompt only tells the agent
  to pass and respect them.
- Running the image, `helm install`, kubeconform, and `golangci-lint` (not
  installed where this was written; `make lint` runs it when present).
