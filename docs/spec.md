# Spot

The **spot** is an always-on teammate for one **workspace** of the [Triage core](https://triageagent.dev). A **supervisor** wakes a Go **agent loop** in turns; the loop asks a **model** and runs **MCP tools** of the core, ego and connectors. It has no shell, no filesystem tools and no cluster RBAC, and the core has no Kubernetes client.

*Diagram: Architecture: inside one pod a supervisor wakes the agent loop, which calls the model API above and, through its tools, the core, ego and connectors over MCP; spans go to ego over OTLP; the supervisor keeps a state directory and listens to the core.*

*Figure 1. Architecture. The model API is outside the pod; the core, ego and connectors are reached over MCP and hold their own credentials. Only the supervisor reads the core between turns.*

## The loop

A **wake** is why the supervisor starts a **turn**. Every wake is an empty conversation: nothing is resumed, so a model call never pays again for earlier tool output. Continuity is what the spot reads at the start of a wake and the core’s memory (`upsert_memory_item`, `search_memory`). A pod with an empty `/state` loses only the day’s token count and the room cursors. A workspace is one tenant of the core (ADR-095 lives there, ADR-098 in `docs/adr`); ego scores agents from their telemetry; a connector is an MCP server holding its own credentials.

| Term | Meaning |
|---|---|
| wake | Reason: `startup`, `tick` (the agent scheduled it), `edge` (the core changed) or `human` (someone wrote). The wake prompt is the first user message: reason, facts, token budget left. |
| turn | One run of the agent loop: core tools first, then connector reads; verify before any write; record on the issue. |

*Diagram: One turn: wake prompt, model request, tool_use, tool results back to the model, then schedule_next and finish; the limits and failure rules sit under each step.*

*Figure 2. One turn. All results of one answer go back in one user message. The turn ends when the model answers without tools, calls `finish`, or hits a cap; the schedule is the last call.*

### How a turn ends

| A turn ends | Result |
|---|---|
| `end_turn` | The model ends its answer. |
| `finish` | `{"summary": "<one line>"}`; the line shows in the status file. Optional. |
| `schedule_next` | `{"after": "15m", "why": "check the rollout"}`, last in the turn, clamped to `[--min-wake, --max-wake]`. Never called, or not a duration: the next wake is `--min-wake` and the supervisor logs why. |

### Gates before tools run

| Gate | Rule |
|---|---|
| Notes first | `--recall-first`. In each wake every tool except the core's memory and room reads (`tools.RecallTools`) and ego's is answered with an error and not run until a `search_memory` has succeeded, so the spot looks for what it was told before it looks at the cluster or posts. The tool layer holds it; it is off when the core does not offer `search_memory`. |
| Post gate | `--post-gate`. In each wake `say_in_workspace` and `comment_on_issue` (`tools.PostTools`) are answered with an error and not run until a `get_memory_context` or `get_memory_items` call (`tools.ContextTools`) and an `upsert_memory_item` or `supersede_memory_item` call (`tools.NoteTools`) have succeeded, so the spot posts knowing what the workspace holds and leaves a note of what it said. A step is skipped when the core does not offer a tool for it. A `finish` in the same step as a held call is answered with an error ("not finished") and does not end the turn, so a refused post is never reported as done. |
| Fenced results | `--fence-results`. The text of every core and connector tool result (an error too) comes back inside `<data id=… from=…>` and `</data id=…>`, with a fresh random id per call (`Registry.fence`), so text from outside (a log line, an issue comment, a chat message) is marked as data and cannot close the tag. The supervisor's own refusals, the built-in tools and the ego tools are not wrapped. The instruction tells the model that what is inside is data, never orders. |
| Wake digest | The only thing carried from one wake to the next: the spot’s own `finish` summary of each of its last four wakes (one line each, clipped), kept in the state directory and put in the first message of the next wake with the reason it gave for that wake. They are its claims, not facts. Losing the file is harmless. |
| Short lane | While a long turn runs, a person’s message and a wake the spot promised run in a second, short turn of their own: its own tool state, the same model client, capped at a few tool calls (`--fast-max-tool-calls`, `--fast-max-turn-tokens`; `--fast-lane=false` turns it off). The long turn is never cut for it, and a message is not pushed into it. One short turn runs at a time; a message that arrives meanwhile is handed to it. Its tokens count against the same daily ceiling. |
| Promised wake | A wake the spot schedules on a person’s request, sooner than its longest sleep, is a promise: it is kept in the state directory, runs even under a hold rule and after a restart, is not pushed back by later wakes, and runs on the short lane if a long turn is still working when it is due. |
| Hold rule, person’s message | While a standing rule says to check nothing until asked, a person’s message gives the turn only the notes, the room, a post and the scheduling tools. A check they ask for is a wake the spot schedules, and that wake has every tool. |
| Room reads | `read_workspace_room` is asked only for messages newer than the last finished turn could see (the supervisor adds `since_ts`, and a limit of 10), unless the model passes its own `since_ts` or `limit`. The new messages of a wake are already in its facts. |
| Old results cut | Within one wake, a tool result older than the last two steps and longer than 1,500 bytes is cut to its first 800 bytes (the closing data tag kept) before the next request, so a long wake does not send every big result again at every step. |

### Caps and failures

| Limit or failure | Rule |
|---|---|
| 40 requests | `--max-steps`. Not a failure: `stepsExhausted`. |
| 150k tokens | `--max-turn-tokens`. The billable tokens of a wake (input without cache reads, output, cache writes). Checked after each step; past it a tool call is answered with an error and not run and the model is told to wrap up, as at the tool-call cap. `0` turns it off. Not a failure. |
| 20 tool calls | `--max-tool-calls`. Past it a call is answered with an error and not run, and the model is told to answer from what it has, call `schedule_next` and `finish`, which are never refused. A model that goes on calling tools ends the turn. Not a failure. |
| 20m | `--turn-timeout`. Failed; reported as cut off on the next wake. |
| SIGTERM | Cancels the turn, counts the tokens seen, marks it cut off, flushes the spans, writes `status.json`, exits. |
| failure | An API error after the SDK’s two retries, a refusal, an answer cut at the output cap twice, a timeout, no core tools. The next wake backs off from `--min-wake`, doubling to `--max-wake`; edges wait too. A 401 or 403 sets `authFailed`. A failing tool is not a failed turn. |

### Between turns

No model is used. With `--core-api-url` the core pushes content-free frames over a websocket (`/v2/messenger/spot/ws`); the supervisor reads the core after a 1.5 s pause, so a burst is read once. While the stream is down, or without the flag, it polls every `--poll-interval`; `status.json` `triggers` says `stream` or `poll`.

*Diagram: The supervisor between turns: ticks, the trigger stream, issue edges and room messages feed a held-edge queue and a wake gate; a wake that fails before the model sees it is put back; ceilings and per-turn reports sit below.*

*Figure 3. Between turns. The supervisor uses no model. A wake whose turn failed before its first model request goes back into the queue; an edge put back three times is dropped, with a log line naming its kind.*

| Edge | Rule |
|---|---|
| issue change | New issue, reopen, severity change or new comment (`list_open_issues`). Wakes after `--edge-cooldown`. Changes during a turn are held; comment counts are re-baselined, since the agent comments itself. |
| message naming the spot | `@<name>` or the name. Wakes 5 s after the last turn. |
| person’s other message | Waits up to `--chat-batch` (10m). |
| machine message | Never wakes alone; a fact in the next prompt. Hop count 2 or more is dropped. |
| held edges | At most 20; machine messages go first, then the oldest. Past the hourly cap, or inside the backoff, edges wait for the scheduled wake. |
| room cursors | Saved in `/state/supervisor`; a message sent while the pod was down is news on start. A room’s first read only sets the cursor. |
| failed wake | Failed before its first model request (no core tools, rejected credential, timeout): its facts are put back. An edge put back three times is dropped. |
| heartbeat | `upsert_memory_item`, kind `note`, `stale_after` twice the next sleep (at least 5m), refreshed after every successful turn. A failed turn leaves it, so reflection resumes. |
| core down | No core connection or tool list fails the turn (`coreDown`); the model never starts without its tools. |

### A message while a turn runs

With the stream up, a person’s message is handed to the running turn: the model sees it with the next tool results, or before the turn would end. Other messages, and one the model never saw, wait for the next wake.

### Engines

The schedule and the turns run in one of two engines, chosen with `--engine`.

| Engine | What it is |
|---|---|
| `local` (default) | The supervisor in this process owns the timers, the promised wake, the hold rule and the two lanes, and runs the loop. A restart resumes at the next wake; a turn it cuts is reported on the next start. |
| `temporal` | A Temporal workflow per spot owns them, and a child workflow runs each turn. The pod stays the only place that calls the model and the tools; the service holds the schedule and the history. Temporal Cloud or a service of your own: the settings are the same. |

| With `--engine=temporal` | Rule |
|---|---|
| Spot workflow | One long-running workflow per spot (`spot-<name>`). The scheduled wake and the wake a person asked for are durable timers: one that comes due while no pod runs is run by the next. The hold rule, the schedule, the failure backoff and the early-wake handling are the same as in `local` (the supervisor’s `schedule.AfterTurn`, shared). A startup wake is skipped when the schedule survived the restart. The history is carried over (continue as new) when the service suggests it. |
| Lanes | A person’s message runs in the short lane, everything else in the long lane, one wake at a time in each, side by side. What arrives while a lane is busy waits and is one wake when it frees. A wake that is due while the long lane works runs in the short lane. The short lane has the caps of `--fast-max-tool-calls` and `--fast-max-turn-tokens`. |
| Turn | A child workflow runs the same loop as `local` (`agent.RunLoop`); each model request and each tool call is an activity that heartbeats, so a pod that dies is noticed in seconds and the next one resumes the turn at its step. The conversation is the workflow’s history. The instruction and the tool list are not in it: they are the pod’s. |
| At most once | A call that may have an effect (every write, every connector tool) is run once. If its pod dies in the middle, the model is told that the call did not finish and may or may not have run, and decides. A read of the core or of ego is run again, up to three times. |
| Payloads | With `--temporal-payload-key-file` every payload, a failure’s message included, is encrypted with AES-256-GCM in the pod before it leaves it: the service holds ciphertext, plus names, timings and sizes. Over TLS or with an API key the spot does not start without the key unless `--temporal-plain-payloads` says the clear text may go. The key is a file; a payload written with another key is refused. |
| What stays in the pod | The intake (trigger stream, polls, held edges, cooldowns, caps), the standing notes, the prompt, the books (usage ledger, digest, heartbeat, stats, status file), the model client, the tools and the spans. The service never calls the model or a tool and holds no credential of them. |
| Failures | A refused key and a request the model API calls wrong are not retried; other model errors are, up to three times. A turn that fails, or runs past `--turn-timeout`, is a failed wake with the same backoff as in `local`. A tool server that is down is left out of the turn, as in `local`. |
| Cost | Each model request, each tool call, the head of a wake (`Prepare`), its books and the typing indicator are one Temporal action each, and a wake that holds under a rule costs two. Hold, caps and the daily ceiling bound them as before. |

## The model

All model traffic goes through `internal/llm`; this section is the Anthropic provider. The request is built for a prefix cache: sorted tools, then the system prompt with a one-hour `cache_control` breakpoint on its last block, and a short breakpoint on the latest message.

*Diagram: Request layout for the prefix cache: sorted tools and the system prompt form a cached prefix with a one-hour breakpoint; the first user message holds everything volatile; the latest message carries a short breakpoint.*

*Figure 4. Request layout. Nothing volatile is in the prefix, so wakes up to an hour apart read it from the cache (the schedule is 2m to 2h). Cache reads and writes are recorded in the usage and in the spans. Gemini caches the same prefix implicitly.*

| Setting | Rule |
|---|---|
| `--model` | `claude-opus-5-5` when empty; on Vertex AI the same id, no prefix. |
| `--effort` | `low`, `medium` (default), `high`, `xhigh`, `max`; sent as `output_config.effort`. |
| thinking | Not configured: the model thinks adaptively, and its thinking blocks go back with the tool results unchanged. |
| `tool_choice` | Not sent (auto). |
| `max_tokens` | 16000 per request, not streamed. A `max_tokens` stop is answered once with “continue” (a tool call cut in the middle is dropped); a second one fails the turn. |
| `refusal` | Ends the turn as failed with the policy category in the error. |
| `pause_turn` | Resent once. |
| 429, 5xx | Retried twice by the SDK, then the turn fails. |

## Model providers

The loop talks to `llm.Client`, with two implementations: Anthropic (`internal/llm/anthropic.go`) and Gemini (`internal/llm/gemini.go`, Google GenAI Go SDK). `--llm-provider` (chart `llm.provider`) picks one; the default is `anthropic`.

|  | `anthropic` | `gemini` |
|---|---|---|
| Auth modes (`--llm-auth`) | `key`, `federation`, `vertex` | `key`, `vertex`; `federation` is refused |
| Vertex AI location | `--vertex-region` | `--vertex-region` (one flag for both; `global` by default) |
| Retries | SDK, twice (429, 5xx) | SDK, three tries (408, 429, 5xx) |
| Caching | Explicit breakpoints | Implicit on 2.5 and later; explicit `CachedContent` is a follow-up |

| Gemini | Rule |
|---|---|
| key | A file passed to the SDK explicitly. An empty or missing file stops the start, so `GOOGLE_API_KEY` and `GEMINI_API_KEY` are never read. No variable supplies the key, project, location or host (`GOOGLE_GEMINI_BASE_URL` cannot redirect). |
| Vertex AI | Application Default Credentials, the pod’s Workload Identity; no key file. |
| request | One system instruction; one `Tool` with a function declaration per tool, sorted, schema unchanged (`parametersJsonSchema`); tool choice automatic; results as function responses (`{"result": ...}` or `{"error": ...}`). |
| thought signature | Gemini 3 returns one on function calls and needs it back unchanged; kept on the block. A call with no id gets a `call_<n>` id, never sent back. |
| stops | A function call is a tool use. `STOP` ends the turn; `MAX_TOKENS` is the cut-off case. `SAFETY`, `RECITATION`, `BLOCKLIST`, `PROHIBITED_CONTENT`, `SPII` and a blocked prompt are a refusal with the reason as category. |
| models | Gemini 2.0 and earlier reject a thinking config; not every Gemini 3 model accepts every level. |
| chart | `llm.provider`; `llm.model` (empty = the provider’s default, then no `--model` is passed); secrets and Vertex values as in section 16. Egress `networkPolicy.google` is required for `gemini` in any mode and for `anthropic` with `vertex`. |

| `--effort` | `gemini-3*` (thinking level) | other models (thinking budget, tokens) |
|---|---|---|
| `low` | `LOW` | 1024 |
| `medium` | `MEDIUM` | 4096 |
| `high` | `HIGH` | 8192 |
| `xhigh`, `max` | `HIGH` | -1 (dynamic) |

## Tools

The model is offered tools named `<server>__<tool>`, sorted, with the server’s JSON input schema passed through as written. The core’s token is read from `--core-token-file` on every request.

| Group | Tools | Rule |
|---|---|---|
| core, allowed (`tools.CoreTools`) | `get_triage_instructions` `get_tenant_situation` `get_tenant_health` `get_tenant_goals` `list_open_issues` `list_issues` `get_issue_detail` `get_issue_graph` `get_recent_events` `search_memory` `search_corpus` `get_memory` `get_memory_context` `get_memory_items` `get_memory_cell` `fetch_memory` `get_world_context` `get_service_analytics` `get_service_prediction` `inspect_service` `list_services` `list_applications` `list_pattern_proposals` `analyze_issue` `verify_hypothesis` `comment_on_issue` `upsert_memory_item` `supersede_memory_item` `refute_memory_item` `note_observation` `recognize_pattern` `link_pattern` `say_in_workspace` `read_workspace_room` `read_workspace_file` `set_workspace_status` `set_spot_status` | Allowed by exact name. `--core-tools all` offers every tool the core offers: an operator’s choice, bounded by the core’s Role (`core.verbs`). |
| core, denied (`tools.CoreToolsDenied`) | `resolve_issue` `forget_issue` `update_issue_severity` `link_issues` `set_tenant_goals` `run_rsi` `emit_event` `classify_event` `decide_event` `guardrail_event` `test_notify` `test_push` `approve_pattern` `expire_memory_item` `consolidate_memory` `build_memory` `build_knowledge` `build_rca` | Refused by name, even if also in the allow list. |
| ego (`tools.EgoTools`) | `list_metrics` `list_sessions` `summarize_session` `explain_session` `list_runs` `get_run_results` `get_usage` `get_quality` `explain_regression` `get_findings` `list_corrections` | Read-only: sessions, usage, quality, findings. Offered when `--ego-mcp-url` is set. |
| built-in | `schedule_next` `finish` | The spot’s own; no prefix. |
| connectors | the names in the connector’s `tools` | By exact name only. |

### Tool calls

| Tool call | Rule |
|---|---|
| timeout | `--tool-timeout` (60s). |
| result | The text blocks joined; an image or a resource becomes a one-line placeholder. Past `--tool-result-max-bytes` (32768) it is cut at a character boundary and ends with a line saying how many bytes were dropped. |
| error | The server’s `isError`, a transport error, a timeout or an unknown tool goes back to the model as an error result and never aborts the turn. |
| connection | Opened at the start of each turn. A core that cannot be reached fails the turn (`coreDown`). A connector or ego that cannot be reached is logged and left out of that turn, and the wake prompt names it. |

## Names

| Name | Rule |
|---|---|
| `--name` | The deployment’s technical name: Deployment, volume, ServiceAccount, memory-note tags, ego thread. People never see it. |
| `spotName` | Set on the workspace in the core; empty means `<workspace name>-spot`. Read with `get_spot_identity` at start, when the trigger stream reconnects, at most every five minutes while it works, and when a person writes an `@` it does not recognise. |
| `--chat-name` | Fallback for a core that cannot say. |
| wake prompt | Says “In the messenger you appear as ...”. A message naming that name, the technical name or `--chat-name` wakes the spot at once. |

## Live conversation

| Behaviour | Rule |
|---|---|
| typing | During a turn the supervisor shows “typing...” through `set_workspace_typing` (renewed every 20 s, cleared at the end): in the team room for a turn a person started, else in the agents room. Without the tool only the indicator is lost. |
| answer first | The prompt tells the spot to reply before it investigates, to slice long work (report, note the rest, wake itself in two minutes), and to report when done. |
| fast wakes | A message that wakes the spot (it names it, or `--chat-batch` has passed; `0s` = every message) waits only 5 s after the last turn. Lower `--poll-interval` to notice sooner. |
| files | A message that carries a file is marked in the wake; the spot looks at it with `read_workspace_file` (an image comes back as an image). |

## Flags

Flags only. No credential is a flag value or an environment variable. `tria-spot version` prints the build.

### Core and tools

| Flag | Default | Meaning |
|---|---|---|
| `--tenant` | required | tenant this spot works for |
| `--name` | `<tenant>-spot` | messenger and machine identity |
| `--chat-name` | empty | name shown until the core says |
| `--core-mcp-url` | required | the core’s `/mcp` (streamable HTTP) |
| `--core-api-url` | empty | the core’s HTTP API; enables the trigger websocket |
| `--core-token-file` | required | core bearer token file |
| `--core-tools` | `curated` | `curated` or `all` (section 4) |
| `--connectors-file` | none | JSON list of connectors (section 17) |
| `--ego-mcp-url` | empty | ego’s MCP endpoint; uses the `--otel-token-file` token |

### Model

| Flag | Default | Meaning |
|---|---|---|
| `--llm-provider` | `anthropic` | `anthropic` or `gemini` (section 3) |
| `--llm-auth` | `key` | `key`, `federation` (anthropic only) or `vertex` (section 8) |
| `--anthropic-key-file` | required with anthropic and key | Anthropic API key file; rejected with the others |
| `--gemini-key-file` | required with gemini and key | Gemini API key file; rejected with the others |
| `--anthropic-identity-token-file` | required with federation | projected identity token (JWT) |
| `--anthropic-federation-rule-id` | required with federation | federation rule id (`fdrl_...`) |
| `--anthropic-org-id` | required with federation | Anthropic organization id |
| `--anthropic-service-account-id` | required with federation | Anthropic service account id (`svac_...`) |
| `--anthropic-workspace-id` | empty | workspace id (`wrkspc_...`), optional |
| `--vertex-project` | required with vertex | Google Cloud project serving the model |
| `--vertex-region` | `global` | Vertex AI region, either provider |
| `--model` | empty | empty = `claude-opus-5-5` or `gemini-3-flash-preview` |
| `--effort` | `medium` | `low`, `medium`, `high`, `xhigh` or `max` |

### Turns and supervisor

| Flag | Default | Meaning |
|---|---|---|
| `--max-steps` | `40` | model requests per wake |
| `--recall-first` | `true` | hold every tool but the memory and room reads until the spot has searched its notes in the wake |
| `--instruction-cell` | `spot-instruction` | id of the core’s memory cell (kind `spot_system_identity`) that holds the system prompt template; read once at start with `get_memory_cell` and retried for two minutes, after which the spot exits. `make prompt-cell` prints the cell from `prompts/instruction.md` |
| `--post-gate` | `true` | hold say_in_workspace and comment_on_issue until the spot has read the core's memory about its subject and written a note in the wake |
| `--fence-results` | `true` | wrap every core and connector tool result in a data tag with a random id, so text from outside is marked as data and cannot close the tag |
| `--max-turn-tokens` | `150000` | billable tokens per wake (input without cache reads, output, cache writes); past it the model is told to wrap up; 0 = none |
| `--max-tool-calls` | `20` | tool calls per wake; every step sends the whole conversation again, so a long run of calls is what makes a turn expensive |
| `--turn-timeout` | `20m` | wall-clock cap per wake |
| `--tool-timeout` | `60s` | cap on one tool call |
| `--tool-result-max-bytes` | `32768` | cap on one tool result the model sees |
| `--state-dir` | `/state` | the supervisor’s files: usage, cursors, status |
| `--min-wake / --max-wake` | `2m` / `2h` | clamp for the agent’s schedule |
| `--daily-token-ceiling` | `2000000` | tokens per UTC day; `0` = none |
| `--poll-interval` | `30s` | poll interval, while the stream is down |
| `--edge-cooldown` | `1m` | gap between a turn and an edge wake |
| `--chat-batch` | `10m` | wait of a message that does not name the spot; `0` = at once |
| `--max-edge-wakes-per-hour` | `12` | early wakes per hour; `0` = no cap |
| `--heartbeat-id` | `system2-heartbeat-<tenant>` | memory cell id of the heartbeat |

### Engine

| Flag | Default | Meaning |
|---|---|---|
| `--engine` | `local` | `local` or `temporal` (above) |
| `--temporal-address` | required with temporal | the service’s frontend `host:port` |
| `--temporal-namespace` | required with temporal | the namespace |
| `--temporal-task-queue` | `tria-spot-<name>` | one queue per spot: its activities need its own state |
| `--temporal-api-key-file` | empty | Temporal Cloud API key file, read again at each request; turns TLS on |
| `--temporal-tls` | `false` | TLS without an API key |
| `--temporal-tls-cert-file / --temporal-tls-key-file` | empty | client certificate for mutual TLS; both or neither |
| `--temporal-payload-key-file` | empty | 32-byte key (64 hex characters or base64) that encrypts every payload |
| `--temporal-plain-payloads` | `false` | send payloads in the clear over TLS or with an API key; excludes the key file |

### Telemetry and logs

| Flag | Default | Meaning |
|---|---|---|
| `--otel-endpoint` | empty | OTLP/HTTP endpoint; empty = off |
| `--otel-token-file` | empty | sender’s bearer token file |
| `--otel-tenant` | the spot’s name | tenant label in ego |
| `--otel-tenant-attr` | `triage.tenant` | attribute ego reads it from |
| `--otel-tenant-name` | the label | name ego shows |
| `--otel-content` | `true` | content on the spans; `=false` = names, timings, counts |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `--log-content` | off | debug logs carry message, issue, tool input, model text |

## Credentials

`--llm-auth` picks the model auth mode; `federation` is Anthropic only. Federation ids are not secrets, but they are flags.

*Diagram: Credentials: each Secret or projected token becomes a file in the pod or an identity binding, and one client reads each; connector credentials are not in the pod at all.*

*Figure 5. Credentials. Every secret is a file read again on every request, so a rotated Secret needs no restart. Vertex AI mounts nothing: the pod’s Workload Identity answers.*

| Credential | How it flows |
|---|---|
| Key (`key`, default) | A Secret file (`--anthropic-key-file` or `--gemini-key-file` for Gemini), re-read on every request. The SDK is built without its environment defaults, so a stray `ANTHROPIC_*` variable cannot replace the key. |
| Federation (`federation`) | No Anthropic secret. A projected token (audience `https://api.anthropic.com`, at `/var/run/secrets/anthropic.com/token`) is exchanged by the SDK for a short-lived one. The SDK reads federation from the environment, so the process sets `ANTHROPIC_FEDERATION_RULE_ID`, `ANTHROPIC_ORGANIZATION_ID`, `ANTHROPIC_SERVICE_ACCOUNT_ID`, `ANTHROPIC_IDENTITY_TOKEN_FILE` and, if set, `ANTHROPIC_WORKSPACE_ID` from its flags: ids and a path, never a credential. `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_IDENTITY_TOKEN` and `ANTHROPIC_PROFILE` are unset, since each outranks federation. `--anthropic-key-file` is refused. |
| Vertex AI (`vertex`) | Application Default Credentials: the pod’s Workload Identity. Annotate the ServiceAccount (`serviceAccount.annotations`, `iam.gke.io/gcp-service-account`) with a Google service account allowed on Vertex AI in `--vertex-project`. The model id has no prefix. |
| core token | A file read on every request: the pod’s projected token, audience `triage-core` (`core.identity: serviceAccount`), or a Secret (`secret`). |
| pod token | The default ServiceAccount token is not mounted (`automountServiceAccountToken: false`). |

### The identity token must be fresh at every exchange

The projected token carries a `jti` claim, and Anthropic rejects one it has seen (`jti_reused` in the Console’s authentication history). The kubelet rewrites the file at about 80% of `tokenExpirationSeconds` (600 s, the minimum) and the SDK reads it on every exchange, so a second exchange in the same token’s life can send a used token. Whether the SDK’s cached access token lasts long enough is **not** tested against the real API, and not worked around.

| Signal | Content |
|---|---|
| log and `status.json` `lastError` | On an authentication-looking failure in federation mode: “Anthropic authentication failed in federation mode; check the authentication history in the Claude Console (jti_reused / match_*)”. |
| the 401 | `Authentication failed` gives no reason; only the Console history does: `jti_reused`, `match_subject_prefix` and the other `match_*` rules, `workspace_id_required`. |

## Boundary

The spot holds no cluster RBAC; a cluster is reached through a connector, whose own identity is the boundary. The model has no web tool, no subagents, no shell and no file tool, because none exists in `internal/tools`. The model cannot read a mounted token. The supervisor’s counters are held in memory.

*Diagram: Boundary: on the left what the spot has, on the right what it never has, and below what bounds it.*

*Figure 6. Boundary. Only offered tools exist; a call to any other name is answered with an error and never sent. The prompt is best effort; the lists and the connector hold.*

### What the spot cannot do

| Question | Answer |
|---|---|
| Can it run a shell command or a program? | No. There is no shell and no tool that runs a command (`internal/tools` has none). |
| Can it read or write a file? | No. There is no file tool, and the model cannot read a mounted token. |
| Can it fetch a URL or search the web? | No. There is no web tool. |
| Can it start subagents? | No. |
| Can it touch the cluster directly? | No. The spot holds no cluster RBAC. A cluster is reached only through a connector, by the exact tool names the connector offers, and the connector’s own identity is the limit. |
| Can it call any core tool? | Only the tools the install offers. By default the allowed list is `tools.CoreTools`; the destructive ones (`tools.CoreToolsDenied`, for example `resolve_issue`) are refused by name, even when also allowed. A call to any other name is answered with an error and never sent. |
| Can it change its own limits? | No. The counters live in the supervisor’s memory and the model has no tool to write them. |
| Can the prompt widen any of this? | No. The prompt is best effort; the tool lists and the connector hold. |

### Identity towards the core

| `core.identity` | Behaviour |
|---|---|
| `serviceAccount` (default) | The spot’s own ServiceAccount: a projected token, audience `triage-core`, no core Secret, validated by TokenReview. The chart renders a Role and RoleBinding `<spot name>-core`, labelled `triageagent.dev/managed: "true"`, granting `core.verbs` (default get, list, create, update) on the one tenant; `core.rbac.create: false` if the grant is made elsewhere. The core accepts only ServiceAccounts from its own namespace, so the chart is installed there. |
| `secret` | A bearer token from the Secret `core.tokenSecret`, usable from any namespace. |
| next | A per-spot minted token (ADR-098). |

## Kill switches

Any one is enough.

| Switch | Effect |
|---|---|
| `spot.enabled=false` | The chart renders nothing. |
| delete the Deployment | The pod stops. |
| delete the connector’s egress, or the connector | The spot can read and write nothing in the cluster. |
| revoke the core token | The spot can neither read nor speak. |
| spend limit on the model credential | Stops the model regardless of the pod. |

## Status

`/state/supervisor/status.json` is world-readable and holds no secrets.

| Field | Content |
|---|---|
| `state` | `starting`, `running`, `idle`, `paused-ceiling`, `stopping`, `stopped`; when it started and was last updated. |
| `lastTurn` | When, why, seconds, billable tokens, `steps` (model requests), `toolCalls`, `model`, the `finish` `summary`, `contextTokens` (conversation size at the last request), and the flags `isError`, `stepsExhausted`, `timedOut`, `authFailed`, `coreDown`. |
| `nextWake` | Time, why, source (`agent`, `agent-clamped`, `default-missing`, `default-garbled`, `ceiling`) and a note when it was changed. |
| `tokensToday` | Against the ceiling, with cache reads and the turn count. |
| `lastError`, `consecutiveFailures`, `coreError` | The failure state. |
| other | Heartbeat time and id, edges held, early wakes in the last hour, how the core is watched (`stream` or `poll`). |

## Image

| Item | Value |
|---|---|
| build | ko only; no Dockerfile and no Docker. |
| base | `gcr.io/distroless/static` pinned by digest in `.ko.yaml`: CA certificates only, no shell, no package manager. |
| commands | `make image` writes an OCI layout to `bin/image` and pushes nothing; `make push REGISTRY=<registry>` pushes. |
| runtime | uid 10001, read-only root filesystem, the runtime’s default seccomp profile; every write goes to `/state` or `/tmp`. |

## Traces

With `telemetry.enabled` (`--otel-endpoint`) the spot exports OpenTelemetry spans over OTLP/HTTP, in batches. The bearer token is a Secret file (`--otel-token-file`), read on every export. The tenant in ego is `<sender>/<label>`, the sender coming from the token.

*Diagram: Spans of one turn: the invoke_agent root with a chat span for each model request and an execute_tool span for each tool call, and the limits on attribute content.*

*Figure 7. Spans of one turn, exported over OTLP/HTTP and flushed at the end of each turn and at shutdown.*

### Resource attributes

| Attribute | Value |
|---|---|
| `service.name`, `gen_ai.agent.name`, `ego.thread` | The spot’s name; every wake is a segment of one thread. |
| `--otel-tenant-attr` (`triage.tenant`, ego’s `--usage-tenant-attr`) | The tenant label, with the value `--otel-tenant` (default the spot’s name, so each spot is its own tenant in ego). |
| `ego.tenant.name` | `--otel-tenant-name` (default the label). Ego keeps the first name it sees and drops an invalid one silently, so the spot refuses a name over 80 characters, or with control characters, commas or equals signs. |

### Spans

| Span | Attributes | With content |
|---|---|---|
| `invoke_agent <name>` | `gen_ai.operation.name`, `gen_ai.conversation.id` (fresh UUID per turn), `gen_ai.agent.name`, `tria.wake.reason` | `gen_ai.input.messages` (the wake prompt) and `gen_ai.output.messages` (the final text). |
| `chat <model>` | `gen_ai.provider.name` (`anthropic` or `gemini`), request and response model, `gen_ai.usage.input_tokens` (whole prompt: uncached input plus cache reads plus cache writes, as ego counts a call), `output_tokens`, `cache_read.input_tokens` and `cache_creation.input_tokens` (the split ego prices with), `gen_ai.response.finish_reasons` | `gen_ai.input.messages`: the request’s messages as a JSON array in the GenAI shape (`role`, `parts` of `text`, `tool_call`, `tool_call_response`; thinking left out); `gen_ai.output.messages`. The first `chat` of a turn also has `gen_ai.system_instructions`. |
| `execute_tool <name>` | `gen_ai.tool.name`, `gen_ai.tool.call.id`, an error status when the tool errored | `gen_ai.tool.call.arguments` (a JSON object string) and `gen_ai.tool.call.result` (`{"result": <text>, "isError": <bool>}` as a string). |

**Content.** By default (`telemetry.content: true`, `--otel-content`) the spans carry prompts, answers and every tool call’s arguments and result, because ego cannot evaluate a turn without them. With `telemetry.content: false` only names, timings and token counts go out and ego reports a turn it cannot score; use it when the workspace’s data must not leave the pod. A long string is cut in the middle with a marker. `--log-content` governs only the process log. Traces carry no spend control.

## Spend

| Control | Rule |
|---|---|
| `--daily-token-ceiling` | Input plus output plus cache-write tokens per UTC day (cache reads not counted), held in memory and written through to `usage.json`, read once at start. At the ceiling no wake starts, edges are held, the loop sleeps to UTC midnight. A turn in flight can overshoot by one turn (bounded by `--max-steps`, `--turn-timeout`). |
| context | Every wake starts empty; `status.json` shows each turn’s `contextTokens`. |
| the core’s cap | Policy `budgets.spotTenantDailyTokens` over `budgets.spotDailyDefaultTokens`: new input plus output per workspace per UTC day. After every turn a `turn` frame goes up the trigger stream (model, input incl. cache reads and writes, cached, output, duration, failed); the core books it as usage kind `spot` (TUI usage panel) and answers with the budget: the workspace’s `spent` and `cap`, and `own`, what this spot spent as the core counted its turns, which the spot takes as the floor of its own daily counter (so a lost state directory does not give the day back). Over the cap it refuses `say_in_workspace` and `set_workspace_status`, and the supervisor pauses all wakes until the budget lifts or UTC midnight. Frames sent while the stream is down wait (the last 100 are kept). |
| `stats` frame | State, turns and wakes by reason today, failures in a row, context size, own token counts and ceiling, why it is paused (its own ceiling or the workspace cap), its limits and choices (model, step, tool-call and token caps, wake timing, core-tools mode, gates, connector names with their tool names; never a URL, path or credential), next wake. After each turn, on a trigger-mode change and every two minutes, for the TUI’s workspace panel. Counts and labels only. |
| dollars | No ceiling: the spot has no price table. The spend limit on the Anthropic workspace or Google project that serves the model is the real stop. |

## Logs

Logs carry what happened, never what was said.

| Kind | Rule |
|---|---|
| always | Counts, kinds, ids, tool names, token numbers and timings, at every level: an `edges` line says `kinds="room-message=1" human=true`, not the message. An error from the model API or a tool is operational and is logged, because it diagnoses a failed login. |
| content | A person’s message or issue text, tool input and the model’s words reach the log only with `--log-content` (chart `agent.logContent`) at debug level. |
| level | `--log-level`: `debug`, `info` (default), `warn`, `error`. |
| model text | `status.json` `nextWake.why` and `lastTurn.summary` are written by the model; they stay in the pod’s volume. |

## Chart

`charts/tria-spot` renders nothing unless `spot.enabled=true`. The chart fails to render when a required value is missing.

| Part | Content |
|---|---|
| workload | Deployment, 1 replica, `Recreate`, read-only root, non-root, all capabilities dropped, runtime seccomp; a ServiceAccount; 100m/128Mi requested, 500m/512Mi limit. |
| storage | A small PVC for `/state`; an emptyDir for `/tmp`. |
| core identity | A projected token with audience `triage-core` plus the `<name>-core` Role and RoleBinding, or a Secret mounted as a file, mode 0440 with `fsGroup` (section 9). |
| `llm.auth: key` (default) | The Secret from `llm.keySecret` (anthropic) or `llm.geminiKeySecret` (gemini, at `/secrets/gemini/<key>`), as a file. |
| `llm.auth: vertex` | Nothing mounted. `llm.vertex.project` required, `region` defaults to `global`; the binding goes in `serviceAccount.annotations`. |
| `llm.auth: federation` | A projected token volume at `/var/run/secrets/anthropic.com` (`tokenExpirationSeconds` 600; no Secret). `llm.federation.ruleId`, `orgId` and `serviceAccountId` are required, `workspaceId` is optional. Refused with `gemini`. |
| `engine.type: temporal` | `engine.temporal.address` and `namespace` required. `apiKeySecret` (Temporal Cloud) and `payloadKeySecret` are Secrets mounted as files, at `/secrets/temporal/api` and `/secrets/temporal/payload`. Read with `dig`, so a release upgraded with `--reuse-values` renders as `local` until the values are set. |
| model values | `llm.provider` (`anthropic` default, `gemini`), `llm.model` (empty = the provider’s default, no `--model` passed), `llm.effort`, `llm.maxSteps`, `llm.maxToolCalls`, `llm.maxTurnTokens`, `agent.toolTimeout`, `agent.toolResultMaxBytes`. |

### NetworkPolicy

Egress only; nothing in. It allows cluster DNS, the model API, the core, the connectors and, with `telemetry.enabled`, the OTLP endpoint from `networkPolicy.telemetry`. The model API is addressed by CIDR.

| Destination | Addressed by |
|---|---|
| `anthropic` with `key` or `federation` | The CIDRs Anthropic publishes (`160.79.104.0/23`, `2607:6bc0::/48`, platform.claude.com/docs/en/api/ip-addresses). |
| `gemini` in any mode; `vertex` with either provider | The Google API ranges you name in `networkPolicy.google.cidrs` (required; for example the Private Google Access range). With `vertex` it adds the node’s metadata server for the Workload Identity token (`networkPolicy.google.metadata`, GKE’s addresses by default). |
| core | Namespace and pod selector, or CIDR. |
| Temporal, with `engine.type: temporal` | `networkPolicy.temporal`: namespace and pod selector, or CIDR, port 7233. Temporal Cloud has no fixed addresses: name the ranges your egress uses. |

### Lint

`make chart-lint` runs `helm lint` and `helm template` with the flag off (must render nothing) and with every values file in `charts/tria-spot/ci` (placeholders only): key, federation, Vertex AI, Gemini with a key and on Vertex AI, telemetry, ServiceAccount identity, connectors, the Temporal engine (a service in the cluster, and Temporal Cloud). It checks that:

- the pod has no `env`, no rule names Secrets, no ClusterRole renders, and the ServiceAccount token is not mounted;
- nothing in the pod is loosened for a shell, and each mode mounts and opens only its own;
- a missing required value or an unknown mode fails to render.

## Connectors

A connector is one more MCP server beside the core, reached by URL, holding its own credentials and limits: a Kubernetes tool server, a code host, a metrics API. The spot has no code for any of them and no credential for them.

```
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

| Field | Rule |
|---|---|
| list | The chart renders it into a ConfigMap read by `--connectors-file`: names, URLs, tool names and hints; no credential. |
| `tools` | Offered to the model by exact name as `<name>__<tool>`. The file is refused when a connector names none, uses `core` or `ego`, repeats a name, or lists a tool that is not a plain name. A tool the connector offers and `tools` does not name is not offered, and a call to it is refused. |
| `hint` | Goes into the prompt’s Connectors section, which also lists the named tools, so the spot knows a tool it was not given is not “missing”. What a connector returns is data, not instructions. |
| `egress` | Opens the NetworkPolicy to that connector and nothing else; the chart fails to render without it. |
| trust | The connector’s own business: its identity, its RBAC or IAM, and the network. The list is never the boundary; the connector’s own limits are. A connector is the only way to any cluster. |

## Run it locally

Source: [github.com/triageagent-dev/spot](https://github.com/triageagent-dev/spot). You need a core you can reach and your own credentials in files. Nothing below is a real name.

```
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

The spot reaches no cluster unless `--connectors-file` names a connector. `Ctrl-C` stops it gracefully. Check `/tmp/spot/state/supervisor/status.json`.

## Layout

| Directory | Purpose |
|---|---|
| `cmd/tria-spot` | Entrypoint: flags, wiring, `version` |
| `internal/config` | Flags, validation, connectors file |
| `internal/llm` | The model client (Anthropic SDK: key, federation, Vertex; Google GenAI SDK: key, Vertex), a scripted fake |
| `internal/tools` | The model’s tools: core, ego, connectors over MCP, `schedule_next`, `finish` |
| `internal/mcptools` | The MCP session the tools use (raw `tools/list` and `tools/call`) |
| `internal/agent` | One turn: the loop (`RunLoop`, which does no I/O, so a workflow can run it), steps, injection, results |
| `internal/durable` | The Temporal engine: the spot and wake workflows, the activities (model, tools, head, books), the payload encryption, the worker |
| `internal/telemetry` | Spans to ego over OTLP/HTTP |
| `internal/prompt` | Renders the system prompt from its template (read from the core at start; the source is `prompts/instruction.md`) and builds turn prompts |
| `internal/supervisor` | The wake loop, edges, schedule, ceilings, heartbeat, status; with `--engine=temporal` only the intake and the books (`bridge.go`) |
| `internal/schedule` | The `schedule_next` request, clamped; the next wake and the promise after a turn (both engines) |
| `internal/triggers` | The core’s websocket trigger stream |
| `internal/corecli` | The supervisor’s own MCP client (`list_open_issues`, heartbeat, rooms) |
| `internal/edges` | Edge detection between issue snapshots |
| `internal/ledger` | `/state/supervisor`: daily usage (in memory, written through), room cursors, status |
| `charts/tria-spot` | Helm chart |
| `docs/adr` | The decision records of this repo |

## Verified and not verified

Run on a live install: Gemini 3 on Vertex AI with Workload Identity, a real core and one Kubernetes connector. That covers full turns with parallel tool calls, cache reads across wakes (`read=` in the turn log), the Workload Identity binding, ego spans for every wake, `read_workspace_room`, and the Helm install and rollout of the image. The Anthropic paths below have not been run against a live deployment.

| Area | Not verified |
|---|---|
| full turn, Anthropic | Against the real API: adaptive thinking blocks round-tripping with tool results, one-hour prefix cache hits across wakes, real usage numbers. |
| federation | The exchange from a pod with a valid rule, service account and token; how long the access token lives and whether it is reused (this decides whether `jti_reused` shows up); the SDK’s error on a rejection. The hint matches `Authentication failed`, `authentication_error`, `jti_reused`, `token exchange` and `federation` in the error text. |
| Vertex AI, Anthropic models | The egress ranges and the model id for Claude on a real project. |
| `read_workspace_room` | The reply shape is read loosely (`messages` object or array; `id`, `sender\|from\|author`, `text\|body\|message`, `ts\|time`); re-check when the tool changes. |
| heartbeat | The ADR says `system2-heartbeat-<tenant>`; the core looks only for `system2-heartbeat`, so until it reads the per-tenant id this heartbeat does not change reflection. `--heartbeat-id` sets either. |
| comments | `list_open_issues` gives a comment count, not the author (all MCP comments are `mcp-client`), so every new comment is a `human` wake and the agent reads the issue to see who. Comments made during a turn are absorbed by the post-turn baseline. |
| hop counts | Hop counts and caps are enforced by the core; the prompt only tells the agent to pass and respect them. |
| post gate | Seen live: a post refused, then released after a memory read, in a wake that then posted. Also seen live: a `finish` in the same step as a refused post ended the turn with no post; the fix runs in tests only. |
| Temporal engine | Tests run against Temporal’s local dev server with the real tool layer and a scripted model: a message beside a long turn, a promised wake across a worker restart, a hold rule, a crash in a turn, a write cut by a crash (reported, not repeated), a paused spot, and a history with no clear text. Not verified: a live install with a real model, Temporal Cloud, and a service of your own in a cluster; the cost in actions of a real day. |
| tooling | `kubeconform`, and `golangci-lint` (`make lint` runs it when present). |
