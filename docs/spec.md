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

No model is used. With `--core-api-url` the core pushes content-free frames over a websocket (`/v2/messenger/spot/ws` with a core token; `/v2/messenger/spot/join` with a certificate, section 9); the supervisor reads the core after a 1.5 s pause, so a burst is read once. While the stream is down, or without the flag, it polls every `--poll-interval`; `status.json` `triggers` says `stream` or `poll`.

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
| core down | No core connection or tool list fails the turn (`coreDown`); the model never starts without its tools. The supervisor’s own client of the core waits after a failed connection (1 s, doubling to 5 min) and says so once a minute; when the core refuses the token it sends nothing more until the token file holds another one. |

### A message while a turn runs

With the stream up, a person’s message is handed to the running turn: the model sees it with the next tool results, or before the turn would end. Other messages, and one the model never saw, wait for the next wake.

### Who wakes the spot

The core keeps the spot’s schedule (the core’s ADR-106): the spot decides what to do at a wake and plans the next one, and the core remembers the plan and sends the wake when it is due. The supervisor in this process is the executor: it runs the turns, the two lanes and the tasks.

| Part | Rule |
|---|---|
| Plan reported | At the end of every wake of the long or the short lane, the `wake` frame carries the plan as it stands: `next` (ms), `nextSource`, `nextWhy` and `promise` (`{at, why, human}`; absent means none). The core replaces its record with it. A task’s wake (it has a `key`) never touches the plan. After the core’s `hello` the spot sends one `schedule` frame with `next`, `nextWhy`, `nextSource`, `promise`, `backoffMs` (the wait a failed wake left) and `maxWakeMs`, so a core that restarted gets the plan back and a spot that restarted hands over what it kept (a promise a person asked for is on disk). |
| The core sends the tick | With the stream up the supervisor keeps no timer for the plan. When the planned wake or the promise is due, the core sends `{"wake":{"reason","why"}}` (`reason` is `tick` or `promise`, `why` the spot’s own words, 200 bytes at most) and the spot starts a wake of the long lane with that reason. A tick for a time more than 30 s ahead of the spot’s own plan (the spot ran a wake meanwhile), a promise the spot no longer holds and a reason it does not know are dropped. The core keeps one wake outstanding per spot until its report arrives. |
| The net | Without the stream (poll mode) the spot wakes itself at the planned time. With the stream up and no `wake` frame, it wakes itself once `--max-wake` after its last wake (a wake planned later, at a ceiling, is not hurried). A spot never sleeps for ever because the core is away or has no scheduler. |
| What the spot keeps | The hold rule, the edges and their cooldowns, the two lanes, the short-lane wake on a person’s message, the caps and ceilings, the quiet-wake skip, the standing notes, the prompt, the books (usage ledger, digest, heartbeat, stats, status file), the model client, the tools and the spans. |
| Lanes | A person’s message and a wake a person asked for run in a short turn of their own while a long turn works (`--fast-lane`, on by default), with the caps of `--fast-max-tool-calls` and `--fast-max-turn-tokens`; a message that arrives while the short turn works is handed to it. Everything else runs in the long lane, one wake at a time. |
| A wake asked for | A person can ask for a wake from the console (the `wake` control). It is a wake of the long lane with the reason `manual`, or of the short lane while the long one works. It runs under a hold rule like a person’s message, with every tool, is never skipped as quiet, and does not fire the promise. |
| Tasks | `start_task {"title", "goal"}` (up to 3 per turn; not offered under a hold rule or with `--max-tasks 0`) starts a task when the turn that asked ends: a turn of its own in the task lane, run beside the other lanes, with a new conversation that holds only the goal, the standing notes and the digest, the usual caps, the same tools and no `schedule_next` or `start_task` (the tool list is the same as for any turn, so the cached prefix is too; the call is refused). Up to `--max-tasks` (3) run at once, each with its own tool connections and gates; up to 10 more wait for a place and a task past that is dropped. A task changes no schedule and no promise. When it ends, its one-line result is a digest entry (`task <title>`) that the next wake reads, and the core gets a line of activity for it. A task posts to the room itself when its goal says so. Under a hold rule or at a ceiling a task does not run. An operator can list, stop, kill, restart and drop what the spot has in hand through the core (the `control` frame). |
| Restart | A restart resumes at the next wake; a turn it cuts is reported on the next start and redone from its first step. The caps bound what a redo costs, and the spot’s writes to the core are upserts and comments, so repeating one is safe. A task a restart cuts is not resumed. The record of what each model step did is the span ego holds. |
| Failures | A turn that fails, or runs past `--turn-timeout`, is a failed wake with a backoff of `--min-wake`, doubling to `--max-wake`; the backoff is in the `schedule` frame. A tool server that is down is left out of the turn. |

### Runtimes

`--runtime deployment` (the default) is a pod with a websocket to the core. `--runtime substrate` runs the spot as an actor of Agent Substrate (the core’s ADR-107): the platform suspends an idle actor to a snapshot and resumes it on a request, so the spot holds nothing between wakes but what is on `/state`. The supervisor, the lanes, the tools and the prompt are the same.

| Part | Rule |
|---|---|
| The process | One binary, one image. It serves HTTP on `--listen` (`:80`): `GET /readyz`, which answers 200 once the spot has joined the core and said hello (the actor’s wake-up probe), and `POST /frame`. The snapshot keeps only the volumes, so every wake starts a new process. There is no startup wake and no timer of its own: the core’s schedule (ADR-106) is the only clock, and an order from the core is taken as it is, because a new process holds neither the plan nor the promise. |
| Link to the core | Requests, no websocket (the egress gateway refuses every upgrade). `POST /v2/messenger/spot/link` carries `challenge`, `join`, `renew` and `hello` (the certificate join of section 9, with `runtime: substrate` in the hello); `POST /v2/messenger/spot/link/report` carries the reports, with the bearer token hello returned. The core does not renew that token (30 minutes): the spot says hello again when under 3 minutes are left, and after any 401 on a report. |
| The core’s requests | `POST /frame`, one frame per request, the shapes of the stream. `wake` (`tick` or `promise`), `rooms`, `name` and `budget` are answered 202 at once; `control` is answered 200 with `{"controlResult"}` within 8 s, and the `wake` control asks for a manual wake. The core sends an actor no `issues` frame, so a change of the issues waits for the next tick. |
| Reports | Kept until the supervisor is idle (no turn, no task, nothing waiting), then sent in this order: turns, stats, the wake reports last. The core may suspend the actor once it has the wake report, so nothing follows it. A frame that made no wake (the rooms had nothing new) is answered with `{"wake":{"reason":"rooms","lane":"long","outcome":"idle"}}`, which ends the outstanding wake and leaves the stored plan. The `schedule` frame follows hello only when the process holds a plan, because one without would replace the core’s. |
| What survives | `/state`, the actor’s durable directory: the usage ledger, the room cursors and read marks, the digest, a promise a person asked for, the identity, and `window.json`: the tokens of the last hour (the hourly ceiling), the early wakes of the last hour (their cap) and the issue state edges are told against. Held edges do not survive a suspension, so `--edge-cooldown` and `--chat-batch` are 0 in this runtime and the next tick reads whatever a cap left. |
| Model | `--llm-auth injected`: the Anthropic client sends a fixed placeholder `x-api-key` to `https://api.anthropic.com` (or `--llm-base-url`) and the environment is not read. The egress policy of the actor has a rule for that host whose effect replaces the header with the key of a Secret (`ate-secret://…`), and only a request that carries the header is changed; the spot never holds the key. Anthropic provider only. The other auth modes stay for the deployment runtime. |
| No credential | The actor holds no model key, no token file and no enrollment key, and mounts no Secret or ConfigMap: connectors are given inline (`--connectors-json`), what ego checks on a request is put there by the egress policy, and a first join waits for a person to approve the spot’s fingerprint. The one thing it keeps is its own key and certificate on `/state`. |
| Chart | `runtime: substrate` renders two documents of the substrate API and no Kubernetes object: an ActorTemplate (the image by digest, the command and flags, `/state` as a durable directory, the wake-up probe, the volume snapshot, the limits) and an EgressPolicy with one rule per destination the spot uses, the one for the model API replacing `x-api-key` with the key of a Secret (http for in-cluster names, https for the rest, hostnames only; `substrate.egress.extraRules` adds more). Apply them with the substrate CLI. The chart refuses a model auth other than injected, a core identity other than certificate, a missing image digest and a missing model-key Secret. |

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
| Auth modes (`--llm-auth`) | `key`, `federation`, `vertex`, `injected` | `key`, `vertex`; `federation` and `injected` are refused |
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
| built-in | `schedule_next` `finish` `start_task` | The spot’s own; no prefix. `start_task` is offered with `--max-tasks` above 0, and not under a hold rule. |
| connectors | the names in the connector’s `tools` | By exact name only. |

### Tool calls

| Tool call | Rule |
|---|---|
| timeout | `--tool-timeout` (60s). |
| result | The text blocks joined; an image or a resource becomes a one-line placeholder. Past `--tool-result-max-bytes` (12288) it is cut at a character boundary and ends with a line saying how many bytes were dropped and asking the model for less (a name, a namespace, a limit). A server prefix on a built-in (`core__schedule_next`) is the built-in. |
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
| `--core-token-file` | required without `--core-ca-fingerprint` | core bearer token file; refused with `--core-ca-fingerprint`, where the token file is the spot’s own |
| `--core-ca-fingerprint` | empty | `SHA256:<64 lowercase hex>` of the core’s CA. Set, the spot joins the core by certificate (section 9); needs `--core-api-url` |
| `--enroll-key-file` | empty | file holding the one-time enrollment key the core minted for this spot; only with `--core-ca-fingerprint` |
| `--identity-dir` | `<state-dir>/identity` | the spot’s key, certificate and access token; only with `--core-ca-fingerprint` |
| `--core-tools` | `curated` | `curated` or `all` (section 4) |
| `--connectors-file` | none | JSON list of connectors (section 17) |
| `--connectors-json` | none | the same list inline, for a runtime with no file to read (the substrate actor); not with `--connectors-file` |
| `--runtime` | `deployment` | `deployment` or `substrate` (Runtimes, in section 1) |
| `--listen` | `:80` | substrate: the address of `/readyz` and the core’s requests |
| `--ego-mcp-url` | empty | ego’s MCP endpoint; uses the `--otel-token-file` token |

### Model

| Flag | Default | Meaning |
|---|---|---|
| `--llm-provider` | `anthropic` | `anthropic` or `gemini` (section 3) |
| `--llm-auth` | `key` | `key`, `federation` (anthropic only), `vertex` or `injected` (anthropic only; section 8) |
| `--llm-base-url` | empty | injected: the API’s URL when it is not `https://api.anthropic.com`; only with `--llm-auth=injected` |
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
| `--tool-result-max-bytes` | `12288` | cap on one tool result the model sees |
| `--state-dir` | `/state` | the supervisor’s files: usage, cursors, status; with a certificate identity also the identity directory |
| `--min-wake / --max-wake` | `2m` / `2h` | clamp for the agent’s schedule |
| `--daily-token-ceiling` | `2000000` | tokens per UTC day; `0` = none |
| `--hourly-token-ceiling` | `600000` | tokens in any rolling hour; `0` = none. At the ceiling no wake starts until the oldest turns of the hour have aged out; the hour’s samples are in memory (a restart starts it empty, the daily ceiling still holds) |
| `--skip-quiet-wakes` | `true` | a startup or timer wake with no facts, no held edge and no open issue in the core ends without a model call, unless a person or the agent itself asked for that wake; the next wake is the longest the spot allows, or an event |
| `--poll-interval` | `30s` | poll interval, while the stream is down |
| `--edge-cooldown` | `1m` | gap between a turn and an edge wake |
| `--chat-batch` | `10m` | wait of a message that does not name the spot; `0` = at once |
| `--max-edge-wakes-per-hour` | `12` | early wakes per hour; `0` = no cap |
| `--heartbeat-id` | `system2-heartbeat-<tenant>` | memory cell id of the heartbeat |

### Tasks

| Flag | Default | Meaning |
|---|---|---|
| `--max-tasks` | `3` | how many tasks (`start_task`) run at once, each a turn of its own; 0 takes the tool away; 10 at most |

`--engine` and every `--temporal-*` flag are gone: the spot refuses to start with one, and the message names the core’s ADR-106.

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

`--llm-auth` picks the model auth mode; `federation` and `injected` are Anthropic only. With `injected` the spot holds no key: it sends a placeholder, and the egress in front of the API replaces it with the real one. Federation ids are not secrets, but they are flags.

*Diagram: Credentials: each Secret or projected token becomes a file in the pod or an identity binding, and one client reads each; connector credentials are not in the pod at all.*

*Figure 5. Credentials. Every secret is a file read again on every request, so a rotated Secret needs no restart. Vertex AI mounts nothing: the pod’s Workload Identity answers.*

| Credential | How it flows |
|---|---|
| Key (`key`, default) | A Secret file (`--anthropic-key-file` or `--gemini-key-file` for Gemini), re-read on every request. The SDK is built without its environment defaults, so a stray `ANTHROPIC_*` variable cannot replace the key. |
| Federation (`federation`) | No Anthropic secret. A projected token (audience `https://api.anthropic.com`, at `/var/run/secrets/anthropic.com/token`) is exchanged by the SDK for a short-lived one. The SDK reads federation from the environment, so the process sets `ANTHROPIC_FEDERATION_RULE_ID`, `ANTHROPIC_ORGANIZATION_ID`, `ANTHROPIC_SERVICE_ACCOUNT_ID`, `ANTHROPIC_IDENTITY_TOKEN_FILE` and, if set, `ANTHROPIC_WORKSPACE_ID` from its flags: ids and a path, never a credential. `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_IDENTITY_TOKEN` and `ANTHROPIC_PROFILE` are unset, since each outranks federation. `--anthropic-key-file` is refused. |
| Vertex AI (`vertex`) | Application Default Credentials: the pod’s Workload Identity. Annotate the ServiceAccount (`serviceAccount.annotations`, `iam.gke.io/gcp-service-account`) with a Google service account allowed on Vertex AI in `--vertex-project`. The model id has no prefix. This annotation belongs to `vertex` only; `key` and `federation` need none, and the core identity never needs one. |
| core token | A file read on every request: the pod’s projected token, audience `triage-core` (`core.identity: serviceAccount`), a Secret (`secret`), or, with `certificate`, the access token the core sends over the stream and the spot writes to `<identity-dir>/token` (section 9). |
| enrollment key | Certificate identity only. A Secret mounted as a file (`core.enrollKeySecret`, `--enroll-key-file`), read at each join and sent only while the spot has no certificate. It is a one-time key: the first join spends it. |
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
| `certificate` | The spot joins the core the way an agent does (the core’s ADR-105) and holds no static credential: no core Secret for a token, no projected token, no Role. It needs `core.apiUrl` and `core.caFingerprint` (the core’s CA) and, for its first join, a one-time enrollment key (`core.enrollKeySecret`). The mechanics are below. The spot’s name is its identity: keep `spot.name` stable. Losing `/state` loses the key, so the core needs a new enrollment key for the spot. |

### Certificate join

| Part | Rule |
|---|---|
| state | In `--identity-dir` (`/state/identity`), mode 0600: `key.pem` (an Ed25519 key made on the first start, never sent), `cert.der` (the issued certificate), `ca.der` (the core’s CA) and `token` (the access token). Files are written atomically. |
| route | A websocket to `GET /v2/messenger/spot/join` on `--core-api-url` (`ws://` for `http://`, `wss://` for `https://`), with the header `X-Triage-Agent-Nonce`: base64 of 32 random bytes. The same socket becomes the trigger stream (section 14) once the spot has said hello. |
| trust in the core | The core opens with a `challenge` frame (its CA, a nonce, a signature over both nonces). The spot checks the CA against `--core-ca-fingerprint` and the signature before it sends anything; a core that does not match is not talked to. On `wss://` the challenge is bound to the TLS session; on `ws://` in the cluster it is bound to the spot’s nonce. |
| first join | The spot sends a `join` frame: a certificate request (Ed25519 key, common name `spot:<name>`, URIs `triage:spot/<name>` and `triage:tenant/<workspace>`), a proof (the key signs the core’s nonce and the request’s hash) and the enrollment key from `--enroll-key-file`. The key is sent only while the spot has no certificate. The core answers `join_result` (`issued` with the certificate, `pending` or `refused` with a message) and closes; on `issued` the spot checks the certificate against its key, name and workspace, keeps it and connects again. `pending` is retried with the stream’s backoff, and the spot logs its fingerprint. `refused` is logged with the core’s message and retried the same way. A join without a key the core minted for that name and workspace is refused. |
| enrollment key | Minted by a workspace admin in the core, for the spot’s name (`POST /v2/tenants/{workspace}/keys` with kind `spot`); valid for 7 days and one use. The first join spends it. A spent key left in the Secret is harmless: the spot reads it only while it has no certificate, and a spot that kept its approval but lost its certificate gets a new one with it. A spot whose approval was revoked needs a new key. |
| hello | With a usable certificate the spot answers the challenge with `hello` (name, workspace, version, certificate, and a proof over the nonce, name and workspace). The core then sends its frames on that socket (section 14). |
| renewal | A certificate lives 24 hours. When a third of its life is left, the spot sends `renew` (the current certificate, a new request, a proof from the current key) in place of `hello`, and takes the new certificate. A failed renewal, or one that issued a certificate already due, is not tried again for 5 minutes; the current certificate keeps opening the stream. |
| closes | Close code 1008 means the core does not accept the certificate (revoked, expired, not approved, wrong name): the spot drops the certificate, keeps its key and joins afresh, sending the enrollment key still in the Secret; a spot whose approval was revoked needs a new key. 1013 means the core cannot say yet: the certificate is kept. Any other close or error reconnects with `hello` and a backoff of 1 s doubling to 30 s. |
| access token | After a good hello the core sends `{"token":{"value","exp"}}`: a CA-signed token (`spt1.…`) valid 10 minutes, sent again at half its life. The spot writes it to `<identity-dir>/token`; the core client reads that file on every request and the model never sees it. It is accepted by the core’s MCP endpoint only, as the caller `spot:<workspace>/<name>` with read and write on its workspace and never execute, ingest or voice. `control_spot_task` is refused to it. |
| start | The stream starts before the system prompt is read. The spot waits for its first access token (“identity: the core accepted the spot”), then reads the `spot-instruction` cell (section 7), then opens its tools. |

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
| `stats` frame | State, turns and wakes by reason today, failures in a row, context size, own token counts and ceiling, why it is paused (its own ceiling or the workspace cap), its limits and choices (model, step, tool-call and token caps, wake timing, core-tools mode, gates, connector names with their tool names; never a URL, path or credential), next wake, and `tasks`: every wake the spot has in hand, the turns of the two lanes and the tasks, running or waiting for a place, each as `{key, title, lane, state, startedMs, ageMs}` (a title is 80 bytes, a key 128; empty when there are none). A snapshot replaces the last one. After each turn, on a trigger-mode change and every two minutes, for the TUI’s workspace panel. Counts and labels only. |
| `wake` frame (spot to core) | One line of activity when a wake ends, for the TUI’s timeline: when, the reason (`startup`, `tick`, `edge`, `human`, `manual`, `task`), the lane (`long`, `short` or `task`), the `key` and `title` of the wake when it is a task (the same as in `tasks`), the outcome (`turn`, `failed`, or a wake with no model call: `quiet`, `hold`, `ceiling`), duration, steps, tool calls, paid tokens, cache reads and output, and the plan: when the spot wakes next (`next`, ms), why (`nextWhy`) and from what (`nextSource`), and the `promise` (`{at, why, human}`) when there is one. A wake of the long or the short lane replaces the core’s record of the plan, so each carries it whole; a task’s wake does not. `trace`: one line per model request and tool call of the wake, `{kind, name, ms, tokens}` (`kind` is `model` or `tool`; `name` the model id or the tool’s name, 64 bytes at most; `tokens` the billable tokens of a request), 40 lines at most: the first 39, then a last line of kind `cut` that says how many were left out. Names and numbers only, never an argument or a result. Counts and labels only, never what was said. Dropped while the stream is down: a gap in the timeline is true, a replay of old wakes is not. |
| `wake` frame (core to spot) | `{"wake":{"reason","why"}}`: the planned wake or the promise is due (see Who wakes the spot). `reason` is `tick` or `promise`; `why` is the spot’s own words from its last report, 200 bytes at most. The spot starts a wake of the long lane; one it cannot place is dropped. |
| `schedule` frame (spot to core) | `{"schedule":{"next","nextWhy","nextSource","promise","backoffMs","maxWakeMs"}}`: sent once after each `hello`, so the core has the plan again after either side restarted. Each member is optional; `next` is in ms. It replaces the core’s record and clears its outstanding wake. |
| `control` frame (core to spot) | `{"control":{"id","op","task","why"}}`: an operator (a person through the core, never a spot) acts on one wake the spot has in hand. `op` is `wake` (a person asked for a wake from the console, no task key; `why` is who asked and what they wrote, 200 bytes at most: the spot starts a long-lane wake with the reason `manual`, on the short lane if the long one is busy, and answers at once with `wake asked`), `list`, `stop` (cancel: the turn ends at its next step and closes its tools; not a failure), `kill` (cancel and take it off the list at once; its tokens are still booked), `restart` (a task: stop, then start the same task again) or `drop` (a task that waits for a place). `task` is a key a list gave (128 bytes at most), empty for `list`; the spot checks it against what it has in hand and acts on that wake only. |
| `controlResult` frame (spot to core) | `{"controlResult":{"id","ok","msg","tasks":[{"key","title","lane","state","startedMs","ageMs"}]}}`: sent once for every control, with the same `id`, within 8 s, a refusal included (an unknown key, an operation that does not fit the state). `tasks` is what the spot has in hand afterwards, running and waiting, 100 at most; a title is 80 bytes. Titles and counts, never a goal or what a turn said. |
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
| `spot.replicas` | `1` runs the spot, `0` parks it: the Deployment, the PVC, the ServiceAccount and the Secrets stay, and an upgrade keeps it parked. More than 1 is not supported (one writer on `/state`). |
| core identity `serviceAccount` | A projected token with audience `triage-core` plus the `<name>-core` Role and RoleBinding (section 9). |
| core identity `secret` | The Secret `core.tokenSecret` mounted as a file, mode 0440 with `fsGroup`. |
| core identity `certificate` | No core Secret volume, no Role or RoleBinding, no projected core token, and no cloud annotation: the identity is in `/state/identity` on the PVC. `core.apiUrl` and `core.caFingerprint` (`SHA256:<64 lowercase hex>`) are required, the chart fails to render without them. `core.enrollKeySecret.name` (key `key`) is mounted at `/secrets/enroll` and passed as `--enroll-key-file`; leave it empty once the spot has joined, or leave the spent key in place. |
| system prompt | Not in the chart: the cell `spot-instruction` must exist in the workspace’s core before the spot starts (`make prompt-cell`), or the spot exits after two minutes. |
| `llm.auth: key` (default) | The Secret from `llm.keySecret` (anthropic) or `llm.geminiKeySecret` (gemini, at `/secrets/gemini/<key>`), as a file. |
| `llm.auth: vertex` | Nothing mounted. `llm.vertex.project` required, `region` defaults to `global`; the binding goes in `serviceAccount.annotations`. |
| `runtime: substrate` | No Deployment, PVC, ServiceAccount, NetworkPolicy, Secret or ConfigMap: an ActorTemplate and an EgressPolicy, as documents of the substrate API (Runtimes, in section 1). `llm.auth: injected`, `core.identity: certificate`, `substrate.atespace`, `substrate.imageDigest` and `substrate.modelKey` (the Secret the egress takes the key from) are required. |
| `llm.auth: injected` | Nothing mounted; the egress policy of a substrate actor carries the key rule. |
| `llm.auth: federation` | A projected token volume at `/var/run/secrets/anthropic.com` (`tokenExpirationSeconds` 600; no Secret). `llm.federation.ruleId`, `orgId` and `serviceAccountId` are required, `workspaceId` is optional. Refused with `gemini`. |
| model values | `llm.provider` (`anthropic` default, `gemini`), `llm.model` (empty = the provider’s default, no `--model` passed), `llm.effort`, `llm.maxSteps`, `llm.maxToolCalls`, `llm.maxTurnTokens`, `agent.toolTimeout`, `agent.toolResultMaxBytes`. |

### NetworkPolicy

Egress only; nothing in. It allows cluster DNS, the model API, the core, the connectors and, with `telemetry.enabled`, the OTLP endpoint from `networkPolicy.telemetry`. The model API is addressed by CIDR.

| Destination | Addressed by |
|---|---|
| `anthropic` with `key` or `federation` | The CIDRs Anthropic publishes (`160.79.104.0/23`, `2607:6bc0::/48`, platform.claude.com/docs/en/api/ip-addresses). |
| `gemini` in any mode; `vertex` with either provider | The Google API ranges you name in `networkPolicy.google.cidrs` (required; for example the Private Google Access range). With `vertex` it adds the node’s metadata server for the Workload Identity token (`networkPolicy.google.metadata`, GKE’s addresses by default). |
| core | `networkPolicy.core` is required whatever the identity: namespace and pod selector, or CIDR, and `ports` for the MCP endpoint and, with `core.apiUrl`, the API (the stream and the certificate join). |

### Lint

`make chart-lint` runs `helm lint` and `helm template` with the flag off (must render nothing) and with every values file in `charts/tria-spot/ci` (placeholders only): key, federation, Vertex AI, Gemini with a key and on Vertex AI, telemetry, ServiceAccount identity, certificate identity, a parked spot (`spot.replicas: 0`), connectors. It checks that:

- the pod has no `env`, no rule names Secrets, no ClusterRole renders, and the ServiceAccount token is not mounted;
- nothing in the pod is loosened for a shell, and each mode mounts and opens only its own;
- a missing required value or an unknown mode fails to render, and so does `engine.type: temporal`, with a message that names the core’s ADR-106;
- `agent.maxTasks` is passed as `--max-tasks`.

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
| `internal/agent` | One turn: the loop (`RunLoop`, which does no I/O), steps, injection, results, the tasks the turn asked for |
| `internal/telemetry` | Spans to ego over OTLP/HTTP |
| `internal/prompt` | Renders the system prompt from its template (read from the core at start; the source is `prompts/instruction.md`) and builds turn prompts |
| `internal/supervisor` | The wake loop, edges, the wakes the core asks for, ceilings, heartbeat, status; the lanes and the tasks, and the table of what the spot has in hand that the core’s controls act on |
| `internal/schedule` | The `schedule_next` request, clamped; the next wake and the promise after a turn |
| `internal/triggers` | The core’s websocket trigger stream |
| `internal/corecli` | The supervisor’s own MCP client (`list_open_issues`, heartbeat, rooms) |
| `internal/edges` | Edge detection between issue snapshots |
| `internal/ledger` | `/state/supervisor`: daily usage (in memory, written through), room cursors, status |
| `charts/tria-spot` | Helm chart |
| `docs/adr` | The decision records of this repo |

## Verified and not verified

Run on a live install: Gemini 3 on Vertex AI with Workload Identity, a real core and one Kubernetes connector. That covers full turns with parallel tool calls, cache reads across wakes (`read=` in the turn log), the Workload Identity binding, ego spans for every wake, `read_workspace_room`, and the Helm install and rollout of the image. Anthropic with federation also runs live: several spots, each with its own federation rule, make full turns with parallel tool calls and read the cache across steps and wakes. What follows was not seen.

| Area | Not verified |
|---|---|
| full turn, Anthropic | Whether the one-hour lifetime of the prefix cache holds across wakes an hour apart (cache reads are seen in every turn log); the same turns with `key` auth, which no live spot uses. |
| federation | The exchange works from a pod with a valid rule, service account and token. A rule that does not match the pod’s ServiceAccount shows as a 401 on the first model call. Not seen: how long the access token lives and whether it is reused (this decides whether `jti_reused` shows up); the SDK’s error text on a rejection. The hint matches `Authentication failed`, `authentication_error`, `jti_reused`, `token exchange` and `federation` in the error text. |
| certificate join | First join with an enrollment key, the access token and the stream were run against live cores. The renewal at two thirds of a certificate’s life, the 1008 close and the rejoin after it were run only against a stand-in core in the tests. |
| Vertex AI, Anthropic models | The egress ranges and the model id for Claude on a real project. |
| `read_workspace_room` | The reply shape is read loosely (`messages` object or array; `id`, `sender\|from\|author`, `text\|body\|message`, `ts\|time`); re-check when the tool changes. |
| heartbeat | The ADR says `system2-heartbeat-<tenant>`; the core looks only for `system2-heartbeat`, so until it reads the per-tenant id this heartbeat does not change reflection. `--heartbeat-id` sets either. |
| comments | `list_open_issues` gives a comment count, not the author (all MCP comments are `mcp-client`), so every new comment is a `human` wake and the agent reads the issue to see who. Comments made during a turn are absorbed by the post-turn baseline. |
| hop counts | Hop counts and caps are enforced by the core; the prompt only tells the agent to pass and respect them. |
| post gate | Seen live: a post refused, then released after a memory read, in a wake that then posted. Also seen live: a `finish` in the same step as a refused post ended the turn with no post; the fix runs in tests only. |
| Wakes from the core, tasks | Tests drive the supervisor with a scripted model and a fake stream: the tick sent by the core, the net at `--max-wake`, a stale tick dropped, a promise sent by the core, the plan and the promise on every report, tasks that queue, stop, restart, drop and are killed, a long turn stopped by an operator, a task held at a ceiling. Not verified: the two halves together on a live core. |
| tooling | `kubeconform`, and `golangci-lint` (`make lint` runs it when present). |
