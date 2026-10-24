# ADR-098: The spot runs on its own agent loop

- Status: Accepted
- Date: 2026-10-24
- Builds on: the core's ADR-095 (the spot as a teammate that acts through connectors)

## Context

ADR-095 put an always-on teammate in a pod for each workspace. The first
implementation ran a vendor's coding-agent CLI as the spot: a small Go
supervisor started the CLI in turns, handed it the core's MCP tools and the
connectors, and read its output.

It worked, and it taught us what the spot needs. It also cost us:

- **A large prompt on every wake.** The CLI brings its own system prompt, its
  own tool descriptions and its own memory features. Every wake paid for them
  before the spot said a word, and none of them is about triage.
- **A permission system tamed from the outside.** The CLI decides which tool
  calls run. To make it behave we rendered allow and deny rules, a settings
  file and a permission mode, checked at every turn which mode the CLI had
  really started in, and fell back when it was not the one we asked for. The
  rules lived in a language that is not ours and changes with the CLI's
  releases.
- **A sandbox and binaries in the image.** A shell for the model needed
  bubblewrap, user namespaces, an unmasked `/proc` and the runtime's seccomp and
  AppArmor profiles switched off for the pod. The image carried the CLI, `jq`,
  `socat` and their libraries as fetched, digest-pinned binaries, on a base
  image with a shell.
- **One vendor's product inside ours.** Authentication, telemetry, credential
  order and the meaning of exit codes were the CLI's, and we followed them:
  scrubbing its environment, rendering profiles for federation, reading its
  stream format for usage, matching text in stderr to find a rejected key.

What it taught us is what we keep:

- the **trigger stream**: the core tells the spot when something changed, and
  the supervisor reads the core only then, without a model;
- an **empty conversation for every wake**, so a model call never pays again for
  the tool output of earlier wakes;
- **curated tools**: the core's tools allowed by name, the destructive ones
  refused by name, a connector's tools by exact name, and the connector, not the
  prompt, as the boundary;
- **budget reports**: the spot tells the core what each turn cost, and the core
  can hold the spot back.

The spot does not need a coding agent. It needs a loop that sends a prompt to a
model, runs the tools the model asks for and stops.

## Decision

**The spot is Tria's conversational and investigative half, as its own Go agent
loop, one per workspace.** The supervisor stays. The CLI is replaced by
`internal/agent`, which calls Claude through the official Anthropic Go SDK
(`internal/llm`) and takes its tools from MCP servers (`internal/tools`).

- **Tria is reused through the core's MCP surface, not by importing core code.**
  The core's tools, rooms, memory, instructions, budgets and ledger are already
  what the spot works with. The core's own agent loop is internal to the core,
  bound to Gemini and to in-process state; sharing it would
  mean two binaries depending on code written for one of them. The spot speaks
  to the core as any other client does.
- **The machine path stays in the core.** Ingest, gating, classification and
  decisions, the issue lifecycle, memory cells, policy and the self-improvement
  loop are not the spot's and do not move. The spot reads their results and
  speaks about them.
- **The loop is small and ours.** A turn lists the tools, sends the instruction,
  the tools and the wake prompt, runs the tools the model asks for (all results
  of one answer go back in one message), and stops when the model ends its
  answer, calls `finish`, reaches `--max-steps` or runs out of time. A tool
  error is a tool result. A refusal, an answer cut twice, a rejected credential
  and a core without tools each end the turn with a named reason.
- **Tools are named `<server>__<tool>`**, sorted, with each server's schema
  unchanged. The spot adds two of its own, `schedule_next` (replacing the
  schedule file the CLI wrote) and `finish`. It has no shell, no filesystem
  tool and no cluster RBAC; a cluster is reached through a connector.
- **The cache is designed in.** Tools and the instruction are the stable
  prefix, with a one-hour breakpoint on the last system block, so wakes between
  two minutes and two hours apart read it. Everything that changes per wake is
  in the first user message.
- **Three ways to reach the model**: an API key read from a file, Anthropic
  Workload Identity Federation (the pod's projected token), and Claude on
  Google Vertex AI through the pod's Workload Identity. The federation ids and
  the path of the token file are set in the process environment for the SDK to
  find; they are not credentials, there is no child process, and the variables
  that would shadow federation are unset.
- **The loop is provider-neutral.** It talks to `llm.Client` and ships two
  implementations: Anthropic (key, federation, Vertex AI) and Gemini through the
  Google GenAI Go SDK (key, Vertex AI), chosen with `--llm-provider`. The
  request model, the tool surface, the budget and the spans are the same for
  both; a Gemini key is a file like every other credential.
- **The spot emits its own spans** to ego: a root span per wake, a span per
  model request and one per tool call, with the resource attributes ego reads.
- **The pod is stateless.** The spot's memory is the core's memory
  (`upsert_memory_item`, `search_memory`); there is no notes directory and no
  file the model can write. `/state/supervisor` holds the day's token count,
  room cursors and the status file, and losing it is harmless. A platform that
  clones or restarts the pod gets a spot that starts fresh from the core.
- **Identity stays the ServiceAccount token for now.** The core accepts the
  pod's projected token; a per-spot minted token is the next ADR.

## Consequences

What we lose:

- **Open-ended coding.** The spot cannot edit files, run commands or build
  anything. It investigates and acts only through the tools it is given.
- **The CLI's skills, subagents and plugins.** None exists in the new loop. Work
  that needed them is either a tool on a connector or not the spot's work.
- **The vendor's polish.** Retries beyond the SDK's two, context management and
  the handling of every odd stop reason are ours to write and keep. The first
  version handles `end_turn`, `tool_use`, `max_tokens` (continue once),
  `refusal` and `pause_turn` (resend once).
- **A dollar ceiling.** The CLI estimated cost; the SDK reports tokens only, and
  the spot carries no price table. The token ceiling and the spend limit on the
  model credential remain.
- **Nothing here has met the real API yet.** The SDK calls are tested against a
  local server and the loop against a scripted model.

What we gain:

- **A small, stable prompt.** The prefix is our instruction and our tools, and
  nothing else. A wake pays for what the spot reads.
- **Control.** Which tools exist, which are refused and what the model sees of
  each result is code in this repository, tested, not rules rendered for
  another program. A tool result is capped, a tool call is timed out, and a
  failed tool is information for the model.
- **A smaller, safer pod.** A distroless static image with no shell, the
  runtime's default seccomp profile, no unmasked `/proc`, no user namespace
  request, no rendered configuration, no bundled binaries and 128Mi of memory
  requested.
- **Choice of model route.** The same loop runs on Anthropic's API (key or
  federation) or on Vertex AI.
- **Our own telemetry.** Spans follow the GenAI conventions and carry the
  fields ego needs, instead of whatever the CLI exported. By default they also
  carry the turn's messages, tool arguments and results, which the receiver
  needs to evaluate a turn; an install that must not send them turns that off.
- **A spot that can be cloned.** With no state worth keeping in the pod, a
  sandbox platform can start many from one image.

## Alternatives considered

- **Keep the CLI.** Rejected. The cost in prompt size, the control we did not
  have over what it runs, and the weight of a vendor's product in our image and
  our code are the reasons above, and they grow with each CLI release we have to
  follow.
- **Move the core's agent loop out of `internal/` and import it.** Rejected for
  now. It is some sixteen thousand lines bound to Gemini and
  to the core's own state. Two binaries sharing that code would break each other
  at every change, and the spot would inherit what it does not need. Reusing
  Tria through MCP gives the same tools and memory with a stable contract. If a
  small part proves worth sharing, it can move to its own package then.
- **The vendor's agent SDK.** Rejected. It carries the same prompt overhead as
  the CLI, needs a Node runtime in the image, and is a filesystem and shell
  agent by design, which is what we do not want the spot to be.
