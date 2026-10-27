# The schedule and the turns can run in a Temporal workflow

- Status: Proposed (the number is the core's to give)
- Date: 2026-10-27
- Builds on: ADR-098 (the spot runs its own agent loop)

## Context

The supervisor of ADR-098 is a small loop in the pod: it wakes the agent, keeps
the schedule, holds edges, honours a hold rule, runs a short turn beside a long
one, and writes the books. It grew by what each complaint needed: a promise for
a wake a person asked for, kept in a file; a short lane for a message that
arrived during a long turn; a hold rule read from the notes; caps. Each is
correct, and each is a piece of a scheduler that lives in memory:

- a wake that comes due while the pod is down is run when the pod is back, not
  when it was due;
- a turn that a restart cuts is lost and redone from the first step, with its
  tokens paid twice and its writes possibly made twice;
- a second kind of work (a background task, a sub-agent) would be another
  special case in the same loop;
- what the spot did and why is in logs and spans, not in a record that can be
  read, queried or resumed.

These are what a workflow engine is for.

## Decision

Add an engine, `--engine=temporal`, beside the local one, and keep the local one
the default.

- A workflow per spot is the scheduler: the tick and the promised wake are
  durable timers; the hold rule, the schedule, the backoff and the lanes are its
  state. A child workflow per wake runs the turn.
- The turn is the same loop as the local one (`agent.RunLoop`, which does no I/O
  and reads no clock), with the model request and each tool call as activities.
  A crash resumes at the step.
- The pod keeps what only it can do: the intake from the core, the notes and the
  prompt, the books, the model client, the tools and the credentials. The
  service never calls the model or a tool.
- A call that may have an effect runs once. If its pod dies in it, the model is
  told it may or may not have run. A read runs again.
- Payloads are encrypted in the pod with a key from a file before they leave it,
  and a spot that reaches a service over the network does not start without the
  key unless it is told the clear text may go.
- The service is a setting: Temporal Cloud first, then a service of one's own in
  a cluster. Nothing else changes.

## Consequences

- Timers, the hold rule and a turn survive a restart; the history is a record
  of every wake.
- The local engine stays, with its tests, as the default and as the fallback; the
  planning of the next wake is one function used by both.
- A running spot pays for the service: an action for each model request, tool
  call and book of a wake. The hold rule and the caps bound it as before.
- The workflow code must stay deterministic and its payloads must stay small:
  the instruction and the tool list are the pod's, not the history's.
- Changing the workflow's code while one runs needs versioning; until there is a
  second release of it, the workflow is replaced by stopping the old one.

## Not decided

- Background tasks and sub-agents as child workflows: the lanes are the first
  step, and the turn is already a workflow that another could start.
- Moving the books (usage, digest, cursors) from the pod's disk into the
  workflow, which would let any pod serve any spot.
