# CLAUDE.md

Guidance for agents working in this repo.

## What this is

`tria-spot` is the supervisor, the agent loop, the image and the chart for the
Tria spot (the core's ADR-095, and ADR-098 in `docs/adr`): an always-on teammate for one
workspace of the Triage core, running as its own Go agent loop over the
Anthropic Go SDK or the Google GenAI Go SDK, with tools taken from the core's MCP server and connectors.
Read the ADRs, `README.md` and `docs/spec.html` before changing behaviour.

## Rules

- **Commits** follow conventional commits (`feat:`, `fix:`, `docs:`,
  `refactor:`, `test:`, `chore:`). Do NOT add `Co-Authored-By` trailers.
- **Tests before commit.** Run `make check` (gofmt, `go vet`, golangci-lint if
  installed, `go test -race`, `helm lint` and `helm template` with every
  `charts/tria-spot/ci/*-values.yaml`). Never commit a failing tree.
- **Push only when asked.** The remote `origin` is the public repository
  `triageagent-dev/tria-spot`. Never push unasked, and never
  push a branch other than the one asked for. The image is built with ko only
  (no Docker, no Dockerfile): `make image` only builds, and `make push
  REGISTRY=...` publishes an image when the user has asked for a deploy.
- **No hard-coded deployment names.** No cluster, tenant, company, customer or
  user names in code, charts, docs, tests or commit messages. Use placeholders
  (`example`, `<tenant>`). Everything specific to an install is a flag or a
  Helm value.
- **No credentials in environment variables or arguments**, anywhere. A secret
  is a file path. If you add a setting that holds a secret, it takes a file.
  The one place the process sets `ANTHROPIC_*` variables is federation mode in
  `internal/llm`, for the SDK to find the federation ids and the path of the
  identity token file (not credentials); it also unsets the variables that
  would shadow federation. Keep it so.
- **Two providers, anthropic and gemini, both behind `llm.Client`.** A Gemini
  key is a file (`--gemini-key-file`), never `GOOGLE_API_KEY` or
  `GEMINI_API_KEY`; the client passes the key and the host to the SDK explicitly
  and fails on an empty key file. Federation is an Anthropic auth mode only.
- **The model is called only through `internal/llm`; tools only through
  `internal/tools`; the spot has no shell, no filesystem tools and no cluster
  RBAC.** Do not add a tool that runs a command, reads or writes a file, or
  fetches a URL.
- **Three model auth modes: key, federation and vertex.** In federation mode
  never let `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`,
  `ANTHROPIC_IDENTITY_TOKEN` or `ANTHROPIC_PROFILE` stay set: they outrank
  federation and silently shadow it. The ids are not secrets but are still
  flags.
- **The connector is the boundary.** The prompt is best effort. A cluster is
  reached only through a connector, whose tools are offered by exact name. By
  default the core's tools are allowed by name (`tools.CoreTools`) and the
  destructive ones are refused by name (`tools.CoreToolsDenied`); a denied name
  stays denied even when it is also allowed. `--core-tools all`
  (`agent.coreTools: all`) offers every tool the core offers and drops that
  list: an operator's explicit choice for an install, with the core's Role
  (`core.verbs`) as the bound. Never make it the default.
- **The prefix stays stable.** The tool list (sorted by name) and the system
  prompt are the cached prefix of every request. Nothing that changes between
  wakes (time, wake facts, budget, step cap, which connectors are down) goes in
  them; it goes in the first user message.
- **The model must not be able to edit its own limits.** It has no filesystem
  tool and no shell. The supervisor's counters are held in memory and only
  written through. Keep it so.
- **A tool failure is a tool result.** `tools.Registry.Call` never fails a turn;
  the model sees the error and decides.
- The core's MCP server is `github.com/mark3labs/mcp-go`; the spot's clients
  (`internal/corecli`, `internal/mcptools`) use the same library.

## Layout

See the Layout section of `docs/spec.html`. The supervisor's tests drive the loop with a
scripted `Turner` in `internal/supervisor/supervisor_test.go`; the agent's
tests use the scripted `llm.Fake`; the tools' tests use an in-process MCP
server. Extend those rather than mocking around them.
