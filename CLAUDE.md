# CLAUDE.md

Guidance for Claude Code in this repo.

## What this is

`tria-spot` is the supervisor, image and chart for the Claude Code spot in
ADR-095 (in the core repo, `docs/adr/095-claude-spot-acts-on-the-cluster.md`).
Read the ADR and `README.md` before changing behaviour.

## Rules

- **Commits** follow conventional commits (`feat:`, `fix:`, `docs:`,
  `refactor:`, `test:`, `chore:`). Do NOT add `Co-Authored-By` trailers.
- **Tests before commit.** Run `make check` (gofmt, `go vet`, golangci-lint if
  installed, `go test -race`, `helm lint` and `helm template`). Never commit a
  failing tree.
- **Never `git push`.** There is no remote. Do not add one. The image is built
  with ko only (no Docker, no Dockerfile): `make image` only builds, and `make
  push` publishes an image when the user has asked for a deploy.
- **No hard-coded deployment names.** No cluster, tenant, company, customer or
  user names in code, charts, docs, tests or commit messages. Use placeholders
  (`example`, `<tenant>`). Everything specific to an install is a flag or a
  Helm value.
- **No credentials in environment variables or arguments**, anywhere. A secret
  is a file path. If you add a setting that holds a secret, it takes a file.
- **Two Anthropic auth modes, key and federation.** In federation mode never
  render `apiKeyHelper` or pass any `ANTHROPIC_*` variable except
  `ANTHROPIC_CONFIG_DIR` and `ANTHROPIC_PROFILE`: the key sources outrank
  federation and silently shadow it. Never start the CLI with `--bare`, which
  ignores profiles and federation. The ids are not secrets but are still flags.
- **Flag names come from the docs.** Claude Code flags and settings are
  verified against https://code.claude.com/docs before use; do not guess. If
  you cannot verify one, say so in the README's "Not verified" list.
- **The connector is the boundary.** The prompt is best effort. The spot holds
  no cluster RBAC. A shell exists only inside the CLI's sandbox (`--sandbox`,
  chart `sandbox.enabled`): `Bash` stays in `runner.Disallowed` unless that flag
  is set, and then `runner.WithSandbox` allows the bare tool, which is safe only
  because the rendered settings require the sandbox (`failIfUnavailable`), forbid
  an unsandboxed retry and give it no network. Never add a `Bash(...)` allow
  rule, never set `excludedCommands`, `enableWeakerNestedSandbox` or an allowed
  domain, and never run a shell outside the sandbox. A cluster is reached only through a
  connector, whose tools are allowed by exact name. By default the core's tools
  are allowed by name (`runner.CoreTools`) and the destructive ones are
  disallowed by name (`runner.CoreToolsDenied`). `--core-tools all`
  (`agent.coreTools: all`) allows `mcp__core__*` and drops that list: an
  operator's explicit choice for an install, with the core's Role
  (`core.verbs`) as the bound. Never make it the default.
- **The agent must not be able to edit its own limits.** Supervisor files live
  under `/state/supervisor` (denied to Edit and Write by rendered rules), the usage counter is
  held in memory, and there is no shell. Keep it so.
- The core's MCP server is `github.com/mark3labs/mcp-go`; the supervisor's
  client uses the same library.

## Layout

See the README's Layout section. The supervisor's tests drive the loop with
`internal/supervisor/testdata/fake-claude.sh` in place of the CLI; extend that
script's modes rather than mocking the runner.
