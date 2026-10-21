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
- **Never push.** There is no remote. Do not add one, and do not run
  `docker push`; `make image` only builds.
- **No hard-coded deployment names.** No cluster, tenant, company, customer or
  user names in code, charts, docs, tests or commit messages. Use placeholders
  (`example`, `<tenant>`). Everything specific to an install is a flag or a
  Helm value.
- **No credentials in environment variables or arguments**, anywhere. A secret
  is a file path. If you add a setting that holds a secret, it takes a file.
- **Flag names come from the docs.** Claude Code flags and settings are
  verified against https://code.claude.com/docs before use; do not guess. If
  you cannot verify one, say so in the README's "Not verified" list.
- **RBAC is the boundary.** The prompt and the PreToolUse hook are best effort.
  Never widen the chart's Read stage to Secrets, and keep the Act stage opt-in
  per namespace.
- The core's MCP server is `github.com/mark3labs/mcp-go`; the supervisor's
  client uses the same library.

## Layout

See the README's Layout section. The supervisor's tests drive the loop with
`internal/supervisor/testdata/fake-claude.sh` in place of the CLI; extend that
script's modes rather than mocking the runner.
