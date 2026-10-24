# Contributing to the Tria spot

Thanks for your interest. The spot is an always-on agent for one workspace of the
[Triage core](https://triageagent.dev). Read the [README](README.md), [docs/spec.html](docs/spec.html) and
[docs/adr](docs/adr) before changing behavior.

**Status: work in progress.** Expect breaking changes between releases.

## Ground rules

- **No shell, no filesystem tools, no cluster RBAC.** The model is called only through `internal/llm` and tools
  only through `internal/tools`. Don't add a tool that runs a command, reads or writes a file, or fetches a URL.
- **A secret is a file path.** No credential in an environment variable or an argument; a setting that holds a
  secret takes a file.
- **No deployment names.** No cluster, tenant, company, customer or user names in code, charts, docs, tests or
  commit messages. Use placeholders (`example`, `<tenant>`).
- **Test what you change.** The supervisor's tests drive the loop with a scripted `Turner`, the agent's tests use
  the scripted `llm.Fake`, and the tools' tests use an in-process MCP server. Extend those rather than mocking
  around them.
- **Small, focused PRs.** One behavior or fix per PR.
- **Comments say why.** Don't narrate where code came from.

## Development setup

You need the Go version in `go.mod` and [helm](https://helm.sh). [golangci-lint](https://golangci-lint.run) is used
when it is installed.

```bash
make check    # everything below
make build    # bin/tria-spot
```

`make check` runs `gofmt -l .` (must print nothing), `go vet ./...`, `golangci-lint run ./...` if the binary is
installed, `go test -race ./...`, and `helm lint` plus `helm template` with every `charts/tria-spot/ci/*-values.yaml`.

## Building the image

The image is built with [ko](https://ko.build) only: the Go binary on a distroless static base, configured in
`.ko.yaml`. There is no Dockerfile and no Docker.

```bash
make image                                      # an OCI layout in bin/image, pushes nothing
make push REGISTRY=<registry>/<namespace>       # builds and pushes <registry>/<namespace>/spot
```

## Commits and PRs

- Use [conventional commits](https://www.conventionalcommits.org): `feat:`, `fix:`, `docs:`, `refactor:`, `test:`,
  `ci:`, `chore:`.
- No `Co-Authored-By` or other trailers.
- Run `make check` before every commit; a PR must not break it.

## Reporting issues

Open an issue with what you ran, what you expected and what happened. For a security problem, follow
[SECURITY.md](SECURITY.md) instead.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).
