<p align="center"><img src="docs/spot.svg" width="96" alt="spot"></p>

# spot

An always-on teammate for one workspace of the [Triage core](https://triageagent.dev).

The spot runs in a pod, wakes when the core reports something or someone
writes to it, investigates through read-only connectors, and says what it
found in the workspace's chat and on the issue. It calls Claude or Gemini and
works only through tools. It has no shell, no filesystem and no cluster RBAC.

## Install

```sh
helm install my-spot oci://ghcr.io/triageagent-dev/charts/tria-spot \
  --namespace spots --create-namespace --values values.yaml
```

```yaml
spot:
  enabled: true
  tenant: <workspace id>
  name: my-spot
core:
  mcpUrl: http://<core-mcp-service>.<core-namespace>.svc:8083/mcp
  apiUrl: http://<core-api-service>.<core-namespace>.svc:8080
  identity: secret
  tokenSecret: {name: my-spot-core-token}
llm:
  provider: anthropic           # or gemini
  auth: key                     # key | federation | vertex
  keySecret: {name: my-spot-model-key}
networkPolicy:
  core:
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: <core-namespace>}}
    ports: [8083, 8080]
```

Every credential is a file from a Secret. Egress is an allow list. Each
setting is explained in [values.yaml](charts/tria-spot/values.yaml).

## Read more

- [Specification](docs/spec.html): the loop, tools, credentials, spans, spend, flags.
- [ADR-098](docs/adr/098-spot-on-tria-runtime.md): why the spot is its own agent loop.
- [Contributing](CONTRIBUTING.md), [Security](SECURITY.md), [License](LICENSE) (Apache 2.0).
