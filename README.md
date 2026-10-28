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

## Engines

The schedule and the turns run in the pod by default. With
`engine.type: temporal` a Temporal workflow owns them instead (Temporal Cloud or
a service of your own): the next wake and the wake a person asked for are
durable timers, a turn that a crash cuts resumes at its step, and a person's
message is answered beside a long turn. The pod still makes every model and
tool call, and no payload leaves it readable: it is encrypted with a key from a
Secret, or kept in the pod's state directory with only a reference sent to the
service. See the specification.

## Read more

- [Specification](https://triageagent.dev/spot/spec.html): the loop, tools, credentials, spans, spend, flags.
- [ADR-098](docs/adr/098-spot-on-tria-runtime.md): why the spot is its own agent loop.
- [Temporal engine](docs/adr/temporal-engine.md): why the schedule and the turns can run in a workflow.
- [Contributing](CONTRIBUTING.md), [Security](SECURITY.md), [License](LICENSE) (Apache 2.0).
