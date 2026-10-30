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
  identity: certificate
  caFingerprint: SHA256:<64 lowercase hex>      # the core's CA
  enrollKeySecret: {name: my-spot-enroll-key}   # the one-time key the core minted for my-spot
llm:
  provider: anthropic           # or gemini
  auth: key                     # key | federation | vertex
  keySecret: {name: my-spot-model-key}
networkPolicy:
  core:
    namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: <core-namespace>}}
    ports: [8083, 8080]
```

The spot joins the core by certificate: a workspace admin mints a one-time
spot key in the core (label `my-spot`), you put it in the Secret
`my-spot-enroll-key` (key `key`), and the spot keeps its own key and certificate
in `/state`. The core's `spot-instruction` cell must exist first (`make
prompt-cell`). Every other credential is a file from a Secret. Egress is an
allow list. Each
setting is explained in [values.yaml](charts/tria-spot/values.yaml).

## Schedule

The core keeps the spot's schedule (the core's ADR-106). The spot reports its
plan, the next wake and the wake it promised a person, with every wake, and the
core sends the wake when it is due. The pod runs the turns, the two lanes and
the tasks, and wakes itself only when the core has sent nothing for `--max-wake`.
The Temporal engine is gone: a release whose values still set `engine.type:
temporal` fails to render. See the specification.

## Run as an actor of Agent Substrate

`runtime: substrate` (the core's ADR-107) runs the spot as an actor: it is
suspended while it is idle and holds no credential of any kind. The model auth
is `injected` (the spot sends a placeholder key and the egress policy puts the real one on the request from a Secret), the core identity is the
certificate, and the chart renders an ActorTemplate and an EgressPolicy for the
substrate CLI and no Kubernetes object. The spot serves `/readyz` and `/frame`
on port 80, talks to the core in requests and reports when it is idle. See the
specification.

## Read more

- [Specification](https://triageagent.dev/spot/spec.html): the loop, tools, credentials, spans, spend, flags.
- [ADR-098](docs/adr/098-spot-on-tria-runtime.md): why the spot is its own agent loop.
- [Contributing](CONTRIBUTING.md), [Security](SECURITY.md), [License](LICENSE) (Apache 2.0).
