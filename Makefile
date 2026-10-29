# tria-spot
.DEFAULT_GOAL := help

BIN         := bin/tria-spot
PKG         := ./...
REGISTRY    ?= ghcr.io/triageagent-dev
IMAGE       ?= $(REGISTRY)/spot
BUILD_DATE  ?= $(shell date -u '+%Y-%m-%d')
GIT_COMMIT  ?= $(shell echo "$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)$$(test -n "$$(git status --porcelain 2>/dev/null)" && echo -dirty)")
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -s -w -X main.version=$(VERSION) -X main.gitCommit=$(GIT_COMMIT) -X main.buildDate=$(BUILD_DATE)
CHART       := charts/tria-spot

.PHONY: help
help: ## List the targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

.PHONY: fmt
fmt: ## gofmt the tree
	gofmt -w $$(git ls-files '*.go' 2>/dev/null || find . -name '*.go')

.PHONY: vet
vet: ## go vet
	go vet $(PKG)

.PHONY: lint
lint: vet ## gofmt check, go vet, and golangci-lint when it is installed
	@test -z "$$(gofmt -l . )" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run $(PKG); else echo "golangci-lint not installed; skipped"; fi

.PHONY: test
test: ## go test with the race detector
	go test -race -count=1 $(PKG)

.PHONY: build
build: ## Build bin/tria-spot
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/tria-spot

# The image is built with ko only: the Go binary on a distroless static base
# (no shell, no package manager), pinned by digest in .ko.yaml. There is no
# kubectl in the image: a cluster is reached through connectors.
.PHONY: image
image: ## Build the image as an OCI layout in bin/image (pushes nothing)
	VERSION=$(VERSION) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) \
	  KO_DOCKER_REPO=$(IMAGE) ko build --bare --push=false --oci-layout-path=bin/image ./cmd/tria-spot

.PHONY: push
push: ## Build and push the image with ko: make push IMAGE=<registry>/<repo>
	VERSION=$(VERSION) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) \
	  KO_DOCKER_REPO=$(IMAGE) ko build --bare --tags=$(VERSION) ./cmd/tria-spot

# The system prompt is not in the image: the spot reads it from a memory cell of
# the core when it starts. This prints that cell (kind spot_system_identity, content =
# prompts/instruction.md); apply it to the namespace of the core:
#   make prompt-cell | kubectl -n <core namespace> apply -f -
# The cell name is <id>-<first 8 hex of sha1(id)>, as the core names its own.
PROMPT_ID ?= spot-instruction

.PHONY: prompt-cell
prompt-cell: ## Print the MemoryCell that holds the system prompt: make prompt-cell PROMPT_ID=<id>
	@printf '%s\n' \
	  'apiVersion: xray.triageagent.dev/v1' \
	  'kind: MemoryCell' \
	  'metadata:' \
	  "  name: $(PROMPT_ID)-$$(printf %s '$(PROMPT_ID)' | shasum | cut -c1-8)" \
	  '  labels:' \
	  '    xray.triageagent.dev/kind: spot_system_identity' \
	  '    xray.triageagent.dev/managed: "true"' \
	  'spec:' \
	  '  id: $(PROMPT_ID)' \
	  '  kind: spot_system_identity' \
	  '  scope: system' \
	  '  status: active' \
	  '  title: "Spot system prompt"' \
	  '  confidence: "1.0"' \
	  "  createdAt: \"$$(date -u +%Y-%m-%dT%H:%M:%SZ)\"" \
	  "  updatedAt: \"$$(date -u +%Y-%m-%dT%H:%M:%SZ)\"" \
	  '  content: |'
	@sed 's/^/    /' prompts/instruction.md

CHART_TMP := $(or $(TMPDIR),/tmp)

.PHONY: chart-lint
chart-lint: ## helm lint and helm template: flag off, ServiceAccount identity, Anthropic key, federation and Vertex AI modes, Gemini key and Vertex AI modes
	helm lint $(CHART)
	helm lint $(CHART) -f $(CHART)/ci/enabled-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/key-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/federation-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/vertex-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/gemini-key-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/gemini-vertex-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/telemetry-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/connectors-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/substrate-values.yaml
	@test -z "$$(helm template t $(CHART))" || { echo "spot.enabled=false must render nothing"; exit 1; }
	helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml > $(CHART_TMP)/tria-spot-key.yaml
	helm template t $(CHART) -f $(CHART)/ci/key-values.yaml > $(CHART_TMP)/tria-spot-keyset.yaml
	helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml > $(CHART_TMP)/tria-spot-fed.yaml
	helm template t $(CHART) -f $(CHART)/ci/vertex-values.yaml > $(CHART_TMP)/tria-spot-vertex.yaml
	helm template t $(CHART) -f $(CHART)/ci/gemini-key-values.yaml > $(CHART_TMP)/tria-spot-gmkey.yaml
	helm template t $(CHART) -f $(CHART)/ci/gemini-vertex-values.yaml > $(CHART_TMP)/tria-spot-gmvertex.yaml
	helm template t $(CHART) -f $(CHART)/ci/telemetry-values.yaml > $(CHART_TMP)/tria-spot-tel.yaml
	helm template t $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml > $(CHART_TMP)/tria-spot-sa.yaml
	helm template t $(CHART) -f $(CHART)/ci/connectors-values.yaml > $(CHART_TMP)/tria-spot-conn.yaml
	helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set agent.maxTasks=5 | grep -q -e --max-tasks=5 || { echo "agent.maxTasks must pass --max-tasks"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set engine.type=temporal >/dev/null 2>&1 || { echo "engine.type: temporal must fail to render"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set engine.type=temporal 2>&1 | grep -q 'ADR-106' || { echo "the failure for engine.type: temporal must name ADR-106"; exit 1; }
	@for f in key keyset fed vertex gmkey gmvertex tel sa conn; do \
	  ! grep -qiE 'sandbox|hostUsers|Unconfined|procMount|--run-dir|permission-mode|--persistent|--allowed-tools|claude-bin|medium: Memory' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "the pod carries nothing for a shell, a sandbox or a rendered config ($$f)"; exit 1; }; \
	  grep -q 'type: RuntimeDefault' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q 'readOnlyRootFilesystem: true' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q 'allowPrivilegeEscalation: false' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "the pod keeps its runtime seccomp profile, a read-only root and no privilege escalation ($$f)"; exit 1; }; \
	  ! grep -qE '^[[:space:]]+env(From)?:' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "the pod must carry no env ($$f)"; exit 1; }; \
	  ! grep -qE 'resources:.*secrets' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "no RBAC rule may name secrets ($$f)"; exit 1; }; \
	  ! grep -qE 'kind: (ClusterRole|ClusterRoleBinding)' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "the spot holds no cluster RBAC; a cluster is reached through a connector ($$f)"; exit 1; }; \
	  grep -q 'automountServiceAccountToken: false' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "the ServiceAccount token must not be mounted ($$f)"; exit 1; }; \
	  grep -q -- '--llm-provider=' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q -- '--llm-auth=' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q -- '--effort=' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q -- '--max-steps=' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q -- '--tool-timeout=' $(CHART_TMP)/tria-spot-$$f.yaml && grep -q -- '--tool-result-max-bytes=' $(CHART_TMP)/tria-spot-$$f.yaml || { echo "the provider, auth and loop flags are always passed ($$f)"; exit 1; }; \
	done
	@grep -q -- '--connectors-file=' $(CHART_TMP)/tria-spot-conn.yaml && grep -q 'connectors.json' $(CHART_TMP)/tria-spot-conn.yaml && grep -q 'Connector example-kube' $(CHART_TMP)/tria-spot-conn.yaml || { echo "connectors: the flag, a ConfigMap and an egress rule each"; exit 1; }
	@! grep -q -- '--connectors-file' $(CHART_TMP)/tria-spot-key.yaml || { echo "no connectors, no flag"; exit 1; }
	@! sed -n '/kind: ConfigMap/,/^---/p' $(CHART_TMP)/tria-spot-conn.yaml | grep -qiE 'token|authorization|secret' || { echo "the connectors ConfigMap must hold no credential"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/connectors-values.yaml --set 'connectors[0].tools=null' >/dev/null 2>&1 || { echo "a connector without named tools must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/connectors-values.yaml --set 'connectors[0].egress=null' >/dev/null 2>&1 || { echo "a connector without an egress target must fail"; exit 1; }
	@grep -q -- '--otel-tenant-attr=triage.tenant' $(CHART_TMP)/tria-spot-tel.yaml && ! grep -q -- '--otel-tenant=' $(CHART_TMP)/tria-spot-tel.yaml && ! grep -q -- '--otel-tenant-attr' $(CHART_TMP)/tria-spot-key.yaml || { echo "telemetry: the label attribute is always passed, the label itself only when set (default: the spot's name)"; exit 1; }
	@grep -q -- '--otel-content=true' $(CHART_TMP)/tria-spot-tel.yaml && ! grep -q -- '--otel-content' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q -- '--otel-tool-details' $(CHART_TMP)/tria-spot-tel.yaml || { echo "telemetry: the content flag is passed with telemetry, on by default, and the old tool-details flag is gone"; exit 1; }
	helm template t $(CHART) -f $(CHART)/ci/substrate-values.yaml > $(CHART_TMP)/tria-spot-substrate.yaml
	@grep -q 'preferredFidelity: SNAPSHOT_FIDELITY_VOLUMES' $(CHART_TMP)/tria-spot-substrate.yaml && grep -q 'path: /readyz' $(CHART_TMP)/tria-spot-substrate.yaml && grep -qE 'image: .*@sha256:[0-9a-f]{64}' $(CHART_TMP)/tria-spot-substrate.yaml && grep -q -- '--runtime=substrate' $(CHART_TMP)/tria-spot-substrate.yaml && grep -q -- '--llm-auth=gateway' $(CHART_TMP)/tria-spot-substrate.yaml || { echo "substrate: an ActorTemplate with the volume snapshot, the wake-up probe, the image by digest and the runtime flags"; exit 1; }
	@! grep -qE '^kind: ' $(CHART_TMP)/tria-spot-substrate.yaml || { echo "substrate renders no Kubernetes object: no Deployment, PVC, ServiceAccount, NetworkPolicy, Secret or ConfigMap"; exit 1; }
	@! grep -qE -- '--(anthropic-key-file|core-token-file|enroll-key-file|otel-token-file|gemini-key-file|connectors-file)|mountPath: /secrets' $(CHART_TMP)/tria-spot-substrate.yaml || { echo "substrate: an actor holds no credential and mounts no file but /state"; exit 1; }
	@grep -q 'core.example-core.svc' $(CHART_TMP)/tria-spot-substrate.yaml && sed -n '/^rules:/,$$p' $(CHART_TMP)/tria-spot-substrate.yaml | grep -q 'gateway.example-gateway.svc' && sed -n '/^rules:/,$$p' $(CHART_TMP)/tria-spot-substrate.yaml | grep -q -- '- https:' || { echo "substrate: the egress policy names the core, the gateway and each connector, http in the cluster and https outside"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/substrate-values.yaml --set llm.auth=key >/dev/null 2>&1 || { echo "substrate with a model key must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/substrate-values.yaml --set substrate.imageDigest= >/dev/null 2>&1 || { echo "substrate without an image digest must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/substrate-values.yaml --set core.identity=serviceAccount >/dev/null 2>&1 || { echo "substrate with the ServiceAccount identity must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set runtime=lambda >/dev/null 2>&1 || { echo "an unknown runtime must fail"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/telemetry-values.yaml --set telemetry.content=false | grep -q -- '--otel-content=false' || { echo "telemetry: telemetry.content=false reaches the flag"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/telemetry-values.yaml --set telemetry.tenant=example --set 'telemetry.tenantName=Example workspace' | grep -q -- '--otel-tenant-name="Example workspace"' || { echo "telemetry: a set tenant name reaches the flag"; exit 1; }
	@! grep -q -- '--model=' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q -- '--model=' $(CHART_TMP)/tria-spot-gmkey.yaml && grep -q -- '--model=gemini-3-pro-preview' $(CHART_TMP)/tria-spot-gmvertex.yaml || { echo "an empty llm.model passes no --model (the provider's default); a set one reaches the flag"; exit 1; }
	@grep -q -- '--llm-provider=anthropic' $(CHART_TMP)/tria-spot-key.yaml && grep -q -- '--llm-provider=anthropic' $(CHART_TMP)/tria-spot-vertex.yaml && grep -q -- '--llm-provider=gemini' $(CHART_TMP)/tria-spot-gmkey.yaml && ! grep -q -- '--gemini' $(CHART_TMP)/tria-spot-key.yaml || { echo "llm.provider reaches the flag, anthropic by default"; exit 1; }
	@grep -q -- '--llm-auth=key' $(CHART_TMP)/tria-spot-key.yaml && grep -q -- '--anthropic-key-file=/secrets/anthropic/key' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q -- '--vertex' $(CHART_TMP)/tria-spot-key.yaml || { echo "key mode: key file, no projected token, no Vertex flags"; exit 1; }
	@grep -q -- '--model=claude-opus-5-5' $(CHART_TMP)/tria-spot-keyset.yaml && grep -q -- '--effort=high' $(CHART_TMP)/tria-spot-keyset.yaml && grep -q -- '--max-steps=25' $(CHART_TMP)/tria-spot-keyset.yaml && grep -q -- '--tool-timeout=30s' $(CHART_TMP)/tria-spot-keyset.yaml && grep -q -- '--tool-result-max-bytes=16384' $(CHART_TMP)/tria-spot-keyset.yaml && grep -q -- '--anthropic-key-file=/secrets/anthropic/api-key' $(CHART_TMP)/tria-spot-keyset.yaml || { echo "the llm and agent values reach the flags"; exit 1; }
	@grep -q -- '--llm-auth=federation' $(CHART_TMP)/tria-spot-fed.yaml && grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-fed.yaml && ! grep -q -- '--anthropic-key-file' $(CHART_TMP)/tria-spot-fed.yaml && ! grep -q -- '--vertex' $(CHART_TMP)/tria-spot-fed.yaml && [ "$$(grep -c 'secretName:' $(CHART_TMP)/tria-spot-fed.yaml)" = 1 ] || { echo "federation mode: projected token, no key Secret"; exit 1; }
	@grep -q -- '--llm-auth=vertex' $(CHART_TMP)/tria-spot-vertex.yaml && grep -q -- '--vertex-project=example-project' $(CHART_TMP)/tria-spot-vertex.yaml && grep -q -- '--vertex-region=europe-west1' $(CHART_TMP)/tria-spot-vertex.yaml && ! grep -q -- '--anthropic' $(CHART_TMP)/tria-spot-vertex.yaml && ! grep -q 'anthropic.com' $(CHART_TMP)/tria-spot-vertex.yaml && ! grep -q 'secretName:' $(CHART_TMP)/tria-spot-vertex.yaml && grep -q 'iam.gke.io/gcp-service-account' $(CHART_TMP)/tria-spot-vertex.yaml && grep -q '199.36.153.8/30' $(CHART_TMP)/tria-spot-vertex.yaml && grep -q '169.254.169.254/32' $(CHART_TMP)/tria-spot-vertex.yaml && ! grep -q '160.79.104.0/23' $(CHART_TMP)/tria-spot-vertex.yaml || { echo "vertex mode: project and region, the pod's Workload Identity, Google egress, no Anthropic key or token"; exit 1; }
	@grep -q '160.79.104.0/23' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q '199.36.153.8/30' $(CHART_TMP)/tria-spot-key.yaml || { echo "key mode: egress to the Anthropic API only"; exit 1; }
	@grep -q -- '--llm-auth=key' $(CHART_TMP)/tria-spot-gmkey.yaml && grep -q -- '--gemini-key-file=/secrets/gemini/api-key' $(CHART_TMP)/tria-spot-gmkey.yaml && grep -q 'mountPath: /secrets/gemini' $(CHART_TMP)/tria-spot-gmkey.yaml && grep -q 'secretName: example-spot-gemini-key' $(CHART_TMP)/tria-spot-gmkey.yaml && ! grep -q -- '--anthropic' $(CHART_TMP)/tria-spot-gmkey.yaml && ! grep -q -- '--vertex' $(CHART_TMP)/tria-spot-gmkey.yaml && ! grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-gmkey.yaml && [ "$$(grep -c 'secretName:' $(CHART_TMP)/tria-spot-gmkey.yaml)" = 2 ] || { echo "gemini key mode: the key file and its Secret, no Anthropic or Vertex flags"; exit 1; }
	@grep -q '199.36.153.8/30' $(CHART_TMP)/tria-spot-gmkey.yaml && ! grep -q '160.79.104.0/23' $(CHART_TMP)/tria-spot-gmkey.yaml && ! grep -q '169.254.169.254/32' $(CHART_TMP)/tria-spot-gmkey.yaml || { echo "gemini key mode: egress to the Google APIs only, no metadata server"; exit 1; }
	@grep -q -- '--llm-auth=vertex' $(CHART_TMP)/tria-spot-gmvertex.yaml && grep -q -- '--vertex-project=example-project' $(CHART_TMP)/tria-spot-gmvertex.yaml && grep -q -- '--vertex-region=europe-west1' $(CHART_TMP)/tria-spot-gmvertex.yaml && ! grep -q -- '--gemini-key-file' $(CHART_TMP)/tria-spot-gmvertex.yaml && ! grep -q 'secretName:' $(CHART_TMP)/tria-spot-gmvertex.yaml && grep -q 'iam.gke.io/gcp-service-account' $(CHART_TMP)/tria-spot-gmvertex.yaml && grep -q '199.36.153.8/30' $(CHART_TMP)/tria-spot-gmvertex.yaml && grep -q '169.254.169.254/32' $(CHART_TMP)/tria-spot-gmvertex.yaml && ! grep -q '160.79.104.0/23' $(CHART_TMP)/tria-spot-gmvertex.yaml || { echo "gemini vertex mode: project and region, the pod's Workload Identity, Google egress, no key Secret"; exit 1; }
	@grep -q 'audience: triage-core' $(CHART_TMP)/tria-spot-sa.yaml && grep -q 'resourceNames:' $(CHART_TMP)/tria-spot-sa.yaml && grep -q 'triageagent.dev/managed: "true"' $(CHART_TMP)/tria-spot-sa.yaml && [ "$$(grep -c 'secretName:' $(CHART_TMP)/tria-spot-sa.yaml)" = 1 ] || { echo "serviceAccount identity: projected core token, Role with resourceNames, one Secret (the Anthropic key)"; exit 1; }
	@grep -q 'resourceNames:' $(CHART_TMP)/tria-spot-key.yaml && { echo "secret identity must render no core Role"; exit 1; } || true
	@! helm template t $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml --set core.identity=token >/dev/null 2>&1 || { echo "unknown core identity must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set core.tokenSecret.name= >/dev/null 2>&1 || { echo "secret identity needs a core Secret name"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml --set networkPolicy.telemetry.namespaceSelector=null --set networkPolicy.telemetry.podSelector=null >/dev/null 2>&1 || { echo "telemetry egress needs a target"; exit 1; }
	@! grep -q 'namespaceSelector: {}' $(CHART_TMP)/tria-spot-sa.yaml || { echo "an empty namespaceSelector would match every namespace"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set llm.federation.ruleId= >/dev/null 2>&1 || { echo "federation needs a rule id"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set llm.federation.orgId= >/dev/null 2>&1 || { echo "federation needs an organization id"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set llm.keySecret.name= >/dev/null 2>&1 || { echo "key mode needs a key Secret name"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/vertex-values.yaml --set llm.vertex.project= >/dev/null 2>&1 || { echo "vertex mode needs a project"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/vertex-values.yaml --set networkPolicy.google.cidrs=null >/dev/null 2>&1 || { echo "vertex mode needs egress addresses for the Google APIs"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/gemini-key-values.yaml --set networkPolicy.google.cidrs=null >/dev/null 2>&1 || { echo "the gemini provider needs egress addresses for the Google APIs"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/gemini-key-values.yaml --set llm.geminiKeySecret.name= >/dev/null 2>&1 || { echo "gemini key mode needs a key Secret name"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/gemini-key-values.yaml --set llm.auth=federation >/dev/null 2>&1 || { echo "federation is an Anthropic auth mode"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/gemini-vertex-values.yaml --set llm.vertex.project= >/dev/null 2>&1 || { echo "gemini vertex mode needs a project"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set llm.provider=openai >/dev/null 2>&1 || { echo "unknown provider must fail"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set llm.geminiKeySecret.name= >/dev/null || { echo "the Gemini key Secret is only needed for gemini key mode"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set llm.auth=oauth >/dev/null 2>&1 || { echo "unknown auth mode must fail"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set llm.keySecret.name= >/dev/null || { echo "federation must not need a key Secret"; exit 1; }
	@echo "chart ok"

.PHONY: check
check: lint test chart-lint ## Everything run before a commit
