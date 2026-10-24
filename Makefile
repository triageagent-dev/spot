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

# The image is built with ko only. The Claude Code CLI and jq are fetched into
# kodata, each checked against a pinned digest, and ko adds them to the image
# under /var/run/ko (KO_DATA_PATH). README.md says where each pin came from.
# Both are linux/amd64. There is no kubectl in the image: a cluster is reached
# through connectors.
KODATA                ?= cmd/tria-spot/kodata
CLAUDE_CODE_VERSION   ?= 2.1.304
CLAUDE_CODE_INTEGRITY ?= sha512-yZ/zZk3y7e0zbkMs740y9Z+bs40MmWApwM9qHQ8Adis01SycQlyaZnHYhB/+UOeFJcL+hzbiIGCIyuTKyL2U7Q==
JQ_VERSION            ?= 1.7.1
JQ_SHA256             ?= 5942c9b0934e510ee61eb3e30273f1b3fe2590df93933a93d7c58b81d19c8ff5

.PHONY: kodata
kodata: ## Fetch and verify the CLI and jq into cmd/tria-spot/kodata
	@mkdir -p $(KODATA)
	@# ko packs the whole directory: anything an earlier fetch left (a kubectl) would ship.
	@find $(KODATA) -mindepth 1 ! -name .gitkeep ! -name claude ! -name jq -exec rm -rf {} +
	@set -eu; tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	 (cd "$$tmp" && npm pack "@anthropic-ai/claude-code-linux-x64@$(CLAUDE_CODE_VERSION)" --silent >/dev/null); \
	 got="sha512-$$(openssl dgst -sha512 -binary "$$tmp"/*.tgz | base64 | tr -d '\n')"; \
	 [ "$$got" = "$(CLAUDE_CODE_INTEGRITY)" ] || { echo "claude-code tarball digest $$got != pinned $(CLAUDE_CODE_INTEGRITY)"; exit 1; }; \
	 tar -xzf "$$tmp"/*.tgz -C "$$tmp" package/claude; \
	 install -m 0755 "$$tmp/package/claude" $(KODATA)/claude; \
	 curl -fsSL -o "$$tmp/jq" "https://github.com/jqlang/jq/releases/download/jq-$(JQ_VERSION)/jq-linux-amd64"; \
	 echo "$(JQ_SHA256)  $$tmp/jq" | shasum -a 256 -c -; \
	 install -m 0755 "$$tmp/jq" $(KODATA)/jq
	@test "$$(ls -A $(KODATA) | grep -v '^\.gitkeep$$' | sort | tr '\n' ' ')" = "claude jq " || { echo "kodata holds more than claude and jq:"; ls -A $(KODATA); exit 1; }
	@echo "kodata ready: claude $(CLAUDE_CODE_VERSION), jq $(JQ_VERSION)"

.PHONY: image
image: kodata ## Build the image as an OCI layout in bin/image (pushes nothing)
	VERSION=$(VERSION) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) \
	  KO_DOCKER_REPO=$(IMAGE) ko build --bare --push=false --oci-layout-path=bin/image ./cmd/tria-spot

.PHONY: push
push: kodata ## Build and push the image with ko: make push IMAGE=<registry>/<repo>
	VERSION=$(VERSION) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) \
	  KO_DOCKER_REPO=$(IMAGE) ko build --bare --tags=$(VERSION) ./cmd/tria-spot

CHART_TMP := $(or $(TMPDIR),/tmp)

.PHONY: chart-lint
chart-lint: ## helm lint and helm template: flag off, ServiceAccount identity, key mode, federation mode
	helm lint $(CHART)
	helm lint $(CHART) -f $(CHART)/ci/enabled-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/federation-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/telemetry-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/connectors-values.yaml
	@test -z "$$(helm template t $(CHART))" || { echo "spot.enabled=false must render nothing"; exit 1; }
	helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml > $(CHART_TMP)/tria-spot-key.yaml
	helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml > $(CHART_TMP)/tria-spot-fed.yaml
	helm template t $(CHART) -f $(CHART)/ci/telemetry-values.yaml > $(CHART_TMP)/tria-spot-tel.yaml
	helm template t $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml > $(CHART_TMP)/tria-spot-sa.yaml
	helm template t $(CHART) -f $(CHART)/ci/connectors-values.yaml > $(CHART_TMP)/tria-spot-conn.yaml
	@grep -q -- '--connectors-file=' $(CHART_TMP)/tria-spot-conn.yaml && grep -q 'connectors.json' $(CHART_TMP)/tria-spot-conn.yaml && grep -q 'Connector example-kube' $(CHART_TMP)/tria-spot-conn.yaml && grep -q -- '--persistent' $(CHART_TMP)/tria-spot-conn.yaml && ! grep -q -- '--persistent' $(CHART_TMP)/tria-spot-key.yaml || { echo "connectors: the flag, a ConfigMap and an egress rule each"; exit 1; }
	@! sed -n '/kind: ConfigMap/,/^---/p' $(CHART_TMP)/tria-spot-conn.yaml | grep -qiE 'token|authorization|secret' || { echo "the connectors ConfigMap must hold no credential"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/connectors-values.yaml --set 'connectors[0].tools=null' >/dev/null 2>&1 || { echo "a connector without named tools must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/connectors-values.yaml --set 'connectors[0].egress=null' >/dev/null 2>&1 || { echo "a connector without an egress target must fail"; exit 1; }
	@for f in $(CHART_TMP)/tria-spot-key.yaml $(CHART_TMP)/tria-spot-fed.yaml $(CHART_TMP)/tria-spot-tel.yaml $(CHART_TMP)/tria-spot-sa.yaml $(CHART_TMP)/tria-spot-conn.yaml; do \
	  ! grep -qE '^[[:space:]]+env(From)?:' $$f || { echo "the pod must carry no env ($$f)"; exit 1; }; \
	  ! grep -qE 'resources:.*secrets' $$f || { echo "no RBAC rule may name secrets ($$f)"; exit 1; }; \
	  ! grep -qE 'kind: (ClusterRole|ClusterRoleBinding)' $$f || { echo "the spot holds no cluster RBAC; a cluster is reached through a connector ($$f)"; exit 1; }; \
	  grep -q 'automountServiceAccountToken: false' $$f || { echo "the ServiceAccount token must not be mounted ($$f)"; exit 1; }; \
	done
	@grep -q -- '--otel-tenant-attr=triage.tenant' $(CHART_TMP)/tria-spot-tel.yaml && ! grep -q -- '--otel-tenant=' $(CHART_TMP)/tria-spot-tel.yaml && ! grep -q -- '--otel-tenant-attr' $(CHART_TMP)/tria-spot-key.yaml || { echo "telemetry: the label attribute is always passed, the label itself only when set (default: the spot's name)"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/telemetry-values.yaml --set telemetry.tenant=example --set 'telemetry.tenantName=Example workspace' | grep -q -- '--otel-tenant-name="Example workspace"' || { echo "telemetry: a set tenant name reaches the flag"; exit 1; }
	@grep -q -- '--anthropic-key-file=' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-key.yaml || { echo "key mode: key file, no projected token"; exit 1; }
	@grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-fed.yaml && ! grep -q -- '--anthropic-key-file' $(CHART_TMP)/tria-spot-fed.yaml && [ "$$(grep -c 'secretName:' $(CHART_TMP)/tria-spot-fed.yaml)" = 1 ] || { echo "federation mode: projected token, no key Secret"; exit 1; }
	@grep -q 'audience: triage-core' $(CHART_TMP)/tria-spot-sa.yaml && grep -q 'resourceNames:' $(CHART_TMP)/tria-spot-sa.yaml && grep -q 'triageagent.dev/managed: "true"' $(CHART_TMP)/tria-spot-sa.yaml && [ "$$(grep -c 'secretName:' $(CHART_TMP)/tria-spot-sa.yaml)" = 1 ] || { echo "serviceAccount identity: projected core token, Role with resourceNames, one Secret (the Anthropic key)"; exit 1; }
	@grep -q 'resourceNames:' $(CHART_TMP)/tria-spot-key.yaml && { echo "secret identity must render no core Role"; exit 1; } || true
	@! helm template t $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml --set core.identity=token >/dev/null 2>&1 || { echo "unknown core identity must fail"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set core.tokenSecret.name= >/dev/null 2>&1 || { echo "secret identity needs a core Secret name"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/serviceaccount-values.yaml --set networkPolicy.telemetry.namespaceSelector=null --set networkPolicy.telemetry.podSelector=null >/dev/null 2>&1 || { echo "telemetry egress needs a target"; exit 1; }
	@! grep -q 'namespaceSelector: {}' $(CHART_TMP)/tria-spot-sa.yaml || { echo "an empty namespaceSelector would match every namespace"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set anthropic.federation.ruleId= >/dev/null 2>&1 || { echo "federation needs a rule id"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set anthropic.keySecret.name= >/dev/null 2>&1 || { echo "key mode needs a key Secret name"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set anthropic.auth=oauth >/dev/null 2>&1 || { echo "unknown auth mode must fail"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set anthropic.keySecret.name= >/dev/null || { echo "federation must not need a key Secret"; exit 1; }
	@echo "chart ok"

.PHONY: check
check: lint test chart-lint ## Everything run before a commit
