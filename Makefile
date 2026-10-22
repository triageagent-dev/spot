# tria-spot
.DEFAULT_GOAL := help

BIN         := bin/tria-spot
PKG         := ./...
REGISTRY    ?= ghcr.io/triageagent-dev
IMAGE       ?= $(REGISTRY)/tria-spot
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

.PHONY: image
image: ## Build the container image (does not push)
	docker build \
	  --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -t $(IMAGE):$(VERSION) .

CHART_TMP := $(or $(TMPDIR),/tmp)

.PHONY: chart-lint
chart-lint: ## helm lint and helm template: flag off, key mode, federation mode
	helm lint $(CHART)
	helm lint $(CHART) -f $(CHART)/ci/enabled-values.yaml
	helm lint $(CHART) -f $(CHART)/ci/federation-values.yaml
	@test -z "$$(helm template t $(CHART))" || { echo "spot.enabled=false must render nothing"; exit 1; }
	helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml > $(CHART_TMP)/tria-spot-key.yaml
	helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml > $(CHART_TMP)/tria-spot-fed.yaml
	@for f in $(CHART_TMP)/tria-spot-key.yaml $(CHART_TMP)/tria-spot-fed.yaml; do \
	  ! grep -qE '^[[:space:]]+env(From)?:' $$f || { echo "the pod must carry no env ($$f)"; exit 1; }; \
	  ! grep -qE 'resources:.*secrets' $$f || { echo "no RBAC rule may name secrets ($$f)"; exit 1; }; \
	done
	@grep -q -- '--anthropic-key-file=' $(CHART_TMP)/tria-spot-key.yaml && ! grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-key.yaml || { echo "key mode: key file, no projected token"; exit 1; }
	@grep -q 'serviceAccountToken' $(CHART_TMP)/tria-spot-fed.yaml && ! grep -q -- '--anthropic-key-file' $(CHART_TMP)/tria-spot-fed.yaml && [ "$$(grep -c 'secretName:' $(CHART_TMP)/tria-spot-fed.yaml)" = 1 ] || { echo "federation mode: projected token, no key Secret"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set anthropic.federation.ruleId= >/dev/null 2>&1 || { echo "federation needs a rule id"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set anthropic.keySecret.name= >/dev/null 2>&1 || { echo "key mode needs a key Secret name"; exit 1; }
	@! helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml --set anthropic.auth=oauth >/dev/null 2>&1 || { echo "unknown auth mode must fail"; exit 1; }
	@helm template t $(CHART) -f $(CHART)/ci/federation-values.yaml --set anthropic.keySecret.name= >/dev/null || { echo "federation must not need a key Secret"; exit 1; }
	@echo "chart ok"

.PHONY: check
check: lint test chart-lint ## Everything run before a commit
