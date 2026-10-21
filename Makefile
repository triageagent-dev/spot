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

.PHONY: chart-lint
chart-lint: ## helm lint and helm template, flag off and flag on
	helm lint $(CHART)
	helm lint $(CHART) -f $(CHART)/ci/enabled-values.yaml
	@test -z "$$(helm template t $(CHART))" || { echo "spot.enabled=false must render nothing"; exit 1; }
	helm template t $(CHART) -f $(CHART)/ci/enabled-values.yaml > $(or $(TMPDIR),/tmp)/tria-spot-chart.yaml
	@! grep -qE '^[[:space:]]+env(From)?:' $(or $(TMPDIR),/tmp)/tria-spot-chart.yaml || { echo "the pod must carry no env"; exit 1; }
	@! grep -qE 'resources:.*secrets' $(or $(TMPDIR),/tmp)/tria-spot-chart.yaml || { echo "no RBAC rule may name secrets"; exit 1; }
	@echo "chart ok"

.PHONY: check
check: lint test chart-lint ## Everything run before a commit
