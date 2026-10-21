# syntax=docker/dockerfile:1.7
#
# tria-spot image: the Go supervisor plus the Claude Code CLI, kubectl and jq.
# Build with `make image`. Every version is an ARG; the README says where each
# value came from and how it was checked.

ARG GO_VERSION=1.27.3
ARG NODE_VERSION=24.22.0

FROM golang:${GO_VERSION}-bookworm AS build
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT} -X main.buildDate=${BUILD_DATE}" \
      -o /out/tria-spot ./cmd/tria-spot

FROM node:${NODE_VERSION}-bookworm-slim
# The Claude Code CLI, pinned. The integrity value is the registry's
# dist.integrity for that exact version; the build fails if the registry
# serves anything else.
ARG CLAUDE_CODE_VERSION=2.1.304
ARG CLAUDE_CODE_INTEGRITY=sha512-oAD2KLEUdAbsAMxQMWYHq/AHCUlkYuZ80ovkL0mVSy07z+EeDhIo19/XpRA2NVI+9/LZCC+NXCJm+/hIvhNWWQ==
# kubectl, pinned, checked against the published sha256 for each architecture.
ARG KUBECTL_VERSION=v1.37.1
ARG KUBECTL_SHA256_AMD64=65691ff77eb6fa44c908b77a1082c9f092c3b9733b5cefabec0d1104890e21a8
ARG KUBECTL_SHA256_ARM64=ff749f4b78d9c4f1ec87307df9b50119ed819e2094aa9810cb9acffc3286c8c7
ARG TARGETARCH

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl jq procps \
 && rm -rf /var/lib/apt/lists/*

RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
      amd64) sum="${KUBECTL_SHA256_AMD64}" ;; \
      arm64) sum="${KUBECTL_SHA256_ARM64}" ;; \
      *) echo "unsupported architecture ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH:-amd64}/kubectl"; \
    echo "${sum}  /usr/local/bin/kubectl" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/kubectl

RUN set -eu; \
    got="$(npm view "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" dist.integrity)"; \
    if [ "${got}" != "${CLAUDE_CODE_INTEGRITY}" ]; then \
      echo "claude-code ${CLAUDE_CODE_VERSION}: registry integrity ${got} != pinned ${CLAUDE_CODE_INTEGRITY}" >&2; exit 1; \
    fi; \
    npm install -g --no-audit --no-fund "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}"; \
    npm cache clean --force; \
    claude --version

COPY --from=build /out/tria-spot /usr/local/bin/tria-spot

# A fixed non-root user. The root filesystem can be read-only: everything the
# process writes goes to /state (a volume), /run/tria-spot (memory-backed) or
# /tmp (an emptyDir).
RUN groupadd --gid 10001 spot \
 && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /state --shell /usr/sbin/nologin spot \
 && mkdir -p /state /run/tria-spot \
 && chown 10001:10001 /state /run/tria-spot
USER 10001:10001
WORKDIR /state
VOLUME ["/state", "/run/tria-spot"]

# No credential is baked in or read from the environment: the supervisor takes
# file paths (--core-token-file, --anthropic-key-file).
ENTRYPOINT ["/usr/local/bin/tria-spot"]
