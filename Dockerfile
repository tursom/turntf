# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine3.22 AS builder

WORKDIR /src

ARG ENABLE_ZEROMQ=true
ARG TARGETOS
ARG TARGETARCH

RUN set -eu; \
    retry() { \
        n=0; \
        until "$@"; do \
            n=$((n + 1)); \
            if [ "$n" -ge 5 ]; then \
                return 1; \
            fi; \
            sleep $((n * 2)); \
        done; \
    }; \
    retry apk add --no-cache build-base pkgconfig; \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then retry apk add --no-cache zeromq-dev; fi

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    retry() { \
        n=0; \
        until "$@"; do \
            n=$((n + 1)); \
            if [ "$n" -ge 5 ]; then \
                return 1; \
            fi; \
            sleep $((n * 2)); \
        done; \
    }; \
    retry go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY proto ./proto

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    BUILD_TAGS=""; \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then BUILD_TAGS="-tags zeromq"; fi; \
    CGO_ENABLED=1 CGO_CFLAGS="-D_LARGEFILE64_SOURCE -D_GNU_SOURCE" GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build ${BUILD_TAGS} -trimpath -ldflags="-s -w" -o /out/turntf ./cmd/turntf

FROM alpine:3.22

ARG ENABLE_ZEROMQ=true

RUN set -eu; \
    retry() { \
        n=0; \
        until "$@"; do \
            n=$((n + 1)); \
            if [ "$n" -ge 5 ]; then \
                return 1; \
            fi; \
            sleep $((n * 2)); \
        done; \
    }; \
    retry apk add --no-cache ca-certificates tzdata; \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then retry apk add --no-cache zeromq; fi; \
    addgroup -S -g 10001 turntf; \
    adduser -S -D -H -h /app -u 10001 -G turntf turntf

WORKDIR /app

COPY --from=builder /out/turntf /usr/local/bin/turntf

RUN mkdir -p /app/data && \
    chown -R turntf:turntf /app

USER turntf:turntf

EXPOSE 8080
VOLUME ["/app/data"]

ENTRYPOINT ["/usr/local/bin/turntf"]
CMD ["serve", "--config", "/app/config.toml"]
