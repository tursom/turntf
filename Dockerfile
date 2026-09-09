# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine3.22 AS builder

WORKDIR /src

ARG ENABLE_ZEROMQ=true
ARG TARGETOS
ARG TARGETARCH

# Alpine's libzmq package lacks the draft C ABI required by ROUTER_NOTIFY.
# Build and ship the same draft-enabled library, with libsodium for CURVE.
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
    mkdir -p /out/lib; \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then \
        retry apk add --no-cache curl libsodium-dev; \
        retry curl -fsSL https://github.com/zeromq/libzmq/releases/download/v4.3.5/zeromq-4.3.5.tar.gz -o /tmp/zeromq.tar.gz; \
        echo '6653ef5910f17954861fe72332e68b03ca6e4d9c7160eb3a8de5a5a913bfab43  /tmp/zeromq.tar.gz' | sha256sum -c -; \
        tar -xzf /tmp/zeromq.tar.gz -C /tmp; \
        cd /tmp/zeromq-4.3.5; \
        ./configure --prefix=/usr/local --enable-drafts --disable-static --with-libsodium --without-docs; \
        make -j"$(getconf _NPROCESSORS_ONLN)"; \
        make install; \
        cp -a /usr/local/lib/libzmq.so* /out/lib/; \
        rm -rf /tmp/zeromq-4.3.5 /tmp/zeromq.tar.gz; \
    fi

ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

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
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then retry apk add --no-cache libstdc++ libsodium; fi

WORKDIR /app

COPY --from=builder /out/turntf /usr/local/bin/turntf
COPY --from=builder /out/lib/ /usr/local/lib/

RUN mkdir -p /app/data

EXPOSE 8080
VOLUME ["/app/data"]

ENTRYPOINT ["/usr/local/bin/turntf"]
CMD ["serve", "--config", "/app/config.toml"]
