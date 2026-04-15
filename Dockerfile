FROM golang:1.26-alpine AS builder

WORKDIR /src

ARG ENABLE_ZEROMQ=true

RUN apk add --no-cache build-base pkgconfig && \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then apk add --no-cache zeromq-dev; fi

ARG TARGETOS=linux
ARG TARGETARCH=amd64

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN set -eu; \
    BUILD_TAGS=""; \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then BUILD_TAGS="-tags zeromq"; fi; \
    CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build ${BUILD_TAGS} -trimpath -ldflags="-s -w" -o /out/turntf ./cmd/turntf

FROM alpine:3.22

ARG ENABLE_ZEROMQ=true

RUN apk add --no-cache ca-certificates tzdata && \
    if [ "${ENABLE_ZEROMQ}" = "true" ]; then apk add --no-cache zeromq; fi && \
    addgroup -S -g 10001 turntf && \
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
