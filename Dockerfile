FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache build-base pkgconfig

ARG TARGETOS=linux
ARG TARGETARCH=amd64

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S -g 10001 notifier \
 && adduser -S -D -H -h /app -u 10001 -G notifier notifier

WORKDIR /app

COPY --from=builder /out/notifier /usr/local/bin/notifier

RUN mkdir -p /app/data \
 && chown -R notifier:notifier /app

USER notifier:notifier

EXPOSE 8080
VOLUME ["/app/data"]

ENTRYPOINT ["/usr/local/bin/notifier"]
CMD ["serve", "-config", "/app/config.toml"]
