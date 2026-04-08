FROM golang:1.25-alpine AS builder

ARG VERSION=main
ARG COMMIT=none
ARG DATE=unknown

WORKDIR /app

# Cache Go module downloads
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-mod \
    go mod download

# Cache Go build cache
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build \
    -ldflags "-s -w -X config.version=${VERSION} -X config.commit=${COMMIT} -X config.date=${DATE}" \
    -trimpath \
    -o /app/build/redka \
    cmd/redka/main.go

# Final stage
FROM alpine:3.21 AS runtime

RUN apk add --no-cache ca-certificates \
    && addgroup -S redka \
    && adduser -S redka -G redka \
    && mkdir -p /data /etc/redka /var/log \
    && chown -R redka:redka /data /etc/redka /var/log

WORKDIR /data

COPY --from=builder /app/build/redka /usr/local/bin/redka

# Default config file
COPY --chmod=644 <<'EOF' /etc/redka/redka.yaml
host: 0.0.0.0
port: 6379
db_dsn: "/data/redka.db"
password: ""
verbose: false
log_file: ""
EOF

EXPOSE 6379/tcp

VOLUME ["/data"]

USER redka

ENTRYPOINT ["redka"]
CMD ["-c", "/etc/redka/redka.yaml"]
