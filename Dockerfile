FROM golang:1.25-alpine AS builder

ARG VERSION=1.5.0
ARG COMMIT=none
ARG DATE=unknown

WORKDIR /redka

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY config ./config
COPY internal ./internal
COPY server ./server
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w \
        -X github.com/tsmask/redka/config.Version=${VERSION} \
        -X github.com/tsmask/redka/config.Commit=${COMMIT} \
        -X github.com/tsmask/redka/config.Date=${DATE}" \
    -trimpath -o /out/redka ./cmd/redka

FROM alpine:3.21 AS runtime

RUN apk add --no-cache ca-certificates

WORKDIR /usr/local/etc/redka

COPY --from=builder /out/redka /usr/local/bin/redka
COPY scripts/build/redka.yaml /usr/local/etc/redka/redka.yaml
COPY LICENSE /usr/share/licenses/redka/LICENSE
COPY NOTICE /usr/share/licenses/redka/NOTICE
COPY THIRD_PARTY_LICENSES.md /usr/share/licenses/redka/THIRD_PARTY_LICENSES.md

EXPOSE 6379/tcp 6360/tcp

ENTRYPOINT ["/usr/local/bin/redka"]
CMD []

# Build:
# docker build --build-arg VERSION=1.5.0 \
#   --build-arg COMMIT=$(git rev-parse --short HEAD) \
#   --build-arg DATE=$(date -u '+%Y-%m-%dT%H:%M:%S') \
#   -t redka:1.5.0 .
#
# Run with the built-in default configuration:
# docker run --rm -p 6380:6379 \
#   redka:1.5.0
#
# Or run with a mounted configuration file:
# docker run --rm -p 6380:6379 \
#   -v "$PWD/redka.yaml:/etc/redka/redka.yaml:ro" \
#   redka:1.5.0 -c /etc/redka/redka.yaml
