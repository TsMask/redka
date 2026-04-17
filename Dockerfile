FROM golang:1.25-alpine AS builder

ARG VERSION=main
ARG COMMIT=none
ARG DATE=unknown

WORKDIR /redka

COPY . .
RUN go mod download

# Build the application
RUN go build \
    -ldflags "-s -w \
       	-X github.com/tsmask/redka/config.Version=${VERSION} \
       	-X github.com/tsmask/redka/config.Commit=${COMMIT} \
       	-X github.com/tsmask/redka/config.Date=${DATE}" \
    -trimpath -o redka main.go

# Final stage
FROM alpine:3.21 AS runtime

RUN apk add --no-cache ca-certificates && mkdir -p /usr/local/etc/redka /var/log

WORKDIR /usr/local/etc/redka

COPY --from=builder /redka/redka /usr/local/bin/redka
COPY --from=builder /redka/scripts/redka.yaml /usr/local/etc/redka/redka.yaml

EXPOSE 6379/tcp

ENTRYPOINT ["/usr/local/bin/redka"]
CMD ["-c", "/usr/local/etc/redka/redka.yaml"]

# Build the image
# docker build --build-arg VERSION=2.0.0 --build-arg COMMIT=$(git rev-parse --short HEAD) --build-arg DATE=$(date -u '+%Y-%m-%dT%H:%M:%S') -t redka .
# docker run --rm -p 6380:6379 redka -a hello1234
