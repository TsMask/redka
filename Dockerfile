FROM golang:1.25-alpine AS builder

ARG VERSION=1.5.0
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
    -trimpath -o build/redka cmd/redka/main.go

# Final stage
FROM alpine:3.21 AS runtime

RUN apk add --no-cache ca-certificates && mkdir -p /usr/local/etc/redka /var/log

WORKDIR /usr/local/etc/redka

COPY --from=builder /redka/build/redka /usr/local/bin/redka
COPY --from=builder /redka/scripts/build/redka.yaml /usr/local/etc/redka/redka.yaml

EXPOSE 6379/tcp

ENTRYPOINT ["/usr/local/bin/redka"]
CMD ["-c", "/usr/local/etc/redka/redka.yaml"]

# Build the image
# docker build --build-arg VERSION=1.5.0 --build-arg COMMIT=$(git rev-parse --short HEAD) --build-arg DATE=$(date -u '+%Y-%m-%dT%H:%M:%S') -t redka:1.5.0 .
# docker run --rm -p 6380:6379 redka:1.5.0 -a hello1234

# Image
# docker save redka:1.5.0 -o build/redka_1.5.0-docker.tar
# docker load -i redka_1.5.0-docker.tar
