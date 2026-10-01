# syntax=docker/dockerfile:1.4
FROM golang:alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Build metadata-svc binary
FROM builder AS build-metadata
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/metadata-svc ./cmd/metadata-svc && \
    mkdir -p /data && chown -R 65532:65532 /data

# Build data-svc binary
FROM builder AS build-data
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/data-svc ./cmd/data-svc && \
    mkdir -p /data && chown -R 65532:65532 /data

# Final image: metadata-svc
FROM gcr.io/distroless/static-debian12:nonroot AS metadata-svc
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build-metadata --chown=65532:65532 /data /data
COPY --from=build-metadata /bin/metadata-svc /bin/metadata-svc
USER nonroot:nonroot
VOLUME ["/data"]
EXPOSE 9090 9091
ENTRYPOINT ["/bin/metadata-svc"]

# Final image: data-svc
FROM gcr.io/distroless/static-debian12:nonroot AS data-svc
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build-data --chown=65532:65532 /data /data
COPY --from=build-data /bin/data-svc /bin/data-svc
USER nonroot:nonroot
VOLUME ["/data"]
EXPOSE 9101
ENTRYPOINT ["/bin/data-svc"]
