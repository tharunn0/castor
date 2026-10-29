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
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/tharunn0/castor/internal/version.Version=${VERSION} -X github.com/tharunn0/castor/internal/version.Commit=${COMMIT}" \
    -o /bin/metadata-svc ./cmd/metadata-svc && \
    mkdir -p /data && chown -R 65532:65532 /data

# Build data-svc binary
FROM builder AS build-data
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/tharunn0/castor/internal/version.Version=${VERSION} -X github.com/tharunn0/castor/internal/version.Commit=${COMMIT}" \
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
EXPOSE 9090 9091 9096
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
EXPOSE 9101 9104
ENTRYPOINT ["/bin/data-svc"]

# Build gateway-svc binary
FROM builder AS build-gateway
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/tharunn0/castor/internal/version.Version=${VERSION} -X github.com/tharunn0/castor/internal/version.Commit=${COMMIT}" \
    -o /bin/gateway-svc ./cmd/gateway-svc

# Final image: gateway-svc
FROM gcr.io/distroless/static-debian12:nonroot AS gateway-svc
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build-gateway /bin/gateway-svc /bin/gateway-svc
USER nonroot:nonroot
EXPOSE 9000 9001
ENTRYPOINT ["/bin/gateway-svc"]

# Build auth-svc binary
FROM builder AS build-auth
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/tharunn0/castor/internal/version.Version=${VERSION} -X github.com/tharunn0/castor/internal/version.Commit=${COMMIT}" \
    -o /bin/auth-svc ./cmd/auth-svc

# Final image: auth-svc
FROM gcr.io/distroless/static-debian12:nonroot AS auth-svc
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build-auth /bin/auth-svc /bin/auth-svc
USER nonroot:nonroot
EXPOSE 9095
ENTRYPOINT ["/bin/auth-svc"]

