# Builds both the coordinator and catalog binaries (docs/atlas-implementation-spec.md
# Phase 2/3) — same Go module, so one image serves both; docker-compose picks
# which binary a given service runs via `entrypoint:`.
FROM golang:1.24-bookworm AS builder
WORKDIR /build/coordinator
COPY coordinator/ .
RUN go build -o /out/coordinator ./cmd/coordinator \
    && go build -o /out/catalog ./cmd/catalog

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/coordinator /usr/local/bin/coordinator
COPY --from=builder /out/catalog /usr/local/bin/catalog
ENTRYPOINT ["/usr/local/bin/coordinator"]
