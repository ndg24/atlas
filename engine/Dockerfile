# Builds atlas-worker (docs/atlas-implementation-spec.md Phase 3). Build
# context must be the repo root (not engine/), since build.rs paths and the
# workspace both reach up to ../../../proto relative to each crate.
FROM rust:1.82-bookworm AS builder
WORKDIR /build
COPY proto/ proto/
COPY engine/ engine/
WORKDIR /build/engine
RUN cargo build --release -p atlas-worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/engine/target/release/atlas-worker /usr/local/bin/atlas-worker
EXPOSE 9100 9101
ENTRYPOINT ["/usr/local/bin/atlas-worker"]
