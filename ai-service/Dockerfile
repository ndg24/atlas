# Builds the ai-service gRPC server (docs/atlas-implementation-spec.md
# Phase 6). Build context must be the repo root (not ai-service/), matching
# engine/Dockerfile's convention, since generated code under atlas_ai/pb/ is
# committed the same way the Rust/Go .pb.go equivalents are (see
# proto/generate.sh) rather than regenerated at image-build time.
FROM python:3.12-slim AS builder
RUN pip install --no-cache-dir uv
WORKDIR /build/ai-service
COPY ai-service/pyproject.toml .
COPY ai-service/atlas_ai/ atlas_ai/
RUN uv sync --no-dev

FROM python:3.12-slim
COPY --from=builder /build/ai-service /app
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 9092
ENTRYPOINT ["python", "-m", "atlas_ai.server"]
