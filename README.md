# Atlas

A distributed analytics engine that turns SQL, natural language, and open-ended research questions into query results — a Rust storage/query engine, a Go coordinator, a Python AI service, and a Next.js dashboard, all sharing one query IR and reading/writing an open columnar format alongside native Parquet read/write (Iceberg interop planned).

## Overview

Data gets ingested into Atlas's own `.atlas` columnar format (or Parquet), split into Hive-style partitions, and registered as an immutable snapshot in a Postgres-backed catalog. A query — whether it arrives as SQL, natural language, or a multi-step research question — is compiled into the same `LogicalPlan` protobuf before it touches anything else. A rule-based optimizer rewrites that plan (column pruning, predicate and partition pushdown, cache lookup), the Go coordinator fans the resulting tasks out across a fleet of Rust worker processes over gRPC, and workers stream Arrow IPC batches back to be merged — a two-phase combine for aggregates, a k-way merge for sorts. Results come back through a REST API to the CLI, SDKs, or the dashboard, optionally narrated in plain English or bundled into a research report that cites structured results and retrieved literature separately. No layer downstream of the logical plan cares whether a query started as SQL or a sentence.

## Features

- **Columnar storage with real column pruning** — the `.atlas` format (page-per-column, protobuf footer, min/max/null-count statistics per column) is read by seeking straight to the byte ranges of the requested columns, so scanning 2 of 20 columns only ever touches those 2 columns' pages. Parquet is a first-class alternative format on the same ingestion path (`atlas-cli ingest --format parquet`) — a dataset's manifests carry a `format` field so a per-file worker dispatch can pick the right reader, and a single dataset's manifests can already mix `.atlas` and Parquet files. Iceberg (and Delta) tables created by other engines being queryable as external tables is planned, extending the same `format` field rather than requiring another migration.
- **Immutable, snapshotted metadata catalog** — every ingest commits a new snapshot (Postgres-backed: `datasets` / `snapshots` / `manifests` / `query_history`) in a single transaction, so a crash mid-commit never leaves the catalog pointing at a half-written snapshot. Queries always resolve against `current_snapshot_id`, giving every query a consistent, point-in-time view of the data.
- **Distributed, fault-tolerant execution** — the coordinator schedules one task per manifest/partition across registered workers (heartbeat-tracked, least-loaded assignment), streams partial results back over gRPC, and merges them per plan shape — a second aggregation pass for `GROUP BY`, a k-way merge for `ORDER BY` + `LIMIT`. A task whose worker misses its heartbeat or errors mid-stream is retried on a different live worker (up to 3 attempts) without failing the query.
- **Rule-based query optimization** — column pruning and predicate pushdown rewrite the logical plan itself; partition pruning drops whole manifests using their partition values and column statistics before scheduling; a Redis-backed result cache is keyed on the hash of the *optimized* plan plus the dataset's snapshot id, so equivalent queries hit cache and a new ingest invalidates exactly the datasets it touched. `POST /explain` surfaces the plan before and after optimization, plus which manifests survived pruning and whether the result was served from cache.
- **Natural-language querying, same execution path as SQL** — an LLM (Anthropic, OpenAI, Gemini, or a local Ollama model, selected purely by environment variable through a provider-agnostic layer) compiles a question into the identical `LogicalPlan` a SQL query would produce, validated against the protobuf schema and re-prompted once on failure. From there it runs through the unchanged optimize → schedule → execute path — an NL query and its SQL equivalent return byte-identical results.
- **AI analyst** — dataset summaries, outlier detection, trend detection, and data-quality flags (null rates, zero-variance columns, duplicates) are computed as plain statistical functions over query results, not invented by an LLM; the LLM's only job is narrating those pre-computed findings into readable sentences, one claim per input finding. Suggested example questions are only ever shown after they've been round-tripped through the NL compiler and confirmed to produce a valid, runnable plan.
- **Multi-agent research mode** — a sequential Planner → Query → Execution → Visualization → Explanation → Report pipeline decomposes an open-ended question into structured sub-questions (run through the existing query engine) and literature sub-questions (answered via `pgvector`-backed retrieval over an ingested corpus). The final report tags every claim `[data]` or `[literature:doc_id]`, so structured results and retrieved literature are never blended without attribution.
- **Observability and auth from day one of the API surface** — OpenTelemetry traces a request from the REST entry point through gRPC calls to workers and the AI service under one trace id; Prometheus tracks worker task duration, cache hit rate, and LLM latency/token counts; JWT-based auth with workspace scoping guards the coordinator's REST API.

## Architecture & Design Decisions

- **One shared `LogicalPlan` IR for every query source** — SQL and natural-language queries both compile to the same protobuf-defined plan (`proto/plan.proto`), with Rust (`prost`), Go, and Python (`grpcio-tools`) structs generated from it rather than hand-written in parallel per language. This is what lets the NL path reuse the SQL path's optimizer and executor completely unchanged.
- **Arrow as the type system, not a custom one** — schema types are Arrow's own (`arrow::datatypes::{DataType, Field, Schema}`) re-exported directly; a parallel type system would only ever mirror Arrow's.
- **The `.atlas` format is deliberately Parquet/Iceberg-shaped** — footer-plus-pages-plus-statistics, Hive-style partition paths — so that Phase 5's Parquet/Iceberg interop is an integration exercise against `parquet-rs`/`iceberg-rust`, not a fight against an incompatible internal format. Reading an external Iceberg table means translating its manifests into Atlas's own `Manifest` struct once, after which pruning, scheduling, and execution don't know or care that the source wasn't Atlas's own writer.
- **Catalog commits are transactional, not eventually consistent** — inserting a snapshot row, its manifests, and updating `datasets.current_snapshot_id` all happen in one Postgres transaction, so queries never observe a partially-committed snapshot.
- **Coordinator and catalog are separate services even though they're usually co-deployed** — the catalog is passive metadata storage; the coordinator is an active scheduler. Keeping them as separate binaries keeps that distinction real rather than nominal.
- **Two-phase distributed aggregation over shuffling raw rows** — each worker computes a partial aggregate for its partition; the coordinator combines partials into the final result, so `GROUP BY` over a distributed scan never requires moving raw rows between workers.
- **Optimizer is a fixed-point rule loop, not a cost-based planner** — column pruning, predicate pushdown, and partition pruning are applied repeatedly until the plan stops changing (capped at a fixed iteration count to guarantee termination). There's no cardinality-estimating cost model yet; the rule set targets the pruning wins that matter most before a query ever reaches a worker.
- **Cache key is the optimized plan's hash plus the snapshot id, not the raw SQL string** — two differently-worded but equivalent queries share a cache entry once optimized, and a cache entry is treated as stale the instant its dataset's `current_snapshot_id` advances, rather than requiring an explicit invalidation sweep on every ingest.
- **New file formats are thin wrappers over existing crates, not reimplementations** — Parquet and Iceberg support lean on `parquet-rs` and `iceberg-rust` for the actual format logic; Atlas's own code is the translation layer that lets pruning, scheduling, and execution treat every format identically once a file's manifest is loaded.
- **LLM access is provider-agnostic by construction** — every model call goes through one `litellm`-backed adapter selected via `ATLAS_LLM_PROVIDER` / `ATLAS_LLM_MODEL`, so switching between Anthropic, OpenAI, Gemini, or a local Ollama model is an environment variable change, never a code branch.
- **The engine is the source of truth; the AI service never sees raw data** — the AI service only ever receives already-executed result sets (Arrow IPC) or structured statistical findings, never the underlying dataset. Every narrated explanation is checked against the numbers it was given, so an explanation can't state a figure that didn't come from an actual query result.
- **Suggested questions and insights are grounded, not purely generative** — a suggested example question is only surfaced after it's been compiled to a plan and confirmed runnable; insight narration is constrained to one sentence per one structured finding object, so there's no free-floating LLM-invented statistic anywhere in the analyst output.
- **The research pipeline is a plain sequential agent chain, not a graph-orchestration framework** — `Planner → Query → Execution → Visualization → Explanation → Report` runs in fixed order over a typed, accumulating state object. Query and Execution agents are thin wrappers around the existing NL-compile-and-execute path rather than new query logic, and every agent's input/output is logged so the pipeline stays inspectable without needing branching or looping machinery that nothing here actually requires.
- **Research claims are attributed at the sentence level** — every claim in a generated report is tagged `[data]` (traceable to an Execution-agent result) or `[literature:doc_id]` (traceable to a specific retrieved document), so structured findings and retrieved literature are never merged into an unattributed claim.

## Tech Stack

| Layer | Tech |
|---|---|
| Engine (Rust) | `arrow` / `arrow-csv`, `sqlparser`, `parquet-rs` (`iceberg-rust`/`delta-rs` planned), `lz4_flex` / `zstd`, `object_store`, `tonic` (gRPC), `clap` |
| Coordinator (Go) | Go 1.22+, `pgx` (Postgres), `go-redis`, `golang-migrate`/`goose`, gRPC + REST handlers, `testcontainers-go` for integration tests |
| AI service (Python) | Python 3.11+, `litellm` (Anthropic/OpenAI/Gemini/Ollama behind one adapter), `grpc.aio`, `pyarrow`, `pgvector`, Pydantic |
| Metadata, cache & retrieval | Postgres (catalog: datasets/snapshots/manifests/query_history), Redis (result cache), `pgvector` (literature embeddings) |
| Dashboard | Next.js, Node 20+ |
| SDK / CLI | Go CLI (`sdk/cli`, thin client over the generated proto/REST contract), Python SDK (`sdk/python`) |
| Contracts | Protobuf (`proto/plan.proto`, `format.proto`, `catalog.proto`, `worker.proto`, `ai.proto`) — source of truth for cross-language types, codegen via `prost` (Rust), `protoc-gen-go` (Go), `grpcio-tools` (Python) |
| Infra | Docker Compose (Postgres, MinIO, Redis, coordinator, N workers, Ollama), Kubernetes manifests (`deploy/k8s`) for production |
| Observability & auth | OpenTelemetry (trace propagation from REST → gRPC → AI service), Prometheus metrics per component, JWT auth with workspace scoping |
| Testing | `cargo test` + Criterion benches (Rust), `go test` with `testcontainers-go` against real Postgres (Go), golden-file NL→plan tests with a mocked LLM by default (Python), full-stack integration tests over Docker Compose |

## Layout

```
engine/             # Rust workspace
  crates/
    atlas-format/    # schema types (re-exports Arrow) + .atlas/Parquet/Iceberg readers & writers
    atlas-storage/   # CSV + columnar reads, object-store abstraction
    atlas-query/     # SQL parsing + logical plan construction
    atlas-optimizer/ # column pruning, predicate pushdown, rule engine
    atlas-exec/      # logical plan execution, statistical insight functions
    atlas-worker/    # gRPC worker: executes assigned task against one partition
    atlas-cli/       # the `atlas-cli` binary (ingest / query / explain)
coordinator/         # Go: REST API, scheduler, worker registry, result cache
  cmd/
    coordinator/
    catalog/          # standalone metadata catalog service
  internal/
    catalog/
    scheduler/
    api/
ai-service/          # Python: LLM abstraction, NL→plan, insights, research agents
  atlas_ai/
    providers/
    planner/
    insights/
    agents/
    retrieval/
proto/               # shared .proto contracts: plan, format, catalog, worker, ai
dashboard/           # Next.js frontend: query console, plan viewer, insights, research reports
sdk/
  python/
  cli/               # Go CLI, reuses the generated proto client
deploy/
  docker-compose.yml
  k8s/
docs/                # architecture plan + implementation spec
```

## Getting Started

### Prerequisites

- Rust (stable, 2021 edition)
- Go 1.22+
- Python 3.11+ (`uv` or `poetry`)
- Node 20+ (dashboard)
- Docker (Postgres, MinIO, Redis, Ollama)

### Installation

```
git clone <repo-url>
cd atlas
docker compose -f deploy/docker-compose.yml up -d postgres minio redis
cargo build --workspace --manifest-path engine/Cargo.toml
(cd coordinator && go build ./...)
(cd ai-service && uv sync)
(cd dashboard && npm install)
```

### Configuration

```
ATLAS_LLM_PROVIDER=ollama        # or anthropic / openai / gemini
ATLAS_LLM_MODEL=<model-name>
DATABASE_URL=postgres://...      # catalog + query_history
REDIS_URL=redis://...            # result cache
```

No hosted-LLM API key is required if `ATLAS_LLM_PROVIDER=ollama` — the local model path is a first-class option, not a fallback.

### Run

```
# ingest a CSV into the columnar format and register a snapshot
atlas-cli ingest --file patients.csv --dataset patients

# query by SQL — goes through optimize -> schedule -> distributed execute
atlas-cli query --dataset patients --sql "SELECT diagnosis, COUNT(*) AS n FROM t GROUP BY diagnosis ORDER BY n DESC"

# query by natural language -- compiles to the same LogicalPlan as SQL
curl -X POST localhost:8080/query/nl -d '{"dataset": "patients", "question": "which diagnosis is most common?"}'

# inspect optimization: pre/post plan, manifests pruned, cache hit
curl -X POST localhost:8080/explain -d '{"dataset": "patients", "sql": "..."}'

# statistically-grounded summary + LLM-narrated insights
curl -X POST localhost:8080/datasets/patients/summary
curl -X POST localhost:8080/datasets/patients/insights

# multi-agent research report over structured data + retrieved literature
curl -X POST localhost:8080/research -d '{"question": "...", "dataset": "patients", "corpus_id": "..."}'

# dashboard
(cd dashboard && npm run dev)   # http://localhost:3000
```

## Testing

```
# Rust
cargo fmt --check --manifest-path engine/Cargo.toml
cargo clippy --workspace --manifest-path engine/Cargo.toml -- -D warnings
cargo test --workspace --manifest-path engine/Cargo.toml

# Go
(cd coordinator && golangci-lint run && go test ./...)

# Python
(cd ai-service && pytest)
```

- **Rust**: unit tests per crate (schema inference, format round-trips, per-operator execution, optimizer rules) plus Criterion benchmarks that assert pruning reduces bytes actually read, not just wall-clock time.
- **Go**: `testcontainers-go` spins up a real Postgres for catalog/scheduler tests — snapshot chaining, transactional commits, partition pruning against real manifests, and a worker-killed-mid-query fault-tolerance test.
- **Python**: golden-file structural tests for NL→plan (assert correct node types/columns/aggregates, not exact LLM text) against a mocked provider by default, with real multi-provider runs (including Ollama) gated behind an integration-test flag; insight and suggested-question tests assert every suggested question compiles to a valid plan and every narrated number traces back to a supplied finding.
- **Cross-service**: an integration test ingests a partitioned dataset and runs a `GROUP BY` through the full coordinator → N-worker → merge path, asserting the distributed result exactly matches a known-correct single-node baseline.
- **CI**: one workflow per language (`ci-rust.yml`, `ci-go.yml`, `ci-python.yml`), each running lint + unit tests + build on every PR touching its directory.

## Deployment

`deploy/docker-compose.yml` runs the full stack locally: Postgres, MinIO, Redis, the coordinator, a configurable number of worker replicas, the AI service, Ollama, and the dashboard. `deploy/k8s/` holds the equivalent Kubernetes manifests for production, scaling workers and the coordinator independently.

| Service | Protocol | Default port |
|---|---|---|
| Coordinator (REST) | HTTP/JSON | 8080 |
| Coordinator (internal) | gRPC | 9090 |
| Catalog service | gRPC | 9091 |
| Worker | gRPC | 9100+ (one per worker) |
| AI service | gRPC | 9092 |
| Dashboard | HTTP | 3000 |
