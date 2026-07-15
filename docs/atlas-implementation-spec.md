# Atlas — Implementation Spec

This document is written to be handed to an AI coding agent and executed **phase by phase**. Each phase is self-contained: it states its goal, prerequisites, exact deliverable, an ordered task list, the concrete interfaces to implement, and acceptance criteria that define "done." Do not start a phase until the previous phase's acceptance criteria pass. Do not implement functionality described in a later phase early — the boundaries are stated explicitly per phase precisely so an agent doesn't scope-creep.

Companion doc: `atlas-architecture-plan.md` has the narrative rationale for *why* each component is designed this way. This doc is the *what to build, in what order, with what exact interface*.

---

## 0. How to use this document

- Work top to bottom. Phase N's tasks assume Phase N-1's acceptance criteria are met.
- Every phase ends with a **Definition of Done** — a literal checklist. Do not mark a phase complete until every line is true.
- Section 1 defines contracts (protobuf, JSON schemas, SQL DDL, file formats) that multiple phases depend on. Treat these as fixed once a phase starts consuming them; changing a contract mid-project means updating every consumer, so get the shape right in Section 1 rather than improvising per-phase.
- Repo is a monorepo. Top-level layout, created in Phase 1 and extended per phase:

```
atlas/
  engine/            # Rust workspace: storage, query engine, optimizer, worker runtime
    crates/
      atlas-storage/
      atlas-query/
      atlas-optimizer/
      atlas-exec/
      atlas-worker/
      atlas-format/   # shared columnar file format (used by storage + ingestion)
    Cargo.toml
  coordinator/        # Go: coordinator service, REST API, metadata catalog service
    cmd/
      coordinator/
      catalog/
    internal/
      catalog/
      scheduler/
      api/
    go.mod
  ai-service/         # Python: LLM abstraction, NL→plan, insights, agents
    atlas_ai/
      providers/
      planner/
      insights/
      agents/
    pyproject.toml
  proto/               # shared .proto definitions, source of truth for all service contracts
  dashboard/           # Next.js
  sdk/
    python/
    cli/               # Go CLI, reuses generated proto client
  deploy/
    docker-compose.yml
    k8s/
  docs/
    atlas-architecture-plan.md
    atlas-implementation-spec.md
```

- Language toolchains: Rust stable (workspace via Cargo), Go 1.22+, Python 3.11+ (uv or poetry), Node 20+ for the dashboard.
- Every phase's task list ends with a "wire it into CI" step — assume GitHub Actions, one workflow per language (`ci-rust.yml`, `ci-go.yml`, `ci-python.yml`), each running lint + unit tests + build on every PR touching that directory.

---

## 1. Shared Contracts (define before Phase 2)

These are referenced by multiple phases. Define them once, in `proto/` and `docs/`, before any service depends on them.

### 1.1 Logical Plan schema

Every query — whether it arrives as SQL or natural language — is converted into this shape before touching the optimizer. Define as protobuf in `proto/plan.proto`:

```protobuf
syntax = "proto3";
package atlas.plan;

message LogicalPlan {
  oneof node {
    ScanNode scan = 1;
    FilterNode filter = 2;
    ProjectNode project = 3;
    AggregateNode aggregate = 4;
    SortNode sort = 5;
    LimitNode limit = 6;
    JoinNode join = 7;         // Phase 4+
  }
}

message ScanNode {
  string dataset = 1;
  repeated string columns = 2;   // empty = all columns; populated by column pruning
  string snapshot_id = 3;        // optional, empty = current snapshot
}

message FilterNode {
  LogicalPlan input = 1;
  Expr predicate = 2;
}

message ProjectNode {
  LogicalPlan input = 1;
  repeated Expr exprs = 2;
  repeated string aliases = 3;
}

message AggregateNode {
  LogicalPlan input = 1;
  repeated Expr group_by = 2;
  repeated AggExpr aggregates = 3;
}

message AggExpr {
  enum Fn { COUNT = 0; SUM = 1; AVG = 2; MIN = 3; MAX = 4; }
  Fn fn = 1;
  Expr arg = 2;
  string alias = 3;
}

message SortNode {
  LogicalPlan input = 1;
  repeated SortKey keys = 2;
}
message SortKey { Expr expr = 1; bool descending = 2; }

message LimitNode {
  LogicalPlan input = 1;
  uint64 n = 2;
}

message JoinNode {
  LogicalPlan left = 1;
  LogicalPlan right = 2;
  enum JoinType { INNER = 0; LEFT = 1; RIGHT = 2; FULL = 3; }
  JoinType join_type = 3;
  Expr on = 4;
}

message Expr {
  oneof kind {
    string column_ref = 1;
    Literal literal = 2;
    BinaryExpr binary = 3;
  }
}
message Literal {
  oneof value { int64 int_val = 1; double float_val = 2; string str_val = 3; bool bool_val = 4; }
}
message BinaryExpr {
  enum Op { EQ=0; NEQ=1; LT=2; LTE=3; GT=4; GTE=5; AND=6; OR=7; ADD=8; SUB=9; MUL=10; DIV=11; }
  Op op = 1;
  Expr left = 2;
  Expr right = 3;
}
```

This is the one artifact both the SQL path (Phase 1) and the NL path (Phase 6) must produce. Generate Rust structs (`prost`) and Python classes (`grpcio-tools`/`protobuf`) from this file; do not hand-write parallel structs in each language.

### 1.2 Columnar File Format (`.atlas` format)

Defined in full in Phase 2, but the byte layout is fixed here so later phases (Parquet/Iceberg interop in Phase 5) can be designed against it:

```
[Page 0][Page 1]...[Page N]     <- one section per column, pages within a column are contiguous
[Footer]
[Footer length: 4 bytes LE][Magic: "ATL1" 4 bytes]
```

Footer (serialized as protobuf, `proto/format.proto`):

```protobuf
message FileFooter {
  repeated ColumnChunk columns = 1;
  uint64 row_count = 2;
  string schema_json = 3;   // Arrow schema, JSON-serialized
}
message ColumnChunk {
  string name = 1;
  repeated PageMeta pages = 2;
  Statistics stats = 3;      // file-level stats for this column, used by predicate/partition pruning
}
message PageMeta {
  uint64 offset = 1;
  uint64 compressed_length = 2;
  uint64 uncompressed_length = 3;
  uint32 row_count = 4;
  enum Compression { NONE = 0; LZ4 = 1; ZSTD = 2; }
  Compression compression = 5;
}
message Statistics {
  bytes min = 1;
  bytes max = 2;
  uint64 null_count = 3;
  uint64 distinct_count_estimate = 4;  // HyperLogLog-derived, optional until Phase 4
}
```

Reader implementation: seek to end of file, read last 8 bytes to get footer length + magic, read footer, then random-access individual column pages by offset. This is what makes column pruning physically possible — a scan of 2 columns out of 20 only touches those 2 columns' byte ranges.

### 1.3 Metadata Catalog schema (Postgres, Phase 2)

```sql
CREATE TABLE datasets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT UNIQUE NOT NULL,
  schema_json JSONB NOT NULL,
  current_snapshot_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES datasets(id),
  parent_snapshot_id UUID REFERENCES snapshots(id),
  manifest_list_path TEXT NOT NULL,   -- object store path to manifest list file
  operation TEXT NOT NULL,             -- 'append' | 'overwrite' | 'delete'
  summary_json JSONB,                  -- row count delta, file count, etc.
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE manifests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES snapshots(id),
  file_path TEXT NOT NULL,
  partition_values JSONB,
  row_count BIGINT NOT NULL,
  file_size_bytes BIGINT NOT NULL,
  column_stats JSONB NOT NULL          -- {column_name: {min, max, null_count}}
);

CREATE TABLE query_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL,                -- 'sql' | 'nl'
  raw_input TEXT NOT NULL,
  logical_plan_json JSONB NOT NULL,
  physical_plan_json JSONB,
  status TEXT NOT NULL,                -- 'running' | 'succeeded' | 'failed'
  duration_ms INTEGER,
  error TEXT
);
```

`ALTER TABLE datasets ADD CONSTRAINT fk_snapshot FOREIGN KEY (current_snapshot_id) REFERENCES snapshots(id);` after both tables exist (circular FK).

### 1.4 Service ports & protocol summary

| Service | Protocol | Default port | Defined in |
|---|---|---|---|
| Coordinator (REST) | HTTP/JSON | 8080 | Phase 3 |
| Coordinator (internal) | gRPC | 9090 | Phase 3 |
| Catalog service | gRPC | 9091 | Phase 2 |
| Worker | gRPC | 9100+ (one per worker) | Phase 3 |
| AI service | gRPC | 9092 | Phase 6 |
| Dashboard | HTTP | 3000 | Phase 3+ |

---

## Phase 1 — Single-Node Engine (Rust only)

**Goal**: a CLI binary that loads a CSV file and runs a `SELECT ... WHERE ... GROUP BY ...` query against it, no network, no persistence beyond the source file.

**Prerequisites**: none. This is the starting point.

**Deliverable**: `cargo run -p atlas-cli -- query --file patients.csv --sql "SELECT diagnosis, COUNT(*) FROM t WHERE age > 50 GROUP BY diagnosis"` prints a result table to stdout.

### Tasks

1. `cargo new --lib engine/crates/atlas-format` — define `Schema`, `DataType` (Int64, Float64, Utf8, Bool, Date32), `Field { name, data_type, nullable }`. Re-export `arrow::datatypes` types directly rather than wrapping them — no value in a parallel type system.
2. `atlas-format`: implement `pub fn infer_schema(sample: &[csv::StringRecord], headers: &[String]) -> Schema` — for each column, try parse as Int64 → Float64 → Bool → fallback Utf8, across a sample of the first 1000 rows; a column is only typed non-Utf8 if 100% of sampled non-null values parse.
3. `engine/crates/atlas-storage`: implement `pub fn read_csv(path: &Path, schema: &Schema) -> Result<Vec<RecordBatch>>` using the `csv` crate + `arrow-csv`, batching into `RecordBatch`es of 8192 rows.
4. `engine/crates/atlas-query`: add `sqlparser` dependency. Implement `pub fn parse_sql(sql: &str) -> Result<sqlparser::ast::Statement>`.
5. `atlas-query`: implement `pub fn build_logical_plan(stmt: &Statement, schema: &Schema) -> Result<LogicalPlan>` (the protobuf type from §1.1) — handle `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, and aggregate functions `COUNT/SUM/AVG/MIN/MAX` in the select list.
6. `engine/crates/atlas-exec`: implement one function per logical node type, each taking `&[RecordBatch]` (or an iterator of batches) and producing `Vec<RecordBatch>`:
   - `exec_scan`, `exec_filter` (use `arrow::compute::filter_record_batch`), `exec_project`, `exec_aggregate` (group-by via a `HashMap<Vec<ScalarValue>, AggregatorState>`), `exec_sort` (use `arrow::compute::sort_to_indices` + `take`), `exec_limit`.
7. `atlas-exec`: implement `pub fn execute(plan: &LogicalPlan, source: Vec<RecordBatch>) -> Result<Vec<RecordBatch>>` — walk the plan tree bottom-up, dispatching to the functions from step 6. No physical planning or optimization yet — logical plan is executed directly, node by node.
8. `engine/crates/atlas-cli`: binary with `clap`-based subcommand `query --file <path> --sql <sql>`. Wire: read CSV → infer schema → parse SQL → build logical plan → execute → pretty-print result (`arrow::util::pretty::print_batches`).
9. Unit tests: one test file per crate. `atlas-format`: schema inference on mixed-type columns, including a nulls/"N/A" case. `atlas-exec`: one test per operator (filter, group-by-count, group-by-sum, sort+limit) against a small in-memory `RecordBatch` fixture — do not read from disk in these tests.
10. Integration test: `tests/csv_end_to_end.rs` in `atlas-cli` — ship a fixture `patients.csv` (~50 rows, mixed types, at least one null-heavy column) in `tests/fixtures/`, run 5 SQL queries against it, assert exact output.
11. Wire `ci-rust.yml`: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test --workspace`.

### Definition of Done

- [ ] `atlas-cli query --file tests/fixtures/patients.csv --sql "SELECT diagnosis, COUNT(*) as n FROM t GROUP BY diagnosis ORDER BY n DESC"` runs and produces correct output.
- [ ] All 5 aggregate functions (COUNT, SUM, AVG, MIN, MAX) work in isolation and combined with GROUP BY.
- [ ] WHERE supports `=, !=, <, <=, >, >=, AND, OR` on int/float/string columns.
- [ ] Schema inference correctly types a column that's entirely integers, entirely floats, mixed-with-nulls, and falls back to Utf8 for anything else.
- [ ] `cargo test --workspace` passes; CI green.
- [ ] No networking code, no gRPC, no persistence beyond reading the source CSV — those are later phases.

---

## Phase 2 — Columnar Storage + Metadata Catalog

**Goal**: data gets converted into the `.atlas` columnar format (§1.2) and tracked in a Postgres-backed catalog (§1.3) with immutable snapshots.

**Prerequisites**: Phase 1 complete (`atlas-format`, `RecordBatch` pipeline exists).

**Deliverable**: `atlas-cli ingest --file patients.csv --dataset patients` writes `.atlas` files to disk/MinIO and registers a snapshot in the catalog; `atlas-cli query --dataset patients --sql "..."` reads from the catalog instead of a raw CSV path.

### Tasks — Storage format (Rust)

1. `engine/crates/atlas-format`: implement the writer — `pub fn write_atlas_file(path: &Path, batches: &[RecordBatch]) -> Result<FileFooter>`. Per column: concatenate all batches' arrays for that column, split into pages of ≤8192 rows, compress each page (start with LZ4 via the `lz4_flex` crate — simpler API than `zstd` for a first pass, add Zstd as a second `Compression` variant once LZ4 path works), write page bytes sequentially, record `PageMeta` (offset/lengths) as you go. Compute column-level `Statistics` (min/max via `arrow::compute::min`/`max`, null_count from the array's null buffer) while writing. Write the footer (protobuf-encoded) + footer length + magic bytes at the end.
2. Implement the reader — `pub fn read_atlas_file(path: &Path, columns: Option<&[String]>) -> Result<Vec<RecordBatch>>`: read last 8 bytes, seek to footer, decode `FileFooter`, then for each requested column (default: all), seek to each `PageMeta.offset`, read+decompress, reassemble into an Arrow array, zip columns into `RecordBatch`es. This function is what column pruning (Phase 4) will call with a restricted `columns` list — verify by test that requesting 1 of 5 columns only reads that column's byte ranges (assert via a wrapped `Read` that counts bytes read).
3. Partitioning: `pub fn write_partitioned(dir: &Path, batches: &[RecordBatch], partition_by: &[String]) -> Result<Vec<(PartitionValues, PathBuf, FileFooter)>>` — group rows by the partition column(s)' values (e.g. partition healthcare data by `hospital`), write one `.atlas` file per partition value under `dir/<col>=<value>/part-0.atlas` (Hive-style partition paths — this convention pays off directly in Phase 5's Iceberg/Parquet interop).
4. Object store abstraction: wrap local filesystem and MinIO/S3 behind one trait using the `object_store` crate (`pub trait AtlasFileSystem { fn put(&self, path, bytes); fn get_range(&self, path, range) -> Bytes; }` — or just use `object_store::ObjectStore` directly rather than a custom trait, since it already covers both backends).
5. Tests: round-trip test (write batches → read back → assert equality) for each supported `DataType`, including a column with nulls. Compression test: assert LZ4 output is smaller than uncompressed for a repetitive string column. Partitioning test: 3 partitions written, each readable independently.

### Tasks — Metadata Catalog (Go)

6. `coordinator/internal/catalog`: Postgres migration files implementing §1.3 schema (use `golang-migrate` or plain SQL files run via `goose`).
7. Define `proto/catalog.proto`:
```protobuf
service CatalogService {
  rpc CreateDataset(CreateDatasetRequest) returns (Dataset);
  rpc GetDataset(GetDatasetRequest) returns (Dataset);
  rpc ListDatasets(ListDatasetsRequest) returns (ListDatasetsResponse);
  rpc CommitSnapshot(CommitSnapshotRequest) returns (Snapshot);
  rpc GetCurrentSnapshot(GetSnapshotRequest) returns (Snapshot);
  rpc ListManifests(ListManifestsRequest) returns (ListManifestsResponse);
}
```
8. Implement the service in `coordinator/internal/catalog/service.go` backed by `pgx`. `CommitSnapshot` must be transactional: insert the new `snapshots` row, insert all `manifests` rows, then update `datasets.current_snapshot_id` — all in one Postgres transaction, so a crash mid-commit never leaves the catalog pointing at a partially-written snapshot.
9. `coordinator/cmd/catalog/main.go`: standalone binary serving `CatalogService` on port 9091 (kept separate from the coordinator binary even though they'll often run together, per the architecture doc's separation of passive-state vs. active-scheduler).
10. Go tests: use `testcontainers-go` to spin up a real Postgres for `service_test.go` — cover create dataset → commit snapshot → get current snapshot → commit a second snapshot → verify `parent_snapshot_id` chains correctly.

### Tasks — Wiring

11. Extend `atlas-cli` with an `ingest` subcommand: read CSV (Phase 1 code) → write `.atlas` file(s) via `atlas-format` → call `CatalogService.CommitSnapshot` (Rust gRPC client via `tonic`, generated from `catalog.proto`) with the resulting manifest info.
12. Extend `atlas-cli query` to accept `--dataset <name>` instead of `--file`: call `CatalogService.GetCurrentSnapshot` + `ListManifests`, read the relevant `.atlas` file(s), feed into the Phase 1 execution path.
13. `deploy/docker-compose.yml`: add `postgres` and `minio` services.
14. Wire `ci-go.yml` (lint via `golangci-lint`, `go test ./...`).

### Definition of Done

- [ ] `ingest` produces `.atlas` files with correct footer statistics, verified by a test that reads the footer back and checks min/max/null_count against the source data.
- [ ] Reading a subset of columns demonstrably skips bytes for unrequested columns (test asserts this, not just "output looks right").
- [ ] Two successive `ingest` calls on the same dataset create two snapshots with correct parent chaining; querying always uses `current_snapshot_id`.
- [ ] `atlas-cli query --dataset patients --sql "..."` produces output identical to Phase 1's `--file` path on the same underlying data.
- [ ] Catalog commit is transactional (kill the process mid-commit in a test via a Postgres statement timeout or similar; catalog is left in the pre-commit state, not partially written).

---

## Phase 3 — Distributed Execution

**Goal**: queries run across multiple worker processes instead of in the CLI's own process; a Go coordinator schedules scan tasks and merges results.

**Prerequisites**: Phase 2 (catalog + partitioned `.atlas` files to distribute scans over).

**Deliverable**: `atlas-cli` no longer executes queries itself — it submits to the coordinator's REST API, which fans a scan out across N worker processes and streams back a merged result.

### Tasks

1. `proto/worker.proto`:
```protobuf
service WorkerService {
  rpc ExecuteTask(TaskRequest) returns (stream ResultBatch);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
message TaskRequest {
  string task_id = 1;
  PhysicalOp op = 2;           // scan+filter+partial-aggregate for this partition
  string file_path = 3;
  repeated string columns = 4;
}
message ResultBatch { bytes arrow_ipc = 1; }   // Arrow IPC-serialized RecordBatch
```
2. `engine/crates/atlas-worker`: binary that starts a `tonic` gRPC server implementing `WorkerService`. `ExecuteTask` reads the assigned `.atlas` file (via `atlas-format` + `atlas-storage`), runs the assigned operators (reuse Phase 1's `atlas-exec` functions — a task is just "run this subtree of the plan against this one file"), streams `RecordBatch`es back as Arrow IPC frames. `Heartbeat` returns liveness + current in-flight task count.
3. `coordinator/internal/scheduler`: `Coordinator` struct holding a worker registry (`map[workerID]*WorkerConn`, updated by a heartbeat-listening goroutine that marks workers dead after 3 missed heartbeats). Implement `Schedule(plan LogicalPlan, manifests []Manifest) ([]Task, error)` — one task per manifest/file (this is where partition-level parallelism comes from), round-robin (or least-loaded, using in-flight count from heartbeats) assignment across registered workers.
4. Implement `Coordinator.RunQuery(ctx, plan, manifests) (<-chan arrow.Record, error)`: dispatch tasks via `WorkerService.ExecuteTask` (one goroutine per task, gRPC streaming), fan partial results into a merge stage. Merge logic depends on the plan's top node: plain `UNION` of batches for scan+filter+project; a second aggregation pass over all workers' partial `AggregateNode` outputs for GROUP BY (this is the standard two-phase distributed aggregate — partial aggregates per worker, final combine at the coordinator); a k-way merge for ORDER BY + LIMIT.
5. Retry/fault tolerance: if a task's gRPC stream errors or the assigned worker misses its heartbeat mid-task, re-submit that one task to a different live worker. Cap at 3 retries per task before failing the whole query. Test this by killing a worker process mid-query in an integration test and asserting the query still completes.
6. `coordinator/internal/api`: REST handlers per §1.4 — implement `POST /query` (body: `{sql: string}` or `{dataset: string, sql: string}`), `POST /datasets`, `GET /datasets`, `GET /history` (reads `query_history` table — insert a row at query start, update it with status/duration at the end, per §1.3).
7. `atlas-cli`: replace direct execution with an HTTP client hitting `POST /query` on the coordinator; print the streamed/returned result the same way Phase 1 did.
8. `deploy/docker-compose.yml`: add `coordinator` and 3x `worker` services (parameterize worker count).
9. Integration test (`coordinator/internal/scheduler/integration_test.go`, using `testcontainers-go` or docker-compose in CI): ingest a dataset partitioned into 3 files, run a GROUP BY query through the full coordinator→3-workers→merge path, assert result matches the Phase 1 single-process result on the same data (this cross-checks distributed execution against the known-correct sequential baseline — keep the Phase 1 path around as a test oracle, not just historical code).

### Definition of Done

- [ ] A query against a 3-partition dataset visibly dispatches 3 tasks to 3 different workers (assert via task IDs / worker IDs in logs or a test hook).
- [ ] Distributed GROUP BY result exactly matches the Phase 1 single-node result on identical data (this is the correctness bar — parallelism must not change answers).
- [ ] Killing one worker mid-query does not fail the query (task retried on a live worker).
- [ ] `GET /history` shows accurate status/duration for both successful and failed queries.
- [ ] No optimizer yet — full scans every time, no pruning. That's Phase 4.

---

## Phase 4 — Query Optimization

**Goal**: rule-based logical-plan rewrites that reduce actual work done: column pruning, predicate pushdown, partition pruning, result caching, stats-driven decisions.

**Prerequisites**: Phase 3 (something to optimize — a working distributed query path) and Phase 2 (manifest statistics to prune against).

**Deliverable**: `POST /explain` returns both the pre- and post-optimization plan; `EXPLAIN`-verified pruning measurably reduces bytes scanned on a benchmark dataset.

### Tasks

1. `engine/crates/atlas-optimizer`: define `pub trait Rule { fn apply(&self, plan: LogicalPlan) -> LogicalPlan; }` and `pub fn optimize(plan: LogicalPlan, rules: &[Box<dyn Rule>]) -> LogicalPlan` — apply all rules in a fixed-point loop (repeat until no rule changes the plan, cap at e.g. 20 iterations to guarantee termination).
2. Implement `ColumnPruningRule`: walk the plan top-down accumulating the set of columns actually referenced (from `ProjectNode.exprs`, `FilterNode.predicate`, `AggregateNode.group_by`/`aggregates`); push that set down into the leaf `ScanNode.columns`. Test: a query selecting 1 of 10 columns produces a `ScanNode` with `columns = ["that_one"]`.
3. Implement `PredicatePushdownRule`: move `FilterNode`s below `ProjectNode`s (filtering before projecting is always valid when the filter only references columns present pre-projection) and as close to `ScanNode` as possible. Test with a `Project(Filter(Scan))` vs `Filter(Project(Scan))` input, assert both normalize to filter-closest-to-scan.
4. Implement `PartitionPruningRule`: this one runs at the coordinator, not purely on the logical plan — given the `FilterNode`'s predicate and the manifest list's per-file `partition_values` + `column_stats` (min/max) from the catalog, drop manifests that cannot possibly satisfy the predicate (e.g. `WHERE year = 2024` skips every manifest whose partition value or stats range excludes 2024). Implement as `func PrunePartitions(predicate *plan.Expr, manifests []Manifest) []Manifest` in `coordinator/internal/scheduler`, called before `Schedule` builds tasks. Test: 5 manifests with distinct `year` partition values, a `year = 2024` filter prunes to exactly 1.
5. Result caching: `coordinator/internal/cache` using Redis (`go-redis` client). Key = SHA256 of the *optimized* logical plan's canonical JSON (not the raw SQL string — two differently-worded but equivalent queries should still hit different keys unless you also normalize, which is out of scope; same SQL string always optimizes to the same plan so this is sufficient for now). On `RunQuery`, check cache before scheduling; on success, store the final `RecordBatch` set (Arrow IPC-serialized) with a TTL (default 5 min, configurable). Invalidate proactively: cache entries also store the `snapshot_id` they were computed against; if the dataset's `current_snapshot_id` has advanced, treat as a miss even if the key exists (do this instead of scanning to invalidate on every ingest — it's simpler and correct).
6. Statistics-based decisions: use `manifests.column_stats` (already collected in Phase 2) to choose join build-side (Phase 4+/JOIN work, smaller table by row-count estimate becomes the hash-build side) once JOIN exists; until then, expose the stats via `POST /explain` so they're visible even before anything consumes them for a cost decision.
7. `POST /explain`: return `{logical_plan, optimized_plan, manifests_before_pruning, manifests_after_pruning, cache_hit: bool}` as JSON — this is what the dashboard's plan viewer (Phase 3-built dashboard, extended here) renders.
8. Benchmark harness: `engine/benches/pruning_bench.rs` (Criterion) or a simple Go benchmark script — ingest a synthetic 1M-row, 10-partition, 20-column dataset, run a query that should prune to 2 columns / 1 partition, assert bytes-read is within expected bounds (not just "faster," an actual byte-count assertion using the same counting wrapper from Phase 2 task 2).

### Definition of Done

- [ ] `/explain` output visibly differs pre/post optimization for a query with an unused column and a partition-selective filter.
- [ ] Partition pruning benchmark shows scanning only the matching partition's manifests, verified by manifest count, not just wall-clock time.
- [ ] Column pruning benchmark shows bytes-read scales with selected columns, not total columns.
- [ ] Repeating an identical query hits the Redis cache (verify via a "cache_hit" flag in the response, and that no tasks are dispatched to workers on the second call).
- [ ] Ingesting a new snapshot invalidates cached results for that dataset (test: cache a query, ingest new data, re-run, assert cache_hit=false and the new row is reflected).

---

## Phase 5 — Modern Data Formats

**Goal**: read (and where reasonable, write) Parquet natively, and read Iceberg tables — validating that the catalog/format design from Phase 2 wasn't a toy imitation.

**Prerequisites**: Phase 2 (catalog + `.atlas` format, structurally similar to Parquet/Iceberg by design) and Phase 4 (pruning logic to reuse against externally-sourced stats).

### Tasks

1. Parquet read: `atlas-format::read_parquet(path, columns) -> Vec<RecordBatch>` using `parquet-rs`'s `ArrowReader` — this should be a thin wrapper, since `arrow-rs`/`parquet-rs` already do the heavy lifting; the work here is integration, not reimplementation. Column projection and row-group-level predicate pushdown (via `parquet-rs`'s `RowFilter`) map directly onto the same `ColumnPruningRule`/`PredicatePushdownRule` outputs from Phase 4 — no new optimizer rules needed, just a new leaf executor that accepts a `columns: Vec<String>` + optional predicate.
2. Parquet write: `atlas-format::write_parquet(path, batches) -> Result<()>` for the ingestion path — add `--format parquet` to `atlas-cli ingest` alongside the existing native `.atlas` writer.
3. Catalog: add a `format` column to the `manifests` table (`'atlas' | 'parquet' | 'iceberg'`), so a single dataset's manifests can mix formats and the reader dispatches per-file.
4. Iceberg read: use the `iceberg-rust` crate (or implement a minimal reader against Iceberg's documented spec if the crate's API doesn't fit — either is acceptable, but prefer the crate first to avoid re-deriving a spec that's already well-implemented) to read an existing Iceberg table's manifest list + manifests, translate Iceberg's manifest entries into Atlas's internal `Manifest` struct so the rest of the pipeline (pruning, scheduling, execution) doesn't need to know the source format. This is an **external-table** read path — Iceberg tables created by Spark/other engines should be queryable by Atlas without going through Atlas's own ingestion.
5. Delta Lake read (stretch, only after Iceberg read is solid): same pattern via `delta-rs`.
6. Tests: round-trip Parquet write→read matches `.atlas` write→read on identical source data (same test fixture, both formats, assert equal `RecordBatch` output). Iceberg: use a small Iceberg table fixture generated by PyIceberg or Spark locally, check into `tests/fixtures/iceberg_sample/`, assert Atlas reads it correctly including partition pruning against its real partition spec.

### Definition of Done

- [ ] `atlas-cli ingest --format parquet` and default `.atlas` format produce query-equivalent results.
- [ ] A dataset can contain both `.atlas` and Parquet manifests simultaneously and query correctly across both.
- [ ] Atlas can query an Iceberg table it did not create (fixture generated by another tool), with partition pruning working against Iceberg's actual partition spec, not a reimplementation.

---

## Phase 6 — AI-Native Interface

**Goal**: natural language queries produce the same `LogicalPlan` (§1.1) that SQL produces, executed by the unchanged engine; results get a plain-English explanation. New Python service enters the codebase here.

**Prerequisites**: Phase 4 (optimizer/executor stable — this is what the AI layer sits on top of, unmodified) and the catalog (Phase 2, for schema context in prompts).

### Tasks

1. `ai-service/atlas_ai/providers/`: define `class ModelProvider(Protocol): def complete(self, prompt: str, **kwargs) -> str`. Implement via `litellm.completion(...)` under the hood so one adapter covers Anthropic/OpenAI/Gemini/Ollama — provider + model selected by `ATLAS_LLM_PROVIDER` / `ATLAS_LLM_MODEL` env vars, no per-provider branching in application code beyond the litellm model-string format.
2. `ai-service/atlas_ai/planner/`: `def nl_to_plan(question: str, schema: Schema) -> LogicalPlan`. Prompt template includes: the dataset's schema (column names + types from the catalog), 2-3 few-shot examples of question → JSON logical plan, and an instruction to output *only* JSON matching the protobuf-derived schema. Parse the LLM's JSON output into the generated `LogicalPlan` Python class (from `proto/plan.proto`, §1.1); if parsing/validation fails, re-prompt once with the validation error appended ("your last output failed validation: <error>; fix and resend"), then give up and return a clear error to the caller after 1 retry.
3. `proto/ai.proto`:
```protobuf
service AIService {
  rpc NLToQuery(NLRequest) returns (NLResponse);
  rpc Explain(ExplainRequest) returns (ExplainResponse);
}
message NLRequest { string question = 1; string dataset = 2; }
message NLResponse { plan.LogicalPlan plan = 1; string raw_llm_output = 2; }
message ExplainRequest { string question = 1; bytes result_arrow_ipc = 2; }
message ExplainResponse { string explanation = 1; }
```
4. `ai-service`: gRPC server (`grpc.aio` for async) implementing `AIService` on port 9092. `Explain` takes already-computed results (Arrow IPC bytes, deserialized via `pyarrow`) and asks the LLM to narrate them in plain English — the LLM never receives raw source data, only the already-executed result set, preserving the "engine is source of truth" boundary from the architecture doc.
5. Coordinator: add `POST /query/nl` REST endpoint — calls `AIService.NLToQuery`, takes the returned `LogicalPlan`, runs it through the *existing* optimize→schedule→execute path unchanged (this is the point of the design: no new execution code needed for NL queries), then optionally calls `AIService.Explain` on the result if the request asks for narration.
6. Tests: golden-file tests for `nl_to_plan` — a fixed set of ~15 question/schema pairs with expected plan shapes (not exact LLM output matching, since that's nondeterministic, but structural assertions: right node types, right columns referenced, right aggregate function). Use a cheap/fast model (or a mocked provider returning canned JSON) for CI; gate actual multi-provider LLM calls behind an integration-test flag that requires API keys, not run on every PR.
7. `ATLAS_LLM_PROVIDER=ollama` path: document + test against a local Ollama instance (add to `docker-compose.yml`) so BYO-LLM is demonstrably provider-agnostic, not just Anthropic-shaped.

### Definition of Done

- [ ] The 15-question golden test suite produces structurally correct plans for at least 2 different providers (e.g. Claude and a local Ollama model), proving the abstraction isn't accidentally provider-specific.
- [ ] An NL query and its SQL equivalent produce byte-identical result sets (proves the "LLM only produces plans, engine executes" boundary actually holds).
- [ ] Switching `ATLAS_LLM_PROVIDER` requires only an env var change, no code change, verified by running the same golden tests against 2 providers in CI (Ollama local model + one hosted provider, hosted one skipped if no API key present in the environment).
- [ ] `Explain` output never contains numbers that don't trace back to the supplied result set (spot-checked in the golden tests: assert the explanation text contains figures matching the input result, not fabricated ones — approximate via regex-extracted numbers compared against the result set's values).

---

## Phase 7 — AI Analyst

**Goal**: automatic dataset summaries, statistically-grounded insights (not LLM-invented ones), suggested questions, and data-quality flags.

**Prerequisites**: Phase 6 (LLM plumbing) and Phase 4 (stats already flowing through manifests — reused here as the seed for insight detection).

### Tasks

1. `engine/crates/atlas-exec` (or a new `atlas-insights` crate): implement statistical checks as plain functions over `RecordBatch`es/aggregated results, each returning a structured finding, not text:
   - `detect_outlier_groups(grouped: &RecordBatch, value_col: &str) -> Vec<OutlierFinding>` — z-score or IQR-based, flags groups whose aggregate value deviates >2 std devs from the group-of-groups mean (e.g., "Hospital A" readmission rate vs. all hospitals).
   - `detect_trend(time_series: &RecordBatch, time_col: &str, value_col: &str) -> Option<TrendFinding>` — simple linear regression slope + significance over a rolling window, flags consistent up/down trends (e.g., "spikes every December" would need seasonal decomposition — flag this as a stretch/future refinement rather than building full seasonal decomposition now).
   - `detect_data_quality_issues(schema: &Schema, batches: &[RecordBatch]) -> Vec<QualityFinding>` — null-rate per column above a threshold, single-value (zero-variance) columns, duplicate row detection.
2. Expose these via a new coordinator endpoint `POST /datasets/{name}/summary` — runs a fixed set of aggregate queries (row/column count, per-column null rate and distinct-count-estimate reusing manifest stats from §1.2's `distinct_count_estimate`) through the *existing* query engine, then runs the Rust insight functions from step 1 over the results, returns structured JSON findings (no LLM involved yet — this step is pure engine).
3. `ai-service/atlas_ai/insights/`: `def narrate_findings(findings: list[Finding]) -> str` — LLM call that turns the structured findings from step 2 into readable sentences, one input finding per output claim (prompt explicitly instructs: "do not state any number not present in the input JSON"). `def suggest_questions(schema: Schema, summary: DatasetSummary) -> list[str]` — the one genuinely generative piece; given schema + summary stats, ask the LLM for 5 example questions this dataset could answer, validated by actually running each through `nl_to_plan` (Phase 6) and discarding any that fail to produce a valid plan, so suggested questions are guaranteed answerable.
4. Coordinator: `POST /datasets/{name}/insights` — orchestrates: call engine summary (step 2) → call `AIService.narrate_findings` → return `{findings: [...], narrative: str, suggested_questions: [...]}`.
5. Tests: engine-level insight functions get exact-value unit tests (construct a `RecordBatch` with a known outlier, assert it's detected, assert a non-outlier isn't). `suggest_questions` test asserts 100% of returned questions produce valid plans (this is a hard requirement enforced by the filtering in step 3, so the test should never see a bad one pass through).

### Definition of Done

- [ ] `/datasets/{name}/summary` returns row/column counts, null rates, and distinct-count estimates computed via the query engine (not ad hoc pandas/python — this must go through the same execution path as user queries, since that's the whole "engine is source of truth" bit).
- [ ] Every number in a narrated insight is traceable to a specific finding object from step 1 (no free-floating LLM-generated statistics).
- [ ] Every suggested question, when run through `nl_to_plan` + execution, succeeds (filtering in step 3 guarantees this; test confirms it holds).

---

## Phase 8 — Research Agent (multi-agent, RAG)

**Goal**: multi-agent pipeline (Planner → Query → Execution → Visualization → Explanation → Report) and a research mode that combines structured query results with retrieved literature, with citations separating the two sources.

**Prerequisites**: Phase 7 (single-LLM insight path proven end to end — do not start multi-agent orchestration before this works, per the roadmap's explicit sequencing).

### Tasks

1. `ai-service/atlas_ai/agents/`: define each agent as `class Agent(Protocol): def run(self, state: PipelineState) -> PipelineState`, where `PipelineState` is a typed (Pydantic) object accumulating: `question`, `plan` (from Planner), `results` (from Execution — a thin wrapper calling the exact same coordinator `/query` path used elsewhere, no separate execution logic), `chart_spec` (from Visualization), `explanation` (from Explanation), `report_draft` (from Report). Each agent reads only the state fields it needs and writes only its own — log every agent's input/output state transition (structured logging, not just text) so the pipeline is inspectable, which is the actual point of doing this as discrete agents instead of one mega-prompt.
2. `PlannerAgent`: decomposes a research question into sub-questions answerable by the structured engine vs. ones needing literature retrieval (e.g. "what factors are associated with diabetes readmission" → structured sub-question "readmission rate by comorbidity in the dataset" + literature sub-question "known diabetes readmission risk factors in clinical literature").
3. `QueryAgent` / `ExecutionAgent`: reuse Phase 6's `nl_to_plan` + coordinator execution directly for the structured sub-questions — these two agents should be thin orchestration around already-built Phase 6/7 code, not new query logic.
4. Literature retrieval: `ai-service/atlas_ai/retrieval/` — ingest a corpus (PDFs/abstracts) into a vector store. Use `pgvector` on the existing Postgres instance (avoids standing up a second datastore) with embeddings via the configured provider's embedding endpoint (or a local sentence-transformers model as the Ollama-equivalent option, keeping BYO-model consistent). `def retrieve(query: str, k: int = 5) -> list[Document]`.
5. `VisualizationAgent`: rule-based chart-type selection (per architecture doc §"Visualization recommendation" — query shape drives chart type, LLM only as fallback for ambiguous cases), producing a `ChartSpec` (chart type + column mappings) the dashboard can render directly.
6. `ExplanationAgent` / `ReportAgent`: compose the final report from `results` + `retrieve()` documents, with every claim tagged `[data]` or `[literature:doc_id]` in the output so the report visibly separates what came from Atlas's query engine vs. retrieved papers — this tagging is a hard requirement, not a nice-to-have, given the architecture doc's emphasis on not blending the two silently.
7. Orchestration: a simple sequential pipeline (`for agent in [Planner, Query, Execution, Visualization, Explanation, Report]: state = agent.run(state)`) is sufficient — do not introduce a heavyweight graph framework unless a real need for branching/looping emerges; sequential composition matches the spec's stated pipeline shape.
8. Coordinator: `POST /research` accepting `{question: str, dataset: str, corpus_id: str}`, running the full pipeline, returning the final report + full `PipelineState` (for debugging/transparency in the dashboard).
9. Tests: each agent unit-tested in isolation with a fixed input `PipelineState` and mocked LLM/retrieval calls. One end-to-end test with a small fixture dataset + a small fixture literature corpus (3-5 short documents), asserting the final report contains both `[data]`- and `[literature:...]`-tagged claims.

### Definition of Done

- [ ] Full pipeline runs on a fixture healthcare dataset + fixture literature corpus, producing a report with both data-sourced and literature-sourced claims, each tagged with its source.
- [ ] Every `[data]`-tagged claim traces to an actual query result from the Execution agent; every `[literature:doc_id]`-tagged claim traces to a retrieved document actually returned by step 4.
- [ ] Each agent is independently testable (unit tests from step 9 don't require the full pipeline or live LLM calls to run).

---

## Cross-cutting, addressed incrementally (not a separate phase)

Add these as each phase naturally touches the relevant surface, rather than as a bolt-on at the end:

- **OpenTelemetry**: add a trace span at the REST entry point (Phase 3) and propagate the trace context through gRPC metadata to workers (Phase 3) and the AI service (Phase 6), so one trace ID covers a full request. Add this when each service is first built, not retrofitted later — retrofitting tracing is where most of the pain is.
- **Prometheus metrics**: worker task duration/count (Phase 3), cache hit rate (Phase 4), LLM call latency/token counts (Phase 6) — instrument each as its owning component is built.
- **Auth**: add the `users`/`workspaces` tables and JWT middleware once the REST API exists (Phase 3), even if initially permissive (single default workspace), so later phases don't require retrofitting workspace-scoping into every catalog/query call.
- **CLI/SDK**: the Go CLI already exists from Phase 1 onward; keep it as a thin client as the REST API grows each phase. Python SDK (`sdk/python`) can be added once Phase 6 makes notebook-style NL querying worth having a client for.
