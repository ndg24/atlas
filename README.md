# Atlas

A distributed analytics engine: Rust storage/query engine, Go coordinator, Python AI service, Next.js dashboard. Built phase by phase per `docs/atlas-implementation-spec.md`.

## Status

- **Phase 1 — Single-Node Engine (Rust only): in progress.** A CLI that loads a CSV and runs `SELECT ... WHERE ... GROUP BY ...` against it, no network, no persistence beyond the source file.
- Phases 2-8 (columnar storage, distributed execution, optimizer, modern formats, AI-native interface, AI analyst, research agent) not started.

## Layout

```
engine/            # Rust workspace: storage, query engine, worker runtime
  crates/
    atlas-format/   # shared schema types + schema inference
    atlas-storage/  # CSV reading
    atlas-query/    # SQL parsing + logical plan construction
    atlas-exec/     # logical plan execution
    atlas-cli/      # CLI binary
proto/              # shared .proto contracts (source of truth for cross-service schemas)
docs/               # spec documents
```

Other top-level directories from the spec (`coordinator/`, `ai-service/`, `dashboard/`, `sdk/`, `deploy/`) are introduced in the phases that need them.

## Build & test

```
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Run

```
cargo run -p atlas-cli -- query --file <path-to-csv> --sql "SELECT diagnosis, COUNT(*) AS n FROM t GROUP BY diagnosis ORDER BY n DESC"
```
