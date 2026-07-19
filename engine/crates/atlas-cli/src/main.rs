use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::{Parser, Subcommand};

mod catalog_pb;

use catalog_pb::catalog_service_client::CatalogServiceClient;
use catalog_pb::{CommitSnapshotRequest, CreateDatasetRequest, GetDatasetRequest, ManifestInput};

const DEFAULT_CATALOG_ADDR: &str = "http://127.0.0.1:9091";
const DEFAULT_COORDINATOR_ADDR: &str = "http://127.0.0.1:8080";

#[derive(Parser)]
#[command(name = "atlas-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a SQL query, either against a raw CSV file or a catalog dataset.
    Query {
        /// Query a CSV file directly, no catalog involved (Phase 1 path).
        #[arg(long, conflicts_with = "dataset")]
        file: Option<PathBuf>,
        /// Query the current snapshot of a catalog-registered dataset —
        /// submitted to the coordinator's REST API, which fans it out across
        /// workers and merges the result (Phase 3); the CLI no longer
        /// executes catalog-backed queries itself.
        #[arg(long, conflicts_with = "file")]
        dataset: Option<String>,
        #[arg(long)]
        sql: String,
        #[arg(long, default_value = DEFAULT_COORDINATOR_ADDR)]
        coordinator_addr: String,
        /// Bearer token for the coordinator's REST API (every route requires
        /// one). Mint one with `go run ./coordinator/cmd/tokengen` against
        /// the coordinator's JWT_SECRET; falls back to $ATLAS_TOKEN.
        #[arg(long, env = "ATLAS_TOKEN")]
        token: Option<String>,
    },
    /// Ingest a CSV file into a dataset: write `.atlas` (or Parquet) file(s)
    /// and commit a new snapshot to the catalog.
    Ingest {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        dataset: String,
        /// Root directory partition files are written under (one
        /// subdirectory per dataset).
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,
        #[arg(long, default_value = DEFAULT_CATALOG_ADDR)]
        catalog_addr: String,
        /// File format to write: "atlas" (default) or "parquet".
        #[arg(long, default_value = "atlas")]
        format: String,
    },
    /// Register an existing Iceberg table (created by another engine, e.g.
    /// Spark or PyIceberg) as an external-table dataset: no data is copied or
    /// rewritten, only its current snapshot's data files + schema are
    /// recorded in the catalog, exactly as `ingest` would for files Atlas
    /// wrote itself.
    IngestIceberg {
        /// Path to the table's current `metadata/*.metadata.json` — the
        /// pointer a real Iceberg catalog (Hive/Glue/REST) would hand back
        /// for "the current metadata location of this table".
        #[arg(long)]
        metadata: PathBuf,
        #[arg(long)]
        dataset: String,
        #[arg(long, default_value = DEFAULT_CATALOG_ADDR)]
        catalog_addr: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Query {
            file: Some(file),
            sql,
            ..
        } => run_query_file(&file, &sql),
        Command::Query {
            dataset: Some(dataset),
            sql,
            coordinator_addr,
            token,
            ..
        } => run_query_dataset(&dataset, &sql, &coordinator_addr, token.as_deref()).await,
        Command::Query { .. } => bail!("query requires exactly one of --file or --dataset"),
        Command::Ingest {
            file,
            dataset,
            data_dir,
            catalog_addr,
            format,
        } => run_ingest(&file, &dataset, &data_dir, &catalog_addr, &format).await,
        Command::IngestIceberg {
            metadata,
            dataset,
            catalog_addr,
        } => run_ingest_iceberg(&metadata, &dataset, &catalog_addr).await,
    }
}

fn run_query_file(file: &Path, sql: &str) -> Result<()> {
    let (headers, sample) = atlas_storage::sample_headers_and_records(file, 1000)?;
    let schema = atlas_format::infer_schema(&sample, &headers);

    let stmt = atlas_query::parse_sql(sql)?;
    let plan = atlas_query::build_logical_plan(&stmt, &schema)?;

    let batches = atlas_storage::read_csv(file, &schema)?;
    let result = atlas_exec::execute(&plan, batches)?;

    arrow::util::pretty::print_batches(&result)?;
    Ok(())
}

#[derive(serde::Deserialize)]
struct QueryResponse {
    #[serde(default)]
    #[allow(dead_code)]
    query_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    duration_ms: i64,
    arrow_ipc_batches: Vec<String>,
}

#[derive(serde::Deserialize)]
struct ErrorResponse {
    error: String,
}

async fn run_query_dataset(
    dataset: &str,
    sql: &str,
    coordinator_addr: &str,
    token: Option<&str>,
) -> Result<()> {
    let http = reqwest::Client::new();
    let mut req = http
        .post(format!("{coordinator_addr}/query"))
        .json(&serde_json::json!({ "dataset": dataset, "sql": sql }));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("calling coordinator at {coordinator_addr}"))?;

    let status = resp.status();
    if !status.is_success() {
        let body: Result<ErrorResponse, _> = resp.json().await;
        match body {
            Ok(err) => bail!("coordinator returned {status}: {}", err.error),
            Err(_) => bail!("coordinator returned {status}"),
        }
    }

    let query_resp: QueryResponse = resp.json().await.context("parsing coordinator response")?;
    let mut batches = Vec::new();
    for encoded in &query_resp.arrow_ipc_batches {
        let bytes = BASE64
            .decode(encoded)
            .context("decoding arrow_ipc_batches base64")?;
        batches.extend(decode_arrow_ipc(&bytes)?);
    }

    arrow::util::pretty::print_batches(&batches)?;
    Ok(())
}

/// Decode one self-contained Arrow IPC stream, as produced by
/// `atlas-worker`'s `ResultBatch.arrow_ipc` (see `proto/worker.proto`).
fn decode_arrow_ipc(bytes: &[u8]) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .context("opening Arrow IPC stream from coordinator response")?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("reading Arrow IPC batches from coordinator response")
}

async fn run_ingest(
    file: &Path,
    dataset: &str,
    data_dir: &Path,
    catalog_addr: &str,
    format: &str,
) -> Result<()> {
    if format != "atlas" && format != "parquet" {
        bail!("unsupported format {format:?}, expected \"atlas\" or \"parquet\"");
    }

    let (headers, sample) = atlas_storage::sample_headers_and_records(file, 1000)?;
    let schema = atlas_format::infer_schema(&sample, &headers);
    let batches = atlas_storage::read_csv(file, &schema)?;
    let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    let dataset_dir = data_dir.join(dataset);
    std::fs::create_dir_all(&dataset_dir)
        .with_context(|| format!("creating dataset directory {}", dataset_dir.display()))?;
    let ext = if format == "parquet" {
        "parquet"
    } else {
        "atlas"
    };
    let out_path = dataset_dir.join(format!("part-{}.{ext}", uuid::Uuid::new_v4()));
    if format == "parquet" {
        atlas_format::write_parquet(&out_path, &batches)?;
    } else {
        atlas_format::write_atlas_file(&out_path, &batches)?;
    }
    let file_size_bytes = std::fs::metadata(&out_path)
        .with_context(|| format!("statting {}", out_path.display()))?
        .len() as i64;
    let absolute_path = out_path
        .canonicalize()
        .with_context(|| format!("resolving absolute path for {}", out_path.display()))?;

    let mut client = connect(catalog_addr).await?;
    let schema_json = serde_json::to_string(&schema).context("serializing dataset schema")?;
    let dataset_id = ensure_dataset(&mut client, dataset, &schema_json).await?;

    let column_stats_json = serde_json::to_string(&column_stats_by_name(&batches)?)
        .context("serializing column stats")?;
    let manifest = ManifestInput {
        file_path: absolute_path.to_string_lossy().into_owned(),
        partition_values_json: "{}".to_string(),
        row_count,
        file_size_bytes,
        column_stats_json,
        format: format.to_string(),
    };
    let summary_json = serde_json::json!({ "row_count": row_count, "file_count": 1 }).to_string();

    let snapshot = client
        .commit_snapshot(CommitSnapshotRequest {
            dataset_id,
            // No dedicated manifest-list file yet (Phase 5/Iceberg-interop
            // territory) — the dataset's partition directory doubles as the
            // manifest list location for now.
            manifest_list_path: dataset_dir.to_string_lossy().into_owned(),
            operation: "append".to_string(),
            summary_json,
            manifests: vec![manifest],
        })
        .await
        .context("committing snapshot")?
        .into_inner();

    println!(
        "ingested {row_count} rows into dataset '{dataset}' (snapshot {})",
        snapshot.id
    );
    Ok(())
}

/// Register an external Iceberg table's current snapshot into the catalog.
/// Unlike `run_ingest`, no file is written — `atlas_format::read_iceberg_table`
/// already resolved the table's own data files, so each becomes a manifest
/// pointing directly at that existing Parquet file.
async fn run_ingest_iceberg(metadata: &Path, dataset: &str, catalog_addr: &str) -> Result<()> {
    let table = atlas_format::read_iceberg_table(metadata)?;
    if table.data_files.is_empty() {
        bail!(
            "iceberg table at {} has no live data files",
            metadata.display()
        );
    }

    let mut client = connect(catalog_addr).await?;
    let schema_json = serde_json::to_string(&table.schema).context("serializing iceberg schema")?;
    let dataset_id = ensure_dataset(&mut client, dataset, &schema_json).await?;

    let mut total_rows: i64 = 0;
    let manifests = table
        .data_files
        .iter()
        .map(|f| {
            total_rows += f.row_count;
            Ok(ManifestInput {
                file_path: f.file_path.to_string_lossy().into_owned(),
                partition_values_json: serde_json::to_string(&f.partition_values)
                    .context("serializing iceberg partition values")?,
                row_count: f.row_count,
                file_size_bytes: f.file_size_bytes,
                column_stats_json: serde_json::to_string(&f.column_stats)
                    .context("serializing iceberg column stats")?,
                format: "iceberg".to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let file_count = manifests.len();

    let summary_json =
        serde_json::json!({ "row_count": total_rows, "file_count": file_count }).to_string();
    let snapshot = client
        .commit_snapshot(CommitSnapshotRequest {
            dataset_id,
            manifest_list_path: metadata.to_string_lossy().into_owned(),
            operation: "append".to_string(),
            summary_json,
            manifests,
        })
        .await
        .context("committing snapshot")?
        .into_inner();

    println!(
        "registered iceberg table '{dataset}': {total_rows} rows across {file_count} files (snapshot {})",
        snapshot.id
    );
    Ok(())
}

async fn connect(catalog_addr: &str) -> Result<CatalogServiceClient<tonic::transport::Channel>> {
    CatalogServiceClient::connect(catalog_addr.to_string())
        .await
        .with_context(|| format!("connecting to catalog at {catalog_addr}"))
}

/// Look up `dataset` in the catalog, creating it (with `schema_json`) on
/// first ingest. Returns the dataset's id.
async fn ensure_dataset(
    client: &mut CatalogServiceClient<tonic::transport::Channel>,
    dataset: &str,
    schema_json: &str,
) -> Result<String> {
    match client
        .get_dataset(GetDatasetRequest {
            name: dataset.to_string(),
        })
        .await
    {
        Ok(resp) => Ok(resp.into_inner().id),
        Err(status) if status.code() == tonic::Code::NotFound => Ok(client
            .create_dataset(CreateDatasetRequest {
                name: dataset.to_string(),
                schema_json: schema_json.to_string(),
            })
            .await
            .context("creating dataset")?
            .into_inner()
            .id),
        Err(status) => Err(status).context("looking up dataset"),
    }
}

fn column_stats_by_name(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<HashMap<String, serde_json::Value>> {
    Ok(atlas_format::compute_batches_column_stats(batches)?
        .into_iter()
        .map(|(name, stats)| {
            let value = serde_json::json!({
                "min": BASE64.encode(&stats.min),
                "max": BASE64.encode(&stats.max),
                "null_count": stats.null_count,
            });
            (name, value)
        })
        .collect())
}
