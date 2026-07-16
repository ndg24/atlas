use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::{Parser, Subcommand};

mod catalog_pb;

use catalog_pb::catalog_service_client::CatalogServiceClient;
use catalog_pb::{
    CommitSnapshotRequest, CreateDatasetRequest, GetDatasetRequest, GetSnapshotRequest,
    ListManifestsRequest, ManifestInput,
};

const DEFAULT_CATALOG_ADDR: &str = "http://127.0.0.1:9091";

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
        /// Query the current snapshot of a catalog-registered dataset.
        #[arg(long, conflicts_with = "file")]
        dataset: Option<String>,
        #[arg(long)]
        sql: String,
        #[arg(long, default_value = DEFAULT_CATALOG_ADDR)]
        catalog_addr: String,
    },
    /// Ingest a CSV file into a dataset: write `.atlas` file(s) and commit a
    /// new snapshot to the catalog.
    Ingest {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        dataset: String,
        /// Root directory `.atlas` files are written under (one subdirectory
        /// per dataset).
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,
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
            catalog_addr,
            ..
        } => run_query_dataset(&dataset, &sql, &catalog_addr).await,
        Command::Query { .. } => bail!("query requires exactly one of --file or --dataset"),
        Command::Ingest {
            file,
            dataset,
            data_dir,
            catalog_addr,
        } => run_ingest(&file, &dataset, &data_dir, &catalog_addr).await,
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

async fn run_query_dataset(dataset: &str, sql: &str, catalog_addr: &str) -> Result<()> {
    let mut client = connect(catalog_addr).await?;

    let ds = client
        .get_dataset(GetDatasetRequest {
            name: dataset.to_string(),
        })
        .await
        .with_context(|| format!("looking up dataset '{dataset}'"))?
        .into_inner();
    if ds.current_snapshot_id.is_empty() {
        bail!("dataset '{dataset}' has no committed snapshot yet — run `ingest` first");
    }
    let schema: atlas_format::Schema =
        serde_json::from_str(&ds.schema_json).context("parsing dataset schema from catalog")?;

    let snapshot = client
        .get_current_snapshot(GetSnapshotRequest {
            dataset_name: dataset.to_string(),
        })
        .await
        .context("fetching current snapshot")?
        .into_inner();

    let manifests = client
        .list_manifests(ListManifestsRequest {
            snapshot_id: snapshot.id,
        })
        .await
        .context("listing manifests")?
        .into_inner()
        .manifests;
    if manifests.is_empty() {
        bail!("dataset '{dataset}' snapshot has no manifests");
    }

    let mut batches = Vec::new();
    for manifest in &manifests {
        batches.extend(
            atlas_format::read_atlas_file(Path::new(&manifest.file_path), None)
                .with_context(|| format!("reading manifest file {}", manifest.file_path))?,
        );
    }

    let stmt = atlas_query::parse_sql(sql)?;
    let plan = atlas_query::build_logical_plan(&stmt, &schema)?;
    let result = atlas_exec::execute(&plan, batches)?;

    arrow::util::pretty::print_batches(&result)?;
    Ok(())
}

async fn run_ingest(file: &Path, dataset: &str, data_dir: &Path, catalog_addr: &str) -> Result<()> {
    let (headers, sample) = atlas_storage::sample_headers_and_records(file, 1000)?;
    let schema = atlas_format::infer_schema(&sample, &headers);
    let batches = atlas_storage::read_csv(file, &schema)?;
    let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    let dataset_dir = data_dir.join(dataset);
    std::fs::create_dir_all(&dataset_dir)
        .with_context(|| format!("creating dataset directory {}", dataset_dir.display()))?;
    let atlas_path = dataset_dir.join(format!("part-{}.atlas", uuid::Uuid::new_v4()));
    let footer = atlas_format::write_atlas_file(&atlas_path, &batches)?;
    let file_size_bytes = std::fs::metadata(&atlas_path)
        .with_context(|| format!("statting {}", atlas_path.display()))?
        .len() as i64;
    let absolute_path = atlas_path
        .canonicalize()
        .with_context(|| format!("resolving absolute path for {}", atlas_path.display()))?;

    let mut client = connect(catalog_addr).await?;
    let schema_json = serde_json::to_string(&schema).context("serializing dataset schema")?;
    let dataset_id = ensure_dataset(&mut client, dataset, &schema_json).await?;

    let column_stats_json = serde_json::to_string(&column_stats_by_name(&footer))
        .context("serializing column stats")?;
    let manifest = ManifestInput {
        file_path: absolute_path.to_string_lossy().into_owned(),
        partition_values_json: "{}".to_string(),
        row_count,
        file_size_bytes,
        column_stats_json,
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

fn column_stats_by_name(footer: &atlas_format::FileFooter) -> HashMap<String, serde_json::Value> {
    footer
        .columns
        .iter()
        .map(|chunk| {
            let stats = chunk.stats.clone().unwrap_or_default();
            let value = serde_json::json!({
                "min": BASE64.encode(&stats.min),
                "max": BASE64.encode(&stats.max),
                "null_count": stats.null_count,
            });
            (chunk.name.clone(), value)
        })
        .collect()
}
