use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "atlas-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a SQL query against a CSV file.
    Query {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        sql: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Query { file, sql } => run_query(&file, &sql),
    }
}

fn run_query(file: &Path, sql: &str) -> Result<()> {
    let (headers, sample) = atlas_storage::sample_headers_and_records(file, 1000)?;
    let schema = atlas_format::infer_schema(&sample, &headers);

    let stmt = atlas_query::parse_sql(sql)?;
    let plan = atlas_query::build_logical_plan(&stmt, &schema)?;

    let batches = atlas_storage::read_csv(file, &schema)?;
    let result = atlas_exec::execute(&plan, batches)?;

    arrow::util::pretty::print_batches(&result)?;
    Ok(())
}
