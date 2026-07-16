//! CSV reading for Atlas: sampling (for schema inference) and full,
//! schema-driven reads into Arrow `RecordBatch`es.

mod store;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_csv::ReaderBuilder;
use regex::Regex;

pub use store::{get_bytes, get_range, local_store, put_file};

const BATCH_SIZE: usize = 8192;

/// Read the header row plus up to `sample_size` data rows as raw string
/// records, for schema inference — does not interpret types.
pub fn sample_headers_and_records(
    path: &Path,
    sample_size: usize,
) -> Result<(Vec<String>, Vec<csv::StringRecord>)> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("opening CSV at {}", path.display()))?;
    let headers: Vec<String> = reader
        .headers()
        .context("reading CSV header row")?
        .iter()
        .map(String::from)
        .collect();

    let mut records = Vec::with_capacity(sample_size);
    for result in reader.records().take(sample_size) {
        records.push(result.context("reading CSV record")?);
    }
    Ok((headers, records))
}

/// Read an entire CSV file into `RecordBatch`es of up to 8192 rows each,
/// using the given (already-inferred) schema to parse column values.
pub fn read_csv(path: &Path, schema: &Schema) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("opening CSV at {}", path.display()))?;
    let schema_ref = Arc::new(schema.clone());
    let null_regex =
        Regex::new(atlas_format::NULL_REGEX_PATTERN).expect("NULL_REGEX_PATTERN is valid");
    let csv_reader = ReaderBuilder::new(schema_ref)
        .with_header(true)
        .with_batch_size(BATCH_SIZE)
        .with_null_regex(null_regex)
        .build(file)
        .context("building CSV reader")?;

    csv_reader
        .into_iter()
        .map(|batch| batch.context("reading CSV batch"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::io::Write;

    fn write_temp_csv(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f
    }

    #[test]
    fn reads_headers_and_sample() {
        let csv = write_temp_csv("a,b\n1,x\n2,y\n3,z\n");
        let (headers, records) = sample_headers_and_records(csv.path(), 10).unwrap();
        assert_eq!(headers, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn reads_full_csv_into_batches() {
        let csv = write_temp_csv("a,b\n1,x\n2,y\n3,z\n");
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let batches = read_csv(csv.path(), &schema).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        assert_eq!(batches[0].schema().field(0).name(), "a");
    }

    #[test]
    fn null_tokens_become_real_nulls() {
        let csv = write_temp_csv("a,b\n1,x\nN/A,y\n3,N/A\n");
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let batches = read_csv(csv.path(), &schema).unwrap();
        assert_eq!(batches[0].num_rows(), 3);
        assert!(batches[0].column(0).is_null(1));
        assert!(batches[0].column(1).is_null(2));
    }
}
