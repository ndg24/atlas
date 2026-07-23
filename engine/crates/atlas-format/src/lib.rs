//! Shared schema types for Atlas. Re-exports `arrow::datatypes` directly —
//! there is no value in a parallel type system that just wraps Arrow's.

pub mod footer;
#[cfg(test)]
mod format_tests;
mod hll;
mod iceberg;
mod page;
mod parquet;
pub mod partition;
mod reader;
mod stats;
mod writer;

pub use arrow::datatypes::{DataType, Field, Schema};
pub use footer::page_meta::Compression;
pub use footer::{ColumnChunk, FileFooter, PageMeta, Statistics};
pub use iceberg::{read_iceberg_table, IcebergDataFile, IcebergTable};
pub use parquet::{read_parquet, write_parquet};
pub use partition::{write_partitioned, PartitionValues};
pub use reader::{read_atlas_file, read_footer};
pub use stats::{compute_batches_column_stats, compute_statistics};
pub use writer::write_atlas_file;

const SAMPLE_ROWS: usize = 1000;
const NULL_TOKENS: &[&str] = &["", "n/a", "na", "null", "nan"];

/// Case-insensitive regex matching the same "null-like" tokens `infer_schema`
/// treats as null. Exposed so CSV readers can be configured to actually null
/// out these tokens, keeping the inferred nullability consistent with what's
/// read off disk.
pub const NULL_REGEX_PATTERN: &str = r"(?i)^(|n/a|na|null|nan)$";

fn is_null_token(value: &str) -> bool {
    NULL_TOKENS.contains(&value.trim().to_ascii_lowercase().as_str())
}

fn is_bool_token(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "false")
}

/// Infer a [`Schema`] from a sample of CSV records. For each column, tries
/// Int64 -> Float64 -> Bool -> falls back to Utf8, over the first
/// [`SAMPLE_ROWS`] rows. A column is only typed non-Utf8 if 100% of its
/// sampled non-null values parse as that type.
pub fn infer_schema(sample: &[csv::StringRecord], headers: &[String]) -> Schema {
    let fields: Vec<Field> = headers
        .iter()
        .enumerate()
        .map(|(col_idx, name)| infer_field(sample, col_idx, name))
        .collect();
    Schema::new(fields)
}

fn infer_field(sample: &[csv::StringRecord], col_idx: usize, name: &str) -> Field {
    let mut saw_value = false;
    let mut nullable = false;
    let mut all_int = true;
    let mut all_float = true;
    let mut all_bool = true;

    for record in sample.iter().take(SAMPLE_ROWS) {
        let Some(value) = record.get(col_idx) else {
            continue;
        };
        if is_null_token(value) {
            nullable = true;
            continue;
        }
        saw_value = true;
        let trimmed = value.trim();
        all_int &= trimmed.parse::<i64>().is_ok();
        all_float &= trimmed.parse::<f64>().is_ok();
        all_bool &= is_bool_token(trimmed);
    }

    let data_type = if !saw_value {
        DataType::Utf8
    } else if all_int {
        DataType::Int64
    } else if all_float {
        DataType::Float64
    } else if all_bool {
        DataType::Boolean
    } else {
        DataType::Utf8
    };

    Field::new(name, data_type, nullable)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(rows: &[&[&str]]) -> Vec<csv::StringRecord> {
        rows.iter().map(|r| csv::StringRecord::from(*r)).collect()
    }

    #[test]
    fn all_integers() {
        let sample = records(&[&["1"], &["2"], &["3"]]);
        let schema = infer_schema(&sample, &["n".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn all_floats() {
        let sample = records(&[&["1.5"], &["2.0"], &["3.25"]]);
        let schema = infer_schema(&sample, &["n".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn mixed_int_and_float_is_float() {
        let sample = records(&[&["1"], &["2.5"], &["3"]]);
        let schema = infer_schema(&sample, &["n".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn all_bool() {
        let sample = records(&[&["true"], &["false"], &["TRUE"]]);
        let schema = infer_schema(&sample, &["flag".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
    }

    #[test]
    fn nulls_dont_prevent_typing_and_mark_nullable() {
        let sample = records(&[&["1"], &["N/A"], &["3"], &[""]]);
        let schema = infer_schema(&sample, &["n".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn no_nulls_means_not_nullable() {
        let sample = records(&[&["1"], &["2"], &["3"]]);
        let schema = infer_schema(&sample, &["n".to_string()]);
        assert!(!schema.field(0).is_nullable());
    }

    #[test]
    fn falls_back_to_utf8() {
        let sample = records(&[&["alice"], &["bob"], &["42"]]);
        let schema = infer_schema(&sample, &["name".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn all_nulls_falls_back_to_utf8() {
        let sample = records(&[&["N/A"], &[""], &["null"]]);
        let schema = infer_schema(&sample, &["x".to_string()]);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn multi_column_independent_inference() {
        let sample = records(&[&["1", "alice", "1.5"], &["2", "bob", "2.5"]]);
        let headers = vec!["id".to_string(), "name".to_string(), "score".to_string()];
        let schema = infer_schema(&sample, &headers);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    }
}
