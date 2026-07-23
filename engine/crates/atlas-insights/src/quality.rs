//! Data-quality checks: null-rate threshold, zero-variance (single-value)
//! columns, and duplicate-row detection. Null-rate/zero-variance read
//! directly off an already-built `DatasetSummary`'s per-column stats;
//! duplicate detection needs actual rows, so it operates over a bounded
//! sample of rows (fetched by the caller — see the coordinator's
//! `LIMIT 10000` sample query) that stats alone can't reveal.

use crate::summary::ColumnSummary;
use arrow::array::{Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const NULL_RATE_THRESHOLD: f64 = 0.2;
const MAX_SAMPLE_INDICES: usize = 20;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum QualityFinding {
    HighNullRate { column: String, null_rate: f64 },
    ZeroVariance { column: String, value: String },
    DuplicateRows { count: u64, sample_row_indices: Vec<u64> },
}

pub fn detect_data_quality_issues(
    columns: &[ColumnSummary],
    sample: &[RecordBatch],
) -> Vec<QualityFinding> {
    let mut findings = Vec::new();

    for col in columns {
        if col.null_rate > NULL_RATE_THRESHOLD {
            findings.push(QualityFinding::HighNullRate {
                column: col.name.clone(),
                null_rate: col.null_rate,
            });
        }
        if col.null_rate < 1.0 {
            if let (Some(min), Some(max)) = (&col.min, &col.max) {
                if min == max {
                    findings.push(QualityFinding::ZeroVariance {
                        column: col.name.clone(),
                        value: min.clone(),
                    });
                }
            }
        }
    }

    findings.extend(detect_duplicate_rows(sample));
    findings
}

fn detect_duplicate_rows(sample: &[RecordBatch]) -> Vec<QualityFinding> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut duplicate_count: u64 = 0;
    let mut sample_indices = Vec::new();
    let mut global_row: u64 = 0;

    for batch in sample {
        for row in 0..batch.num_rows() {
            let key = row_key(batch, row);
            if !seen.insert(key) {
                duplicate_count += 1;
                if sample_indices.len() < MAX_SAMPLE_INDICES {
                    sample_indices.push(global_row);
                }
            }
            global_row += 1;
        }
    }

    if duplicate_count == 0 {
        Vec::new()
    } else {
        vec![QualityFinding::DuplicateRows {
            count: duplicate_count,
            sample_row_indices: sample_indices,
        }]
    }
}

/// A row's identity for duplicate detection: a delimited string built from
/// each cell's textual value (or a null sentinel), stable across the
/// column types the engine supports.
fn row_key(batch: &RecordBatch, row: usize) -> String {
    let mut parts = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        if col.is_null(row) {
            parts.push("\u{0}NULL".to_string());
            continue;
        }
        let part = match col.data_type() {
            DataType::Int64 => col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_default(),
            DataType::Float64 => col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_default(),
            DataType::Utf8 => col
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_default(),
            DataType::Boolean => col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_default(),
            DataType::Date32 => col
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_default(),
            _ => String::new(),
        };
        parts.push(part);
    }
    parts.join("\u{1}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn col(name: &str, null_rate: f64, min: Option<&str>, max: Option<&str>) -> ColumnSummary {
        ColumnSummary {
            name: name.to_string(),
            data_type: "Utf8".to_string(),
            null_rate,
            distinct_count_estimate: 1,
            min: min.map(|s| s.to_string()),
            max: max.map(|s| s.to_string()),
        }
    }

    #[test]
    fn flags_high_null_rate() {
        let columns = vec![col("notes", 0.3, Some("a"), Some("z"))];
        let findings = detect_data_quality_issues(&columns, &[]);
        assert!(matches!(
            findings[0],
            QualityFinding::HighNullRate { ref column, .. } if column == "notes"
        ));
    }

    #[test]
    fn does_not_flag_low_null_rate() {
        let columns = vec![col("notes", 0.05, Some("a"), Some("z"))];
        let findings = detect_data_quality_issues(&columns, &[]);
        assert!(findings.is_empty());
    }

    #[test]
    fn flags_zero_variance_column() {
        let columns = vec![col("status", 0.0, Some("active"), Some("active"))];
        let findings = detect_data_quality_issues(&columns, &[]);
        assert!(matches!(
            findings[0],
            QualityFinding::ZeroVariance { ref column, ref value }
                if column == "status" && value == "active"
        ));
    }

    #[test]
    fn all_null_column_is_not_flagged_zero_variance() {
        // 100% null is itself a HighNullRate finding, but must not also be
        // reported as ZeroVariance -- there's no non-null value to point to.
        let columns = vec![col("status", 1.0, Some("active"), Some("active"))];
        let findings = detect_data_quality_issues(&columns, &[]);
        assert!(!findings
            .iter()
            .any(|f| matches!(f, QualityFinding::ZeroVariance { .. })));
    }

    fn sample_batch(names: &[&str], ages: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(ages.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn detects_duplicate_rows() {
        let batch = sample_batch(&["alice", "bob", "alice"], &[30, 40, 30]);
        let findings = detect_data_quality_issues(&[], &[batch]);
        assert_eq!(findings.len(), 1);
        assert!(matches!(
            findings[0],
            QualityFinding::DuplicateRows { count: 1, .. }
        ));
    }

    #[test]
    fn no_duplicates_produces_no_finding() {
        let batch = sample_batch(&["alice", "bob", "carol"], &[30, 40, 50]);
        let findings = detect_data_quality_issues(&[], &[batch]);
        assert!(findings.is_empty());
    }
}
