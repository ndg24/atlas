//! Outlier detection over an already-grouped aggregate result (e.g. an
//! `AVG(value) GROUP BY group_col` query's output): flags groups whose
//! value deviates more than 2 standard deviations from the mean across all
//! groups in the batch.

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

const Z_SCORE_THRESHOLD: f64 = 2.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutlierFinding {
    pub group: String,
    pub value: f64,
    pub group_mean: f64,
    pub group_stddev: f64,
    pub z_score: f64,
    pub group_col: String,
    pub value_col: String,
}

pub fn detect_outlier_groups(
    grouped: &RecordBatch,
    group_col: &str,
    value_col: &str,
) -> Vec<OutlierFinding> {
    let Some(group_idx) = grouped.schema().index_of(group_col).ok() else {
        return Vec::new();
    };
    let Some(value_idx) = grouped.schema().index_of(value_col).ok() else {
        return Vec::new();
    };

    let Some(group_array) = grouped
        .column(group_idx)
        .as_any()
        .downcast_ref::<StringArray>()
    else {
        return Vec::new();
    };

    let value_array = grouped.column(value_idx);
    let values: Option<Vec<Option<f64>>> = match value_array.data_type() {
        DataType::Float64 => value_array.as_any().downcast_ref::<Float64Array>().map(|a| {
            (0..a.len())
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect()
        }),
        DataType::Int64 => value_array.as_any().downcast_ref::<Int64Array>().map(|a| {
            (0..a.len())
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i) as f64) })
                .collect()
        }),
        _ => None,
    };
    let Some(values) = values else {
        return Vec::new();
    };

    let pairs: Vec<(String, f64)> = (0..grouped.num_rows())
        .filter_map(|i| {
            if group_array.is_null(i) {
                return None;
            }
            let v = values.get(i).copied().flatten()?;
            Some((group_array.value(i).to_string(), v))
        })
        .collect();

    if pairs.len() < 2 {
        return Vec::new();
    }

    let n = pairs.len() as f64;
    let mean: f64 = pairs.iter().map(|(_, v)| v).sum::<f64>() / n;
    let variance: f64 = pairs.iter().map(|(_, v)| (v - mean).powi(2)).sum::<f64>() / n;
    let stddev = variance.sqrt();

    if stddev == 0.0 {
        return Vec::new();
    }

    pairs
        .into_iter()
        .filter_map(|(group, value)| {
            let z = (value - mean) / stddev;
            if z.abs() > Z_SCORE_THRESHOLD {
                Some(OutlierFinding {
                    group,
                    value,
                    group_mean: mean,
                    group_stddev: stddev,
                    z_score: z,
                    group_col: group_col.to_string(),
                    value_col: value_col.to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn grouped_batch(groups: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("hospital", DataType::Utf8, false),
            Field::new("readmit_rate", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(groups.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn flags_a_clear_outlier_group() {
        let batch = grouped_batch(
            &["A", "B", "C", "D", "E", "F"],
            &[10.0, 11.0, 9.0, 12.0, 10.0, 50.0],
        );
        let findings = detect_outlier_groups(&batch, "hospital", "readmit_rate");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].group, "F");
        assert!(findings[0].z_score > 2.0);
    }

    #[test]
    fn does_not_flag_similar_values() {
        let batch = grouped_batch(&["A", "B", "C", "D"], &[10.0, 11.0, 9.5, 10.5]);
        let findings = detect_outlier_groups(&batch, "hospital", "readmit_rate");
        assert!(findings.is_empty());
    }

    #[test]
    fn unknown_column_returns_no_findings() {
        let batch = grouped_batch(&["A", "B"], &[10.0, 20.0]);
        let findings = detect_outlier_groups(&batch, "hospital", "nonexistent");
        assert!(findings.is_empty());
    }
}
