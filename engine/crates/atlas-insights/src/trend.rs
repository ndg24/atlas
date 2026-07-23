//! Trend detection over a time-ordered aggregate result (e.g. an
//! `<agg> GROUP BY <time_col> ORDER BY <time_col>` query's output): simple
//! linear regression of value against row order, flagging a consistent
//! (non-trivial R^2) up/down slope. Full seasonal decomposition (e.g.
//! "spikes every December") is explicitly out of scope for this pass — the
//! spec calls it out as a stretch/future refinement, not a Phase 7 goal.

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

const MIN_POINTS: usize = 3;
const MIN_R_SQUARED: f64 = 0.5;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendFinding {
    pub time_col: String,
    pub value_col: String,
    pub slope: f64,
    pub direction: String,
    pub r_squared: f64,
}

/// `time_series` is assumed already ordered ascending by `time_col` (the
/// query that produced it did the ordering) — row position, not the
/// `time_col` values themselves, is used as the regression's x-axis.
/// `time_col` is only checked for presence, to keep the finding
/// self-describing.
pub fn detect_trend(time_series: &RecordBatch, time_col: &str, value_col: &str) -> Option<TrendFinding> {
    time_series.schema().index_of(time_col).ok()?;
    let value_idx = time_series.schema().index_of(value_col).ok()?;

    let value_array = time_series.column(value_idx);
    let values: Vec<Option<f64>> = match value_array.data_type() {
        DataType::Float64 => value_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| {
                (0..a.len())
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect()
            })?,
        DataType::Int64 => value_array.as_any().downcast_ref::<Int64Array>().map(|a| {
            (0..a.len())
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i) as f64) })
                .collect()
        })?,
        _ => return None,
    };

    let points: Vec<(f64, f64)> = values
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| v.map(|v| (i as f64, v)))
        .collect();
    if points.len() < MIN_POINTS {
        return None;
    }

    let n = points.len() as f64;
    let mean_x: f64 = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y: f64 = points.iter().map(|(_, y)| y).sum::<f64>() / n;

    let mut cov = 0.0;
    let mut var_x = 0.0;
    for (x, y) in &points {
        cov += (x - mean_x) * (y - mean_y);
        var_x += (x - mean_x).powi(2);
    }
    if var_x == 0.0 {
        return None;
    }
    let slope = cov / var_x;
    let intercept = mean_y - slope * mean_x;

    let ss_tot: f64 = points.iter().map(|(_, y)| (y - mean_y).powi(2)).sum();
    let ss_res: f64 = points
        .iter()
        .map(|(x, y)| {
            let predicted = slope * x + intercept;
            (y - predicted).powi(2)
        })
        .sum();
    let r_squared = if ss_tot == 0.0 { 0.0 } else { 1.0 - ss_res / ss_tot };

    if r_squared < MIN_R_SQUARED || slope == 0.0 {
        return None;
    }

    Some(TrendFinding {
        time_col: time_col.to_string(),
        value_col: value_col.to_string(),
        slope,
        direction: if slope > 0.0 {
            "increasing".to_string()
        } else {
            "decreasing".to_string()
        },
        r_squared,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn series(values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("month", DataType::Int64, false),
            Field::new("cost", DataType::Float64, false),
        ]));
        let months: Vec<i64> = (0..values.len() as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(months)),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn detects_increasing_trend() {
        let batch = series(&[10.0, 20.0, 31.0, 39.0, 51.0, 60.0]);
        let finding = detect_trend(&batch, "month", "cost").unwrap();
        assert_eq!(finding.direction, "increasing");
        assert!(finding.slope > 0.0);
        assert!(finding.r_squared > 0.9);
    }

    #[test]
    fn detects_decreasing_trend() {
        let batch = series(&[60.0, 51.0, 39.0, 31.0, 20.0, 10.0]);
        let finding = detect_trend(&batch, "month", "cost").unwrap();
        assert_eq!(finding.direction, "decreasing");
        assert!(finding.slope < 0.0);
    }

    #[test]
    fn flat_series_has_no_trend() {
        let batch = series(&[5.0, 5.0, 5.0, 5.0, 5.0]);
        assert!(detect_trend(&batch, "month", "cost").is_none());
    }

    #[test]
    fn too_few_points_has_no_trend() {
        let batch = series(&[1.0, 2.0]);
        assert!(detect_trend(&batch, "month", "cost").is_none());
    }

    #[test]
    fn noisy_series_below_r_squared_threshold_has_no_trend() {
        let batch = series(&[10.0, 40.0, 5.0, 45.0, 8.0, 50.0]);
        assert!(detect_trend(&batch, "month", "cost").is_none());
    }
}
