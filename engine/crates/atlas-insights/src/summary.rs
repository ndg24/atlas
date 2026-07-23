//! Combines an engine-computed row/null-count batch with per-manifest
//! catalog stats (min/max/distinct_count_estimate, one entry per
//! (manifest, column) — not pre-reduced by the caller) into one
//! `DatasetSummary`. This is the "no ad hoc pandas" summary exposed at
//! `/datasets/{name}/summary` — row and null counts come from an ordinary
//! `COUNT(*)`/`COUNT(col)` aggregate query run through the unmodified
//! engine; only the JSON assembly and cross-manifest reduction happen here.
//!
//! The coordinator deliberately does *not* reduce min/max itself: the
//! stats' `min_base64`/`max_base64` are little-endian bytes for numeric
//! types (`atlas_format::stats`'s own encoding), and little-endian byte
//! order does not correspond to numeric order, so a byte-lexicographic
//! reduction in Go would silently be wrong. Decoding stays in Rust, next to
//! the encoder it has to match.

use arrow::array::{Array, Int64Array, RecordBatch};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSummary {
    pub name: String,
    pub data_type: String,
    pub null_rate: f64,
    pub distinct_count_estimate: u64,
    pub min: Option<String>,
    pub max: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSummary {
    pub row_count: u64,
    pub columns: Vec<ColumnSummary>,
}

/// One (manifest, column) pair's catalog stats, exactly as stored in that
/// manifest's `column_stats_json` — the coordinator forwards these
/// verbatim, one entry per manifest per column (the same column name
/// repeats once per manifest in a multi-file dataset); `build_summary`
/// does the cross-manifest reduction. `min_base64`/`max_base64` are
/// base64-encoded bytes in `atlas_format::stats`'s own encoding
/// (little-endian for numeric types, raw UTF-8 for strings, a single byte
/// for booleans).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergedColumnStats {
    pub name: String,
    pub data_type: String,
    pub distinct_count_estimate: u64,
    pub min_base64: Option<String>,
    pub max_base64: Option<String>,
}

/// A decoded stats value, kept typed (rather than immediately stringified)
/// so cross-manifest min/max reduction compares numerically, not
/// lexicographically.
#[derive(Debug, Clone, PartialEq)]
enum DecodedValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

impl DecodedValue {
    /// Mirrors `atlas_format::stats`'s `LeBytes` write-side convention
    /// exactly so this round-trips. Returns `None` for empty/unparseable
    /// bytes (e.g. an all-null column, whose stats writer emits an empty
    /// byte vec) or an unrecognized `data_type`.
    fn decode(data_type: &str, base64_value: &str) -> Option<Self> {
        let bytes = BASE64.decode(base64_value).ok()?;
        if bytes.is_empty() {
            return None;
        }
        match data_type {
            "Int64" => Some(DecodedValue::Int(i64::from_le_bytes(bytes.try_into().ok()?))),
            "Date32" => Some(DecodedValue::Int(
                i32::from_le_bytes(bytes.try_into().ok()?) as i64,
            )),
            "Float64" => Some(DecodedValue::Float(f64::from_le_bytes(
                bytes.try_into().ok()?,
            ))),
            "Boolean" => Some(DecodedValue::Bool(bytes[0] != 0)),
            "Utf8" => String::from_utf8(bytes).ok().map(DecodedValue::Str),
            _ => None,
        }
    }

    fn display(&self) -> String {
        match self {
            DecodedValue::Int(v) => v.to_string(),
            DecodedValue::Float(v) => v.to_string(),
            DecodedValue::Str(v) => v.clone(),
            DecodedValue::Bool(v) => v.to_string(),
        }
    }
}

/// Picks the better of `existing`/`candidate` for a running min (or max, if
/// `want_min` is false). Mismatched variants (shouldn't happen — every
/// entry for one column shares that column's `data_type`) are treated as
/// incomparable and `existing` is kept.
fn pick(existing: Option<DecodedValue>, candidate: Option<DecodedValue>, want_min: bool) -> Option<DecodedValue> {
    match (existing, candidate) {
        (None, c) => c,
        (e, None) => e,
        (Some(e), Some(c)) => {
            let c_is_better = match (&e, &c) {
                (DecodedValue::Int(a), DecodedValue::Int(b)) => {
                    if want_min {
                        b < a
                    } else {
                        b > a
                    }
                }
                (DecodedValue::Float(a), DecodedValue::Float(b)) => {
                    if want_min {
                        b < a
                    } else {
                        b > a
                    }
                }
                (DecodedValue::Str(a), DecodedValue::Str(b)) => {
                    if want_min {
                        b < a
                    } else {
                        b > a
                    }
                }
                (DecodedValue::Bool(a), DecodedValue::Bool(b)) => {
                    if want_min {
                        b < a
                    } else {
                        b > a
                    }
                }
                _ => false,
            };
            Some(if c_is_better { c } else { e })
        }
    }
}

struct Reduced {
    data_type: String,
    distinct_count_estimate: u64,
    min: Option<DecodedValue>,
    max: Option<DecodedValue>,
}

fn reduce_across_manifests(manifest_stats: &[MergedColumnStats]) -> BTreeMap<String, Reduced> {
    let mut grouped: BTreeMap<String, Reduced> = BTreeMap::new();
    for stat in manifest_stats {
        let entry = grouped.entry(stat.name.clone()).or_insert_with(|| Reduced {
            data_type: stat.data_type.clone(),
            distinct_count_estimate: 0,
            min: None,
            max: None,
        });
        entry.distinct_count_estimate += stat.distinct_count_estimate;
        let min_val = stat
            .min_base64
            .as_deref()
            .and_then(|b| DecodedValue::decode(&stat.data_type, b));
        let max_val = stat
            .max_base64
            .as_deref()
            .and_then(|b| DecodedValue::decode(&stat.data_type, b));
        entry.min = pick(entry.min.take(), min_val, true);
        entry.max = pick(entry.max.take(), max_val, false);
    }
    grouped
}

/// `count_batch` is the single-row result of
/// `SELECT COUNT(*) AS total_rows, COUNT(col) AS "<col>_non_null" FROM dataset`
/// — an ordinary aggregate query with an empty `GROUP BY`, run through the
/// unmodified engine. `manifest_stats` is the dataset's manifests' stats,
/// unreduced (one entry per manifest per column) — this function does the
/// cross-manifest reduction. Columns without a matching
/// `"<name>_non_null"` field in `count_batch` are silently skipped.
pub fn build_summary(
    count_batch: &RecordBatch,
    manifest_stats: &[MergedColumnStats],
) -> Option<DatasetSummary> {
    let total_idx = count_batch.schema().index_of("total_rows").ok()?;
    let total_array = count_batch
        .column(total_idx)
        .as_any()
        .downcast_ref::<Int64Array>()?;
    if total_array.is_empty() || total_array.is_null(0) {
        return None;
    }
    let row_count = total_array.value(0).max(0) as u64;

    let reduced = reduce_across_manifests(manifest_stats);

    let columns = reduced
        .into_iter()
        .filter_map(|(name, r)| {
            let non_null_col = format!("{name}_non_null");
            let idx = count_batch.schema().index_of(&non_null_col).ok()?;
            let non_null_count = count_batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(0)
                .max(0) as u64;

            let null_rate = if row_count == 0 {
                0.0
            } else {
                row_count.saturating_sub(non_null_count) as f64 / row_count as f64
            };

            Some(ColumnSummary {
                name,
                data_type: r.data_type,
                null_rate,
                distinct_count_estimate: r.distinct_count_estimate.min(row_count),
                min: r.min.map(|v| v.display()),
                max: r.max.map(|v| v.display()),
            })
        })
        .collect();

    Some(DatasetSummary { row_count, columns })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn count_batch(total: i64, age_non_null: i64, name_non_null: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("total_rows", DataType::Int64, false),
            Field::new("age_non_null", DataType::Int64, false),
            Field::new("name_non_null", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![total])),
                Arc::new(Int64Array::from(vec![age_non_null])),
                Arc::new(Int64Array::from(vec![name_non_null])),
            ],
        )
        .unwrap()
    }

    fn int_b64(v: i64) -> String {
        BASE64.encode(v.to_le_bytes())
    }

    fn str_b64(v: &str) -> String {
        BASE64.encode(v.as_bytes())
    }

    fn int_stat(name: &str, distinct: u64, min: i64, max: i64) -> MergedColumnStats {
        MergedColumnStats {
            name: name.to_string(),
            data_type: "Int64".to_string(),
            distinct_count_estimate: distinct,
            min_base64: Some(int_b64(min)),
            max_base64: Some(int_b64(max)),
        }
    }

    #[test]
    fn combines_engine_counts_with_manifest_stats() {
        let batch = count_batch(10, 8, 10);
        let stats = vec![
            int_stat("age", 5, 10, 90),
            MergedColumnStats {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                distinct_count_estimate: 10,
                min_base64: Some(str_b64("alice")),
                max_base64: Some(str_b64("zoe")),
            },
        ];

        let summary = build_summary(&batch, &stats).unwrap();
        assert_eq!(summary.row_count, 10);
        assert_eq!(summary.columns.len(), 2);

        let age = summary.columns.iter().find(|c| c.name == "age").unwrap();
        assert!((age.null_rate - 0.2).abs() < 1e-9);
        assert_eq!(age.distinct_count_estimate, 5);
        assert_eq!(age.min.as_deref(), Some("10"));
        assert_eq!(age.max.as_deref(), Some("90"));

        let name = summary.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name.null_rate, 0.0);
        assert_eq!(name.min.as_deref(), Some("alice"));
        assert_eq!(name.max.as_deref(), Some("zoe"));
    }

    #[test]
    fn reduces_min_max_and_sums_distinct_count_across_manifests() {
        let batch = count_batch(20, 20, 20);
        // Three manifests (partitions), each with a slice of the age range —
        // the true dataset-wide min/max is 5..120, not any single file's.
        let stats = vec![
            int_stat("age", 3, 40, 60),
            int_stat("age", 2, 5, 30),
            int_stat("age", 4, 70, 120),
        ];
        let summary = build_summary(&batch, &stats).unwrap();
        let age = summary.columns.iter().find(|c| c.name == "age").unwrap();
        assert_eq!(age.min.as_deref(), Some("5"));
        assert_eq!(age.max.as_deref(), Some("120"));
        assert_eq!(age.distinct_count_estimate, 9); // capped sum, per Option 1
    }

    #[test]
    fn little_endian_bytes_would_sort_wrong_as_raw_strings_but_reduce_correctly_here() {
        // 255's LE byte0 is 0xFF and 256's LE byte0 is 0x00 (256 = 0x100),
        // so a naive byte-lexicographic comparison would conclude
        // 256 < 255 — exactly the failure mode the module doc warns about.
        // Typed decode-then-compare gets the numerically correct answer.
        let batch = count_batch(2, 2, 2);
        let stats = vec![int_stat("age", 1, 256, 256), int_stat("age", 1, 255, 255)];
        let summary = build_summary(&batch, &stats).unwrap();
        let age = summary.columns.iter().find(|c| c.name == "age").unwrap();
        assert_eq!(age.min.as_deref(), Some("255"));
        assert_eq!(age.max.as_deref(), Some("256"));
    }

    #[test]
    fn distinct_count_estimate_is_capped_at_row_count() {
        let batch = count_batch(5, 5, 5);
        let stats = vec![int_stat("age", 9999, 1, 5)];
        let summary = build_summary(&batch, &stats).unwrap();
        assert_eq!(summary.columns[0].distinct_count_estimate, 5);
    }

    #[test]
    fn empty_bytes_decode_to_no_min_max() {
        let batch = count_batch(5, 5, 5);
        let stats = vec![MergedColumnStats {
            name: "age".to_string(),
            data_type: "Int64".to_string(),
            distinct_count_estimate: 0,
            min_base64: Some(BASE64.encode([])),
            max_base64: None,
        }];
        let summary = build_summary(&batch, &stats).unwrap();
        assert!(summary.columns[0].min.is_none());
        assert!(summary.columns[0].max.is_none());
    }
}
