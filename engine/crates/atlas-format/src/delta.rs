//! Reads an existing Delta Lake table (created by another engine, e.g.
//! Spark or the `deltalake` Python package) as an external table: replays
//! its `_delta_log/*.json` transaction log to recover the current schema
//! and the set of currently-live data files, then translates each into the
//! same shape Atlas's own ingestion path produces for a manifest — file
//! path, partition values, row/byte counts, and per-column min/max/null-count
//! stats (see `atlas-cli`'s `ManifestInput`). Everything downstream (pruning,
//! scheduling, execution) then treats a Delta-sourced manifest exactly like
//! one Atlas wrote itself.
//!
//! This is a from-spec reader (Delta's own PROTOCOL.md), not a wrapper over
//! the `deltalake` crate: that crate pulls in an async runtime and its own
//! object-store/table abstractions this read-only, filesystem-pointed path
//! doesn't need. Delta's transaction log is plain JSON lines (unlike
//! Iceberg's Avro manifests), so this needs no new dependency at all —
//! `serde_json`, already a dependency for `.atlas`'s own footer, is enough.
//!
//! Deliberately unsupported for now (bail with a clear error, or fall back
//! to a sensible default, rather than silently returning wrong data):
//! checkpoint files (`_last_checkpoint` / `*.checkpoint.parquet` — only the
//! plain JSON commit log is replayed, so a table whose early JSON commits
//! have been removed after checkpointing won't read correctly), non-Parquet
//! data files, and nested/temporal Delta types beyond the primitives
//! Atlas's own `Schema` already models.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::Deserialize;
use serde_json::Value as Json;

/// One live data file from a Delta table's current state, already
/// translated into Atlas's own manifest shape.
pub struct DeltaDataFile {
    pub file_path: PathBuf,
    pub row_count: i64,
    pub file_size_bytes: i64,
    /// `{column: value}`, native JSON scalars — same shape the coordinator's
    /// partition-pruning code expects (and the same shape Iceberg's own
    /// partition values already arrive in).
    pub partition_values: HashMap<String, Json>,
    /// `{column: {min, max, null_count}}`, `min`/`max` base64-encoded to
    /// match `atlas-cli`'s own `column_stats_by_name` encoding: LE bytes for
    /// numeric columns, raw UTF-8 for strings. Unlike Iceberg (whose bounds
    /// arrive pre-serialized in that exact byte convention), Delta's
    /// `stats` JSON holds plain JSON-typed values, so this is a real encode
    /// step, not a pass-through.
    pub column_stats: HashMap<String, Json>,
}

pub struct DeltaTable {
    pub schema: Schema,
    pub data_files: Vec<DeltaDataFile>,
}

#[derive(Deserialize)]
struct DeltaSchemaString {
    fields: Vec<DeltaField>,
}

#[derive(Deserialize)]
struct DeltaField {
    name: String,
    #[serde(rename = "type")]
    type_: Json,
    #[serde(default = "default_true")]
    nullable: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
struct MetaDataAction {
    #[serde(rename = "schemaString")]
    schema_string: String,
}

#[derive(Deserialize, Clone)]
struct AddAction {
    path: String,
    #[serde(rename = "partitionValues", default)]
    partition_values: HashMap<String, String>,
    size: i64,
    #[serde(default)]
    stats: Option<String>,
}

#[derive(Deserialize)]
struct RemoveAction {
    path: String,
}

#[derive(Deserialize, Default)]
struct DeltaStats {
    #[serde(rename = "numRecords", default)]
    num_records: i64,
    #[serde(rename = "minValues", default)]
    min_values: HashMap<String, Json>,
    #[serde(rename = "maxValues", default)]
    max_values: HashMap<String, Json>,
    #[serde(rename = "nullCount", default)]
    null_count: HashMap<String, Json>,
}

/// Read `table_path` (a Delta table's root directory, containing
/// `_delta_log/`) and return its current schema plus every live data file.
pub fn read_delta_table(table_path: &Path) -> Result<DeltaTable> {
    let log_dir = table_path.join("_delta_log");
    let mut commit_files: Vec<PathBuf> = std::fs::read_dir(&log_dir)
        .with_context(|| format!("reading delta log directory {}", log_dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    commit_files.sort();
    if commit_files.is_empty() {
        bail!("no commit files found under {}", log_dir.display());
    }

    let mut schema_string: Option<String> = None;
    let mut live_files: HashMap<String, AddAction> = HashMap::new();

    for commit_file in &commit_files {
        let contents = std::fs::read_to_string(commit_file)
            .with_context(|| format!("reading delta commit {}", commit_file.display()))?;
        for line in contents.lines().filter(|l| !l.trim().is_empty()) {
            let action: Json = serde_json::from_str(line).with_context(|| {
                format!("parsing delta commit line in {}", commit_file.display())
            })?;
            let Json::Object(map) = &action else { continue };
            if let Some(meta) = map.get("metaData") {
                let meta: MetaDataAction = serde_json::from_value(meta.clone())
                    .context("parsing delta metaData action")?;
                schema_string = Some(meta.schema_string);
            }
            if let Some(add) = map.get("add") {
                let add: AddAction =
                    serde_json::from_value(add.clone()).context("parsing delta add action")?;
                live_files.insert(add.path.clone(), add);
            }
            if let Some(remove) = map.get("remove") {
                let remove: RemoveAction = serde_json::from_value(remove.clone())
                    .context("parsing delta remove action")?;
                live_files.remove(&remove.path);
            }
        }
    }

    let schema_string = schema_string.ok_or_else(|| {
        anyhow!(
            "no metaData action found in delta log at {}",
            log_dir.display()
        )
    })?;
    let delta_schema: DeltaSchemaString =
        serde_json::from_str(&schema_string).context("parsing delta schemaString")?;
    let schema = translate_schema(&delta_schema)?;
    let field_types: HashMap<String, DataType> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();

    let mut data_files = Vec::new();
    for add in live_files.into_values() {
        data_files.push(translate_add_action(add, table_path, &field_types)?);
    }

    Ok(DeltaTable { schema, data_files })
}

fn translate_schema(schema: &DeltaSchemaString) -> Result<Schema> {
    let fields = schema
        .fields
        .iter()
        .map(|f| {
            let data_type = translate_type(&f.type_)
                .with_context(|| format!("translating delta field {:?}", f.name))?;
            Ok(Field::new(&f.name, data_type, f.nullable))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

/// Only primitive types Atlas's own `Schema` already models are supported —
/// mirrors `iceberg.rs`'s `translate_type` scoping. Nested types
/// (struct/array/map), `timestamp`, `decimal`, and `binary` are out of scope
/// until Atlas's own type set grows to match.
fn translate_type(delta_type: &Json) -> Result<DataType> {
    let name = delta_type
        .as_str()
        .ok_or_else(|| anyhow!("unsupported (non-primitive) delta type: {delta_type}"))?;
    Ok(match name {
        "boolean" => DataType::Boolean,
        "byte" | "short" | "integer" | "long" => DataType::Int64,
        "float" | "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "date" => DataType::Date32,
        other => bail!("unsupported delta primitive type: {other}"),
    })
}

fn translate_add_action(
    add: AddAction,
    table_path: &Path,
    field_types: &HashMap<String, DataType>,
) -> Result<DeltaDataFile> {
    let file_path = table_path.join(&add.path);

    let mut partition_values = HashMap::new();
    for (name, raw) in &add.partition_values {
        let data_type = field_types.get(name);
        partition_values.insert(name.clone(), partition_string_to_json(raw, data_type)?);
    }

    let stats: DeltaStats = match &add.stats {
        Some(s) => serde_json::from_str(s).context("parsing delta add.stats")?,
        None => DeltaStats::default(),
    };

    let mut column_stats = HashMap::new();
    for (name, data_type) in field_types {
        let min = stats.min_values.get(name);
        let max = stats.max_values.get(name);
        if min.is_none() && max.is_none() {
            continue;
        }
        let null_count = stats
            .null_count
            .get(name)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let min_encoded = min
            .map(|v| encode_stat_value(v, data_type))
            .transpose()?
            .unwrap_or_default();
        let max_encoded = max
            .map(|v| encode_stat_value(v, data_type))
            .transpose()?
            .unwrap_or_default();
        column_stats.insert(
            name.clone(),
            serde_json::json!({
                "min": min_encoded,
                "max": max_encoded,
                "null_count": null_count,
            }),
        );
    }

    Ok(DeltaDataFile {
        file_path,
        row_count: stats.num_records,
        file_size_bytes: add.size,
        partition_values,
        column_stats,
    })
}

/// Delta always serializes partition values as their canonical string
/// representation in the log regardless of column type — convert back to a
/// native JSON scalar matching the partitioned column's type, the same
/// native-scalar shape Iceberg's partition values already arrive in.
fn partition_string_to_json(raw: &str, data_type: Option<&DataType>) -> Result<Json> {
    Ok(match data_type {
        Some(DataType::Int64) => Json::from(
            raw.parse::<i64>()
                .with_context(|| format!("parsing delta partition value {raw:?} as int"))?,
        ),
        Some(DataType::Float64) => Json::from(
            raw.parse::<f64>()
                .with_context(|| format!("parsing delta partition value {raw:?} as float"))?,
        ),
        Some(DataType::Boolean) => Json::from(
            raw.parse::<bool>()
                .with_context(|| format!("parsing delta partition value {raw:?} as bool"))?,
        ),
        _ => Json::from(raw.to_string()),
    })
}

/// Base64-encode a `stats.json`-typed min/max value into the same byte
/// convention `atlas-cli`'s own column-stats encoding uses: LE bytes for
/// int64/float64/date32, raw UTF-8 for strings, a single 0/1 byte for
/// booleans. Delta represents date bounds as `"YYYY-MM-DD"` strings in
/// `stats.json` (unlike partition values, which are always strings, date
/// *stats* are the one place Delta's JSON typing doesn't match the column's
/// declared type at all), so those need converting to epoch-day ints first.
fn encode_stat_value(value: &Json, data_type: &DataType) -> Result<String> {
    let bytes = match (data_type, value) {
        (DataType::Int64, Json::Number(n)) => n
            .as_i64()
            .ok_or_else(|| anyhow!("delta stats value {n} is not an int64"))?
            .to_le_bytes()
            .to_vec(),
        (DataType::Float64, Json::Number(n)) => n
            .as_f64()
            .ok_or_else(|| anyhow!("delta stats value {n} is not a float64"))?
            .to_le_bytes()
            .to_vec(),
        (DataType::Utf8, Json::String(s)) => s.as_bytes().to_vec(),
        (DataType::Boolean, Json::Bool(b)) => vec![*b as u8],
        (DataType::Date32, Json::String(s)) => {
            parse_iso_date_to_epoch_days(s)?.to_le_bytes().to_vec()
        }
        (other_type, other_value) => {
            bail!("delta stats value {other_value} doesn't match column type {other_type:?}")
        }
    };
    Ok(BASE64.encode(bytes))
}

/// Parse a `"YYYY-MM-DD"` date string into days-since-1970-01-01 (Arrow's
/// `Date32` representation), via Howard Hinnant's `days_from_civil`
/// algorithm — plain integer arithmetic, so no date/calendar dependency is
/// needed just to decode this one field.
fn parse_iso_date_to_epoch_days(s: &str) -> Result<i32> {
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    let [y, m, d] = parts.as_slice() else {
        bail!("delta date stat {s:?} is not in YYYY-MM-DD form");
    };
    let y: i64 = y
        .parse()
        .with_context(|| format!("parsing year in {s:?}"))?;
    let m: i64 = m
        .parse()
        .with_context(|| format!("parsing month in {s:?}"))?;
    let d: i64 = d.parse().with_context(|| format!("parsing day in {s:?}"))?;

    let y2 = if m <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days = era * 146097 + doe - 719468;
    i32::try_from(days).with_context(|| format!("epoch-day value for {s:?} out of Date32 range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_table_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/delta_sample/patients")
    }

    /// The fixture (`tests/fixtures/delta_sample`) was generated by the
    /// Python `deltalake` package with a partition on `hospital` and 5 rows
    /// split across 2 partitions ("mercy": 2 rows, "stmarys": 3 rows) — a
    /// real, independently-implemented Delta writer, not Atlas's own, so
    /// this exercises the reader against a genuine Delta log rather than a
    /// fixture this same reader could have silently gotten wrong in a way
    /// that agreed with itself.
    #[test]
    fn reads_schema_and_data_files_from_a_real_delta_table() {
        let table = read_delta_table(&fixture_table_path()).unwrap();

        assert_eq!(
            table
                .schema
                .field_with_name("hospital")
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            table.schema.field_with_name("age").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            table.schema.field_with_name("cost").unwrap().data_type(),
            &DataType::Float64
        );

        assert_eq!(table.data_files.len(), 2);
        let total_rows: i64 = table.data_files.iter().map(|f| f.row_count).sum();
        assert_eq!(total_rows, 5);
        for f in &table.data_files {
            assert!(
                f.file_path.exists(),
                "{} should exist",
                f.file_path.display()
            );
        }
    }

    #[test]
    fn partition_values_match_the_partition_on_hospital() {
        let table = read_delta_table(&fixture_table_path()).unwrap();

        let mut by_partition: HashMap<String, i64> = HashMap::new();
        for f in &table.data_files {
            let hospital = f
                .partition_values
                .get("hospital")
                .unwrap()
                .as_str()
                .unwrap();
            *by_partition.entry(hospital.to_string()).or_default() += f.row_count;
        }
        assert_eq!(by_partition.get("mercy").copied(), Some(2));
        assert_eq!(by_partition.get("stmarys").copied(), Some(3));
    }

    #[test]
    fn column_stats_decode_to_atlas_le_byte_convention() {
        let table = read_delta_table(&fixture_table_path()).unwrap();

        let mercy = table
            .data_files
            .iter()
            .find(|f| f.partition_values.get("hospital").and_then(|v| v.as_str()) == Some("mercy"))
            .unwrap();
        let age_stats = mercy.column_stats.get("age").unwrap();
        let min_bytes = BASE64.decode(age_stats["min"].as_str().unwrap()).unwrap();
        let min_age = i64::from_le_bytes(min_bytes.try_into().unwrap());
        assert_eq!(min_age, 34);

        let cost_stats = mercy.column_stats.get("cost").unwrap();
        let max_bytes = BASE64.decode(cost_stats["max"].as_str().unwrap()).unwrap();
        let max_cost = f64::from_le_bytes(max_bytes.try_into().unwrap());
        assert_eq!(max_cost, 120.5);
    }

    #[test]
    fn epoch_day_parsing_matches_known_dates() {
        assert_eq!(parse_iso_date_to_epoch_days("1970-01-01").unwrap(), 0);
        assert_eq!(parse_iso_date_to_epoch_days("1970-01-02").unwrap(), 1);
        assert_eq!(parse_iso_date_to_epoch_days("2024-01-15").unwrap(), 19737);
    }
}
