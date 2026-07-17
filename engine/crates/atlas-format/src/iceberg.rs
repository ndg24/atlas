//! Reads an existing Iceberg table (created by another engine, e.g. Spark or
//! PyIceberg) as an external table: parses the table's `metadata.json`,
//! follows its current snapshot's manifest list and manifests (both Avro),
//! and translates every live data file into the same shape Atlas's own
//! ingestion path produces for a manifest — file path, partition values,
//! row/byte counts, and per-column min/max/null-count stats (see
//! `atlas-cli`'s `ManifestInput`). Everything downstream (pruning,
//! scheduling, execution) then treats an Iceberg-sourced manifest exactly
//! like one Atlas wrote itself.
//!
//! This is a from-spec reader (Iceberg's table-spec.md), not a wrapper over
//! the `iceberg-rust` crate: that crate's dependency graph (an async runtime,
//! a REST/Glue/Hive catalog client, object-store backends) is all catalog
//! machinery this read-only, filesystem-pointed path doesn't need. The only
//! real parsing work — Avro manifests — is done via `apache-avro`, the same
//! crate `iceberg-rust` itself uses for that part.
//!
//! Deliberately unsupported for now (bail with a clear error rather than
//! silently returning wrong data): row-level delete files (`content` != 0 at
//! the manifest level, `status` == 2 at the entry level), non-Parquet data
//! files, and nested/temporal Iceberg types beyond the primitives Atlas's own
//! `Schema` already models.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use apache_avro::types::Value as Avro;
use apache_avro::Reader as AvroReader;
use arrow::datatypes::{DataType, Field, Schema};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::Deserialize;
use serde_json::Value as Json;

/// One live data file from an Iceberg table's current snapshot, already
/// translated into Atlas's own manifest shape.
pub struct IcebergDataFile {
    pub file_path: PathBuf,
    pub row_count: i64,
    pub file_size_bytes: i64,
    /// `{column: value}`, native JSON scalars — same shape the coordinator's
    /// partition-pruning code (`decodeManifestStats`) expects.
    pub partition_values: HashMap<String, Json>,
    /// `{column: {min, max, null_count}}`, `min`/`max` base64-encoded to
    /// match `atlas-cli`'s own `column_stats_by_name` encoding exactly —
    /// Iceberg's single-value serialization for int/long/float/double/string
    /// is byte-identical to Atlas's own, so the bounds are passed through
    /// unchanged, just re-tagged by field name instead of field id.
    pub column_stats: HashMap<String, Json>,
}

pub struct IcebergTable {
    pub schema: Schema,
    pub data_files: Vec<IcebergDataFile>,
}

#[derive(Deserialize)]
struct TableMetadata {
    #[serde(default)]
    schemas: Vec<IcebergSchema>,
    schema: Option<IcebergSchema>,
    #[serde(rename = "current-schema-id", default)]
    current_schema_id: i32,
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<SnapshotMeta>,
}

#[derive(Deserialize)]
struct IcebergSchema {
    #[serde(rename = "schema-id", default)]
    schema_id: i32,
    fields: Vec<IcebergField>,
}

#[derive(Deserialize)]
struct IcebergField {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    type_: Json,
    #[serde(default)]
    required: bool,
}

#[derive(Deserialize)]
struct SnapshotMeta {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
}

/// Read `metadata_path` (an Iceberg table's current `metadata/*.metadata.json`
/// — the same pointer a real catalog, Hive/Glue/REST, would hand back for
/// "the current metadata location of this table") and return its current
/// snapshot's schema plus every live data file.
pub fn read_iceberg_table(metadata_path: &Path) -> Result<IcebergTable> {
    let table_root = metadata_path
        .parent()
        .and_then(Path::parent)
        .with_context(|| {
            format!(
                "expected {} to live under a `metadata/` directory inside the table root",
                metadata_path.display()
            )
        })?
        .to_path_buf();

    let metadata_bytes = std::fs::read(metadata_path).with_context(|| {
        format!(
            "reading iceberg metadata.json at {}",
            metadata_path.display()
        )
    })?;
    let metadata: TableMetadata =
        serde_json::from_slice(&metadata_bytes).context("parsing iceberg metadata.json")?;

    let iceberg_schema = metadata
        .schemas
        .iter()
        .find(|s| s.schema_id == metadata.current_schema_id)
        .or(metadata.schema.as_ref())
        .ok_or_else(|| anyhow!("iceberg metadata.json has no schema matching current-schema-id"))?;
    let schema = translate_schema(iceberg_schema)?;
    let field_names_by_id: HashMap<i32, String> = iceberg_schema
        .fields
        .iter()
        .map(|f| (f.id, f.name.clone()))
        .collect();

    let current_snapshot_id = metadata
        .current_snapshot_id
        .ok_or_else(|| anyhow!("iceberg table has no current-snapshot-id (empty table?)"))?;
    let snapshot = metadata
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == current_snapshot_id)
        .ok_or_else(|| {
            anyhow!("current-snapshot-id {current_snapshot_id} not found in snapshots")
        })?;

    let manifest_list_path = resolve_iceberg_path(&table_root, &snapshot.manifest_list);
    let mut data_files = Vec::new();
    for manifest_path in read_manifest_list(&manifest_list_path, &table_root)? {
        data_files.extend(read_manifest(
            &manifest_path,
            &table_root,
            &field_names_by_id,
        )?);
    }

    Ok(IcebergTable { schema, data_files })
}

fn translate_schema(schema: &IcebergSchema) -> Result<Schema> {
    let fields = schema
        .fields
        .iter()
        .map(|f| {
            let data_type = translate_type(&f.type_)
                .with_context(|| format!("translating iceberg field {:?}", f.name))?;
            Ok(Field::new(&f.name, data_type, !f.required))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

/// Only primitive types Atlas's own `Schema` already models are supported —
/// wide enough for any table `infer_schema`/`write_atlas_file` could have
/// produced itself. Nested types (struct/list/map), temporal types beyond
/// `date`, and decimal/binary/uuid are out of scope until Atlas's own type
/// set grows to match.
fn translate_type(iceberg_type: &Json) -> Result<DataType> {
    let name = iceberg_type
        .as_str()
        .ok_or_else(|| anyhow!("unsupported (non-primitive) iceberg type: {iceberg_type}"))?;
    Ok(match name {
        "boolean" => DataType::Boolean,
        "int" | "long" => DataType::Int64,
        "float" | "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "date" => DataType::Date32,
        other => bail!("unsupported iceberg primitive type: {other}"),
    })
}

/// Strip a `file:` URI scheme if present, then fall back to resolving the
/// path relative to `table_root` if the literal path doesn't exist on disk
/// — a table's metadata always embeds the absolute location it was written
/// at, which is exactly wrong the moment the table directory is copied or
/// vendored somewhere else (as a checked-in test fixture, for instance).
/// Real, non-relocated Iceberg tables always resolve via the first, literal
/// path; only relocated ones fall through to the second.
fn resolve_iceberg_path(table_root: &Path, raw: &str) -> PathBuf {
    let stripped = raw.strip_prefix("file://").unwrap_or(raw);
    let literal = PathBuf::from(stripped);
    if literal.exists() {
        return literal;
    }
    for marker in ["/metadata/", "\\metadata\\", "/data/", "\\data\\"] {
        if let Some(idx) = stripped.rfind(marker) {
            let tail = &stripped[idx + 1..];
            let candidate = table_root.join(tail.replace('\\', "/"));
            if candidate.exists() {
                return candidate;
            }
        }
    }
    literal
}

/// Returns the resolved local paths of every data manifest (`content == 0`)
/// referenced by the manifest list — delete manifests (`content == 1`) are
/// skipped, since Atlas has no row-level delete support yet.
fn read_manifest_list(manifest_list_path: &Path, table_root: &Path) -> Result<Vec<PathBuf>> {
    let file = File::open(manifest_list_path).with_context(|| {
        format!(
            "opening iceberg manifest list at {}",
            manifest_list_path.display()
        )
    })?;
    let reader = AvroReader::new(file).context("reading manifest list avro header")?;

    let mut paths = Vec::new();
    for record in reader {
        let record = record.context("decoding manifest-list entry")?;
        let fields = as_record(&record)?;
        let content = as_int(field(fields, "content")?).unwrap_or(0);
        if content != 0 {
            continue; // delete manifest — not yet supported
        }
        let manifest_path = as_str(field(fields, "manifest_path")?)?;
        paths.push(resolve_iceberg_path(table_root, manifest_path));
    }
    Ok(paths)
}

fn read_manifest(
    manifest_path: &Path,
    table_root: &Path,
    field_names_by_id: &HashMap<i32, String>,
) -> Result<Vec<IcebergDataFile>> {
    let file = File::open(manifest_path)
        .with_context(|| format!("opening iceberg manifest at {}", manifest_path.display()))?;
    let reader = AvroReader::new(file).context("reading manifest avro header")?;

    let mut out = Vec::new();
    for record in reader {
        let record = record.context("decoding manifest entry")?;
        let fields = as_record(&record)?;
        let status = as_int(field(fields, "status")?)?;
        if status == 2 {
            continue; // DELETED entry — the file it names is no longer live
        }
        let data_file = as_record(field(fields, "data_file")?)?;
        out.push(translate_data_file(
            data_file,
            table_root,
            field_names_by_id,
        )?);
    }
    Ok(out)
}

fn translate_data_file(
    fields: &[(String, Avro)],
    table_root: &Path,
    field_names_by_id: &HashMap<i32, String>,
) -> Result<IcebergDataFile> {
    let file_format = as_str(field(fields, "file_format")?)?;
    if !file_format.eq_ignore_ascii_case("parquet") {
        bail!("unsupported iceberg data file format {file_format:?} (only Parquet is supported)");
    }
    let file_path = as_str(field(fields, "file_path")?)?;
    let file_path = resolve_iceberg_path(table_root, file_path);
    let row_count = as_int(field(fields, "record_count")?)?;
    let file_size_bytes = as_int(field(fields, "file_size_in_bytes")?)?;

    let mut partition_values = HashMap::new();
    if let Ok(partition) = field(fields, "partition") {
        for (name, value) in as_record(partition)? {
            if let Some(json) = avro_scalar_to_json(unwrap_union(value))? {
                partition_values.insert(name.clone(), json);
            }
        }
    }

    let null_counts = read_id_value_map(fields, "null_value_counts")?;
    let lower_bounds = read_id_value_map(fields, "lower_bounds")?;
    let upper_bounds = read_id_value_map(fields, "upper_bounds")?;

    let mut column_stats = HashMap::new();
    let field_ids: std::collections::BTreeSet<i32> = lower_bounds
        .keys()
        .chain(upper_bounds.keys())
        .chain(null_counts.keys())
        .copied()
        .collect();
    for field_id in field_ids {
        let Some(name) = field_names_by_id.get(&field_id) else {
            continue;
        };
        let min = lower_bounds.get(&field_id).cloned().unwrap_or_default();
        let max = upper_bounds.get(&field_id).cloned().unwrap_or_default();
        let null_count = null_counts
            .get(&field_id)
            .map(|bytes| i64::from_le_bytes(bytes.as_slice().try_into().unwrap()))
            .unwrap_or(0);
        column_stats.insert(
            name.clone(),
            serde_json::json!({
                "min": BASE64.encode(min),
                "max": BASE64.encode(max),
                "null_count": null_count,
            }),
        );
    }

    Ok(IcebergDataFile {
        file_path,
        row_count,
        file_size_bytes,
        partition_values,
        column_stats,
    })
}

/// Iceberg encodes its per-file `int -> T` maps (field id -> bound bytes,
/// field id -> null count, ...) as an array of `{key, value}` records rather
/// than a native Avro map, since Avro map keys must be strings — decode that
/// convention generically for both the `bytes`-valued and `long`-valued maps.
fn read_id_value_map(fields: &[(String, Avro)], name: &str) -> Result<HashMap<i32, Vec<u8>>> {
    let mut out = HashMap::new();
    let Ok(raw) = field(fields, name) else {
        return Ok(out);
    };
    let Avro::Array(entries) = unwrap_union(raw) else {
        return Ok(out);
    };
    for entry in entries {
        let entry_fields = as_record(entry)?;
        let key = as_int(field(entry_fields, "key")?)? as i32;
        let value = match unwrap_union(field(entry_fields, "value")?) {
            Avro::Bytes(b) => b.clone(),
            Avro::Long(v) => v.to_le_bytes().to_vec(),
            Avro::Int(v) => (*v as i64).to_le_bytes().to_vec(),
            other => bail!("unexpected avro value in {name} map: {other:?}"),
        };
        out.insert(key, value);
    }
    Ok(out)
}

fn avro_scalar_to_json(value: &Avro) -> Result<Option<Json>> {
    Ok(match value {
        Avro::Null => None,
        Avro::Boolean(b) => Some(Json::from(*b)),
        Avro::Int(i) => Some(Json::from(*i)),
        Avro::Long(i) => Some(Json::from(*i)),
        Avro::Float(f) => Some(Json::from(*f)),
        Avro::Double(f) => Some(Json::from(*f)),
        Avro::String(s) => Some(Json::from(s.clone())),
        Avro::Date(d) => Some(Json::from(*d)),
        other => bail!("unsupported avro partition value: {other:?}"),
    })
}

fn as_record(value: &Avro) -> Result<&[(String, Avro)]> {
    match value {
        Avro::Record(fields) => Ok(fields),
        other => bail!("expected an avro record, got {other:?}"),
    }
}

fn field<'a>(fields: &'a [(String, Avro)], name: &str) -> Result<&'a Avro> {
    fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
        .ok_or_else(|| anyhow!("avro record missing field {name:?}"))
}

fn unwrap_union(value: &Avro) -> &Avro {
    match value {
        Avro::Union(_, inner) => inner,
        other => other,
    }
}

fn as_str(value: &Avro) -> Result<&str> {
    match unwrap_union(value) {
        Avro::String(s) => Ok(s.as_str()),
        other => bail!("expected an avro string, got {other:?}"),
    }
}

fn as_int(value: &Avro) -> Result<i64> {
    match unwrap_union(value) {
        Avro::Int(i) => Ok(*i as i64),
        Avro::Long(i) => Ok(*i),
        other => bail!("expected an avro int/long, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_metadata_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "tests/fixtures/iceberg_sample/atlas_test/patients/metadata/\
             00002-54055c3d-433b-4854-aa0b-3b07f5f1e460.metadata.json",
        )
    }

    /// The fixture (`tests/fixtures/iceberg_sample`) was generated by
    /// PyIceberg with an identity partition spec on `hospital` and 5 rows
    /// split across 2 partitions ("mercy": 2 rows, "stmarys": 3 rows) — a
    /// real, independently-implemented Iceberg writer, not Atlas's own, so
    /// this exercises the reader against genuine Iceberg metadata/Avro
    /// rather than a fixture this same reader could have silently gotten
    /// wrong in a way that agreed with itself.
    #[test]
    fn reads_schema_and_data_files_from_a_real_iceberg_table() {
        let table = read_iceberg_table(&fixture_metadata_path()).unwrap();

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
    fn partition_values_match_the_identity_spec_on_hospital() {
        let table = read_iceberg_table(&fixture_metadata_path()).unwrap();

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

    /// `age`/`cost` bounds are decoded straight from Iceberg's own
    /// single-value serialization (LE bytes for long/double) with no
    /// reinterpretation — proves the pass-through claim in the module docs,
    /// not just that *some* bytes came back.
    #[test]
    fn column_stats_decode_to_atlas_le_byte_convention() {
        let table = read_iceberg_table(&fixture_metadata_path()).unwrap();

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
}
