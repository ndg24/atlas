//! `WorkerService` implementation: `Compile` parses SQL into the
//! partial/combine plan pair (`crate::split`); `CompileFromPlan` (Phase 6)
//! does the same optimize+split for a caller-supplied `LogicalPlan` JSON
//! (e.g. the AI service's NL output) instead of SQL text, sharing all of
//! `Compile`'s logic past the parse step. `ExecuteTask` runs one plan
//! (JSON-serialized `atlas_query::LogicalPlan`) against either a file
//! partition or an inline set of already-computed Arrow IPC batches — the
//! combine step handed back to whichever worker runs it — and `Heartbeat`
//! reports liveness plus in-flight task count for the coordinator's worker
//! registry. A file partition's `format` field (`""`/`"atlas"`, `"parquet"`,
//! or `"iceberg"`) picks which `atlas_format` reader `run_task` calls —
//! `"iceberg"` reads via `read_parquet` too, since an Iceberg manifest's data
//! file is itself Parquet; only the catalog's provenance tag differs.

use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::ipc;
use crate::obs_metrics;
use crate::split::split_for_distribution;
use crate::telemetry::span_from_metadata;
use crate::worker_pb::worker_service_server::WorkerService;
use crate::worker_pb::{
    task_request::Source, AnalyzeRequest, AnalyzeResponse, CompileFromPlanRequest,
    CompileRequest, CompileResponse, HeartbeatRequest, HeartbeatResponse, ResultBatch,
    TaskRequest,
};

#[derive(Default)]
pub struct WorkerServiceImpl {
    in_flight: AtomicI32,
}

struct CompiledQuery {
    logical_plan_json: String,
    optimized_plan_json: String,
    partial_plan_json: String,
    combine_plan_json: Option<String>,
}

// Parses SQL into a LogicalPlan against the dataset's schema — the
// SQL-specific half of compiling a query. Shared with `parse_and_compile`
// below; `compile_plan_from_json` skips straight to `compile_plan` instead,
// since its caller (Phase 6's CompileFromPlan) already has a LogicalPlan.
fn parse_sql_to_plan(sql: &str, schema_json: &str) -> Result<atlas_query::LogicalPlan> {
    let schema: atlas_format::Schema =
        serde_json::from_str(schema_json).context("parsing dataset schema_json")?;
    let stmt = atlas_query::parse_sql(sql)?;
    atlas_query::build_logical_plan(&stmt, &schema)
}

// The SQL-agnostic half: optimize + split into partial/combine + serialize
// all four plan JSONs. Identical logic for a plan that started as SQL or as
// an AI-service-produced LogicalPlan — this is the one place either path
// touches atlas_optimizer/split_for_distribution.
fn compile_plan(raw_plan: atlas_query::LogicalPlan) -> Result<CompiledQuery> {
    let optimized_plan = atlas_optimizer::optimize(raw_plan.clone());
    let split = split_for_distribution(optimized_plan.clone());

    let logical_plan_json =
        serde_json::to_string(&raw_plan).context("serializing raw logical plan")?;
    let optimized_plan_json =
        serde_json::to_string(&optimized_plan).context("serializing optimized plan")?;
    let partial_plan_json =
        serde_json::to_string(&split.partial).context("serializing partial plan")?;
    let combine_plan_json = split
        .combine
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing combine plan")?;
    Ok(CompiledQuery {
        logical_plan_json,
        optimized_plan_json,
        partial_plan_json,
        combine_plan_json,
    })
}

fn compile_query(sql: &str, schema_json: &str) -> Result<CompiledQuery> {
    let raw_plan = parse_sql_to_plan(sql, schema_json)?;
    compile_plan(raw_plan)
}

// Phase 6: the AI service already produced a LogicalPlan (JSON, serde shape
// — see proto/plan.proto's header) instead of SQL text. schema_json isn't
// needed to build the plan here (it already exists), but is accepted for
// symmetry with compile_query and in case future validation wants it.
fn compile_query_from_plan(plan_json: &str, _schema_json: &str) -> Result<CompiledQuery> {
    let raw_plan: atlas_query::LogicalPlan =
        serde_json::from_str(plan_json).context("parsing plan_json")?;
    compile_plan(raw_plan)
}

fn run_task(req: TaskRequest) -> Result<Vec<RecordBatch>> {
    let plan: atlas_query::LogicalPlan =
        serde_json::from_str(&req.plan_json).context("parsing task plan_json")?;

    let batches = match req.source.ok_or_else(|| anyhow!("task missing a source"))? {
        Source::File(f) => {
            let columns = if f.columns.is_empty() {
                None
            } else {
                Some(f.columns)
            };
            match f.format.as_str() {
                "" | "atlas" => {
                    atlas_format::read_atlas_file(Path::new(&f.file_path), columns.as_deref())
                        .with_context(|| format!("reading partition file {}", f.file_path))?
                }
                "parquet" | "iceberg" => {
                    // An Iceberg-sourced manifest's file is a data file the
                    // external table already wrote in Parquet — the same
                    // reader as a native Parquet manifest applies unchanged.
                    atlas_format::read_parquet(Path::new(&f.file_path), columns.as_deref())
                        .with_context(|| {
                            format!("reading parquet partition file {}", f.file_path)
                        })?
                }
                other => {
                    return Err(anyhow!(
                        "unsupported file format {other:?} for {}",
                        f.file_path
                    ))
                }
            }
        }
        Source::Inline(inline) => {
            let mut batches = Vec::new();
            for blob in &inline.arrow_ipc_batches {
                batches.extend(ipc::decode_batches(blob)?);
            }
            batches
        }
    };

    atlas_exec::execute(&plan, batches)
}

// Phase 7 (AI Analyst): dispatches one atlas-insights check over batches the
// coordinator already produced via ordinary queries (never a raw file scan
// — the coordinator's summary/quality/outlier/trend queries all go through
// the normal Compile+ExecuteTask path first). This is the one place a
// worker interprets Arrow bytes outside of `run_task`'s query-execution
// path.
fn run_analyze(req: AnalyzeRequest) -> Result<String> {
    match req.kind.as_str() {
        "summary" => {
            let count_batches = ipc::decode_batches(&req.arrow_ipc)?;
            let count_batch = count_batches
                .first()
                .context("summary analyze: missing count batch")?;
            let stats: Vec<atlas_insights::MergedColumnStats> =
                serde_json::from_str(&req.column_stats_json)
                    .context("parsing column_stats_json as MergedColumnStats")?;
            let summary = atlas_insights::build_summary(count_batch, &stats)
                .context("building summary from count batch")?;
            serde_json::to_string(&summary).context("serializing summary")
        }
        "quality" => {
            let columns: Vec<atlas_insights::ColumnSummary> =
                serde_json::from_str(&req.column_stats_json)
                    .context("parsing column_stats_json as ColumnSummary")?;
            let sample = if req.sample_arrow_ipc.is_empty() {
                Vec::new()
            } else {
                ipc::decode_batches(&req.sample_arrow_ipc)?
            };
            let findings = atlas_insights::detect_data_quality_issues(&columns, &sample);
            serde_json::to_string(&findings).context("serializing quality findings")
        }
        "outlier" => {
            let batches = ipc::decode_batches(&req.arrow_ipc)?;
            let batch = batches
                .first()
                .context("outlier analyze: missing grouped batch")?;
            let findings = atlas_insights::detect_outlier_groups(
                batch,
                &req.group_by_column,
                &req.value_column,
            );
            serde_json::to_string(&findings).context("serializing outlier findings")
        }
        "trend" => {
            let batches = ipc::decode_batches(&req.arrow_ipc)?;
            let batch = batches
                .first()
                .context("trend analyze: missing time series batch")?;
            let finding = atlas_insights::detect_trend(batch, &req.time_column, &req.value_column);
            serde_json::to_string(&finding).context("serializing trend finding")
        }
        other => Err(anyhow!("unsupported analyze kind {other:?}")),
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    type ExecuteTaskStream =
        Pin<Box<dyn Stream<Item = Result<ResultBatch, Status>> + Send + 'static>>;

    async fn compile(
        &self,
        request: Request<CompileRequest>,
    ) -> Result<Response<CompileResponse>, Status> {
        let span = span_from_metadata(request.metadata(), "Compile");
        async move {
            let req = request.into_inner();
            let response = match compile_query(&req.sql, &req.schema_json) {
                Ok(compiled) => CompileResponse {
                    needs_combine: compiled.combine_plan_json.is_some(),
                    partial_plan_json: compiled.partial_plan_json,
                    combine_plan_json: compiled.combine_plan_json.unwrap_or_default(),
                    logical_plan_json: compiled.logical_plan_json,
                    optimized_plan_json: compiled.optimized_plan_json,
                    error: String::new(),
                },
                Err(err) => CompileResponse {
                    partial_plan_json: String::new(),
                    combine_plan_json: String::new(),
                    logical_plan_json: String::new(),
                    optimized_plan_json: String::new(),
                    needs_combine: false,
                    error: format!("{err:#}"),
                },
            };
            Ok(Response::new(response))
        }
        .instrument(span)
        .await
    }

    async fn compile_from_plan(
        &self,
        request: Request<CompileFromPlanRequest>,
    ) -> Result<Response<CompileResponse>, Status> {
        let span = span_from_metadata(request.metadata(), "CompileFromPlan");
        async move {
            let req = request.into_inner();
            let response = match compile_query_from_plan(&req.plan_json, &req.schema_json) {
                Ok(compiled) => CompileResponse {
                    needs_combine: compiled.combine_plan_json.is_some(),
                    partial_plan_json: compiled.partial_plan_json,
                    combine_plan_json: compiled.combine_plan_json.unwrap_or_default(),
                    logical_plan_json: compiled.logical_plan_json,
                    optimized_plan_json: compiled.optimized_plan_json,
                    error: String::new(),
                },
                Err(err) => CompileResponse {
                    partial_plan_json: String::new(),
                    combine_plan_json: String::new(),
                    logical_plan_json: String::new(),
                    optimized_plan_json: String::new(),
                    needs_combine: false,
                    error: format!("{err:#}"),
                },
            };
            Ok(Response::new(response))
        }
        .instrument(span)
        .await
    }

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let span = span_from_metadata(request.metadata(), "ExecuteTask");
        async move {
            let req = request.into_inner();
            let task_id = req.task_id.clone();

            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            obs_metrics::set_in_flight(in_flight);
            let started = Instant::now();
            let result = run_task(req);
            let in_flight = self.in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
            obs_metrics::set_in_flight(in_flight);

            let outcome = if result.is_ok() { "success" } else { "failure" };
            metrics::histogram!("atlas_worker_task_duration_seconds", "method" => "execute_task", "outcome" => outcome)
                .record(started.elapsed().as_secs_f64());
            metrics::counter!("atlas_worker_tasks_total", "method" => "execute_task", "outcome" => outcome)
                .increment(1);

            let batches = result
                .map_err(|err| Status::internal(format!("task {task_id} failed: {err:#}")))?;
            let arrow_ipc = ipc::encode_batches(&batches).map_err(|err| {
                Status::internal(format!("task {task_id} encode failed: {err:#}"))
            })?;

            let stream = tokio_stream::iter(vec![Ok(ResultBatch { arrow_ipc })]);
            Ok(Response::new(
                Box::pin(stream) as Self::ExecuteTaskStream
            ))
        }
        .instrument(span)
        .await
    }

    async fn analyze(
        &self,
        request: Request<AnalyzeRequest>,
    ) -> Result<Response<AnalyzeResponse>, Status> {
        let span = span_from_metadata(request.metadata(), "Analyze");
        async move {
            let req = request.into_inner();
            let response = match run_analyze(req) {
                Ok(findings_json) => AnalyzeResponse {
                    findings_json,
                    error: String::new(),
                },
                Err(err) => AnalyzeResponse {
                    findings_json: String::new(),
                    error: format!("{err:#}"),
                },
            };
            Ok(Response::new(response))
        }
        .instrument(span)
        .await
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let span = span_from_metadata(request.metadata(), "Heartbeat");
        async move {
            Ok(Response::new(HeartbeatResponse {
                alive: true,
                in_flight_tasks: self.in_flight.load(Ordering::SeqCst),
            }))
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_pb::FileSource;
    use atlas_format::{DataType, Field, Schema};

    /// A manifest tagged `format: "iceberg"` names a data file the external
    /// table already wrote as Parquet — proves `run_task` dispatches it to
    /// `read_parquet` (not `read_atlas_file`, and not an error) and that the
    /// resulting rows are correct, closing the loop from
    /// `atlas_format::read_iceberg_table`'s manifest translation through to
    /// actual query execution.
    #[test]
    fn iceberg_tagged_manifest_reads_via_the_parquet_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.parquet");
        let schema = Schema::new(vec![
            Field::new("diagnosis", DataType::Utf8, false),
            Field::new("cost", DataType::Float64, false),
        ]);
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(schema.clone()),
            vec![
                std::sync::Arc::new(arrow::array::StringArray::from(vec!["flu", "cold"])),
                std::sync::Arc::new(arrow::array::Float64Array::from(vec![100.0, 50.0])),
            ],
        )
        .unwrap();
        atlas_format::write_parquet(&path, &[batch]).unwrap();

        let compiled = compile_query(
            "SELECT diagnosis, cost FROM t",
            &serde_json::to_string(&schema).unwrap(),
        )
        .unwrap();

        let result = run_task(TaskRequest {
            task_id: "t".to_string(),
            plan_json: compiled.partial_plan_json,
            source: Some(Source::File(FileSource {
                file_path: path.to_string_lossy().into_owned(),
                columns: vec![],
                format: "iceberg".to_string(),
            })),
        })
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
        let diagnosis = result[0]
            .column_by_name("diagnosis")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(diagnosis.value(0), "flu");
        assert_eq!(diagnosis.value(1), "cold");
    }

    fn schema_json() -> String {
        let schema = Schema::new(vec![
            Field::new("diagnosis", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("cost", DataType::Float64, false),
            Field::new("hospital", DataType::Utf8, false),
        ]);
        serde_json::to_string(&schema).unwrap()
    }

    /// Proves column pruning has a real physical effect end to end: the
    /// worker-compiled partial plan's `Scan.columns` is exactly the columns
    /// this query touches, not all 4 schema columns. Composes with Phase 2's
    /// `read_atlas_file` column-skipping test (which proves a restricted
    /// `columns` list physically skips bytes) to establish the full claim
    /// without re-implementing that byte-counting harness here.
    #[test]
    fn compile_prunes_partial_plan_scan_columns() {
        let compiled =
            compile_query("SELECT diagnosis FROM t WHERE age > 50", &schema_json()).unwrap();

        let partial: atlas_query::LogicalPlan =
            serde_json::from_str(&compiled.partial_plan_json).unwrap();
        let atlas_query::LogicalPlan::Project(project) = &partial else {
            panic!("expected Project at partial plan root, got {partial:?}");
        };
        let atlas_query::LogicalPlan::Filter(filter) = project.input.as_ref() else {
            panic!("expected Filter under Project, got {:?}", project.input);
        };
        let atlas_query::LogicalPlan::Scan(scan) = filter.input.as_ref() else {
            panic!("expected Scan under Filter, got {:?}", filter.input);
        };
        assert_eq!(
            scan.columns,
            vec!["age".to_string(), "diagnosis".to_string()]
        );
        assert!(!scan.columns.contains(&"cost".to_string()));
        assert!(!scan.columns.contains(&"hospital".to_string()));
    }

    /// The whole point of `CompileFromPlan`: an NL-produced `LogicalPlan`
    /// (JSON, unopened by anything upstream) must converge on exactly the
    /// same optimized/partial/combine output the equivalent SQL produces via
    /// `compile_query` — proving the NL and SQL paths genuinely share one
    /// execution path past the parse step, not two parallel ones.
    #[test]
    fn compile_from_plan_matches_equivalent_sql() {
        let schema = schema_json();

        let via_sql = compile_query("SELECT diagnosis FROM t WHERE age > 50", &schema).unwrap();

        let stmt = atlas_query::parse_sql("SELECT diagnosis FROM t WHERE age > 50").unwrap();
        let parsed_schema: atlas_format::Schema = serde_json::from_str(&schema).unwrap();
        let raw_plan = atlas_query::build_logical_plan(&stmt, &parsed_schema).unwrap();
        let plan_json = serde_json::to_string(&raw_plan).unwrap();

        let via_plan = compile_query_from_plan(&plan_json, &schema).unwrap();

        assert_eq!(via_sql.logical_plan_json, via_plan.logical_plan_json);
        assert_eq!(via_sql.optimized_plan_json, via_plan.optimized_plan_json);
        assert_eq!(via_sql.partial_plan_json, via_plan.partial_plan_json);
        assert_eq!(via_sql.combine_plan_json, via_plan.combine_plan_json);
    }

    mod analyze_tests {
        use super::*;
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
        use atlas_insights::{DatasetSummary, MergedColumnStats, OutlierFinding, QualityFinding};
        use base64::engine::general_purpose::STANDARD as BASE64;
        use base64::Engine;
        use std::sync::Arc;

        fn count_batch() -> RecordBatch {
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("total_rows", ArrowDataType::Int64, false),
                ArrowField::new("age_non_null", ArrowDataType::Int64, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![10i64])),
                    Arc::new(Int64Array::from(vec![8i64])),
                ],
            )
            .unwrap()
        }

        fn grouped_batch() -> RecordBatch {
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("hospital", ArrowDataType::Utf8, false),
                ArrowField::new("rate", ArrowDataType::Float64, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"])),
                    Arc::new(Float64Array::from(vec![10.0, 11.0, 9.0, 12.0, 10.0, 50.0])),
                ],
            )
            .unwrap()
        }

        #[test]
        fn summary_kind_combines_count_batch_with_manifest_stats() {
            let stats = vec![MergedColumnStats {
                name: "age".to_string(),
                data_type: "Int64".to_string(),
                distinct_count_estimate: 5,
                min_base64: Some(BASE64.encode(10i64.to_le_bytes())),
                max_base64: Some(BASE64.encode(90i64.to_le_bytes())),
            }];
            let req = AnalyzeRequest {
                arrow_ipc: ipc::encode_batches(&[count_batch()]).unwrap(),
                kind: "summary".to_string(),
                group_by_column: String::new(),
                value_column: String::new(),
                time_column: String::new(),
                column_stats_json: serde_json::to_string(&stats).unwrap(),
                sample_arrow_ipc: Vec::new(),
            };

            let findings_json = run_analyze(req).unwrap();
            let summary: DatasetSummary = serde_json::from_str(&findings_json).unwrap();
            assert_eq!(summary.row_count, 10);
            assert_eq!(summary.columns[0].name, "age");
        }

        #[test]
        fn quality_kind_flags_high_null_rate_from_column_summaries() {
            let columns = vec![atlas_insights::ColumnSummary {
                name: "notes".to_string(),
                data_type: "Utf8".to_string(),
                null_rate: 0.5,
                distinct_count_estimate: 2,
                min: Some("a".to_string()),
                max: Some("z".to_string()),
            }];
            let req = AnalyzeRequest {
                arrow_ipc: Vec::new(),
                kind: "quality".to_string(),
                group_by_column: String::new(),
                value_column: String::new(),
                time_column: String::new(),
                column_stats_json: serde_json::to_string(&columns).unwrap(),
                sample_arrow_ipc: Vec::new(),
            };

            let findings_json = run_analyze(req).unwrap();
            let findings: Vec<QualityFinding> = serde_json::from_str(&findings_json).unwrap();
            assert!(matches!(
                findings[0],
                QualityFinding::HighNullRate { ref column, .. } if column == "notes"
            ));
        }

        #[test]
        fn outlier_kind_detects_a_clear_outlier_group() {
            let req = AnalyzeRequest {
                arrow_ipc: ipc::encode_batches(&[grouped_batch()]).unwrap(),
                kind: "outlier".to_string(),
                group_by_column: "hospital".to_string(),
                value_column: "rate".to_string(),
                time_column: String::new(),
                column_stats_json: String::new(),
                sample_arrow_ipc: Vec::new(),
            };

            let findings_json = run_analyze(req).unwrap();
            let findings: Vec<OutlierFinding> = serde_json::from_str(&findings_json).unwrap();
            assert_eq!(findings.len(), 1);
            assert_eq!(findings[0].group, "F");
        }

        #[test]
        fn unsupported_kind_is_an_error() {
            let req = AnalyzeRequest {
                arrow_ipc: Vec::new(),
                kind: "bogus".to_string(),
                group_by_column: String::new(),
                value_column: String::new(),
                time_column: String::new(),
                column_stats_json: String::new(),
                sample_arrow_ipc: Vec::new(),
            };
            assert!(run_analyze(req).is_err());
        }
    }
}
