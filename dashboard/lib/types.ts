// Types mirroring the coordinator's REST API and the AI service's data
// shapes exactly as they appear on the wire (confirmed by reading the
// actual Go/Rust/Python source, not proto/plan.proto -- that file is
// spec-only and not what's serialized). See docs/atlas-implementation-spec.md
// for the coordinator route table.

// ---- Datasets ----

export interface Dataset {
  id: string;
  name: string;
  schema_json: string;
  // omitempty on the Go side: absent from JSON (not "" / null) until a
  // snapshot has been committed.
  current_snapshot_id?: string;
  created_at?: string;
}

// ---- Query / decoded result shape (post Arrow-IPC decode) ----

export interface DecodedResult {
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface QueryResponse {
  query_id: string;
  duration_ms: number;
  result: DecodedResult;
  cache_hit: boolean;
}

export interface NLQueryResponse extends QueryResponse {
  raw_llm_output: string;
  // omitempty: only present when the request set narrate=true AND the
  // result collapsed to exactly one batch.
  explanation?: string;
}

// ---- Explain / LogicalPlan ----
//
// This is engine/crates/atlas-query/src/plan.rs's serde output (externally
// tagged enums), which is what /explain actually returns -- not
// proto/plan.proto's shape.

export type BinaryOp =
  | "Eq"
  | "NotEq"
  | "Lt"
  | "LtEq"
  | "Gt"
  | "GtEq"
  | "And"
  | "Or"
  | "Add"
  | "Sub"
  | "Mul"
  | "Div";

export type Literal =
  | { Int: number }
  | { Float: number }
  | { Str: string }
  | { Bool: boolean };

export type Expr =
  | { Column: string }
  | { Literal: Literal }
  | { Binary: { left: Expr; op: BinaryOp; right: Expr } };

export interface ScanNode {
  dataset: string;
  columns: string[];
  snapshot_id: string;
}
export interface FilterNode {
  input: LogicalPlan;
  predicate: Expr;
}
export interface ProjectNode {
  input: LogicalPlan;
  exprs: Expr[];
  aliases: string[];
}
export type AggFunc = "Count" | "Sum" | "Avg" | "Min" | "Max";
export interface AggregateExpr {
  func: AggFunc;
  arg: Expr | null;
  alias: string;
}
export interface AggregateNode {
  input: LogicalPlan;
  group_by: Expr[];
  aggregates: AggregateExpr[];
}
export interface SortKey {
  expr: Expr;
  descending: boolean;
}
export interface SortNode {
  input: LogicalPlan;
  keys: SortKey[];
}
export interface LimitNode {
  input: LogicalPlan;
  n: number;
}

// No JoinNode yet -- Phase 4+ only, not on the wire today.
export type LogicalPlan =
  | { Scan: ScanNode }
  | { Filter: FilterNode }
  | { Project: ProjectNode }
  | { Aggregate: AggregateNode }
  | { Sort: SortNode }
  | { Limit: LimitNode };

export interface ExplainResponse {
  logical_plan: LogicalPlan | null;
  optimized_plan: LogicalPlan | null;
  manifests_before_pruning: string[];
  manifests_after_pruning: string[];
  cache_hit: boolean;
}

// ---- Insights ----

export interface ColumnSummary {
  name: string;
  data_type: string;
  null_rate: number;
  distinct_count_estimate: number;
  min?: string;
  max?: string;
}
export interface DatasetSummary {
  row_count: number;
  columns: ColumnSummary[];
}

export type QualityFinding =
  | { kind: "HighNullRate"; column: string; null_rate: number }
  | { kind: "ZeroVariance"; column: string; value: string }
  | { kind: "DuplicateRows"; count: number; sample_row_indices: number[] };

export interface OutlierFinding {
  group: string;
  value: number;
  group_mean: number;
  group_stddev: number;
  z_score: number;
  group_col: string;
  value_col: string;
}
export interface TrendFinding {
  time_col: string;
  value_col: string;
  slope: number;
  direction: string;
  r_squared: number;
}

export interface InsightsResponse {
  summary: DatasetSummary;
  quality_findings: QualityFinding[];
  // omitempty: absent (not []) when the heuristic column-picker found no
  // suitable group/value column.
  outlier_findings?: OutlierFinding[];
  // omitempty: absent when no suitable time column was found. A single
  // object, not an array, when present.
  trend_finding?: TrendFinding;
  narrative: string;
  suggested_questions: string[];
}

// ---- Research ----

export interface SubQuestion {
  kind: "structured" | "literature";
  text: string;
}
export interface SubResult {
  sub_question: string;
  rows: Record<string, unknown>[];
  row_count: number;
}
export interface RetrievedDoc {
  doc_id: string;
  text: string;
  score: number;
}
export type ChartType = "stat" | "bar" | "line" | "table";
export interface ChartSpec {
  sub_question: string;
  chart_type: ChartType;
  x?: string;
  y?: string;
  reason: string;
}

export interface PipelineState {
  question: string;
  dataset: string;
  schema_json: string;
  corpus_id?: string;
  sub_questions: SubQuestion[];
  // chart_specs[i] and results[i] are parallel arrays -- same index means
  // same sub-question.
  results: SubResult[];
  documents: RetrievedDoc[];
  chart_specs: ChartSpec[];
  explanation_sentences: string[];
  report: string;
}

export interface ResearchResponse {
  report: string;
  state: PipelineState;
}

// ---- History ----
//
// Unlike Dataset/Insights above, these are real Go pointer fields without
// `omitempty` -- always present in the JSON, but `null` rather than absent.

export interface HistoryEntry {
  id: string;
  submitted_at: string;
  source: "sql" | "nl";
  raw_input: string;
  status: string;
  duration_ms: number | null;
  error: string | null;
  workspace_id: string | null;
  user_id: string | null;
}

// ---- Errors ----

export interface ApiError {
  error: string;
}
