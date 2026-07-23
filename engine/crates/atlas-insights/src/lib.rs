//! Statistical insight-detection over already-computed query results:
//! outlier groups (z-score), trends (linear regression), data-quality
//! issues (null rate / zero variance / duplicate rows), and dataset
//! summaries. Plain functions over Arrow `RecordBatch`es, returning
//! structured findings — no LLM involved here, and no dependency on the
//! rest of the engine's execution pipeline; callers (the worker's `Analyze`
//! RPC) are responsible for producing the input batches via ordinary
//! queries first.

mod outlier;
mod quality;
mod summary;
mod trend;

pub use outlier::{detect_outlier_groups, OutlierFinding};
pub use quality::{detect_data_quality_issues, QualityFinding};
pub use summary::{build_summary, ColumnSummary, DatasetSummary, MergedColumnStats};
pub use trend::{detect_trend, TrendFinding};
