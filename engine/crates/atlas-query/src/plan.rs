//! Hand-written mirror of `proto/plan.proto`'s `LogicalPlan` shape. Both
//! `atlas-query` (producer) and `atlas-exec` (consumer) share these types.
//! No Join variant yet — that's Phase 4+.
//!
//! `serde` derives let a plan cross the coordinator<->worker gRPC boundary
//! (Phase 3) as a JSON string rather than compiling `plan.proto` to Go: the
//! coordinator never needs to construct or inspect a plan, only thread the
//! JSON `atlas-worker`'s `Compile` RPC hands it back out to `ExecuteTask`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Aggregate(AggregateNode),
    Sort(SortNode),
    Limit(LimitNode),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanNode {
    pub dataset: String,
    /// Empty = all columns; populated by column pruning in Phase 4.
    pub columns: Vec<String>,
    /// Empty = current snapshot; snapshots arrive in Phase 2.
    pub snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<Expr>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<AggExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggExpr {
    pub func: AggFunc,
    /// `None` for `COUNT(*)`.
    pub arg: Option<Expr>,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortNode {
    pub input: Box<LogicalPlan>,
    pub keys: Vec<SortKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortKey {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub n: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Column(String),
    Literal(Literal),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}
