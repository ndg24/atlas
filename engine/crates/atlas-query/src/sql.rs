use anyhow::{anyhow, bail, Context, Result};
use atlas_format::Schema;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, OrderByExpr, Select, SelectItem, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::plan::{
    AggExpr, AggFunc, AggregateNode, BinaryOp, Expr, FilterNode, LimitNode, Literal, LogicalPlan,
    ProjectNode, ScanNode, SortKey, SortNode,
};

pub fn parse_sql(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).with_context(|| format!("parsing SQL: {sql}"))?;
    if statements.len() != 1 {
        bail!(
            "expected exactly one SQL statement, got {}",
            statements.len()
        );
    }
    Ok(statements.remove(0))
}

pub fn build_logical_plan(stmt: &Statement, schema: &Schema) -> Result<LogicalPlan> {
    let query = match stmt {
        Statement::Query(query) => query,
        other => bail!("only SELECT queries are supported, got: {other}"),
    };

    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        other => bail!("only simple SELECT queries are supported, got: {other}"),
    };

    let dataset = scan_dataset_name(select)?;
    let mut plan = LogicalPlan::Scan(ScanNode {
        dataset,
        columns: Vec::new(),
        snapshot_id: String::new(),
    });

    if let Some(selection) = &select.selection {
        plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(plan),
            predicate: convert_expr(selection)?,
        });
    }

    let group_by = group_by_exprs(select)?
        .iter()
        .map(convert_expr)
        .collect::<Result<Vec<_>>>()?;

    let mut aggregates = Vec::new();
    let mut projections = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(agg) = try_agg_expr(expr, default_alias(expr))? {
                    aggregates.push(agg);
                } else {
                    projections.push((convert_expr(expr)?, default_alias(expr)));
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(agg) = try_agg_expr(expr, alias.value.clone())? {
                    aggregates.push(agg);
                } else {
                    projections.push((convert_expr(expr)?, alias.value.clone()));
                }
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                for field in schema.fields() {
                    projections.push((Expr::Column(field.name().clone()), field.name().clone()));
                }
            }
        }
    }

    let has_aggregates = !aggregates.is_empty() || !group_by.is_empty();
    if has_aggregates {
        if !projections.is_empty() {
            let non_group_cols: Vec<&String> = projections
                .iter()
                .filter(|(e, _)| !group_by.contains(e))
                .map(|(_, alias)| alias)
                .collect();
            if !non_group_cols.is_empty() {
                bail!(
                    "columns {:?} must appear in GROUP BY or be wrapped in an aggregate function",
                    non_group_cols
                );
            }
        }
        // Preserve SELECT-list order: group-by passthrough columns come from
        // `projections` (already order-checked above to be a subset of
        // group_by), aggregates from `aggregates`.
        let group_by = if group_by.is_empty() {
            projections.iter().map(|(e, _)| e.clone()).collect()
        } else {
            group_by
        };
        plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(plan),
            group_by,
            aggregates,
        });
    } else {
        let (exprs, aliases): (Vec<Expr>, Vec<String>) = projections.into_iter().unzip();
        plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(plan),
            exprs,
            aliases,
        });
    }

    if let Some(order_by) = &query.order_by {
        plan = LogicalPlan::Sort(SortNode {
            input: Box::new(plan),
            keys: order_by
                .exprs
                .iter()
                .map(convert_order_by)
                .collect::<Result<Vec<_>>>()?,
        });
    }

    if let Some(limit_expr) = &query.limit {
        let n = match limit_expr {
            SqlExpr::Value(Value::Number(s, _)) => s
                .parse::<u64>()
                .with_context(|| format!("invalid LIMIT value: {s}"))?,
            other => bail!("unsupported LIMIT expression: {other}"),
        };
        plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(plan),
            n,
        });
    }

    Ok(plan)
}

fn scan_dataset_name(select: &Select) -> Result<String> {
    let table = select
        .from
        .first()
        .ok_or_else(|| anyhow!("SELECT with no FROM clause is not supported"))?;
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        other => bail!("unsupported FROM clause: {other}"),
    }
}

fn group_by_exprs(select: &Select) -> Result<Vec<SqlExpr>> {
    match &select.group_by {
        GroupByExpr::All(_) => bail!("GROUP BY ALL is not supported"),
        GroupByExpr::Expressions(exprs, _) => Ok(exprs.clone()),
    }
}

fn try_agg_expr(expr: &SqlExpr, alias: String) -> Result<Option<AggExpr>> {
    let SqlExpr::Function(func) = expr else {
        return Ok(None);
    };
    let Some(agg_func) = agg_func_from_name(&func.name.to_string()) else {
        return Ok(None);
    };
    let arg = function_arg(func)?;
    let arg = arg.map(|e| convert_expr(&e)).transpose()?;
    Ok(Some(AggExpr {
        func: agg_func,
        arg,
        alias,
    }))
}

fn agg_func_from_name(name: &str) -> Option<AggFunc> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
    }
}

fn function_arg(func: &Function) -> Result<Option<SqlExpr>> {
    let FunctionArguments::List(list) = &func.args else {
        return Ok(None);
    };
    let Some(first) = list.args.first() else {
        return Ok(None);
    };
    match first {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(Some(e.clone())),
        other => bail!("unsupported function argument: {other:?}"),
    }
}

fn default_alias(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Identifier(id) => id.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => {
            parts.last().map(|i| i.value.clone()).unwrap_or_default()
        }
        SqlExpr::Function(f) => f.name.to_string().to_ascii_lowercase(),
        other => other.to_string(),
    }
}

fn convert_order_by(order: &OrderByExpr) -> Result<SortKey> {
    Ok(SortKey {
        expr: convert_expr(&order.expr)?,
        descending: order.asc == Some(false),
    })
}

fn convert_expr(expr: &SqlExpr) -> Result<Expr> {
    match expr {
        SqlExpr::Identifier(id) => Ok(Expr::Column(id.value.clone())),
        SqlExpr::CompoundIdentifier(parts) => Ok(Expr::Column(
            parts
                .last()
                .ok_or_else(|| anyhow!("empty compound identifier"))?
                .value
                .clone(),
        )),
        SqlExpr::Nested(inner) => convert_expr(inner),
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match convert_expr(expr)? {
            Expr::Literal(Literal::Int(i)) => Ok(Expr::Literal(Literal::Int(-i))),
            Expr::Literal(Literal::Float(f)) => Ok(Expr::Literal(Literal::Float(-f))),
            other => bail!("unsupported unary minus operand: {other:?}"),
        },
        SqlExpr::Value(value) => convert_value(value),
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::Binary {
            left: Box::new(convert_expr(left)?),
            op: convert_binary_op(op)?,
            right: Box::new(convert_expr(right)?),
        }),
        other => bail!("unsupported expression: {other}"),
    }
}

fn convert_value(value: &Value) -> Result<Expr> {
    match value {
        Value::Number(s, _) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(Expr::Literal(Literal::Int(i)))
            } else {
                let f = s
                    .parse::<f64>()
                    .with_context(|| format!("invalid numeric literal: {s}"))?;
                Ok(Expr::Literal(Literal::Float(f)))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(Expr::Literal(Literal::Str(s.clone())))
        }
        Value::Boolean(b) => Ok(Expr::Literal(Literal::Bool(*b))),
        other => bail!("unsupported literal: {other}"),
    }
}

fn convert_binary_op(op: &BinaryOperator) -> Result<BinaryOp> {
    match op {
        BinaryOperator::Eq => Ok(BinaryOp::Eq),
        BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        BinaryOperator::Lt => Ok(BinaryOp::Lt),
        BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        BinaryOperator::Gt => Ok(BinaryOp::Gt),
        BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        BinaryOperator::And => Ok(BinaryOp::And),
        BinaryOperator::Or => Ok(BinaryOp::Or),
        BinaryOperator::Plus => Ok(BinaryOp::Add),
        BinaryOperator::Minus => Ok(BinaryOp::Sub),
        BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        BinaryOperator::Divide => Ok(BinaryOp::Div),
        other => bail!("unsupported binary operator: {other}"),
    }
}
