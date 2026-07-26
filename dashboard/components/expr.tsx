import type { BinaryOp, Expr, Literal } from "@/lib/types";

const OP_SYMBOLS: Record<BinaryOp, string> = {
  Eq: "=",
  NotEq: "!=",
  Lt: "<",
  LtEq: "<=",
  Gt: ">",
  GtEq: ">=",
  And: "AND",
  Or: "OR",
  Add: "+",
  Sub: "-",
  Mul: "*",
  Div: "/",
};

function literalToString(lit: Literal): string {
  if ("Int" in lit) return String(lit.Int);
  if ("Float" in lit) return String(lit.Float);
  if ("Str" in lit) return `"${lit.Str}"`;
  return String(lit.Bool);
}

// Renders an Expr node as a short, readable string -- e.g. `price > 100`,
// `region = "west" AND active`.
export function exprToString(expr: Expr): string {
  if ("Column" in expr) return expr.Column;
  if ("Literal" in expr) return literalToString(expr.Literal);
  const { left, op, right } = expr.Binary;
  return `${exprToString(left)} ${OP_SYMBOLS[op]} ${exprToString(right)}`;
}

export function ExprText({ expr }: { expr: Expr }) {
  return <code className="font-mono text-xs">{exprToString(expr)}</code>;
}
