import type { LogicalPlan } from "@/lib/types";
import { EmptyState } from "./async-state";
import { exprToString } from "./expr";

function NodeBox({ title, detail, children }: { title: string; detail?: string; children?: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <div className="inline-flex w-fit items-baseline gap-2 rounded-md border border-neutral-300 bg-neutral-50 px-2 py-1 text-xs dark:border-neutral-700 dark:bg-neutral-900">
        <span className="font-semibold">{title}</span>
        {detail && <span className="font-mono text-neutral-500 dark:text-neutral-400">{detail}</span>}
      </div>
      {children && <div className="ml-4 border-l border-neutral-200 pl-4 dark:border-neutral-800">{children}</div>}
    </div>
  );
}

// Recursively renders the discriminated-union LogicalPlan shape
// (engine/crates/atlas-query/src/plan.rs's serde output) as an indented
// tree, root at top, leaf Scan at the bottom.
export function PlanTree({ plan }: { plan: LogicalPlan | null }) {
  if (!plan) return <EmptyState message="No plan available." />;
  return <PlanNode plan={plan} />;
}

function PlanNode({ plan }: { plan: LogicalPlan }) {
  if ("Scan" in plan) {
    const { dataset, columns, snapshot_id } = plan.Scan;
    const cols = columns.length > 0 ? columns.join(", ") : "*";
    const snap = snapshot_id ? ` @ ${snapshot_id}` : "";
    return <NodeBox title="Scan" detail={`${dataset} (${cols})${snap}`} />;
  }

  if ("Filter" in plan) {
    const { input, predicate } = plan.Filter;
    return (
      <NodeBox title="Filter" detail={exprToString(predicate)}>
        <PlanNode plan={input} />
      </NodeBox>
    );
  }

  if ("Project" in plan) {
    const { input, exprs, aliases } = plan.Project;
    const detail = exprs
      .map((e, i) => (aliases[i] ? `${exprToString(e)} AS ${aliases[i]}` : exprToString(e)))
      .join(", ");
    return (
      <NodeBox title="Project" detail={detail}>
        <PlanNode plan={input} />
      </NodeBox>
    );
  }

  if ("Aggregate" in plan) {
    const { input, group_by, aggregates } = plan.Aggregate;
    const groupDetail = group_by.length > 0 ? `GROUP BY ${group_by.map(exprToString).join(", ")}` : "";
    const aggDetail = aggregates
      .map((a) => `${a.func}(${a.arg ? exprToString(a.arg) : "*"}) AS ${a.alias}`)
      .join(", ");
    return (
      <NodeBox title="Aggregate" detail={[aggDetail, groupDetail].filter(Boolean).join(" | ")}>
        <PlanNode plan={input} />
      </NodeBox>
    );
  }

  if ("Sort" in plan) {
    const { input, keys } = plan.Sort;
    const detail = keys.map((k) => `${exprToString(k.expr)} ${k.descending ? "DESC" : "ASC"}`).join(", ");
    return (
      <NodeBox title="Sort" detail={detail}>
        <PlanNode plan={input} />
      </NodeBox>
    );
  }

  // Limit
  const { input, n } = plan.Limit;
  return (
    <NodeBox title="Limit" detail={String(n)}>
      <PlanNode plan={input} />
    </NodeBox>
  );
}
