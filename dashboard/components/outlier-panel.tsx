import type { OutlierFinding } from "@/lib/types";
import { EmptyState } from "./async-state";

// outlier_findings is absent (not []) when the heuristic column-picker
// found no suitable group/value column -- that's a legitimate "nothing to
// show" state, not an error.
export function OutlierPanel({ findings }: { findings?: OutlierFinding[] }) {
  if (!findings || findings.length === 0) {
    return <EmptyState message="No outlier groups detected for this dataset." />;
  }
  return (
    <ul className="flex flex-col gap-2">
      {findings.map((f, i) => (
        <li key={i} className="rounded-md border border-neutral-200 px-3 py-2 text-sm dark:border-neutral-800">
          <span className="font-medium">{f.group_col} = {f.group}</span>: {f.value_col} is{" "}
          <span className="tabular-nums">{f.value.toLocaleString()}</span> vs. a mean of{" "}
          <span className="tabular-nums">{f.group_mean.toLocaleString()}</span> (z = {f.z_score.toFixed(2)})
        </li>
      ))}
    </ul>
  );
}
