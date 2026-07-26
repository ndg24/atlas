import type { TrendFinding } from "@/lib/types";
import { EmptyState } from "./async-state";

// trend_finding is a single object, absent (not an array) when no suitable
// time column was found.
export function TrendPanel({ finding }: { finding?: TrendFinding }) {
  if (!finding) {
    return <EmptyState message="No trend detected for this dataset." />;
  }
  return (
    <div className="rounded-md border border-neutral-200 px-3 py-2 text-sm dark:border-neutral-800">
      <span className="font-medium">{finding.value_col}</span> is {finding.direction} over{" "}
      <span className="font-medium">{finding.time_col}</span> (slope = {finding.slope.toFixed(3)}, r² ={" "}
      {finding.r_squared.toFixed(2)})
    </div>
  );
}
