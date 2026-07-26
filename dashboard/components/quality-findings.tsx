import type { QualityFinding } from "@/lib/types";
import { EmptyState } from "./async-state";

function describe(finding: QualityFinding): string {
  switch (finding.kind) {
    case "HighNullRate":
      return `${finding.column} is ${(finding.null_rate * 100).toFixed(1)}% null`;
    case "ZeroVariance":
      return `${finding.column} is constant at "${finding.value}"`;
    case "DuplicateRows":
      return `${finding.count} duplicate row(s) found (e.g. rows ${finding.sample_row_indices.slice(0, 5).join(", ")})`;
  }
}

export function QualityFindings({ findings }: { findings: QualityFinding[] }) {
  if (findings.length === 0) {
    return <EmptyState message="No data-quality issues detected." />;
  }
  return (
    <ul className="flex flex-col gap-2">
      {findings.map((f, i) => (
        <li
          key={i}
          className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200"
        >
          <span className="font-medium">{f.kind}</span> — {describe(f)}
        </li>
      ))}
    </ul>
  );
}
