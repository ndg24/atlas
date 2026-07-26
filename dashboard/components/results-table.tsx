import type { DecodedResult } from "@/lib/types";
import { EmptyState } from "./async-state";

function renderCell(value: unknown): React.ReactNode {
  if (value === null || value === undefined) return <span className="text-neutral-400">—</span>;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

// Generic renderer for a {columns, rows} result -- works for any query
// shape rather than hardcoding column names, since dataset schemas vary.
export function ResultsTable({ result }: { result: DecodedResult }) {
  if (result.rows.length === 0) {
    return <EmptyState message="Query returned no rows." />;
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-800">
      <table className="min-w-full divide-y divide-neutral-200 text-sm dark:divide-neutral-800">
        <thead className="bg-neutral-50 dark:bg-neutral-900">
          <tr>
            {result.columns.map((col) => (
              <th key={col} className="px-3 py-2 text-left font-medium text-neutral-600 dark:text-neutral-300">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-neutral-100 dark:divide-neutral-900">
          {result.rows.map((row, i) => (
            <tr key={i}>
              {result.columns.map((col) => (
                <td key={col} className="px-3 py-2 tabular-nums">
                  {renderCell(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
