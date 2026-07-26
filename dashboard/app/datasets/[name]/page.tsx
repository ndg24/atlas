import Link from "next/link";
import { ErrorState } from "@/components/async-state";
import { StatTile } from "@/components/stat-tile";
import { getSummary } from "@/lib/api";
import type { DatasetSummary } from "@/lib/types";

export default async function DatasetDetailPage({ params }: { params: { name: string } }) {
  const name = decodeURIComponent(params.name);

  let summary: DatasetSummary | undefined;
  let error: string | null = null;
  try {
    summary = await getSummary(name);
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">{name}</h1>
        <Link
          href={`/datasets/${encodeURIComponent(name)}/insights`}
          className="text-sm text-neutral-600 hover:underline dark:text-neutral-400"
        >
          View insights →
        </Link>
      </div>

      {error && <ErrorState message={error} />}

      {summary && (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatTile label="Rows" value={summary.row_count.toLocaleString()} />
            <StatTile label="Columns" value={summary.columns.length} />
          </div>

          <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-800">
            <table className="min-w-full divide-y divide-neutral-200 text-sm dark:divide-neutral-800">
              <thead className="bg-neutral-50 dark:bg-neutral-900">
                <tr>
                  {["Column", "Type", "Null rate", "Distinct (est.)", "Min", "Max"].map((h) => (
                    <th key={h} className="px-3 py-2 text-left font-medium text-neutral-600 dark:text-neutral-300">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-100 dark:divide-neutral-900">
                {summary.columns.map((col) => (
                  <tr key={col.name}>
                    <td className="px-3 py-2 font-medium">{col.name}</td>
                    <td className="px-3 py-2 text-neutral-500">{col.data_type}</td>
                    <td className="px-3 py-2 tabular-nums">{(col.null_rate * 100).toFixed(1)}%</td>
                    <td className="px-3 py-2 tabular-nums">{col.distinct_count_estimate.toLocaleString()}</td>
                    <td className="px-3 py-2 text-neutral-500">{col.min ?? "—"}</td>
                    <td className="px-3 py-2 text-neutral-500">{col.max ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
