import { ErrorState } from "@/components/async-state";
import { StatusBadge } from "@/components/status-badge";
import { getHistory } from "@/lib/api";
import type { HistoryEntry } from "@/lib/types";

export default async function HistoryPage() {
  let entries: HistoryEntry[] | undefined;
  let error: string | null = null;
  try {
    entries = await getHistory();
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-xl font-semibold">History</h1>
      <p className="text-xs text-neutral-500 dark:text-neutral-400">
        Showing the latest {entries?.length ?? 0} queries (the coordinator caps /history at 100, newest first).
      </p>

      {error && <ErrorState message={error} />}

      {entries && (
        <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-800">
          <table className="min-w-full divide-y divide-neutral-200 text-sm dark:divide-neutral-800">
            <thead className="bg-neutral-50 dark:bg-neutral-900">
              <tr>
                {["Submitted", "Source", "Input", "Status", "Duration", "Error"].map((h) => (
                  <th key={h} className="px-3 py-2 text-left font-medium text-neutral-600 dark:text-neutral-300">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-100 dark:divide-neutral-900">
              {entries.map((entry) => (
                <tr key={entry.id}>
                  <td className="whitespace-nowrap px-3 py-2 text-neutral-500">{entry.submitted_at}</td>
                  <td className="px-3 py-2">
                    <span className="rounded-full border border-neutral-300 px-2 py-0.5 text-xs dark:border-neutral-700">
                      {entry.source}
                    </span>
                  </td>
                  <td className="max-w-xs truncate px-3 py-2 font-mono text-xs" title={entry.raw_input}>
                    {entry.raw_input}
                  </td>
                  <td className="px-3 py-2">
                    <StatusBadge status={entry.status} />
                  </td>
                  <td className="px-3 py-2 tabular-nums text-neutral-500">
                    {entry.duration_ms !== null ? `${entry.duration_ms} ms` : "—"}
                  </td>
                  <td className="max-w-xs truncate px-3 py-2 text-red-600 dark:text-red-400" title={entry.error ?? undefined}>
                    {entry.error ?? "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
