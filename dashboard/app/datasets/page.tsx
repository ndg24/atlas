import Link from "next/link";
import { CreateDatasetForm } from "@/components/create-dataset-form";
import { EmptyState, ErrorState } from "@/components/async-state";
import { listDatasets } from "@/lib/api";
import type { Dataset } from "@/lib/types";

export default async function DatasetsPage() {
  let datasets: Dataset[] | undefined;
  let error: string | null = null;
  try {
    datasets = await listDatasets();
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-xl font-semibold">Datasets</h1>

      {error && <ErrorState message={error} />}

      {!error && datasets && datasets.length === 0 && (
        <EmptyState message="No datasets registered yet." />
      )}

      {!error && datasets && datasets.length > 0 && (
        <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-800">
          <table className="min-w-full divide-y divide-neutral-200 text-sm dark:divide-neutral-800">
            <thead className="bg-neutral-50 dark:bg-neutral-900">
              <tr>
                <th className="px-3 py-2 text-left font-medium text-neutral-600 dark:text-neutral-300">Name</th>
                <th className="px-3 py-2 text-left font-medium text-neutral-600 dark:text-neutral-300">Snapshot</th>
                <th className="px-3 py-2 text-left font-medium text-neutral-600 dark:text-neutral-300">Created</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-100 dark:divide-neutral-900">
              {datasets.map((ds) => (
                <tr key={ds.id}>
                  <td className="px-3 py-2">
                    <Link href={`/datasets/${encodeURIComponent(ds.name)}`} className="font-medium hover:underline">
                      {ds.name}
                    </Link>
                  </td>
                  <td className="px-3 py-2 text-neutral-500">
                    {ds.current_snapshot_id ?? <span className="text-neutral-400">no snapshot</span>}
                  </td>
                  <td className="px-3 py-2 text-neutral-500">{ds.created_at ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <CreateDatasetForm />
    </div>
  );
}
