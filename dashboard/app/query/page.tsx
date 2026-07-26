import { ErrorState } from "@/components/async-state";
import { QueryConsole } from "@/components/query-console";
import { listDatasets } from "@/lib/api";
import type { Dataset } from "@/lib/types";

export default async function QueryPage({
  searchParams,
}: {
  searchParams: { dataset?: string; mode?: string; question?: string };
}) {
  let datasets: Dataset[] | undefined;
  let error: string | null = null;
  try {
    datasets = await listDatasets();
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-xl font-semibold">Query console</h1>

      {error && <ErrorState message={error} />}

      {datasets && (
        <QueryConsole
          datasets={datasets}
          initialDataset={searchParams.dataset}
          initialMode={searchParams.mode === "nl" ? "nl" : "sql"}
          initialQuestion={searchParams.question}
        />
      )}
    </div>
  );
}
