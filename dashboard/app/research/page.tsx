import { ErrorState } from "@/components/async-state";
import { ResearchForm } from "@/components/research-form";
import { listDatasets } from "@/lib/api";
import type { Dataset } from "@/lib/types";

export default async function ResearchPage() {
  let datasets: Dataset[] | undefined;
  let error: string | null = null;
  try {
    datasets = await listDatasets();
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-xl font-semibold">Research</h1>

      {error && <ErrorState message={error} />}

      {datasets && <ResearchForm datasets={datasets} />}
    </div>
  );
}
