import Link from "next/link";
import { ErrorState } from "@/components/async-state";
import { OutlierPanel } from "@/components/outlier-panel";
import { QualityFindings } from "@/components/quality-findings";
import { StatTile } from "@/components/stat-tile";
import { TrendPanel } from "@/components/trend-panel";
import { getInsights } from "@/lib/api";
import type { InsightsResponse } from "@/lib/types";

export default async function DatasetInsightsPage({ params }: { params: { name: string } }) {
  const name = decodeURIComponent(params.name);

  let insights: InsightsResponse | undefined;
  let error: string | null = null;
  try {
    insights = await getInsights(name);
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-xl font-semibold">{name} — Insights</h1>

      {error && <ErrorState message={error} />}

      {insights && (
        <>
          <p className="rounded-lg border border-neutral-200 px-4 py-3 text-sm leading-relaxed dark:border-neutral-800">
            {insights.narrative}
          </p>

          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatTile label="Rows" value={insights.summary.row_count.toLocaleString()} />
            <StatTile label="Columns" value={insights.summary.columns.length} />
            <StatTile label="Quality findings" value={insights.quality_findings.length} />
            <StatTile label="Outlier groups" value={insights.outlier_findings?.length ?? 0} />
          </div>

          <section className="flex flex-col gap-2">
            <h2 className="text-sm font-medium text-neutral-600 dark:text-neutral-400">Data quality</h2>
            <QualityFindings findings={insights.quality_findings} />
          </section>

          <section className="flex flex-col gap-2">
            <h2 className="text-sm font-medium text-neutral-600 dark:text-neutral-400">Outliers</h2>
            <OutlierPanel findings={insights.outlier_findings} />
          </section>

          <section className="flex flex-col gap-2">
            <h2 className="text-sm font-medium text-neutral-600 dark:text-neutral-400">Trend</h2>
            <TrendPanel finding={insights.trend_finding} />
          </section>

          {insights.suggested_questions.length > 0 && (
            <section className="flex flex-col gap-2">
              <h2 className="text-sm font-medium text-neutral-600 dark:text-neutral-400">Suggested questions</h2>
              <div className="flex flex-wrap gap-2">
                {insights.suggested_questions.map((q) => (
                  <Link
                    key={q}
                    href={`/query?dataset=${encodeURIComponent(name)}&mode=nl&question=${encodeURIComponent(q)}`}
                    className="rounded-full border border-neutral-300 px-3 py-1 text-xs hover:bg-neutral-50 dark:border-neutral-700 dark:hover:bg-neutral-900"
                  >
                    {q}
                  </Link>
                ))}
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}
