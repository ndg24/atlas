"use client";

import { useState } from "react";
import { ChartBar } from "@/components/chart-bar";
import { ChartLine } from "@/components/chart-line";
import { CitationText } from "@/components/citation-text";
import { ErrorState, LoadingState } from "@/components/async-state";
import { ResultsTable } from "@/components/results-table";
import { StatTile } from "@/components/stat-tile";
import { runResearch } from "@/lib/api";
import type { ChartSpec, Dataset, ResearchResponse, SubResult } from "@/lib/types";

type Status = "idle" | "loading" | "error";

function ChartBlock({ spec, result }: { spec: ChartSpec; result?: SubResult }) {
  const rows = result?.rows ?? [];
  return (
    <div className="flex flex-col gap-1">
      <h3 className="text-sm font-medium">{spec.sub_question}</h3>
      <p className="text-xs text-neutral-500 dark:text-neutral-400">{spec.reason}</p>
      {spec.chart_type === "stat" &&
        (rows.length > 0 && spec.y ? (
          <StatTile label={spec.y} value={String(rows[0][spec.y] ?? "—")} />
        ) : (
          <StatTile label={spec.y ?? "value"} value="—" />
        ))}
      {spec.chart_type === "bar" &&
        (spec.x && spec.y ? (
          <ChartBar data={rows} x={spec.x} y={spec.y} />
        ) : (
          <p className="text-xs text-neutral-400">Chart columns unavailable.</p>
        ))}
      {spec.chart_type === "line" &&
        (spec.x && spec.y ? (
          <ChartLine data={rows} x={spec.x} y={spec.y} />
        ) : (
          <p className="text-xs text-neutral-400">Chart columns unavailable.</p>
        ))}
      {spec.chart_type === "table" && (
        <ResultsTable result={{ columns: rows.length > 0 ? Object.keys(rows[0]) : [], rows }} />
      )}
    </div>
  );
}

function ReportBody({ report }: { report: string }) {
  const paragraphs = report.split(/\n{2,}/).filter((p) => p.trim());
  return (
    <div className="flex flex-col gap-3 rounded-lg border border-neutral-200 p-4 dark:border-neutral-800">
      {paragraphs.map((p, i) => {
        const trimmed = p.trim();
        const headingMatch = trimmed.match(/^(#{1,3})\s+(.*)/);
        if (headingMatch) {
          const level = headingMatch[1].length;
          const text = headingMatch[2];
          return level === 1 ? (
            <h2 key={i} className="text-lg font-semibold">
              {text}
            </h2>
          ) : (
            <h3 key={i} className="text-sm font-semibold text-neutral-600 dark:text-neutral-400">
              {text}
            </h3>
          );
        }
        return <CitationText key={i} text={trimmed} />;
      })}
    </div>
  );
}

export function ResearchForm({ datasets }: { datasets: Dataset[] }) {
  const [question, setQuestion] = useState("");
  const [dataset, setDataset] = useState(datasets[0]?.name ?? "");
  const [corpusId, setCorpusId] = useState("");
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);
  const [response, setResponse] = useState<ResearchResponse | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setStatus("loading");
    setError(null);
    try {
      const res = await runResearch(question, dataset, corpusId || undefined);
      setResponse(res);
      setStatus("idle");
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <form onSubmit={onSubmit} className="flex flex-col gap-3">
        <select
          value={dataset}
          onChange={(e) => setDataset(e.target.value)}
          className="w-fit rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        >
          {datasets.map((ds) => (
            <option key={ds.id} value={ds.name}>
              {ds.name}
            </option>
          ))}
        </select>
        <textarea
          required
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="What factors are associated with high readmission rates?"
          rows={3}
          className="rounded-md border border-neutral-300 px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        />
        <input
          value={corpusId}
          onChange={(e) => setCorpusId(e.target.value)}
          placeholder="corpus_id (optional -- enables literature retrieval)"
          className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        />
        <button
          type="submit"
          disabled={status === "loading" || !dataset || !question}
          className="self-start rounded-md bg-neutral-900 px-3 py-1.5 text-sm text-white disabled:opacity-50 dark:bg-neutral-100 dark:text-neutral-900"
        >
          {status === "loading" ? "Running research pipeline…" : "Run research"}
        </button>
      </form>

      {status === "loading" && <LoadingState message="Planner → Execution → Visualization → Explanation → Report…" />}
      {error && <ErrorState message={error} />}

      {response && (
        <div className="flex flex-col gap-6">
          <ReportBody report={response.report} />

          {response.state.chart_specs.length > 0 && (
            <section className="flex flex-col gap-6">
              <h2 className="text-sm font-medium text-neutral-600 dark:text-neutral-400">Sub-question results</h2>
              {response.state.chart_specs.map((spec, i) => (
                <ChartBlock key={i} spec={spec} result={response.state.results[i]} />
              ))}
            </section>
          )}

          <details className="text-xs">
            <summary className="cursor-pointer text-neutral-500 dark:text-neutral-400">
              Pipeline state ({response.state.sub_questions.length} sub-question(s),{" "}
              {response.state.documents.length} document(s) retrieved)
            </summary>
            <div className="mt-2 flex flex-col gap-3">
              <div>
                <h4 className="mb-1 font-medium">Sub-questions</h4>
                <ul className="list-inside list-disc">
                  {response.state.sub_questions.map((sq, i) => (
                    <li key={i}>
                      <span className="text-neutral-500">[{sq.kind}]</span> {sq.text}
                    </li>
                  ))}
                </ul>
              </div>
              <div>
                <h4 className="mb-1 font-medium">Retrieved documents</h4>
                {response.state.documents.length === 0 ? (
                  <p className="text-neutral-400">No documents retrieved.</p>
                ) : (
                  <ul className="flex flex-col gap-1">
                    {response.state.documents.map((doc) => (
                      <li key={doc.doc_id} className="rounded border border-neutral-200 p-2 dark:border-neutral-800">
                        <span className="font-medium">{doc.doc_id}</span> (score {doc.score.toFixed(2)}):{" "}
                        {doc.text}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          </details>
        </div>
      )}
    </div>
  );
}
