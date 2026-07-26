"use client";

import { useState } from "react";
import { ErrorState, LoadingState } from "@/components/async-state";
import { PlanTree } from "@/components/plan-tree";
import { ResultsTable } from "@/components/results-table";
import { explainQuery, runNLQuery, runQuery } from "@/lib/api";
import type { Dataset, ExplainResponse, NLQueryResponse, QueryResponse } from "@/lib/types";

type Mode = "sql" | "nl";
type Status = "idle" | "loading" | "error";

export function QueryConsole({
  datasets,
  initialDataset,
  initialMode,
  initialQuestion,
}: {
  datasets: Dataset[];
  initialDataset?: string;
  initialMode?: Mode;
  initialQuestion?: string;
}) {
  const [dataset, setDataset] = useState(initialDataset ?? datasets[0]?.name ?? "");
  const [mode, setMode] = useState<Mode>(initialMode ?? "sql");
  const [sql, setSql] = useState("");
  const [question, setQuestion] = useState(initialQuestion ?? "");
  const [narrate, setNarrate] = useState(false);

  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryResponse | NLQueryResponse | null>(null);

  const [explainStatus, setExplainStatus] = useState<Status>("idle");
  const [explainError, setExplainError] = useState<string | null>(null);
  const [explain, setExplain] = useState<ExplainResponse | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setStatus("loading");
    setError(null);
    setExplain(null);
    try {
      const res = mode === "sql" ? await runQuery(dataset, sql) : await runNLQuery(dataset, question, narrate);
      setResult(res);
      setStatus("idle");
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function onExplain() {
    setExplainStatus("loading");
    setExplainError(null);
    try {
      const res = await explainQuery(dataset, sql);
      setExplain(res);
      setExplainStatus("idle");
    } catch (err) {
      setExplainStatus("error");
      setExplainError(err instanceof Error ? err.message : String(err));
    }
  }

  const nlResult = mode === "nl" ? (result as NLQueryResponse | null) : null;

  return (
    <div className="flex flex-col gap-6">
      <form onSubmit={onSubmit} className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <select
            value={dataset}
            onChange={(e) => setDataset(e.target.value)}
            className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
          >
            {datasets.map((ds) => (
              <option key={ds.id} value={ds.name}>
                {ds.name}
              </option>
            ))}
          </select>

          <div className="inline-flex rounded-md border border-neutral-300 text-sm dark:border-neutral-700">
            {(["sql", "nl"] as Mode[]).map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setMode(m)}
                className={`px-3 py-1.5 ${
                  mode === m
                    ? "bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900"
                    : "text-neutral-600 dark:text-neutral-400"
                }`}
              >
                {m === "sql" ? "SQL" : "Natural language"}
              </button>
            ))}
          </div>

          {mode === "nl" && (
            <label className="flex items-center gap-1.5 text-sm text-neutral-600 dark:text-neutral-400">
              <input type="checkbox" checked={narrate} onChange={(e) => setNarrate(e.target.checked)} />
              Narrate result
            </label>
          )}
        </div>

        {mode === "sql" ? (
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            placeholder="SELECT * FROM t LIMIT 10"
            rows={4}
            className="rounded-md border border-neutral-300 px-3 py-2 font-mono text-sm dark:border-neutral-700 dark:bg-neutral-900"
          />
        ) : (
          <textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="which diagnosis is most common?"
            rows={2}
            className="rounded-md border border-neutral-300 px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
          />
        )}

        <div className="flex gap-2">
          <button
            type="submit"
            disabled={status === "loading" || !dataset}
            className="rounded-md bg-neutral-900 px-3 py-1.5 text-sm text-white disabled:opacity-50 dark:bg-neutral-100 dark:text-neutral-900"
          >
            {status === "loading" ? "Running…" : "Run"}
          </button>

          {mode === "sql" && (
            <button
              type="button"
              onClick={onExplain}
              disabled={explainStatus === "loading" || !sql || !dataset}
              className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm disabled:opacity-50 dark:border-neutral-700"
            >
              {explainStatus === "loading" ? "Explaining…" : "Explain"}
            </button>
          )}
        </div>
      </form>

      {error && <ErrorState message={error} />}

      {result && (
        <div className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-3 text-xs text-neutral-500 dark:text-neutral-400">
            <span>{result.duration_ms} ms</span>
            <span className={result.cache_hit ? "text-emerald-600 dark:text-emerald-400" : ""}>
              {result.cache_hit ? "cache hit" : "cache miss"}
            </span>
          </div>
          <ResultsTable result={result.result} />
          {nlResult && (
            <details className="text-xs">
              <summary className="cursor-pointer text-neutral-500 dark:text-neutral-400">Raw LLM output</summary>
              <pre className="mt-2 whitespace-pre-wrap rounded-md bg-neutral-50 p-3 dark:bg-neutral-900">
                {nlResult.raw_llm_output}
              </pre>
            </details>
          )}
          {nlResult?.explanation && (
            <p className="rounded-lg border border-neutral-200 px-4 py-3 text-sm leading-relaxed dark:border-neutral-800">
              {nlResult.explanation}
            </p>
          )}
        </div>
      )}

      {explainStatus === "loading" && <LoadingState message="Compiling plan…" />}
      {explainError && <ErrorState message={explainError} />}

      {explain && (
        <div className="flex flex-col gap-4 rounded-lg border border-neutral-200 p-4 dark:border-neutral-800">
          <div className="flex flex-wrap items-center gap-3 text-xs text-neutral-500 dark:text-neutral-400">
            <span>{explain.manifests_before_pruning.length} manifest(s) before pruning</span>
            <span>→</span>
            <span>{explain.manifests_after_pruning.length} after pruning</span>
            <span className={explain.cache_hit ? "text-emerald-600 dark:text-emerald-400" : ""}>
              {explain.cache_hit ? "cache hit" : "cache miss"}
            </span>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <h3 className="mb-2 text-sm font-medium text-neutral-600 dark:text-neutral-400">Logical plan</h3>
              <PlanTree plan={explain.logical_plan} />
            </div>
            <div>
              <h3 className="mb-2 text-sm font-medium text-neutral-600 dark:text-neutral-400">Optimized plan</h3>
              <PlanTree plan={explain.optimized_plan} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
