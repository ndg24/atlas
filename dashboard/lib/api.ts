import type {
  ApiError,
  Dataset,
  DatasetSummary,
  ExplainResponse,
  HistoryEntry,
  InsightsResponse,
  NLQueryResponse,
  QueryResponse,
  ResearchResponse,
} from "./types";

// Base path for the generic proxy (app/api/atlas/[...path]/route.ts).
// /query and /query/nl instead hit their own dedicated routes (see below),
// which decode Arrow IPC server-side before responding.
const ATLAS_BASE = "/api/atlas";

async function apiFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
    cache: "no-store",
  });
  const body = await res.json();
  if (!res.ok) {
    throw new Error((body as ApiError).error ?? `request failed: ${res.status}`);
  }
  return body as T;
}

export const listDatasets = () => apiFetch<Dataset[]>(`${ATLAS_BASE}/datasets`);

export const createDataset = (name: string, schema_json: string) =>
  apiFetch<Dataset>(`${ATLAS_BASE}/datasets`, {
    method: "POST",
    body: JSON.stringify({ name, schema_json }),
  });

export const getSummary = (name: string) =>
  apiFetch<DatasetSummary>(`${ATLAS_BASE}/datasets/${encodeURIComponent(name)}/summary`, {
    method: "POST",
  });

export const getInsights = (name: string) =>
  apiFetch<InsightsResponse>(`${ATLAS_BASE}/datasets/${encodeURIComponent(name)}/insights`, {
    method: "POST",
  });

export const explainQuery = (dataset: string, sql: string) =>
  apiFetch<ExplainResponse>(`${ATLAS_BASE}/explain`, {
    method: "POST",
    body: JSON.stringify({ dataset, sql }),
  });

export const runResearch = (question: string, dataset: string, corpus_id?: string) =>
  apiFetch<ResearchResponse>(`${ATLAS_BASE}/research`, {
    method: "POST",
    body: JSON.stringify({ question, dataset, corpus_id }),
  });

export const getHistory = () => apiFetch<HistoryEntry[]>(`${ATLAS_BASE}/history`);

// These two hit the dedicated Arrow-decoding routes, not the generic proxy.
export const runQuery = (dataset: string, sql: string) =>
  apiFetch<QueryResponse>("/api/query", {
    method: "POST",
    body: JSON.stringify({ dataset, sql }),
  });

export const runNLQuery = (dataset: string, question: string, narrate = false) =>
  apiFetch<NLQueryResponse>("/api/query-nl", {
    method: "POST",
    body: JSON.stringify({ dataset, question, narrate }),
  });
