"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { createDataset } from "@/lib/api";

type Status = "idle" | "loading" | "error";

export function CreateDatasetForm() {
  const router = useRouter();
  const [name, setName] = useState("");
  const [schemaJson, setSchemaJson] = useState("");
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setStatus("loading");
    setError(null);
    try {
      await createDataset(name, schemaJson);
      setName("");
      setSchemaJson("");
      setStatus("idle");
      router.refresh();
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <form onSubmit={onSubmit} className="flex flex-col gap-3 rounded-lg border border-neutral-200 p-4 dark:border-neutral-800">
      <h2 className="text-sm font-medium">Register a dataset</h2>
      <input
        required
        placeholder="name"
        value={name}
        onChange={(e) => setName(e.target.value)}
        className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
      />
      <textarea
        required
        placeholder='schema_json, e.g. {"fields":[{"name":"id","data_type":"Int64"}]}'
        value={schemaJson}
        onChange={(e) => setSchemaJson(e.target.value)}
        rows={3}
        className="rounded-md border border-neutral-300 px-3 py-1.5 font-mono text-xs dark:border-neutral-700 dark:bg-neutral-900"
      />
      <button
        type="submit"
        disabled={status === "loading"}
        className="self-start rounded-md bg-neutral-900 px-3 py-1.5 text-sm text-white disabled:opacity-50 dark:bg-neutral-100 dark:text-neutral-900"
      >
        {status === "loading" ? "Creating…" : "Create dataset"}
      </button>
      {error && <p className="text-sm text-red-600 dark:text-red-400">{error}</p>}
    </form>
  );
}
