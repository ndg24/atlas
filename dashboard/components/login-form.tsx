"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

type Mode = "login" | "signup";
type Status = "idle" | "loading" | "error";

export function LoginForm() {
  const router = useRouter();
  const [mode, setMode] = useState<Mode>("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [workspaceName, setWorkspaceName] = useState("");
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setStatus("loading");
    setError(null);
    try {
      const res = await fetch("/api/auth", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode,
          email,
          password,
          workspace_name: mode === "signup" ? workspaceName || undefined : undefined,
        }),
      });
      const body = await res.json();
      if (!res.ok) {
        throw new Error(body.error ?? `request failed: ${res.status}`);
      }
      router.push("/datasets");
      router.refresh();
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <form
      onSubmit={onSubmit}
      className="mx-auto flex w-full max-w-sm flex-col gap-3 rounded-lg border border-neutral-200 p-6 dark:border-neutral-800"
    >
      <h1 className="text-lg font-semibold">{mode === "login" ? "Log in" : "Sign up"}</h1>
      <input
        required
        type="email"
        placeholder="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
      />
      <input
        required
        type="password"
        placeholder="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
      />
      {mode === "signup" && (
        <input
          placeholder="workspace name (optional -- defaults to the shared default workspace)"
          value={workspaceName}
          onChange={(e) => setWorkspaceName(e.target.value)}
          className="rounded-md border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        />
      )}
      <button
        type="submit"
        disabled={status === "loading"}
        className="rounded-md bg-neutral-900 px-3 py-1.5 text-sm text-white disabled:opacity-50 dark:bg-neutral-100 dark:text-neutral-900"
      >
        {status === "loading" ? "Working…" : mode === "login" ? "Log in" : "Create account"}
      </button>
      {error && <p className="text-sm text-red-600 dark:text-red-400">{error}</p>}
      <button
        type="button"
        onClick={() => {
          setMode(mode === "login" ? "signup" : "login");
          setError(null);
        }}
        className="text-left text-sm text-neutral-600 underline hover:text-neutral-900 dark:text-neutral-400 dark:hover:text-neutral-100"
      >
        {mode === "login" ? "Need an account? Sign up" : "Already have an account? Log in"}
      </button>
    </form>
  );
}
