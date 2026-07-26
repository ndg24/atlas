// Shared inline states so one failed/empty panel never takes down a whole
// page -- every data-fetching page/section wraps its content in a
// try/catch (Server Components) or a status state machine (client forms)
// and renders one of these instead of crashing or showing nothing.

export function ErrorState({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">
      {message}
    </div>
  );
}

export function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-dashed border-neutral-300 px-4 py-3 text-sm text-neutral-500 dark:border-neutral-700 dark:text-neutral-400">
      {message}
    </div>
  );
}

export function LoadingState({ message = "Loading…" }: { message?: string }) {
  return <div className="text-sm text-neutral-500 dark:text-neutral-400">{message}</div>;
}
