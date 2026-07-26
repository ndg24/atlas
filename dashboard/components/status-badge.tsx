const STATUS_STYLE: Record<string, { color: string; icon: string }> = {
  succeeded: { color: "var(--status-good)", icon: "✓" },
  running: { color: "var(--status-warning)", icon: "…" },
  failed: { color: "var(--status-critical)", icon: "✕" },
};

// Status color never carries meaning alone -- always paired with an icon
// and the status word itself (dataviz skill: status palette rule).
export function StatusBadge({ status }: { status: string }) {
  const style = STATUS_STYLE[status] ?? { color: "var(--chart-ink-muted)", icon: "•" };
  return (
    <span className="inline-flex items-center gap-1 text-xs font-medium" style={{ color: style.color }}>
      <span aria-hidden>{style.icon}</span>
      {status}
    </span>
  );
}
