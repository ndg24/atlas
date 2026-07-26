"use client";

import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { EmptyState } from "./async-state";

export function ChartLine({ data, x, y }: { data: Record<string, unknown>[]; x: string; y: string }) {
  if (data.length === 0) return <EmptyState message="No data to chart." />;

  return (
    <div className="h-64 w-full rounded-lg border border-neutral-200 p-2 dark:border-neutral-800">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke="var(--chart-grid)" vertical={false} />
          <XAxis dataKey={x} stroke="var(--chart-ink-muted)" fontSize={12} tickLine={false} axisLine={{ stroke: "var(--chart-baseline)" }} />
          <YAxis stroke="var(--chart-ink-muted)" fontSize={12} tickLine={false} axisLine={false} />
          <Tooltip
            cursor={{ stroke: "var(--chart-baseline)" }}
            contentStyle={{ background: "var(--chart-surface)", border: "1px solid var(--chart-grid)", fontSize: 12 }}
          />
          <Line type="monotone" dataKey={y} stroke="var(--chart-series-1)" strokeWidth={2} dot={{ r: 3 }} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
