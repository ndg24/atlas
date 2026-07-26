"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { EmptyState } from "./async-state";

// Single-series bar chart (per the dataviz skill: one measure -> one
// categorical slot, no legend needed since the section heading already
// names the series).
export function ChartBar({ data, x, y }: { data: Record<string, unknown>[]; x: string; y: string }) {
  if (data.length === 0) return <EmptyState message="No data to chart." />;

  return (
    <div className="h-64 w-full rounded-lg border border-neutral-200 p-2 dark:border-neutral-800">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke="var(--chart-grid)" vertical={false} />
          <XAxis dataKey={x} stroke="var(--chart-ink-muted)" fontSize={12} tickLine={false} axisLine={{ stroke: "var(--chart-baseline)" }} />
          <YAxis stroke="var(--chart-ink-muted)" fontSize={12} tickLine={false} axisLine={false} />
          <Tooltip
            cursor={{ fill: "var(--chart-grid)" }}
            contentStyle={{ background: "var(--chart-surface)", border: "1px solid var(--chart-grid)", fontSize: 12 }}
          />
          <Bar dataKey={y} fill="var(--chart-series-1)" radius={[4, 4, 0, 0]} maxBarSize={48} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
