import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PlanTree } from "@/components/plan-tree";
import type { LogicalPlan } from "@/lib/types";

describe("PlanTree", () => {
  it("renders null as an empty state, not a crash", () => {
    render(<PlanTree plan={null} />);
    expect(screen.getByText(/no plan available/i)).toBeInTheDocument();
  });

  it("renders a bare Scan node", () => {
    const plan: LogicalPlan = { Scan: { dataset: "orders", columns: ["id", "price"], snapshot_id: "" } };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("Scan")).toBeInTheDocument();
    expect(screen.getByText("orders (id, price)")).toBeInTheDocument();
  });

  it("renders a Scan with all columns as *", () => {
    const plan: LogicalPlan = { Scan: { dataset: "orders", columns: [], snapshot_id: "" } };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("orders (*)")).toBeInTheDocument();
  });

  it("renders a nested Filter(Scan) with a Binary predicate", () => {
    const plan: LogicalPlan = {
      Filter: {
        input: { Scan: { dataset: "orders", columns: [], snapshot_id: "" } },
        predicate: {
          Binary: { left: { Column: "price" }, op: "Gt", right: { Literal: { Int: 100 } } },
        },
      },
    };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("Filter")).toBeInTheDocument();
    expect(screen.getByText("price > 100")).toBeInTheDocument();
    expect(screen.getByText("Scan")).toBeInTheDocument();
  });

  it("renders a Project node with aliased exprs", () => {
    const plan: LogicalPlan = {
      Project: {
        input: { Scan: { dataset: "orders", columns: [], snapshot_id: "" } },
        exprs: [{ Column: "price" }],
        aliases: ["p"],
      },
    };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("Project")).toBeInTheDocument();
    expect(screen.getByText("price AS p")).toBeInTheDocument();
  });

  it("renders an Aggregate node with group_by and aggregates", () => {
    const plan: LogicalPlan = {
      Aggregate: {
        input: { Scan: { dataset: "orders", columns: [], snapshot_id: "" } },
        group_by: [{ Column: "city" }],
        aggregates: [{ func: "Avg", arg: { Column: "price" }, alias: "avg_price" }],
      },
    };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("Aggregate")).toBeInTheDocument();
    expect(screen.getByText("Avg(price) AS avg_price | GROUP BY city")).toBeInTheDocument();
  });

  it("renders a Sort node with descending keys", () => {
    const plan: LogicalPlan = {
      Sort: {
        input: { Scan: { dataset: "orders", columns: [], snapshot_id: "" } },
        keys: [{ expr: { Column: "avg_price" }, descending: true }],
      },
    };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("Sort")).toBeInTheDocument();
    expect(screen.getByText("avg_price DESC")).toBeInTheDocument();
  });

  it("renders a Limit node", () => {
    const plan: LogicalPlan = {
      Limit: { input: { Scan: { dataset: "orders", columns: [], snapshot_id: "" } }, n: 10 },
    };
    render(<PlanTree plan={plan} />);
    expect(screen.getByText("Limit")).toBeInTheDocument();
    expect(screen.getByText("10")).toBeInTheDocument();
  });
});
