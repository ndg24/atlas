import { describe, expect, it } from "vitest";
import { RecordBatchStreamWriter, tableFromArrays } from "apache-arrow";
import { decodeArrowBatches } from "@/lib/arrow";

function encodeTable(table: ReturnType<typeof tableFromArrays>): string {
  const writer = RecordBatchStreamWriter.writeAll(table);
  const bytes = writer.toUint8Array(true);
  return Buffer.from(bytes).toString("base64");
}

describe("decodeArrowBatches", () => {
  it("decodes a single batch into {columns, rows}", () => {
    const table = tableFromArrays({
      name: ["alpha", "beta"],
      count: [1, 2],
    });
    const b64 = encodeTable(table);

    const result = decodeArrowBatches([b64]);

    expect(result.columns).toEqual(["name", "count"]);
    expect(result.rows).toEqual([
      { name: "alpha", count: 1 },
      { name: "beta", count: 2 },
    ]);
  });

  it("concatenates rows across multiple batches", () => {
    const first = tableFromArrays({ n: [1, 2] });
    const second = tableFromArrays({ n: [3, 4] });

    const result = decodeArrowBatches([encodeTable(first), encodeTable(second)]);

    expect(result.rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]);
  });

  it("coerces safe bigints (Int64 columns) to number", () => {
    const table = tableFromArrays({ big: [1n, 2n] });

    const result = decodeArrowBatches([encodeTable(table)]);

    expect(result.rows).toEqual([{ big: 1 }, { big: 2 }]);
    expect(typeof result.rows[0].big).toBe("number");
  });

  it("coerces unsafe bigints to a string instead of truncating", () => {
    const huge = BigInt(Number.MAX_SAFE_INTEGER) + 100n;
    const table = tableFromArrays({ big: [huge] });

    const result = decodeArrowBatches([encodeTable(table)]);

    expect(result.rows[0].big).toBe(huge.toString());
  });

  it("returns empty columns/rows for no batches", () => {
    expect(decodeArrowBatches([])).toEqual({ columns: [], rows: [] });
  });
});
