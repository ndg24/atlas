import "server-only";
import { tableFromIPC } from "apache-arrow";
import type { DecodedResult } from "./types";

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

// bigint isn't JSON-serializable, and Int64 columns decode to bigint --
// coerce to number when it's safe to do so, otherwise fall back to a
// string so no value is silently truncated.
function coerce(value: unknown): unknown {
  if (typeof value === "bigint") {
    return value <= MAX_SAFE && value >= MIN_SAFE ? Number(value) : value.toString();
  }
  return value;
}

// Decodes the base64-encoded Arrow IPC streams /query and /query/nl return
// into plain {columns, rows} JSON. Each entry in `base64Batches` is one
// self-contained IPC stream; batches from the same query share a schema, so
// column names come from the first non-empty batch.
export function decodeArrowBatches(base64Batches: string[]): DecodedResult {
  const rows: Record<string, unknown>[] = [];
  let columns: string[] = [];

  for (const b64 of base64Batches) {
    const bytes = Buffer.from(b64, "base64");
    const table = tableFromIPC(bytes);
    if (columns.length === 0) {
      columns = table.schema.fields.map((f) => f.name);
    }
    for (const row of table) {
      const obj: Record<string, unknown> = {};
      for (const col of columns) {
        obj[col] = coerce(row[col]);
      }
      rows.push(obj);
    }
  }

  return { columns, rows };
}
