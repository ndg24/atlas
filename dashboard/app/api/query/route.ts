import { NextRequest, NextResponse } from "next/server";
import { decodeArrowBatches } from "@/lib/arrow";
import { getAtlasToken } from "@/lib/auth-token";
import type { ApiError } from "@/lib/types";

// Dedicated (not the generic [...path] proxy) because the response needs
// server-side Arrow-IPC decoding before it reaches the browser -- this
// keeps apache-arrow out of the client bundle entirely.

const COORDINATOR_URL = process.env.ATLAS_COORDINATOR_URL ?? "http://localhost:8080";

export async function POST(req: NextRequest) {
  const token = getAtlasToken(req);
  if (!token) {
    return NextResponse.json(
      { error: "not logged in, and ATLAS_TOKEN is not set on the dashboard server -- see dashboard/.env.local.example" },
      { status: 401 },
    );
  }

  const upstream = await fetch(new URL("query", COORDINATOR_URL), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: await req.text(),
  });

  if (!upstream.ok) {
    const body = (await upstream.json()) as ApiError;
    return NextResponse.json(body, { status: upstream.status });
  }

  const { arrow_ipc_batches, ...rest } = (await upstream.json()) as {
    arrow_ipc_batches: string[];
    query_id: string;
    duration_ms: number;
    cache_hit: boolean;
  };

  return NextResponse.json({ ...rest, result: decodeArrowBatches(arrow_ipc_batches) });
}
