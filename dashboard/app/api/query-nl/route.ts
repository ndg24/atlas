import { NextRequest, NextResponse } from "next/server";
import { decodeArrowBatches } from "@/lib/arrow";
import type { ApiError } from "@/lib/types";

// Dedicated (see app/api/query/route.ts for why) proxy for POST /query/nl.

const COORDINATOR_URL = process.env.ATLAS_COORDINATOR_URL ?? "http://localhost:8080";
const TOKEN = process.env.ATLAS_TOKEN;

export async function POST(req: NextRequest) {
  if (!TOKEN) {
    return NextResponse.json(
      { error: "ATLAS_TOKEN is not set on the dashboard server -- see dashboard/.env.local.example" },
      { status: 500 },
    );
  }

  const upstream = await fetch(new URL("query/nl", COORDINATOR_URL), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
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
    raw_llm_output: string;
    explanation?: string;
  };

  return NextResponse.json({ ...rest, result: decodeArrowBatches(arrow_ipc_batches) });
}
