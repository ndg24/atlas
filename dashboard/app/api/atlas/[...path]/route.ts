import { NextRequest, NextResponse } from "next/server";

// Generic byte-transparent proxy to the coordinator: reconstructs the
// coordinator path from the catch-all segments, injects the bearer token
// server-side (the browser never sees it), and mirrors status/body back
// untouched. This covers every coordinator route except /query and
// /query/nl, which get their own dedicated handlers so the Arrow-IPC decode
// step (lib/arrow.ts) stays out of this generic pass-through -- see
// app/api/query/route.ts.
//
// A dedicated proxy also sidesteps the coordinator's total lack of CORS
// support: the browser only ever talks to this same-origin Next.js route.

const COORDINATOR_URL = process.env.ATLAS_COORDINATOR_URL ?? "http://localhost:8080";
const TOKEN = process.env.ATLAS_TOKEN;

async function proxy(req: NextRequest, path: string[]) {
  if (!TOKEN) {
    return NextResponse.json(
      { error: "ATLAS_TOKEN is not set on the dashboard server -- see dashboard/.env.local.example" },
      { status: 500 },
    );
  }

  const targetUrl = new URL(path.join("/"), COORDINATOR_URL);
  targetUrl.search = req.nextUrl.search;

  const hasBody = req.method !== "GET" && req.method !== "HEAD";

  const upstream = await fetch(targetUrl, {
    method: req.method,
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
    body: hasBody ? await req.text() : undefined,
  });

  const text = await upstream.text();
  return new NextResponse(text, {
    status: upstream.status,
    headers: { "Content-Type": upstream.headers.get("Content-Type") ?? "application/json" },
  });
}

export async function GET(req: NextRequest, { params }: { params: { path: string[] } }) {
  return proxy(req, params.path);
}

export async function POST(req: NextRequest, { params }: { params: { path: string[] } }) {
  return proxy(req, params.path);
}
