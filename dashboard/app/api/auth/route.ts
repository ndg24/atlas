import { NextRequest, NextResponse } from "next/server";
import { ATLAS_TOKEN_COOKIE } from "@/lib/auth-token";

// Proxies to the coordinator's POST /auth/login or /auth/signup (never
// called from the browser directly -- same reasoning as the generic
// app/api/atlas/[...path] proxy) and, on success, stores the returned JWT
// in an httpOnly cookie instead of returning it to client JS. This is the
// one route that doesn't need a bearer token itself, since its whole job is
// to mint one.

const COORDINATOR_URL = process.env.ATLAS_COORDINATOR_URL ?? "http://localhost:8080";
// Matches the coordinator's own token TTL (tokenTTL in
// coordinator/internal/api/auth_handlers.go) -- the cookie shouldn't outlive
// the token it holds.
const TOKEN_TTL_SECONDS = 60 * 60 * 24;

export async function POST(req: NextRequest) {
  const body = await req.json().catch(() => null);
  const mode = body?.mode;
  if (mode !== "login" && mode !== "signup") {
    return NextResponse.json({ error: `"mode" must be "login" or "signup"` }, { status: 400 });
  }
  const { email, password, workspace_name } = body;
  if (!email || !password) {
    return NextResponse.json({ error: "email and password are required" }, { status: 400 });
  }

  const path = mode === "login" ? "auth/login" : "auth/signup";
  const upstream = await fetch(new URL(path, COORDINATOR_URL), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(mode === "signup" ? { email, password, workspace_name } : { email, password }),
  });

  const upstreamBody = await upstream.json();
  if (!upstream.ok) {
    return NextResponse.json(upstreamBody, { status: upstream.status });
  }

  const res = NextResponse.json({ ok: true });
  res.cookies.set(ATLAS_TOKEN_COOKIE, upstreamBody.token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: TOKEN_TTL_SECONDS,
  });
  return res;
}

// Logout: clear the session cookie. Falls back to ATLAS_TOKEN (if set) for
// the rest of the session, exactly like every proxy route already does.
export async function DELETE() {
  const res = NextResponse.json({ ok: true });
  res.cookies.delete(ATLAS_TOKEN_COOKIE);
  return res;
}
