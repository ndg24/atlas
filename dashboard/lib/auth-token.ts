import type { NextRequest } from "next/server";

// Name of the httpOnly cookie app/api/auth/route.ts sets after a successful
// login/signup.
export const ATLAS_TOKEN_COOKIE = "atlas_token";

// Every server-side proxy route (app/api/atlas/[...path], app/api/query,
// app/api/query-nl) resolves its bearer token the same way: a logged-in
// session's cookie first, falling back to the static ATLAS_TOKEN env var so
// the existing native-dev / docker-compose flow (mint a token with
// tokengen, export ATLAS_TOKEN) keeps working unchanged for anyone who
// hasn't logged in through the dashboard itself.
export function getAtlasToken(req: NextRequest): string | undefined {
  return req.cookies.get(ATLAS_TOKEN_COOKIE)?.value || process.env.ATLAS_TOKEN;
}
