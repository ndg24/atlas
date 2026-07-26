import { NextRequest, NextResponse } from "next/server";
import { getAtlasToken } from "@/lib/auth-token";

// Redirects to /login when there's neither a session cookie nor the
// ATLAS_TOKEN env var fallback (see lib/auth-token.ts) -- every page and
// every proxied API route needs one or the other, so this is the one place
// that decides whether the request gets to see any of them.
export function middleware(req: NextRequest) {
  if (getAtlasToken(req)) {
    return NextResponse.next();
  }
  return NextResponse.redirect(new URL("/login", req.url));
}

export const config = {
  matcher: ["/((?!login|api/auth|_next/static|_next/image|favicon.ico).*)"],
};
