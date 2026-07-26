import { NextRequest } from "next/server";
import { afterEach, describe, expect, it } from "vitest";
import { ATLAS_TOKEN_COOKIE, getAtlasToken } from "@/lib/auth-token";

function requestWithCookie(cookieValue?: string) {
  const headers = new Headers();
  if (cookieValue) headers.set("cookie", `${ATLAS_TOKEN_COOKIE}=${cookieValue}`);
  return new NextRequest("http://localhost/api/atlas/datasets", { headers });
}

describe("getAtlasToken", () => {
  afterEach(() => {
    delete process.env.ATLAS_TOKEN;
  });

  it("prefers the session cookie over ATLAS_TOKEN", () => {
    process.env.ATLAS_TOKEN = "env-token";
    expect(getAtlasToken(requestWithCookie("cookie-token"))).toBe("cookie-token");
  });

  it("falls back to ATLAS_TOKEN when there's no session cookie", () => {
    process.env.ATLAS_TOKEN = "env-token";
    expect(getAtlasToken(requestWithCookie())).toBe("env-token");
  });

  it("returns undefined when neither is set", () => {
    expect(getAtlasToken(requestWithCookie())).toBeUndefined();
  });
});
