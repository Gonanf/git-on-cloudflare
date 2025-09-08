// Authentication helpers shared between routes

import { getAuthStub } from "@/common/stub.ts";

export function getBasicCredentials(req: Request): { username: string; password: string } | null {
  const h = req.headers.get("Authorization") || "";
  const m = /^Basic\s+(.+)$/i.exec(h);
  if (!m) return null;
  try {
    const decoded = atob(m[1]);
    const idx = decoded.indexOf(":");
    if (idx === -1) return { username: decoded, password: "" };
    const username = decoded.slice(0, idx);
    const password = decoded.slice(idx + 1);
    return { username, password };
  } catch {
    return null;
  }
}

export async function verifyAuth(
  env: Env,
  owner: string,
  req: Request,
  _isAdmin: boolean
): Promise<boolean> {
  const stub = getAuthStub(env);
  if (!stub) return true; // no centralized auth configured => open
  // Only allow Basic with username matching :owner
  const basic = getBasicCredentials(req);
  const tok = basic && basic.username === owner ? basic.password : undefined;
  if (!tok) return false;
  try {
    const res = await stub.fetch("https://auth/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ owner, token: tok }),
    });
    if (!res.ok) return false;
    const data = await res.json<any>().catch(() => ({}));
    return !!data?.ok;
  } catch {
    return false;
  }
}
