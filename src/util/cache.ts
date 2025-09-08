/**
 * Small helpers around the Cloudflare Workers Cache API for JSON payloads.
 *
 * Notes
 * - Cache lives per colo; keys should be stable and include all inputs.
 * - Always use the same origin as the incoming request when constructing keys.
 * - Only cache GET responses.
 */

const CACHE_NAME = "git-on-cf:json";

/**
 * Build a same-origin GET Request to use as the cache key.
 *
 * Why same-origin?
 * - Cloudflare recommends keeping the hostname aligned with the Worker hostname
 *   to avoid unnecessary DNS lookups and improve cache efficiency.
 *
 * Key design
 * - Use a dedicated pathname (for example, "/_cache/commits") and include only
 *   the parameters that affect the response in `params`.
 * - Omit empty/undefined params to keep keys clean and deterministic.
 */
export function buildCacheKeyFrom(
  req: Request,
  pathname: string,
  params: Record<string, string | undefined>
): Request {
  const u = new URL(req.url);
  u.pathname = pathname;
  // Reset and set only the parameters we care about
  u.search = "";
  const sp = u.searchParams;
  for (const [k, v] of Object.entries(params)) {
    if (v && v !== "") sp.set(k, v);
  }
  return new Request(u.toString(), { method: "GET" });
}

/**
 * Resolve the zone cache instance used for JSON payloads.
 *
 * We intentionally use a named cache via `caches.open(...)` instead of
 * `caches.default` so that TypeScript does not need Cloudflare-specific
 * ambient declarations for `caches.default`.
 */
async function getZoneCache(): Promise<Cache> {
  // Use a named cache to avoid relying on caches.default typings
  return await caches.open(CACHE_NAME);
}

/**
 * Retrieve JSON from the Workers cache by key request.
 *
 * @param keyReq - The synthetic GET Request produced by `buildCacheKeyFrom()`.
 * @returns Parsed JSON on hit, or null on miss/error.
 */
export async function cacheGetJSON<T = unknown>(keyReq: Request): Promise<T | null> {
  try {
    const cache = await getZoneCache();
    const res = await cache.match(keyReq);
    if (!res || !res.ok) return null;
    const data = (await res.json()) as T;
    return data;
  } catch {
    return null;
  }
}

/**
 * Store JSON into the Workers cache under `keyReq` with a TTL.
 *
 * Implementation
 * - Serializes `payload` to JSON and sets `Cache-Control: public, max-age=...`.
 * - Caller is responsible for picking an appropriate TTL.
 *
 * @param keyReq - Cache key request built via `buildCacheKeyFrom()`
 * @param payload - Any JSON-serializable value
 * @param ttlSeconds - Time to live in seconds
 */
export async function cachePutJSON(
  keyReq: Request,
  payload: unknown,
  ttlSeconds: number
): Promise<void> {
  try {
    const body = JSON.stringify(payload);
    const headers = new Headers();
    headers.set("Content-Type", "application/json; charset=utf-8");
    headers.set("Cache-Control", `public, max-age=${Math.max(0, Math.floor(ttlSeconds))}`);
    const res = new Response(body, { status: 200, headers });
    const cache = await getZoneCache();
    await cache.put(keyReq, res);
  } catch {
    // best-effort only
  }
}
