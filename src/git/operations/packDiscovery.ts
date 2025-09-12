import { doPrefix, r2PackDirPrefix, isPackKey } from "@/keys.ts";
import type { CacheContext } from "@/cache/index.ts";
import type { RepoDurableObject } from "@/do/index.ts";
import { getLimiter, countSubrequest, DEFAULT_SUBREQUEST_BUDGET } from "./limits.ts";
import { createLogger } from "@/common/index.ts";

/**
 * Shared helper to discover candidate pack keys for a repository.
 * Order semantics: newest-first when seeded from DO; R2 scan is best-effort.
 * Results are memoized per request in `cacheCtx.memo.packList` when provided.
 */
export async function getPackCandidates(
  env: Env,
  stub: DurableObjectStub<RepoDurableObject>,
  doId: string,
  heavy: boolean,
  cacheCtx?: CacheContext
): Promise<string[]> {
  // Reuse per-request memo if present
  if (cacheCtx?.memo?.packList && Array.isArray(cacheCtx.memo.packList)) {
    return cacheCtx.memo.packList;
  }
  // Coalesce concurrent discovery calls within the same request
  if (cacheCtx?.memo?.packListPromise) {
    try {
      const existing = await cacheCtx.memo.packListPromise;
      return existing;
    } catch {
      // fall-through to attempt discovery again; promise creators log errors below
    }
  }

  const limiter = getLimiter(cacheCtx);
  // Ensure soft budget is initialized for the request
  if (cacheCtx) cacheCtx.memo = cacheCtx.memo || { subreqBudget: DEFAULT_SUBREQUEST_BUDGET };
  const log = createLogger(env.LOG_LEVEL, { service: "PackDiscovery", doId });

  let packList: string[] = [];
  const dedupe = (arr: string[]) => {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const k of arr) {
      if (!seen.has(k)) {
        seen.add(k);
        out.push(k);
      }
    }
    return out;
  };

  const inflight = (async () => {
    // Seed with latest pack if available
    try {
      const meta = await limiter.run("do:getPackLatest", async () => {
        countSubrequest(cacheCtx);
        return await stub.getPackLatest();
      });
      const latest = meta?.key;
      if (latest && !packList.includes(latest)) packList.push(latest);
    } catch (e) {
      log.debug("packDiscovery:getPackLatest:error", { error: String(e) });
    }

    // Always include DO /packs (deduped) to broaden candidates for multi-pack assembly
    try {
      const list = await limiter.run("do:getPacks", async () => {
        countSubrequest(cacheCtx);
        return await stub.getPacks();
      });
      if (Array.isArray(list) && list.length > 0) {
        // Ensure latest is first
        if (packList.length > 0) {
          const latest = packList[0];
          const i = list.indexOf(latest);
          if (i >= 0) list.splice(i, 1);
          packList = dedupe([latest, ...list]);
        } else {
          packList = list.slice(0);
        }
      }
    } catch {}

    // Last resort: scan R2 under this DO's pack prefix
    if (packList.length === 0) {
      try {
        const prefix = r2PackDirPrefix(doPrefix(doId));
        const MAX = heavy ? 10 : 50;
        let cursor: string | undefined = undefined;
        const found: string[] = [];
        do {
          const res: any = await limiter.run("r2:list:packs", async () => {
            countSubrequest(cacheCtx);
            return await env.REPO_BUCKET.list({ prefix, cursor });
          });
          const objs: any[] = (res && res.objects) || [];
          for (const o of objs) {
            const key = String(o.key);
            if (isPackKey(key)) found.push(key);
            if (found.length >= MAX) break;
          }
          cursor = res && res.truncated ? res.cursor : undefined;
        } while (cursor && found.length < MAX);
        if (found.length > 0) packList = found;
      } catch (e) {
        log.debug("packDiscovery:r2:list:error", { error: String(e) });
      }
    }

    // Throttle noisy logging to once per request
    if (cacheCtx?.memo) {
      cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
      if (!cacheCtx.memo.flags.has("pack-discovery-logged")) {
        log.debug("packDiscovery:candidates", { count: packList.length });
        cacheCtx.memo.flags.add("pack-discovery-logged");
      }
    } else {
      log.debug("packDiscovery:candidates", { count: packList.length });
    }
    return packList;
  })();

  if (cacheCtx?.memo) cacheCtx.memo.packListPromise = inflight;
  try {
    const list = await inflight;
    if (cacheCtx?.memo) cacheCtx.memo.packList = list;
    return list;
  } finally {
    if (cacheCtx?.memo) cacheCtx.memo.packListPromise = undefined;
  }
}
