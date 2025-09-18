import { packIndexKey } from "@/keys.ts";
import { type IdxParsed, parseIdxV2 } from "./packMeta.ts";

// --- In-process LRU cache for parsed .idx files (ephemeral per isolate) ---
const IDX_CACHE_MAX = 64;
const idxCache = new Map<string, IdxParsed>(); // key: packKey

function touchIdxCache(key: string, value: IdxParsed) {
  if (idxCache.has(key)) idxCache.delete(key);
  idxCache.set(key, value);
  if (idxCache.size > IDX_CACHE_MAX) {
    const first = idxCache.keys().next().value;
    if (first) idxCache.delete(first);
  }
}

export async function loadIdxParsed(
  env: Env,
  packKey: string,
  options?: {
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    signal?: AbortSignal;
  }
): Promise<IdxParsed | undefined> {
  const cached = idxCache.get(packKey);
  if (cached) {
    // Touch for LRU
    touchIdxCache(packKey, cached);
    return cached;
  }
  const idxKey = packIndexKey(packKey);
  if (options?.signal?.aborted) return undefined;
  const run = async () => await env.REPO_BUCKET.get(idxKey);
  const idxObj = options?.limiter
    ? await options.limiter.run("r2:get-idx", async () => {
        options.countSubrequest?.();
        return await run();
      })
    : await run();
  if (!idxObj) return undefined;
  const idxBuf = new Uint8Array(await idxObj.arrayBuffer());
  const parsed = parseIdxV2(idxBuf);
  if (!parsed) return undefined;
  touchIdxCache(packKey, parsed);
  return parsed;
}
