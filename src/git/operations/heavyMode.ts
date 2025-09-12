import type { CacheContext } from "@/cache/index.ts";

/**
 * Enter the closure phase for upload-pack where we want to avoid excessive Cache API reads
 * and cap DO-backed loose loader calls. This sets flags and shared budgets in memo.
 */
export function beginClosurePhase(
  cacheCtx?: CacheContext,
  opts?: { loaderCap?: number; doBatchBudget?: number }
) {
  if (!cacheCtx) return;
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  cacheCtx.memo.flags.add("no-cache-read");
  cacheCtx.memo.loaderCalls = 0;
  if (typeof opts?.loaderCap === "number") {
    cacheCtx.memo.loaderCap = opts.loaderCap;
  } else if (typeof cacheCtx.memo.loaderCap !== "number") {
    cacheCtx.memo.loaderCap = 400; // conservative default during closure
  }
  // Initialize shared DO batch budget if not already set
  if (typeof cacheCtx.memo.doBatchBudget !== "number") {
    cacheCtx.memo.doBatchBudget = typeof opts?.doBatchBudget === "number" ? opts.doBatchBudget : 20;
  }
}

/**
 * Exit the closure phase and transition to downstream reads (single-pack/multi-pack/loose).
 * Resets loader counters and adjusts loader cap depending on whether closure timed out.
 */
export function endClosurePhase(cacheCtx?: CacheContext) {
  if (!cacheCtx?.memo) return;
  cacheCtx.memo.loaderCalls = 0;
  cacheCtx.memo.flags?.delete("loader-capped");
  const timedOut = cacheCtx.memo.flags?.has("closure-timeout") === true;
  cacheCtx.memo.loaderCap = timedOut ? 250 : 600;
}
