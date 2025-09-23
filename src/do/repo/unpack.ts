/**
 * Pack unpacking operations
 *
 * This module handles the asynchronous unpacking of Git packfiles,
 * processing objects in time-budgeted chunks to avoid blocking.
 */

import type { RepoStateSchema, UnpackWork } from "./repoState.ts";
import type { UnpackProgress } from "@/common/index.ts";

import { asTypedStorage } from "./repoState.ts";
import { getDb, getPackOidsSlice, getPackObjectCount } from "./db/index.ts";
import { unpackOidsChunkFromPackBytes } from "@/git/index.ts";
import { scheduleAlarmIfSooner, ensureScheduled } from "./scheduler.ts";
import { getConfig } from "./repoConfig.ts";
import { enqueueHydrationTask } from "./hydration.ts";

/**
 * Get current unpack progress
 * @param ctx - Durable Object state context
 * @returns UnpackProgress object with current status
 */
export async function getUnpackProgress(ctx: DurableObjectState): Promise<UnpackProgress> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const work = await store.get("unpackWork");
  const nextKey = await store.get("unpackNext");

  if (!work) return { unpacking: false, queuedCount: nextKey ? 1 : 0 } as UnpackProgress;

  return {
    unpacking: true,
    processed: work.processedCount,
    total: work.totalCount,
    percent: work.totalCount > 0 ? Math.round((work.processedCount / work.totalCount) * 100) : 0,
    currentPackKey: work.packKey,
    queuedCount: nextKey ? 1 : 0,
  } as UnpackProgress;
}

/**
 * Processes pending unpack work from the queue
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param logger - Logger instance
 * @returns true if unpack work was found and processed, false otherwise
 */
export async function handleUnpackWork(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  logger?: {
    debug: (msg: string, data?: any) => void;
    info: (msg: string, data?: any) => void;
    warn: (msg: string, data?: any) => void;
    error: (msg: string, data?: any) => void;
  }
): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const unpackWork = await store.get("unpackWork");
  if (!unpackWork) return false;

  try {
    await processUnpackChunk(ctx, env, prefix, unpackWork, logger);
  } catch (e) {
    logger?.error("alarm:process-unpack-error", { error: String(e) });
    // Best-effort reschedule so we don't get stuck
    await scheduleAlarmIfSooner(ctx, env, Date.now() + 1000);
  }
  return true;
}

/**
 * Process a chunk of unpack work
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param work - Current unpack work
 * @param logger - Logger instance
 */
async function processUnpackChunk(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  work: UnpackWork,
  logger?: {
    debug: (msg: string, data?: any) => void;
    info: (msg: string, data?: any) => void;
    warn: (msg: string, data?: any) => void;
    error: (msg: string, data?: any) => void;
  }
): Promise<void> {
  const packBytes = await loadPackBytes(env, work.packKey, logger);
  if (!packBytes) {
    await abortUnpackWork(ctx, work.packKey);
    return;
  }

  const result = await processUnpackBatch(ctx, env, prefix, work, packBytes, logger);
  await updateUnpackProgress(ctx, env, work, result, logger);
}

/**
 * Load pack bytes from R2
 * @param env - Worker environment
 * @param packKey - Pack key to load
 * @param logger - Logger instance
 * @returns Pack bytes or null if not found
 */
async function loadPackBytes(
  env: Env,
  packKey: string,
  logger?: { warn: (msg: string, data?: any) => void }
): Promise<Uint8Array | null> {
  const packObj = await env.REPO_BUCKET.get(packKey);
  if (!packObj) {
    logger?.warn("unpack:pack-missing", { packKey });
    return null;
  }
  return new Uint8Array(await packObj.arrayBuffer());
}

/**
 * Abort unpack work due to missing pack
 * @param ctx - Durable Object state context
 * @param packKey - Pack key that was missing
 */
async function abortUnpackWork(ctx: DurableObjectState, packKey: string): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await store.delete("unpackWork");
}

/**
 * Process a batch of objects from a pack
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param work - Current unpack work
 * @param packBytes - Pack file bytes
 * @param logger - Logger instance
 * @returns Processing result with counts
 */
async function processUnpackBatch(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  work: UnpackWork,
  packBytes: Uint8Array,
  logger?: {
    debug: (msg: string, data?: any) => void;
    error: (msg: string, data?: any) => void;
  }
): Promise<{ processed: number; processedInRun: number; exceededBudget: boolean }> {
  const cfg = getConfig(env);
  const db = getDb(ctx.storage);
  const startTime = Date.now();
  let processed = work.processedCount;
  let processedInRunTotal = 0;
  let loops = 0;
  let exceededBudget = false;

  logger?.debug("unpack:begin", {
    packKey: work.packKey,
    processed,
    total: work.totalCount,
    budgetMs: cfg.unpackMaxMs,
  });

  while (processed < work.totalCount && !isTimeExceeded(startTime, cfg.unpackMaxMs)) {
    const oidsToProcess = await getPackOidsSlice(db, work.packKey, processed, cfg.unpackChunkSize);
    if (oidsToProcess.length === 0) break;

    logger?.debug("unpack:chunk", {
      packKey: work.packKey,
      from: processed,
      to: processed + oidsToProcess.length,
      total: work.totalCount,
      loop: loops,
    });

    const processedInRun = await unpackChunk(
      ctx,
      env,
      prefix,
      packBytes,
      work.packKey,
      oidsToProcess,
      logger
    );
    if (processedInRun === 0) break;

    processed += processedInRun;
    processedInRunTotal += processedInRun;
    loops++;

    logger?.debug("unpack:chunk-result", {
      packKey: work.packKey,
      processedInRun,
      processed,
      total: work.totalCount,
    });
  }

  if (isTimeExceeded(startTime, cfg.unpackMaxMs)) {
    exceededBudget = true;
    logger?.debug("unpack:budget-exceeded", {
      packKey: work.packKey,
      timeMs: Date.now() - startTime,
      processed,
      total: work.totalCount,
      loops,
    });
  }

  return { processed, processedInRun: processedInRunTotal, exceededBudget };
}

/**
 * Check if time budget has been exceeded
 * @param startTime - Start time in milliseconds
 * @param maxDuration - Maximum duration in milliseconds
 * @returns true if time exceeded
 */
function isTimeExceeded(startTime: number, maxDuration: number): boolean {
  return Date.now() - startTime >= maxDuration;
}

/**
 * Unpack a chunk of objects from a pack
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param packBytes - Pack file bytes
 * @param packKey - Pack key
 * @param oids - Object IDs to unpack
 * @param logger - Logger instance
 * @returns Number of objects processed
 */
async function unpackChunk(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  packBytes: Uint8Array,
  packKey: string,
  oids: string[],
  logger?: { error: (msg: string, data?: any) => void }
): Promise<number> {
  try {
    return await unpackOidsChunkFromPackBytes(packBytes, ctx, env, prefix, packKey, oids);
  } catch (e) {
    logger?.error("unpack:chunk-error", { error: String(e), packKey });
    return 0;
  }
}

/**
 * Update unpack progress after processing
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param work - Current unpack work
 * @param result - Processing result
 * @param logger - Logger instance
 */
async function updateUnpackProgress(
  ctx: DurableObjectState,
  env: Env,
  work: UnpackWork,
  result: { processed: number; processedInRun: number; exceededBudget: boolean },
  logger?: {
    debug: (msg: string, data?: any) => void;
    info: (msg: string, data?: any) => void;
    warn: (msg: string, data?: any) => void;
    error: (msg: string, data?: any) => void;
  }
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);
  const cfg = getConfig(env);

  if (result.processed >= work.totalCount) {
    // Done unpacking current pack
    await store.delete("unpackWork");
    logger?.info("unpack:done", { packKey: work.packKey, total: work.totalCount });

    // If a next pack is queued, promote it and continue without clearing status
    const nextKey = await store.get("unpackNext");
    if (nextKey) {
      // Count OIDs from SQLite for next pack
      const count = await getPackObjectCount(db, nextKey);
      if (count > 0) {
        await store.put("unpackWork", {
          packKey: nextKey,
          totalCount: count,
          processedCount: 0,
          startedAt: Date.now(),
        } as UnpackWork);
        await store.delete("unpackNext");
        // Schedule next alarm soon to continue unpacking
        await scheduleAlarmIfSooner(ctx, env, Date.now() + cfg.unpackDelayMs);
        logger?.info("queue:promote-next", { packKey: nextKey, total: count });
        return;
      } else {
        // No oids recorded for next pack (unexpected); drop it
        await store.delete("unpackNext");
        logger?.warn("queue:next-missing-oids", { packKey: nextKey });
      }
    }
    // No queued work remains: enqueue hydration (deduped) and schedule
    try {
      await enqueueHydrationTask(ctx, env, { dryRun: false, reason: "post-unpack" });
    } catch (e) {
      logger?.error("queue:hydration-failed", { error: String(e) });
    }
    await ensureScheduled(ctx, env);
  } else {
    // Update progress and reschedule
    await store.put("unpackWork", {
      ...work,
      processedCount: result.processed,
    });

    const delay = result.processedInRun === 0 ? cfg.unpackBackoffMs : cfg.unpackDelayMs;
    await scheduleAlarmIfSooner(ctx, env, Date.now() + delay);

    const percent =
      work.totalCount > 0 ? Math.round((result.processed / work.totalCount) * 100) : 0;
    logger?.debug("unpack:progress", {
      packKey: work.packKey,
      processed: result.processed,
      total: work.totalCount,
      percent,
      timeMs: Date.now(),
      exceededBudget: result.exceededBudget,
    });

    logger?.debug("unpack:reschedule", {
      packKey: work.packKey,
      processed: result.processed,
      total: work.totalCount,
      delay,
    });
  }
}
