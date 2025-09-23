import type {
  RepoStateSchema,
  HydrationTask,
  HydrationWork,
  HydrationStage,
  HydrationReason,
} from "./repoState.ts";
import type { GitObjectType } from "@/git/core/index.ts";
import type { Logger } from "@/common/logger.ts";

import { indexPackOnly, readPackHeaderEx, buildPackV2 } from "@/git/pack/index.ts";
import { inflateAndParseHeader } from "@/git/core/index.ts";
import { r2PackKey, packIndexKey, getDoIdFromPath } from "@/keys.ts";
import { createLogger } from "@/common/index.ts";
import { asTypedStorage, objKey } from "./repoState.ts";
import { loadIdxParsed } from "@/git/pack/idxCache.ts";
import { getConfig } from "./repoConfig.ts";
import { getEpochFromWorkId, parseEpochFromHydrPackKey, calculateStableEpochs } from "./packs.ts";
import { ensureScheduled } from "./scheduler.ts";
import {
  getDb,
  insertPackOids,
  insertHydrCoverOids,
  insertHydrPendingOids,
  getHydrPendingOids,
  getHydrPendingCounts,
  deleteHydrPendingOids,
  clearHydrPending,
  deletePackObjects,
  clearHydrCover,
  hasHydrCoverForWork,
  filterUncoveredAgainstHydrCover,
  getPackOids,
  normalizePackKey,
} from "./db/index.ts";

// File-wide constants to avoid magic numbers across stages
const HYDR_SAMPLE_PER_PACK = 128; // sample items per pack during planning
const HYDR_SOFT_SUBREQ_LIMIT = 800; // soft cap on subrequests per slice
const HYDR_LOOSE_LIST_PAGE = 250; // DO storage list page size
const HYDR_SEG_MAX_BYTES = 8 * 1024 * 1024; // 8 MiB per hydration segment
const HYDR_MAX_OBJS_PER_SEGMENT = 2000; // conservative cap to fit budgets
const HYDR_EST_COMPRESSION_RATIO = 0.6; // rough compression ratio estimate
const PACK_TYPE_OFS_DELTA = 6 as const;
const PACK_TYPE_REF_DELTA = 7 as const;

type HydrationCtx = {
  state: DurableObjectState;
  env: Env;
  prefix: string;
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>;
  cfg: ReturnType<typeof getHydrConfig>;
  log: Logger;
};

/**
 * Summary returned by the admin dry-run endpoint to preview hydration work.
 * The numbers are conservative and sampled; the plan is partial by design
 * and intended only to surface whether hydration is likely needed.
 */
export type HydrationPlan = {
  snapshot: { lastPackKey: string | null; packListCount: number };
  window: { packKeys: string[] };
  counts: {
    deltaBases: number;
    looseOnly: number;
    totalCandidates: number;
    alreadyCovered: number;
    toPack: number;
  };
  segments: { estimated: number; maxObjectsPerSegment: number; maxBytesPerSegment: number };
  budgets: { timePerSliceMs: number; softSubrequestLimit: number };
  stats: { examinedPacks: number; examinedObjects: number; examinedLoose: number };
  warnings: string[];
  partial: boolean;
};

function nowMs() {
  return Date.now();
}

/**
 * Compute the stable hydration window (pack keys) based on stable epochs and cfg.
 * Includes lastPackKey only when it is a hydration pack in a stable epoch.
 */
async function computeStableHydrationWindow(
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  cfg: ReturnType<typeof getHydrConfig>
): Promise<{ window: string[]; lastPackKey: string | null }> {
  const lastPackKey = (await store.get("lastPackKey")) || null;
  const packListRaw = (await store.get("packList")) || [];
  const packList = Array.isArray(packListRaw) ? packListRaw : [];

  const { stableEpochs } = calculateStableEpochs(packList, cfg.keepPacks, lastPackKey || undefined);
  const stableSet = new Set(stableEpochs);

  const hydra: string[] = [];
  for (const k of packList) {
    const e = parseEpochFromHydrPackKey(k);
    if (e && stableSet.has(e)) hydra.push(k);
  }

  // Optionally include last when it's a hydration pack in a stable epoch
  const lbase = normalizePackKey(lastPackKey || "");
  const lastEpoch = lastPackKey ? parseEpochFromHydrPackKey(lastPackKey) : null;
  if (lastPackKey && lbase.startsWith("pack-hydr-") && lastEpoch && stableSet.has(lastEpoch)) {
    hydra.unshift(lastPackKey);
  }

  const window = hydra.slice(0, cfg.windowMax);
  return { window, lastPackKey };
}

/**
 * Populate per-work coverage table hydr_cover(work_id, oid) from hydration window packs.
 * Only includes packs whose basename starts with 'pack-hydr-'. Optionally includes
 * lastPackOids when lastPackKey is a hydration pack.
 */
async function ensureHydrCoverForWork(
  state: DurableObjectState,
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  cfg: ReturnType<typeof getHydrConfig>,
  workId: string
): Promise<void> {
  if (!workId) return;
  const db = getDb(state.storage);
  try {
    const exists = await hasHydrCoverForWork(db, workId);
    if (exists) return;
  } catch {}

  // Build hydration-only window list (stable epochs only)
  const { window, lastPackKey } = await computeStableHydrationWindow(store, cfg);

  // Insert memberships into hydr_cover (chunking handled by helper)
  for (const pk of window) {
    try {
      // Fetch pack membership via DAL and insert into coverage table
      const oids = (await getPackOids(db, pk)).map((o) => o.toLowerCase());
      await insertHydrCoverOids(db, workId, oids);
    } catch {}
  }

  // Include recent lastPackOids only when last pack is included in the stable window and is hydration
  const includeLastOids =
    !!lastPackKey &&
    normalizePackKey(lastPackKey).startsWith("pack-hydr-") &&
    window.includes(lastPackKey);
  if (includeLastOids) {
    try {
      const last = ((await store.get("lastPackOids")) || []).slice(0, 10000);
      const oids = (Array.isArray(last) ? last : []).map((oid: string) => oid.toLowerCase());
      await insertHydrCoverOids(db, workId, oids);
    } catch {}
  }
}

/**
 * Builds a coverage set limited to existing hydration packs only (pack-hydr-*).
 * Optionally includes `lastPackOids` only when lastPackKey itself is a hydration pack.
 * Used by the segment builder to avoid re-creating objects already included in
 * previous hydration segments while still allowing thickening across non-hydration packs.
 */
async function buildHydrationCoverageSet(
  state: DurableObjectState,
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  cfg: ReturnType<typeof getHydrConfig>
): Promise<Set<string>> {
  const covered = new Set<string>();
  try {
    const db = getDb(state.storage);
    const { window, lastPackKey } = await computeStableHydrationWindow(store, cfg);
    // Load coverage from SQLite pack_objects to avoid large KV values
    try {
      if (window.length > 0) {
        // Query per key to avoid massive IN clauses; window is capped by cfg.windowMax
        for (const pk of window) {
          const rows = await getPackOids(db, pk);
          for (const oid of rows) covered.add(String(oid).toLowerCase());
        }
      }
    } catch {}
    const includeLastOids =
      !!lastPackKey &&
      normalizePackKey(lastPackKey).startsWith("pack-hydr-") &&
      window.includes(lastPackKey);
    if (includeLastOids) {
      const last = (await store.get("lastPackOids")) || [];
      for (const x of last.slice(0, 10000)) covered.add(x.toLowerCase());
    }
  } catch {}
  return covered;
}

/**
 * Builds a recent window of pack keys with HEAD (lastPackKey) first followed by
 * the remaining packList order, capped by windowMax.
 */
function buildRecentWindowKeys(
  lastPackKey: string | null,
  packList: string[],
  windowMax: number
): string[] {
  const windowKeys: string[] = [];
  if (lastPackKey) windowKeys.push(lastPackKey);
  for (const k of packList) if (!windowKeys.includes(k)) windowKeys.push(k);
  return windowKeys.slice(0, windowMax);
}

/**
 * Creates a logger for hydration work using a doId derived from the pack key/prefix.
 */
function makeHydrationLogger(env: Env, lastPackKey?: string | null): Logger {
  const doId = getDoIdFromPath(lastPackKey || "") || undefined;
  return createLogger(env.LOG_LEVEL, { service: "Hydration", doId });
}

type StageHandlerResult = {
  // whether the alarm should schedule another slice soon
  continue: boolean;
  // whether the driver should persist work; default true
  persist?: boolean;
};

/**
 * Stage handler signature. Handlers must be pure with respect to side effects
 * other than mutating the provided `work` object and performing repo operations
 * via the supplied context (state/env/store). They should only advance the
 * stage via `setStage()` and return a small `StageHandlerResult`.
 */
type StageHandler = (ctx: HydrationCtx, work: HydrationWork) => Promise<StageHandlerResult>;

/** Small helper to set stage with debug logging */
function setStage(work: HydrationWork, stage: HydrationStage, log: Logger) {
  if (work.stage !== stage) {
    log.debug("hydration:transition", { from: work.stage, to: stage });
    work.stage = stage;
  }
}

// Small helpers to reduce repetition when mutating work state
type HydrationProgress = NonNullable<HydrationWork["progress"]>;

function updateProgress(work: HydrationWork, patch: Partial<HydrationProgress>) {
  const base: HydrationProgress = { ...(work.progress ?? {}) };
  Object.assign(base, patch);
  work.progress = base;
}

function clearError(work: HydrationWork) {
  if (work.error) work.error = undefined;
}

/** Stage handlers dispatch table (implementations below) */
const STAGE_HANDLERS: Record<HydrationStage, StageHandler> = {
  plan: handleStagePlan,
  "scan-deltas": handleStageScanDeltas,
  "scan-loose": handleStageScanLoose,
  "build-segment": handleStageBuildSegment,
  done: handleStageDone,
  error: handleStageError,
};

type PackHeaderEx = { type: number; baseRel?: number; baseOid?: string };

/**
 * Precomputes physical ordering and reverse indices for a parsed idx.
 */
function buildPhysicalIndex(parsed: { oids: string[]; offsets: number[] }) {
  const { oids, offsets } = parsed;
  const oidsSet = new Set(oids.map((x) => x.toLowerCase()));
  const sorted = offsets.slice().sort((a, b) => a - b);
  const offToIdx = new Map<number, number>();
  for (let i = 0; i < offsets.length; i++) offToIdx.set(offsets[i], i);
  return { oids, offsets, oidsSet, sorted, offToIdx };
}

/**
 * Analyze a PACK entry header and collect ALL bases in the delta chain for thickening.
 *
 * CRITICAL: We must resolve the FULL delta chain, not just immediate bases.
 * The hydration system's purpose is to eliminate closure computation during initial clones.
 * If we only include immediate bases, we still force fetch-time chain resolution.
 *
 * Example: C (delta) → B (delta) → A (base)
 * - Old behavior: Only includes B when processing C, missing A
 * - New behavior: Includes both B and A when processing C
 *
 * This ensures initial clones can be served entirely from hydration packs without
 * any delta chain traversal at fetch time.
 */
async function analyzeDeltaChain(
  env: Env,
  packKey: string,
  header: PackHeaderEx,
  off: number,
  idx: { offToIdx: Map<number, number>; oids: string[]; offsets: number[]; oidsSet: Set<string> },
  coveredHas: (q: string) => boolean
): Promise<string[]> {
  const chain: string[] = [];
  const seen = new Set<string>();

  // Start with the immediate base
  let baseOid: string | undefined;
  let currentOff = off;
  let currentHeader = header;

  // Traverse the full delta chain
  while (true) {
    baseOid = undefined;

    if (currentHeader.type === PACK_TYPE_OFS_DELTA) {
      const baseOff = currentOff - (currentHeader.baseRel || 0);
      const baseIdx = idx.offToIdx.get(baseOff);
      if (baseIdx !== undefined) baseOid = idx.oids[baseIdx];
      currentOff = baseOff;
    } else if (currentHeader.type === PACK_TYPE_REF_DELTA) {
      baseOid = currentHeader.baseOid;
      // For REF_DELTA, we need to find the offset of the base
      if (baseOid) {
        const searchOid = baseOid.toLowerCase();
        const baseIdx = idx.oids.findIndex((o) => o.toLowerCase() === searchOid);
        if (baseIdx >= 0) {
          currentOff = idx.offsets[baseIdx];
        } else {
          // Base is not in this pack, include it and stop
          if (!coveredHas(searchOid) && !seen.has(searchOid)) {
            chain.push(searchOid);
          }
          break;
        }
      }
    }

    if (!baseOid) break;

    const q = baseOid.toLowerCase();

    // Avoid infinite loops
    if (seen.has(q)) break;
    seen.add(q);

    // If base is not in same pack or not covered, add to chain
    if (!idx.oidsSet.has(q) || !coveredHas(q)) {
      chain.push(q);
      // If not in this pack, we can't continue traversing
      if (!idx.oidsSet.has(q)) break;
    }

    // Read the base's header to continue chain traversal
    try {
      const nextHeader = await readPackHeaderEx(env, packKey, currentOff);
      if (!nextHeader) break;

      // If base is not a delta, we've reached the end
      if (nextHeader.type !== PACK_TYPE_OFS_DELTA && nextHeader.type !== PACK_TYPE_REF_DELTA) {
        break;
      }

      currentHeader = nextHeader;
    } catch {
      break;
    }
  }

  return chain;
}

/**
 * Clears hydration-related state and deletes hydration-generated packs.
 * - Clears hydrationWork and hydrationQueue
 * - Deletes packs whose basename starts with 'pack-hydr-' and their .idx files
 * - Removes their membership entries from DO storage
 * - Updates packList to exclude removed packs
 * - If lastPackKey points to a removed pack, clears lastPackKey/lastPackOids
 */
export async function clearHydrationState(
  state: DurableObjectState,
  env: Env
): Promise<{ clearedWork: boolean; clearedQueue: number; removedPacks: number }> {
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const log = createLogger(env.LOG_LEVEL, { service: "Hydration", doId: state.id.toString() });
  const db = getDb(state.storage);
  let clearedWork = false;
  let clearedQueue = 0;
  let removedPacks = 0;

  // Clear work and queue
  const work = await store.get("hydrationWork");
  if (work) {
    await store.delete("hydrationWork");
    clearedWork = true;
  }
  const queue = (await store.get("hydrationQueue")) || [];
  clearedQueue = Array.isArray(queue) ? queue.length : 0;
  await store.put("hydrationQueue", []);

  // Purge hydration-generated packs
  const list = (await store.get("packList")) || [];
  const toRemove: string[] = [];
  for (const key of list) {
    const base = normalizePackKey(key);
    if (base.startsWith("pack-hydr-")) toRemove.push(key);
  }

  for (const key of toRemove) {
    try {
      await env.REPO_BUCKET.delete(key);
    } catch (e) {
      log.warn("clear:delete-pack-failed", { key, error: String(e) });
    }
    try {
      await env.REPO_BUCKET.delete(packIndexKey(key));
    } catch (e) {
      log.warn("clear:delete-pack-index-failed", { key, error: String(e) });
    }
    // Also remove pack membership rows from SQLite
    try {
      await deletePackObjects(db, key);
    } catch (e) {
      log.warn("clear:delete-packObjects-failed", { key, error: String(e) });
    }
    removedPacks++;
  }

  // Update packList and last* metadata
  if (toRemove.length > 0) {
    const keep = list.filter((k) => !toRemove.includes(k));
    try {
      await store.put("packList", keep);
    } catch (e) {
      log.warn("clear:put-packlist-failed", { error: String(e) });
    }
    try {
      const last = await store.get("lastPackKey");
      if (last && toRemove.includes(String(last))) {
        await store.delete("lastPackKey");
        await store.delete("lastPackOids");
      }
    } catch (e) {
      log.warn("clear:put-lastpack-failed", { error: String(e) });
    }
  }

  return { clearedWork, clearedQueue, removedPacks };
}

/**
 * Returns hydration timing and window configuration derived from repo-level config.
 */
function getHydrConfig(env: Env) {
  // Source common timing knobs from repo-level config for consistency
  const base = getConfig(env);
  // Hydration window should scan ALL packs to ensure completeness.
  // Without scanning all packs, we miss objects in older packs that may be
  // referenced by newer commits, leading to incomplete clones.
  const windowMax = base.packListMax;
  return {
    unpackMaxMs: base.unpackMaxMs,
    unpackDelayMs: base.unpackDelayMs,
    unpackBackoffMs: base.unpackBackoffMs,
    chunk: base.unpackChunkSize,
    keepPacks: base.keepPacks,
    windowMax,
  };
}

/**
 * Enqueue a hydration task (deduping existing admin tasks).
 * Returns an ack with queue length and a generated workId for tracking.
 */
export async function enqueueHydrationTask(
  state: DurableObjectState,
  env: Env,
  options?: { dryRun?: boolean; reason?: HydrationReason }
): Promise<{ queued: boolean; workId: string; queueLength: number }> {
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const log = createLogger(env.LOG_LEVEL, { service: "Hydration" });
  const q = (await store.get("hydrationQueue")) || [];
  const reason = options?.reason || "admin";
  // Deduplicate: keep at most one task queued per reason
  const exists = Array.isArray(q) && q.some((t: HydrationTask) => t?.reason === reason);
  const queue: HydrationTask[] = Array.isArray(q) ? q.slice() : [];
  const workId = `hydr-${nowMs()}`;
  if (!exists) {
    queue.push({ reason, createdAt: nowMs(), options: { dryRun: options?.dryRun } });
    await store.put("hydrationQueue", queue);
    // Schedule alarm soon (unified scheduler)
    await ensureScheduled(state, env);
    log.info("enqueue:ok", { queueLength: queue.length, reason });
  } else {
    log.info("enqueue:dedupe", { queueLength: queue.length, reason });
  }
  return { queued: true, workId, queueLength: queue.length };
}

/**
 * Computes a quick, conservative hydration plan without writing state.
 * This minimal implementation avoids heavy scanning and provides a partial summary.
 *
 * Notes:
 * - Uses `getHydrConfig()` for window sizing and reports `unpackMaxMs` as the
 *   planning time budget in the `budgets` field.
 * - The delta-base scan is sampled and conservative; it does not recurse chains.
 */
export async function summarizeHydrationPlan(
  state: DurableObjectState,
  env: Env,
  prefix: string
): Promise<HydrationPlan> {
  const log = makeHydrationLogger(env, prefix);
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const cfg = getHydrConfig(env);

  const lastPackKey = (await store.get("lastPackKey")) || null;
  const packListRaw = (await store.get("packList")) || [];
  const packList = Array.isArray(packListRaw) ? packListRaw : [];

  // Build recent window, ensuring lastPackKey is first if present
  const window = buildRecentWindowKeys(lastPackKey, packList, cfg.windowMax);

  // Coverage set from hydration packs only (matches what scan/build stages use)
  const covered = await buildHydrationCoverageSet(state, store, cfg);

  // Estimate delta base needs by sampling object headers in the window packs.
  // We keep this very lightweight: small per-pack sample, no recursion, and only
  // count unique base OIDs not already covered by the window.
  let examinedObjects = 0;
  const baseCandidates = new Set<string>();
  try {
    const SAMPLE_PER_PACK = HYDR_SAMPLE_PER_PACK; // small cap to avoid many R2 range reads
    for (const key of window) {
      const parsed = await loadIdxParsed(env, key);
      if (!parsed) continue;
      const phys = buildPhysicalIndex(parsed);
      // Sample evenly across the pack
      const stride = Math.max(1, Math.floor(phys.sorted.length / SAMPLE_PER_PACK));
      let count = 0;
      for (let i = 0; i < phys.sorted.length && count < SAMPLE_PER_PACK; i += stride) {
        const off = phys.sorted[i];
        const header = await readPackHeaderEx(env, key, off);
        if (!header) continue;
        examinedObjects++;
        // For dry-run, just check immediate base (full chain would be too expensive for summary)
        let baseOid: string | undefined;
        if (header.type === PACK_TYPE_OFS_DELTA) {
          const baseOff = off - (header.baseRel || 0);
          const baseIdx = phys.offToIdx.get(baseOff);
          if (baseIdx !== undefined) baseOid = phys.oids[baseIdx];
        } else if (header.type === PACK_TYPE_REF_DELTA) {
          baseOid = header.baseOid;
        }
        if (baseOid) {
          const q = baseOid.toLowerCase();
          if (!phys.oidsSet.has(q) || !covered.has(q)) {
            baseCandidates.add(q);
          }
        }
        count++;
      }
    }
  } catch {}

  // Sample loose-only count
  let examinedLoose = 0;
  let looseOnly = 0;
  try {
    const it = await state.storage.list({ prefix: "obj:", limit: 500 });
    for (const k of it.keys()) {
      const oid = String(k).slice(4).toLowerCase();
      examinedLoose++;
      if (!covered.has(oid)) looseOnly++;
    }
  } catch {}

  const estimatedDeltaBases = baseCandidates.size;
  const counts = {
    deltaBases: estimatedDeltaBases,
    looseOnly,
    totalCandidates: looseOnly + estimatedDeltaBases,
    alreadyCovered: 0,
    toPack: looseOnly + estimatedDeltaBases,
  };

  const segments = {
    estimated: Math.max(0, Math.ceil(counts.toPack / HYDR_MAX_OBJS_PER_SEGMENT)),
    maxObjectsPerSegment: HYDR_MAX_OBJS_PER_SEGMENT,
    maxBytesPerSegment: HYDR_SEG_MAX_BYTES,
  };

  const out: HydrationPlan = {
    snapshot: { lastPackKey, packListCount: packListRaw.length || 0 },
    window: { packKeys: window },
    counts,
    segments,
    budgets: { timePerSliceMs: cfg.unpackMaxMs, softSubrequestLimit: HYDR_SOFT_SUBREQ_LIMIT },
    stats: { examinedPacks: window.length, examinedObjects, examinedLoose },
    warnings: ["summary-partial-simple", "summary-sampled-deltas"],
    partial: true,
  };
  log.debug("dryRun:summary", out);
  return out;
}

/**
 * Perform a small hydration slice.
 *
 * Driver for the hydration finite state machine (FSM):
 * - Initializes `hydrationWork` from the queue when absent.
 * - Dispatches to a typed stage handler via `STAGE_HANDLERS`.
 * - Centralizes persistence and scheduling decisions based on handler result.
 *
 * Returns true if more work remains or the alarm should be scheduled soon.
 */
export async function processHydrationSlice(
  state: DurableObjectState,
  env: Env,
  prefix: string
): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const log = makeHydrationLogger(env, prefix);
  const cfg = getHydrConfig(env);

  // Load or create work from queue
  let work = (await store.get("hydrationWork")) || undefined;
  const queue = (await store.get("hydrationQueue")) || [];

  if (!work) {
    if (!Array.isArray(queue) || queue.length === 0) return false;
    // Start a new work from the head of the queue
    const task = queue[0];
    work = {
      workId: `hydr-${nowMs()}`,
      startedAt: nowMs(),
      dryRun: !!task?.options?.dryRun,
      stage: "plan",
      progress: { packIndex: 0, objCursor: 0, segmentSeq: 0, producedBytes: 0 },
      stats: {},
    };
    await store.put("hydrationWork", work);
    await ensureScheduled(state, env);
    log.info("hydration:start", {
      stage: work.stage,
      reason: (task && task.reason) || "?",
    });
    return true;
  }

  // Dispatch via typed state machine
  const ctx = { state, env, prefix, store, cfg, log };
  const handler = STAGE_HANDLERS[work.stage] as StageHandler | undefined;
  if (!handler) {
    await store.delete("hydrationWork");
    log.warn("reset:unknown-stage", {});
    return false;
  }

  const result = await handler(ctx, work);
  if (result.persist !== false) {
    await store.put("hydrationWork", work);
  }
  if (result.continue) {
    await ensureScheduled(state, env);
  }
  return result.continue;
}

// Stage: plan — snapshot repo layout and initialize cursors
async function handleStagePlan(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { store, cfg, log } = ctx;
  // Build recent window, ensuring lastPackKey is first if present
  const lastPackKey = (await store.get("lastPackKey")) || null;
  const packListRaw = (await store.get("packList")) || [];
  const packList = Array.isArray(packListRaw) ? packListRaw : [];
  const window = buildRecentWindowKeys(lastPackKey, packList, cfg.windowMax);

  // Initialize snapshot and cursors
  work.snapshot = {
    lastPackKey,
    packList: packList.slice(0, cfg.windowMax),
    window,
  };
  work.progress = { ...(work.progress || {}), packIndex: 0, objCursor: 0 };

  // Populate per-work coverage table from hydration window once
  try {
    await ensureHydrCoverForWork(ctx.state, store, cfg, work.workId);
  } catch (e) {
    log.warn("hydration:cover:init-failed", { error: String(e) });
  }

  // Transition to next stage
  if (work.dryRun) {
    // Honor dry-run: compute a quick summary and finish without scanning/building
    try {
      const summary = await summarizeHydrationPlan(ctx.state, ctx.env, ctx.prefix);
      log.info("hydration:dry-run:summary", summary);
    } catch (e) {
      log.warn("hydration:dry-run:summary-failed", { error: String(e) });
    }
    setStage(work, "done", log);
    log.info("hydration:planned(dry-run)", { window: window.length, last: lastPackKey });
    return { continue: true };
  } else {
    setStage(work, "scan-deltas", log);
    log.info("hydration:planned", { window: window.length, last: lastPackKey });
    return { continue: true };
  }
}

// Stage: scan-deltas — examine pack headers and accumulate base candidates
async function handleStageScanDeltas(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { state, env, log, store, cfg } = ctx;
  const db = getDb(state.storage);
  log.debug("hydration:scan-deltas:tick", {
    packIndex: work.progress?.packIndex || 0,
    objCursor: work.progress?.objCursor || 0,
    window: work.snapshot?.window?.length || 0,
  });
  const res = await scanDeltasSlice(state, env, work);
  if (res === "next") {
    setStage(work, "scan-loose", log);
    const counts = await getHydrPendingCounts(db, work.workId);
    log.info("hydration:scan-deltas:done", { needBases: counts.bases });
    // Clear transient error state on success
    clearError(work);
  } else if (res === "error") {
    await handleTransientError(work, store, log, cfg);
  } else {
    // slice progressed successfully; clear any previous error
    clearError(work);
  }
  return { continue: true };
}

// Stage: scan-loose — find loose-only objects not covered by packs
async function handleStageScanLoose(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { state, env, log, store, cfg } = ctx;
  const db = getDb(state.storage);
  log.debug("hydration:scan-loose:tick", {
    cursor: work.progress?.looseCursorKey || null,
  });
  const res = await scanLooseSlice(state, env, work);
  if (res === "next") {
    setStage(work, "build-segment", log);
    const counts = await getHydrPendingCounts(db, work.workId);
    log.info("hydration:scan-loose:done", { needLoose: counts.loose });
    clearError(work);
  } else if (res === "error") {
    await handleTransientError(work, store, log, cfg);
  } else {
    clearError(work);
  }
  return { continue: true };
}

// Stage: build-segment — build one hydration pack segment from pending objects
async function handleStageBuildSegment(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { state, env, prefix, log, store, cfg } = ctx;
  const db = getDb(state.storage);
  const counts = await getHydrPendingCounts(db, work.workId);
  log.info("hydration:build-segment:tick", {
    needBases: counts.bases,
    needLoose: counts.loose,
    segmentSeq: work.progress?.segmentSeq || 0,
  });
  const res = await buildSegmentSlice(state, env, prefix, work);
  if (res === "done") {
    setStage(work, "done", log);
    log.info("hydration:build-segment:done", {
      segmentSeq: work.progress?.segmentSeq || 0,
      producedBytes: work.progress?.producedBytes || 0,
    });
    return { continue: true }; // will transition to done handler on next tick
  }
  if (res === "error") {
    if (work.error?.fatal) {
      setStage(work, "error", log);
      log.error("hydration:fatal-error", { message: work.error.message });
      return { continue: false };
    }
    await handleTransientError(work, store, log, cfg);
  }
  // success progress slice
  if (res !== "error") clearError(work);
  return { continue: true };
}

// Stage: done — finalize and dequeue
async function handleStageDone(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { state, store, log } = ctx;
  const queue = (await store.get("hydrationQueue")) || [];
  const newQ = Array.isArray(queue) ? queue.slice(1) : [];
  await store.put("hydrationQueue", newQ);
  await store.delete("hydrationWork");
  // Cleanup per-work coverage and pending rows for this work
  try {
    const db = getDb(state.storage);
    await clearHydrCover(db, work.workId);
    await clearHydrPending(db, work.workId);
  } catch {}
  log.info("done", { remaining: newQ.length });
  // If queue still has items, schedule another tick; do not persist work we just deleted
  return { continue: newQ.length > 0, persist: false };
}

// Stage: error — manage retries/backoff for transient errors; stop on fatal/max-retries
async function handleStageError(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { log } = ctx;
  // Terminal error stage: requires manual intervention; do not auto-retry
  log.error("error:terminal", { message: work.error?.message, fatal: work.error?.fatal !== false });
  return { continue: false };
}

async function handleTransientError(
  work: HydrationWork,
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  log: Logger,
  cfg: ReturnType<typeof getHydrConfig>
): Promise<void> {
  if (!work.error) return;

  work.error.retryCount = (work.error.retryCount || 0) + 1;
  work.error.firstErrorAt = work.error.firstErrorAt || nowMs();
  // Schedule fixed-interval retry; do not transition to 'error' stage here
  const intervalMs = Math.max(1000, cfg.unpackBackoffMs || 5000);
  work.error.nextRetryAt = nowMs() + intervalMs;
  // Stay in current stage for retry
  log.warn("transient-error:will-retry", {
    message: work.error.message,
    retryCount: work.error.retryCount,
    nextRetryAt: work.error.nextRetryAt,
  });
}

async function scanDeltasSlice(
  state: DurableObjectState,
  env: Env,
  work: HydrationWork
): Promise<"more" | "next" | "error"> {
  /**
   * Stage 2: scan-deltas
   *
   * Examines pack headers across a recent window to identify immediate delta bases
   * that should be thickened. Work is bounded by `cfg.unpackMaxMs` and slices of
   * `cfg.chunk` objects per pack.
   */
  const cfg = getHydrConfig(env);
  const start = nowMs();
  const log = makeHydrationLogger(env, work.snapshot?.lastPackKey || "");
  const db = getDb(state.storage);

  const window = work.snapshot?.window || [];
  if (!window || window.length === 0) return "next";

  // For coverage checks within this slice, defer to hydr_cover using a batched query.
  // We collect in-pack base candidates and filter them in one go at slice boundaries.
  const inPackCoverageCandidates = new Set<string>();
  const needBasesSet = new Set<string>();

  let pIndex = work.progress?.packIndex || 0;
  let objCur = work.progress?.objCursor || 0;

  const SOFT_SUBREQ_LIMIT = HYDR_SOFT_SUBREQ_LIMIT;
  let subreq = 0;

  while (pIndex < window.length && nowMs() - start < cfg.unpackMaxMs) {
    const key = window[pIndex];
    let parsed;
    try {
      parsed = await loadIdxParsed(env, key);
      subreq++;
    } catch (e) {
      // R2 read failure is transient
      log.warn("scan-deltas:idx-load-error", { key, error: String(e) });
      work.error = { message: `Failed to load pack index: ${String(e)}` };
      updateProgress(work, { packIndex: pIndex, objCursor: objCur });
      // Save pending bases to SQLite even on error
      await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
      return "error";
    }
    if (!parsed) {
      // Missing idx: skip
      pIndex++;
      objCur = 0;
      log.warn("scan-deltas:missing-idx", { key });
      continue;
    }
    const phys = buildPhysicalIndex(parsed);

    const end = Math.min(phys.sorted.length, objCur + cfg.chunk);
    for (let j = objCur; j < end; j++) {
      const off = phys.sorted[j];
      let header;
      try {
        header = await readPackHeaderEx(env, key, off);
        subreq++;
      } catch (e) {
        // R2 read failure is transient
        log.warn("scan-deltas:header-read-error", { key, off, error: String(e) });
        work.error = { message: `Failed to read pack header: ${String(e)}` };
        objCur = j;
        updateProgress(work, { packIndex: pIndex, objCursor: objCur });
        // Save pending bases to SQLite even on error
        await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
        return "error";
      }
      if (!header) continue;
      // Analyze the full delta chain to ensure complete thickening.
      // For bases inside the same pack, record candidates and decide coverage in batch later.
      const chain = await analyzeDeltaChain(env, key, header, off, phys, (q: string) => {
        // Only record candidates that are in the same pack; others must always be included
        if (phys.oidsSet.has(q)) inPackCoverageCandidates.add(q);
        return false; // tentatively treat as not covered; we'll filter later using hydr_cover
      });
      for (const oid of chain) needBasesSet.add(oid);
      // stop if time budget exceeded inside loop
      if (nowMs() - start >= cfg.unpackMaxMs || subreq >= SOFT_SUBREQ_LIMIT) {
        objCur = j + 1;
        // Batch filter in-pack candidates against hydr_cover before persisting pending
        try {
          const uncovered = await filterUncoveredAgainstHydrCover(
            db,
            work.workId,
            Array.from(inPackCoverageCandidates)
          );
          const uncoveredSet = new Set(uncovered);
          // Remove covered ones from needBasesSet (only those that were in-pack candidates)
          for (const q of inPackCoverageCandidates) {
            if (!uncoveredSet.has(q)) needBasesSet.delete(q);
          }
        } catch {}
        updateProgress(work, { packIndex: pIndex, objCursor: objCur });
        // Save pending bases to SQLite
        await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
        log.debug("scan-deltas:slice", {
          packIndex: pIndex,
          advanced: j - (work.progress?.objCursor || 0),
          needBases: needBasesSet.size,
        });
        return "more";
      }
    }
    // chunk finished
    objCur = end;
    if (objCur >= phys.sorted.length) {
      // move to next pack
      pIndex++;
      objCur = 0;
    } else {
      // Need another slice for this pack
      updateProgress(work, { packIndex: pIndex, objCursor: objCur });
      // Save pending bases to SQLite
      await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
      log.debug("scan-deltas:continue", {
        packIndex: pIndex,
        objCursor: objCur,
        needBases: needBasesSet.size,
      });
      return "more";
    }
  }

  // Completed all packs within time or finished list
  // Final batch filter for in-pack candidates
  try {
    const uncovered = await filterUncoveredAgainstHydrCover(
      db,
      work.workId,
      Array.from(inPackCoverageCandidates)
    );
    const uncoveredSet = new Set(uncovered);
    for (const q of inPackCoverageCandidates) {
      if (!uncoveredSet.has(q)) needBasesSet.delete(q);
    }
  } catch {}
  updateProgress(work, { packIndex: pIndex, objCursor: objCur });
  // Save final pending bases to SQLite
  await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
  log.info("scan-deltas:complete", { needBases: needBasesSet.size });
  return pIndex < window.length ? "more" : "next";
}

async function scanLooseSlice(
  state: DurableObjectState,
  env: Env,
  work: HydrationWork
): Promise<"more" | "next" | "error"> {
  /**
   * Stage 3: scan-loose
   *
   * Iterates DO storage keys `obj:*` in small pages to collect objects that are
   * not covered by the recent pack window. Resumable via `progress.looseCursorKey`.
   * Work is bounded by `cfg.unpackMaxMs`.
   */
  const cfg = getHydrConfig(env);
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const start = nowMs();
  const log = makeHydrationLogger(env, work.snapshot?.lastPackKey || "");
  const db = getDb(state.storage);

  const needLoose = new Set<string>();

  const LIMIT = HYDR_LOOSE_LIST_PAGE;
  let cursor = work.progress?.looseCursorKey || undefined;
  let done = false;

  while (!done && nowMs() - start < cfg.unpackMaxMs) {
    const opts: { prefix: string; limit: number; startAfter?: string } = {
      prefix: "obj:",
      limit: LIMIT,
      ...(cursor ? { startAfter: cursor } : {}),
    };
    let it;
    try {
      it = await state.storage.list(opts);
    } catch (e) {
      // DO storage list failure is transient
      log.warn("scan-loose:list-error", { cursor, error: String(e) });
      work.error = { message: `Failed to list loose objects: ${String(e)}` };
      updateProgress(work, { looseCursorKey: cursor });
      await insertHydrPendingOids(db, work.workId, "loose", Array.from(needLoose));
      return "error";
    }
    const keys: string[] = [];
    for (const k of it.keys()) keys.push(String(k));
    if (keys.length === 0) {
      done = true;
      break;
    }
    // Convert keys to OIDs and filter against hydr_cover in a single SQL
    const oids = keys.map((k) => String(k).slice(4).toLowerCase());
    let uncovered: string[] = [];
    try {
      uncovered = await filterUncoveredAgainstHydrCover(db, work.workId, oids);
    } catch (e) {
      log.warn("scan-loose:cover-check-failed", { error: String(e) });
      // Fall back to treating all as uncovered in this page
      uncovered = oids;
    }
    for (const oid of uncovered) needLoose.add(oid);
    const lastKey = keys[keys.length - 1];
    if (nowMs() - start >= cfg.unpackMaxMs) {
      await insertHydrPendingOids(db, work.workId, "loose", Array.from(needLoose));
      updateProgress(work, { looseCursorKey: lastKey });
      log.debug("scan-loose:slice", { added: needLoose.size });
      return "more";
    }
    cursor = lastKey;
    // If we got fewer than LIMIT keys, we might be done. Try another list quickly only if time remains.
    if (keys.length < LIMIT) {
      // One more check to confirm end
      const next = await state.storage.list({ prefix: "obj:", limit: 1, startAfter: cursor });
      const hasMore = next && Array.from(next.keys()).length > 0;
      if (!hasMore) {
        done = true;
        break;
      }
    }
  }

  await insertHydrPendingOids(db, work.workId, "loose", Array.from(needLoose));
  if (done) {
    // Clear cursor to mark completion
    const prog: HydrationProgress = { ...(work.progress ?? {}) };
    prog.looseCursorKey = undefined;
    work.progress = prog;
    log.info("scan-loose:complete", { needLoose: needLoose.size });
    return "next";
  }
  // More to scan next slice
  updateProgress(work, { looseCursorKey: cursor });
  return "more";
}

// --- Stage 4: build one segment pack from pending objects ---

async function buildSegmentSlice(
  state: DurableObjectState,
  env: Env,
  prefix: string,
  work: HydrationWork
): Promise<"more" | "done" | "error"> {
  /**
   * Stage 4: build-segment
   *
   * Builds a thick (non-delta) hydration pack from a bounded batch of pending
   * objects. Reads from DO storage only; missing DO loose objects are treated as
   * fatal integrity errors (no R2-loose fallback here by design).
   */
  // Design note: There is intentionally NO R2-loose fallback here.
  // Invariants:
  // - Unpack writes loose objects into Durable Object storage (DO) and must succeed.
  // - We never delete DO loose objects (only R2 copies may be pruned during maintenance).
  // Therefore, if a needed loose object is missing from DO storage, the repository state
  // is considered corrupted and hydration should error out rather than masking it by
  // reading from R2. The error path below reflects this invariant.
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const db = getDb(state.storage);
  const log = makeHydrationLogger(env, prefix);
  const MAX_OBJS = HYDR_MAX_OBJS_PER_SEGMENT; // conservative per-segment cap to fit budgets

  // Fetch pending OIDs from SQLite, limited to a reasonable batch
  const MAX_FETCH = HYDR_MAX_OBJS_PER_SEGMENT * 2; // Fetch more than we'll use to account for filtering
  const needBases = await getHydrPendingOids(db, work.workId, "base", MAX_FETCH);
  const needLoose = await getHydrPendingOids(db, work.workId, "loose", MAX_FETCH);
  const candidatesRaw = Array.from(new Set<string>([...needBases, ...needLoose]));

  // Use per-work hydr_cover for coverage to avoid rebuilding large Sets.
  // Deduplicate ONLY against hydration packs (hydr_cover is populated from hydration window).
  let candidates: string[] = [];
  try {
    candidates = await filterUncoveredAgainstHydrCover(db, work.workId, candidatesRaw);
  } catch (e) {
    // Fallback on failure: proceed with all candidates (lowercased)
    candidates = candidatesRaw.map((x) => String(x).toLowerCase());
  }
  if (candidates.length === 0) {
    log.info("build:empty-pending", {});
    return "done";
  }

  // Segment caps: limit by object count and estimated bytes to avoid large packs.
  const SEG_MAX_BYTES = HYDR_SEG_MAX_BYTES; // 32 MiB
  const EST_RATIO = HYDR_EST_COMPRESSION_RATIO; // rough compression ratio estimate for planning
  const batch: string[] = [];

  // Load and parse objects first from DO storage, then from R2 packs (window membership)
  const objs: { type: GitObjectType; payload: Uint8Array; oid: string }[] = [];
  const missing: string[] = [];
  let estBytes = 0;
  for (const oid of candidates) {
    const z = (await state.storage.get(objKey(oid))) as Uint8Array | ArrayBuffer | undefined;
    if (!z) {
      missing.push(oid);
      continue;
    }
    try {
      const buf = z instanceof Uint8Array ? z : new Uint8Array(z);
      const parsed = await inflateAndParseHeader(buf);
      if (!parsed) continue;
      const est = Math.ceil(parsed.payload.byteLength * EST_RATIO) + 32;
      if (
        objs.length < MAX_OBJS &&
        (estBytes + est <= SEG_MAX_BYTES || objs.length === 0) // always allow at least one
      ) {
        objs.push({ type: parsed.type, payload: parsed.payload, oid });
        batch.push(oid);
        estBytes += est;
      }
    } catch (e) {
      log.debug("build:parse-failed", { oid, error: String(e) });
    }
    if (objs.length >= MAX_OBJS || estBytes >= SEG_MAX_BYTES) break;
  }

  // If some are missing from DO, treat as fatal integrity error
  if (missing.length > 0) {
    log.error("build:missing-loose", { count: missing.length, sample: missing.slice(0, 10) });
    work.error = {
      message: `missing ${missing.length} loose objects in DO`,
      fatal: true, // This is a fatal integrity error - admin must investigate
    };
    return "error";
  }

  if (objs.length === 0) {
    // Nothing loadable; skip and mark done to avoid loops
    log.warn("build:no-objects-loaded", {});
    return "done";
  }

  const packfile = await buildPackV2(objs.map(({ type, payload }) => ({ type, payload })));

  // Generate a new hydration pack key
  const seq = (work.progress?.segmentSeq ?? 0) + 1;
  const epoch = getEpochFromWorkId(work.workId);
  const packKey = r2PackKey(prefix, `pack-hydr-${epoch}-${seq}.pack`);

  // Store the pack to R2
  try {
    await env.REPO_BUCKET.put(packKey, packfile);
    log.info("build:stored-pack", { packKey, bytes: packfile.byteLength, objects: objs.length });
  } catch (e) {
    log.warn("build:store-pack-failed", { packKey, error: String(e) });
    // R2 write failure is transient
    work.error = { message: `Failed to store pack to R2: ${String(e)}` };
    return "error";
  }

  // Persist pack membership to SQLite for exact coverage
  const builtOids = objs.map((o) => o.oid);
  try {
    await insertPackOids(db, packKey, builtOids);
  } catch (e) {
    log.warn("build:store-oids-failed", { packKey, error: String(e) });
  }

  // Index and persist .idx (best-effort)
  let oids: string[] = [];
  try {
    oids = await indexPackOnly(packfile, env, packKey, state, prefix);
    // Update pack OIDs with actual indexed OIDs (may differ from input OIDs)
    if (oids.length > 0) {
      log.info("build:updated-packOids", { packKey, count: oids.length });
      // Store to SQLite (may overwrite earlier entries from builtOids)
      await insertPackOids(db, packKey, oids);
    }
  } catch (e) {
    log.warn("build:index-failed", { packKey, error: String(e) });
    // Best-effort: continue; the pack is still usable for fetch streaming from R2, but we prefer an idx
  }

  // Update hydr_cover for this work to include newly built OIDs for fast coverage checks
  try {
    const all = new Set<string>();
    for (const x of builtOids) all.add(String(x).toLowerCase());
    for (const x of oids) all.add(String(x).toLowerCase());
    const allArr = Array.from(all);
    await insertHydrCoverOids(db, work.workId, allArr);
  } catch (e) {
    log.debug("build:update-hydr_cover-failed", { error: String(e) });
  }

  try {
    const lastPackKey = (await store.get("lastPackKey")) || undefined;
    const list = (await store.get("packList")) || [];
    const out: string[] = [];
    let inserted = false;
    if (lastPackKey) {
      for (let i = 0; i < list.length; i++) {
        out.push(list[i]);
        if (!inserted && list[i] === lastPackKey) {
          out.push(packKey);
          inserted = true;
        }
      }
      if (!inserted) out.unshift(packKey);
    } else {
      out.unshift(packKey);
    }
    await store.put("packList", out);
  } catch (e) {
    log.warn("build:store-packlist-failed", { packKey, error: String(e) });
  }

  // Update progress and remove built OIDs from pending
  const builtSet = new Set(batch.map((x) => x.toLowerCase()));
  const basesToDelete = needBases.filter((x) => builtSet.has(x.toLowerCase()));
  const looseToDelete = needLoose.filter((x) => builtSet.has(x.toLowerCase()));

  // Delete processed OIDs from pending table
  await deleteHydrPendingOids(db, work.workId, "base", basesToDelete);
  await deleteHydrPendingOids(db, work.workId, "loose", looseToDelete);

  updateProgress(work, {
    segmentSeq: seq,
    producedBytes: (work.progress?.producedBytes || 0) + packfile.byteLength,
  });

  // Check if there are still pending objects
  const counts = await getHydrPendingCounts(db, work.workId);
  const remaining = counts.bases + counts.loose;
  log.info("build:segment-done", { packKey, built: objs.length, remaining });
  return remaining > 0 ? "more" : "done";
}
