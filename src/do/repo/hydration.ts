import type {
  RepoStateSchema,
  HydrationTask,
  HydrationWork,
  HydrationStage,
  HydrationReason,
} from "./repoState.ts";
import type { GitObjectType } from "@/git/core/index.ts";
import type { Logger } from "@/common/logger.ts";

import { asTypedStorage, packOidsKey, objKey } from "./repoState.ts";
import { createLogger, BloomFilter } from "@/common/index.ts";
import { loadIdxParsed } from "@/git/pack/idxCache.ts";
import { inflateAndParseHeader } from "@/git/core/index.ts";
import { r2PackKey, packIndexKey, getDoIdFromPath } from "@/keys.ts";
import { getConfig } from "./repoConfig.ts";
import { ensureScheduled } from "./scheduler.ts";
import { indexPackOnly, readPackHeaderEx, buildPackV2 } from "@/git/pack/index.ts";

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
 * Builds a coverage set limited to existing hydration packs only (pack-hydr-*).
 * Optionally includes `lastPackOids` only when lastPackKey itself is a hydration pack.
 * Used by the segment builder to avoid re-creating objects already included in
 * previous hydration segments while still allowing thickening across non-hydration packs.
 */
async function buildHydrationCoverageSet(
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  cfg: ReturnType<typeof getHydrConfig>
): Promise<Set<string>> {
  const covered = new Set<string>();
  try {
    const lastPackKey = (await store.get("lastPackKey")) || null;
    const packListRaw = (await store.get("packList")) || [];
    const hydra: string[] = [];
    if (Array.isArray(packListRaw)) {
      for (const k of packListRaw) {
        const base = k.split("/").pop() || "";
        if (base.startsWith("pack-hydr-")) hydra.push(k);
      }
    }
    const lbase = (lastPackKey || "").split("/").pop() || "";
    if (lastPackKey && lbase.startsWith("pack-hydr-")) hydra.unshift(lastPackKey);
    const window = hydra.slice(0, cfg.windowMax);
    const storageKeys = window.map((k) => packOidsKey(k));
    const fetched = await store.get(storageKeys);
    for (let i = 0; i < window.length; i++) {
      const arr = fetched.get(storageKeys[i]);
      if (Array.isArray(arr)) for (const oid of arr) covered.add(String(oid).toLowerCase());
    }
    if (lastPackKey && lbase.startsWith("pack-hydr-")) {
      const last = (await store.get("lastPackOids")) || [];
      for (const x of last.slice(0, 10000)) covered.add(String(x).toLowerCase());
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
type HydrationPending = NonNullable<HydrationWork["pending"]>;
type HydrationProgress = NonNullable<HydrationWork["progress"]>;

function ensurePending(work: HydrationWork): HydrationPending {
  const pending: HydrationPending = {
    needBases: work.pending?.needBases ?? [],
    needLoose: work.pending?.needLoose ?? [],
  };
  work.pending = pending;
  return pending;
}

function setPendingBases(work: HydrationWork, bases: Iterable<string>) {
  const pending = ensurePending(work);
  pending.needBases = Array.from(bases);
}

function setPendingLoose(work: HydrationWork, loose: Iterable<string>) {
  const pending = ensurePending(work);
  pending.needLoose = Array.from(loose);
}

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
      if (baseIdx !== undefined) {
        baseOid = idx.oids[baseIdx];
        currentOff = baseOff;
      }
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
 * Build or restore a Bloom filter for coverage checks, reused across all stages and slices.
 * The filter is serialized onto work.snapshot.bloom to avoid rebuilding.
 * @param buildCoverage - Function to build the coverage set when bloom is missing
 */
async function getOrBuildBloom(
  work: HydrationWork,
  buildCoverage: () => Promise<Set<string>>
): Promise<BloomFilter> {
  // Restore existing bloom if present
  const snap = work.snapshot || (work.snapshot = { lastPackKey: null, packList: [], window: [] });
  if (snap.bloom) {
    try {
      return BloomFilter.fromJSON(snap.bloom);
    } catch {}
  }
  // Build once from coverage set
  const covered = await buildCoverage();
  const bloom = BloomFilter.create(Math.max(1, covered.size), 0.01);
  for (const oid of covered) bloom.add(oid);
  snap.bloom = bloom.toJSON();
  return bloom;
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
    const base = key.split("/").pop() || "";
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
    try {
      await store.delete(packOidsKey(key));
    } catch (e) {
      log.warn("clear:delete-pack-oids-failed", { key, error: String(e) });
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
  const covered = await buildHydrationCoverageSet(store, cfg);

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
  work.pending = work.pending || { needBases: [], needLoose: [] };
  work.progress = { ...(work.progress || {}), packIndex: 0, objCursor: 0 };

  // Precompute hydration-only coverage Bloom that scan stages will reuse
  try {
    await getOrBuildBloom(work, () => buildHydrationCoverageSet(store, cfg));
  } catch {}

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
  log.debug("hydration:scan-deltas:tick", {
    packIndex: work.progress?.packIndex || 0,
    objCursor: work.progress?.objCursor || 0,
    window: work.snapshot?.window?.length || 0,
  });
  const res = await scanDeltasSlice(state, env, work);
  if (res === "next") {
    setStage(work, "scan-loose", log);
    const nb = Array.isArray(work.pending?.needBases) ? work.pending!.needBases!.length : 0;
    log.info("hydration:scan-deltas:done", { needBases: nb });
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
  log.debug("hydration:scan-loose:tick", {
    cursor: work.progress?.looseCursorKey || null,
    needLoose: Array.isArray(work.pending?.needLoose) ? work.pending!.needLoose!.length : 0,
  });
  const res = await scanLooseSlice(state, env, work);
  if (res === "next") {
    setStage(work, "build-segment", log);
    const nl = Array.isArray(work.pending?.needLoose) ? work.pending!.needLoose!.length : 0;
    log.info("hydration:scan-loose:done", { needLoose: nl });
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
  log.info("hydration:build-segment:tick", {
    needBases: Array.isArray(work.pending?.needBases) ? work.pending!.needBases!.length : 0,
    needLoose: Array.isArray(work.pending?.needLoose) ? work.pending!.needLoose!.length : 0,
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
  const { store, log } = ctx;
  const queue = (await store.get("hydrationQueue")) || [];
  const newQ = Array.isArray(queue) ? queue.slice(1) : [];
  await store.put("hydrationQueue", newQ);
  await store.delete("hydrationWork");
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
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const start = nowMs();
  const log = makeHydrationLogger(env, work.snapshot?.lastPackKey || "");

  const window = work.snapshot?.window || [];
  if (!window || window.length === 0) return "next";

  // Use hydration-only coverage to match what build-segment uses.
  // Cache the bloom filter across slices to avoid rebuilding it.
  const bloom = await getOrBuildBloom(work, () => buildHydrationCoverageSet(store, cfg));

  const needBasesSet = new Set<string>(
    Array.isArray(work.pending?.needBases)
      ? work.pending!.needBases!.map((x) => x.toLowerCase())
      : []
  );

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
      setPendingBases(work, needBasesSet);
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
        setPendingBases(work, needBasesSet);
        return "error";
      }
      if (!header) continue;
      // Analyze the full delta chain to ensure complete thickening
      const chain = await analyzeDeltaChain(env, key, header, off, phys, (q: string) =>
        bloom.has(q)
      );
      for (const oid of chain) needBasesSet.add(oid);
      // stop if time budget exceeded inside loop
      if (nowMs() - start >= cfg.unpackMaxMs || subreq >= SOFT_SUBREQ_LIMIT) {
        objCur = j + 1;
        updateProgress(work, { packIndex: pIndex, objCursor: objCur });
        setPendingBases(work, needBasesSet);
        log.debug("scan-deltas:slice", {
          packIndex: pIndex,
          advanced: j - (work.progress?.objCursor || 0),
          needBases: ensurePending(work).needBases?.length || 0,
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
      setPendingBases(work, needBasesSet);
      log.debug("scan-deltas:continue", {
        packIndex: pIndex,
        objCursor: objCur,
        needBases: ensurePending(work).needBases?.length || 0,
      });
      return "more";
    }
  }

  // Completed all packs within time or finished list
  updateProgress(work, { packIndex: pIndex, objCursor: objCur });
  setPendingBases(work, needBasesSet);
  log.info("scan-deltas:complete", { needBases: ensurePending(work).needBases?.length || 0 });
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

  // Use hydration-only coverage to match what build-segment uses.
  // Cache the bloom filter across slices to avoid rebuilding it.
  const bloom = await getOrBuildBloom(work, () => buildHydrationCoverageSet(store, cfg));

  const needLoose = new Set<string>(
    Array.isArray(work.pending?.needLoose)
      ? work.pending!.needLoose!.map((x) => x.toLowerCase())
      : []
  );

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
      setPendingLoose(work, needLoose);
      return "error";
    }
    const keys: string[] = [];
    for (const k of it.keys()) keys.push(String(k));
    if (keys.length === 0) {
      done = true;
      break;
    }
    let lastKey: string | undefined;
    for (const fullKey of keys) {
      lastKey = fullKey;
      const oid = fullKey.slice(4).toLowerCase(); // strip 'obj:'
      if (!bloom.has(oid)) needLoose.add(oid);
      if (nowMs() - start >= cfg.unpackMaxMs) {
        setPendingLoose(work, needLoose);
        updateProgress(work, { looseCursorKey: lastKey });
        log.debug("scan-loose:slice", { added: ensurePending(work).needLoose?.length || 0 });
        return "more";
      }
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

  setPendingLoose(work, needLoose);
  if (done) {
    // Clear cursor to mark completion
    const prog: HydrationProgress = { ...(work.progress ?? {}) };
    prog.looseCursorKey = undefined;
    work.progress = prog;
    log.info("scan-loose:complete", { needLoose: ensurePending(work).needLoose?.length || 0 });
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
  const log = makeHydrationLogger(env, prefix);
  const MAX_OBJS = HYDR_MAX_OBJS_PER_SEGMENT; // conservative per-segment cap to fit budgets

  const needBases = Array.isArray(work.pending?.needBases) ? work.pending!.needBases! : [];
  const needLoose = Array.isArray(work.pending?.needLoose) ? work.pending!.needLoose! : [];
  const candidatesRaw = Array.from(new Set<string>([...needBases, ...needLoose]));

  // Rebuild a fresh coverage window from current state to avoid duplicates across jobs.
  // IMPORTANT: Deduplicate ONLY against existing hydration packs so we can thicken
  // across non-hydration packs when needed. This avoids skipping work when older
  // non-hydration packs are pruned or incomplete.
  // Note: We rebuild here instead of using bloom because new hydration packs may have
  // been created by previous segment builds in this same hydration run.
  const cfg = getHydrConfig(env);
  const covered = await buildHydrationCoverageSet(store, cfg);

  const candidates = candidatesRaw.filter((oid) => !covered.has(String(oid).toLowerCase()));
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
  const packKey = r2PackKey(prefix, `pack-hydr-${Date.now()}-${seq}.pack`);

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

  // Persist pack membership immediately using the objects we built.
  // This guarantees coverage for subsequent hydration runs even if indexing fails.
  const builtOids = objs.map((o) => o.oid);
  try {
    if (builtOids.length > 0) await state.storage.put(packOidsKey(packKey), builtOids);
  } catch (e) {
    log.warn("build:store-oids-failed", { packKey, error: String(e) });
  }

  // Index and persist .idx (best-effort)
  let oids: string[] = [];
  try {
    oids = await indexPackOnly(packfile, env, packKey, state, prefix);
    // Update pack OIDs with actual indexed OIDs (may differ from input OIDs)
    if (oids.length > 0) {
      await state.storage.put(packOidsKey(packKey), oids);
      log.info("build:updated-packOids", { packKey, count: oids.length });
    }
  } catch (e) {
    log.warn("build:index-failed", { packKey, error: String(e) });
    // Best-effort: continue; the pack is still usable for fetch streaming from R2, but we prefer an idx
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

  // Update progress and pending
  const builtSet = new Set(batch.map((x) => x.toLowerCase()));
  const nextBases = (needBases || []).filter((x) => !builtSet.has(x.toLowerCase()));
  const nextLoose = (needLoose || []).filter((x) => !builtSet.has(x.toLowerCase()));
  const pending = ensurePending(work);
  pending.needBases = nextBases;
  pending.needLoose = nextLoose;
  updateProgress(work, {
    segmentSeq: seq,
    producedBytes: (work.progress?.producedBytes || 0) + packfile.byteLength,
  });

  // If there are still pending objects, continue building more segments
  const remaining = (nextBases?.length || 0) + (nextLoose?.length || 0);
  log.info("build:segment-done", { packKey, built: objs.length, remaining });
  return remaining > 0 ? "more" : "done";
}
