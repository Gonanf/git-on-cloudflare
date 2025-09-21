/**
 * Debug utilities for repository inspection
 *
 * This module provides debug methods to inspect repository state,
 * check object presence, and verify pack membership.
 */

import type {
  RepoStateSchema,
  Head,
  UnpackWork,
  HydrationWork,
  HydrationTask,
  HydrationStage,
} from "./repoState.ts";

import { asTypedStorage, objKey } from "./repoState.ts";
import {
  findPacksContainingOid,
  getDb,
  getHydrPendingCounts,
  getHydrPendingOids,
} from "./db/index.ts";
import { r2LooseKey, doPrefix, packIndexKey } from "@/keys.ts";
import { isValidOid } from "@/common/index.ts";
import { readCommitFromStore } from "./storage.ts";

/**
 * Small helper to run an async map with a concurrency limit.
 */
async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const ret: R[] = new Array(items.length);
  let next = 0;
  async function worker() {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      ret[i] = await fn(items[i], i);
    }
  }
  const n = Math.max(1, Math.min(limit | 0, items.length));
  await Promise.all(new Array(n).fill(0).map(() => worker()));
  return ret;
}

/**
 * Get comprehensive debug state of the repository
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @returns Debug state object with repository metadata and statistics
 */
export async function debugState(
  ctx: DurableObjectState,
  env: Env
): Promise<{
  meta: { doId: string; prefix: string };
  head?: Head;
  refsCount: number;
  refs: { name: string; oid: string }[];
  lastPackKey: string | null;
  lastPackOidsCount: number;
  packListCount: number;
  packList: string[];
  packStats?: Array<{
    key: string;
    packSize?: number;
    hasIndex: boolean;
    indexSize?: number;
  }>;
  unpackWork: {
    packKey: string;
    totalCount: number;
    processedCount: number;
    startedAt: number;
  } | null;
  unpackNext: string | null;
  looseSample: string[];
  hydrationPackCount: number;
  // Last maintenance timestamp (ms since epoch) to help compute next maintenance window in UI
  lastMaintenanceMs?: number;
  // SQLite database size in bytes (includes both SQL tables and KV for SQLite-backed DOs)
  dbSizeBytes?: number;
  // Quick sample of R2 loose mirror usage (first page only)
  looseR2SampleBytes?: number;
  looseR2SampleCount?: number;
  looseR2Truncated?: boolean;
  hydration?: {
    running: boolean;
    stage?: string;
    segmentSeq?: number;
    queued: number;
    needBasesCount?: number;
    needLooseCount?: number;
    packIndex?: number;
    objCursor?: number;
    workId?: string;
    startedAt?: number;
    producedBytes?: number;
    windowCount?: number;
    window?: string[];
    needBasesSample?: string[];
    needLooseSample?: string[];
    error?: {
      message?: string;
      fatal?: boolean;
      retryCount?: number;
      firstErrorAt?: number;
    };
    queueReasons?: ("post-unpack" | "post-maint" | "admin")[];
  };
}> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const refs = (await store.get("refs")) ?? [];
  const head = await store.get("head");
  const lastPackKey = await store.get("lastPackKey");
  const lastPackOids = (await store.get("lastPackOids")) ?? [];
  const packList = (await store.get("packList")) ?? [];
  const unpackWork = await store.get("unpackWork");
  const unpackNext = await store.get("unpackNext");
  const lastMaintenanceMs = await store.get("lastMaintenanceMs");
  const hydrationWork = (await store.get("hydrationWork")) as HydrationWork | undefined;
  const hydrationQueue = ((await store.get("hydrationQueue")) as HydrationTask[] | undefined) || [];

  // Helpers
  const looseSample = await listLooseSample(ctx);
  const {
    bases: basesPending,
    loose: loosePending,
    baseSample: hydrBaseSample,
    looseSample: hydrLooseSample,
  } = await getHydrationPending(ctx, hydrationWork?.workId);
  const packStats = await getPackStatsLimited(env, packList, 20, 6);
  const hydrationPackCount = countHydrationPacks(packList);
  const prefix = doPrefix(ctx.id.toString());
  const dbSizeBytes = getDatabaseSize(ctx);
  const {
    bytes: looseR2SampleBytes,
    count: looseR2SampleCount,
    truncated: looseR2Truncated,
  } = await sampleR2Loose(prefix, env);
  const sanitizedUnpackWork = sanitizeUnpackWork(unpackWork as UnpackWork | null);

  return {
    meta: { doId: ctx.id.toString(), prefix },
    head,
    refsCount: refs.length,
    refs: refs.slice(0, 20),
    lastPackKey: lastPackKey || null,
    lastPackOidsCount: lastPackOids.length,
    packListCount: packList.length,
    packList,
    packStats: packStats.length > 0 ? packStats : undefined,
    unpackWork: sanitizedUnpackWork,
    unpackNext: unpackNext || null,
    looseSample,
    hydrationPackCount,
    lastMaintenanceMs,
    dbSizeBytes,
    looseR2SampleBytes,
    looseR2SampleCount,
    looseR2Truncated,
    hydration: {
      running: !!hydrationWork,
      stage: hydrationWork?.stage,
      segmentSeq: hydrationWork?.progress?.segmentSeq,
      queued: Array.isArray(hydrationQueue) ? hydrationQueue.length : 0,
      needBasesCount: basesPending > 0 ? basesPending : undefined,
      needLooseCount: loosePending > 0 ? loosePending : undefined,
      packIndex: hydrationWork?.progress?.packIndex,
      objCursor: hydrationWork?.progress?.objCursor,
      workId: hydrationWork?.workId,
      startedAt: hydrationWork?.startedAt,
      producedBytes: hydrationWork?.progress?.producedBytes,
      windowCount: Array.isArray(hydrationWork?.snapshot?.window)
        ? hydrationWork.snapshot.window.length
        : undefined,
      window: Array.isArray(hydrationWork?.snapshot?.window)
        ? hydrationWork.snapshot.window.slice(0, 6)
        : undefined,
      needBasesSample: hydrBaseSample.length > 0 ? hydrBaseSample : undefined,
      needLooseSample: hydrLooseSample.length > 0 ? hydrLooseSample : undefined,
      error: hydrationWork?.error
        ? {
            message: hydrationWork.error.message,
            fatal: hydrationWork.error.fatal,
            retryCount: hydrationWork.error.retryCount,
            firstErrorAt: hydrationWork.error.firstErrorAt,
          }
        : undefined,
      queueReasons: Array.isArray(hydrationQueue) ? hydrationQueue.map((q) => q.reason) : [],
    },
  };
}

/**
 * Debug check for a commit and its tree
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param commit - Commit OID to check
 * @returns Detailed commit information and presence in storage
 */
export async function debugCheckCommit(
  ctx: DurableObjectState,
  env: Env,
  commit: string
): Promise<{
  commit: { oid: string; parents: string[]; tree?: string };
  presence: { hasLooseCommit: boolean; hasLooseTree: boolean; hasR2LooseTree: boolean };
  membership: Record<string, { hasCommit: boolean; hasTree: boolean }>;
}> {
  const q = (commit || "").toLowerCase();
  if (!isValidOid(q)) {
    throw new Error("Invalid commit");
  }

  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);
  const packList = (await store.get("packList")) ?? [];
  const membership: Record<string, { hasCommit: boolean; hasTree: boolean }> = {};

  // Check which packs contain the commit - query by OID directly
  try {
    const commitPacks = await findPacksContainingOid(db, q);
    const commitPackSet = new Set(commitPacks);
    for (const key of packList) {
      membership[key] = { hasCommit: commitPackSet.has(key), hasTree: false };
    }
  } catch {}
  // Initialize all packs as not having the commit if query fails
  if (Object.keys(membership).length === 0) {
    for (const key of packList) {
      membership[key] = { hasCommit: false, hasTree: false };
    }
  }

  const prefix = doPrefix(ctx.id.toString());
  let tree: string | undefined = undefined;
  let parents: string[] = [];

  try {
    const info = await readCommitFromStore(ctx, env, prefix, q);
    if (info) {
      tree = info.tree.toLowerCase();
      parents = info.parents;
    }
  } catch {}

  const hasLooseCommit = !!(await ctx.storage.get(objKey(q)));
  let hasLooseTree = false;
  let hasR2LooseTree = false;

  if (tree) {
    hasLooseTree = !!(await ctx.storage.get(objKey(tree)));
    try {
      const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, tree));
      hasR2LooseTree = !!head;
    } catch {}

    // Check which packs contain the tree - query by OID directly
    try {
      const treePacks = await findPacksContainingOid(db, tree);
      const treePackSet = new Set(treePacks);
      for (const key of Object.keys(membership)) {
        membership[key].hasTree = treePackSet.has(key);
      }
    } catch {}
  }

  return {
    commit: { oid: q, parents, tree },
    presence: { hasLooseCommit, hasLooseTree, hasR2LooseTree },
    membership,
  };
}

/**
 * Debug: Check if an OID exists in various storage locations
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param oid - The object ID to check
 * @returns Object presence information
 */
export async function debugCheckOid(
  ctx: DurableObjectState,
  env: Env,
  oid: string
): Promise<{
  oid: string;
  presence: {
    hasLoose: boolean;
    hasR2Loose: boolean;
  };
  inPacks: string[];
}> {
  if (!isValidOid(oid)) {
    throw new Error(`Invalid OID: ${oid}`);
  }

  const prefix = doPrefix(ctx.id.toString());

  // Check DO loose storage
  const hasLoose = !!(await ctx.storage.get(objKey(oid)));

  // Check R2 loose storage
  let hasR2Loose = false;
  try {
    const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, oid));
    hasR2Loose = !!head;
  } catch {}

  // Check which packs contain this OID
  let inPacks: string[] = [];

  // Check which packs contain this OID - query by OID directly
  const db = getDb(ctx.storage);
  try {
    inPacks = await findPacksContainingOid(db, oid);
  } catch {}

  return {
    oid,
    presence: {
      hasLoose,
      hasR2Loose,
    },
    inPacks,
  };
}

/**
 * Helpers
 */

async function listLooseSample(ctx: DurableObjectState): Promise<string[]> {
  const out: string[] = [];
  try {
    const it = await ctx.storage.list({ prefix: "obj:", limit: 10 });
    for (const k of it.keys()) out.push(String(k).slice(4));
  } catch {}
  return out;
}

async function getHydrationPending(
  ctx: DurableObjectState,
  workId?: string
): Promise<{
  bases: number;
  loose: number;
  baseSample: string[];
  looseSample: string[];
}> {
  if (!workId) return { bases: 0, loose: 0, baseSample: [], looseSample: [] };
  try {
    const db = getDb(ctx.storage);
    const counts = await getHydrPendingCounts(db, workId);
    const baseSample = await getHydrPendingOids(db, workId, "base", 10);
    const looseSample = await getHydrPendingOids(db, workId, "loose", 10);
    return { bases: counts.bases, loose: counts.loose, baseSample, looseSample };
  } catch {
    return { bases: 0, loose: 0, baseSample: [], looseSample: [] };
  }
}

async function getPackStatsLimited(
  env: Env,
  packList: string[],
  limit: number,
  concurrency: number
): Promise<
  Array<{
    key: string;
    packSize?: number;
    hasIndex: boolean;
    indexSize?: number;
  }>
> {
  const keys = packList.slice(0, Math.max(0, limit | 0));
  if (keys.length === 0) return [];
  try {
    const results = await mapLimit(
      keys,
      Math.max(1, concurrency | 0),
      async (
        packKey
      ): Promise<{
        key: string;
        packSize?: number;
        hasIndex: boolean;
        indexSize?: number;
      }> => {
        const stat: { key: string; packSize?: number; hasIndex: boolean; indexSize?: number } = {
          key: packKey,
          hasIndex: false,
        };
        try {
          const packHead = await env.REPO_BUCKET.head(packKey);
          if (packHead) stat.packSize = packHead.size;
        } catch {}
        try {
          const indexHead = await env.REPO_BUCKET.head(packIndexKey(packKey));
          if (indexHead) {
            stat.hasIndex = true;
            stat.indexSize = indexHead.size;
          }
        } catch {}
        return stat;
      }
    );
    return results;
  } catch {
    return [];
  }
}

function countHydrationPacks(packList: string[]): number {
  let n = 0;
  try {
    for (const k of packList) {
      const base = k.split("/").pop() || "";
      if (base.startsWith("pack-hydr-")) n++;
    }
  } catch {}
  return n;
}

function getDatabaseSize(ctx: DurableObjectState): number | undefined {
  try {
    const size = ctx.storage.sql.databaseSize;
    return typeof size === "number" ? size : undefined;
  } catch {
    return undefined;
  }
}

async function sampleR2Loose(
  prefix: string,
  env: Env
): Promise<{ bytes?: number; count?: number; truncated?: boolean }> {
  try {
    const prefixLoose = r2LooseKey(prefix, "");
    const list = await env.REPO_BUCKET.list({ prefix: prefixLoose, limit: 250 });
    let sum = 0;
    for (const obj of list.objects || []) sum += obj.size || 0;
    return {
      bytes: sum,
      count: (list.objects || []).length,
      truncated: !!list.truncated,
    };
  } catch {
    return {};
  }
}

function sanitizeUnpackWork(unpackWork: UnpackWork | null): {
  packKey: string;
  totalCount: number;
  processedCount: number;
  startedAt: number;
} | null {
  if (!unpackWork) return null;
  return {
    packKey: unpackWork.packKey,
    totalCount: unpackWork.totalCount || 0,
    processedCount: unpackWork.processedCount,
    startedAt: unpackWork.startedAt,
  };
}
