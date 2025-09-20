/**
 * Debug utilities for repository inspection
 *
 * This module provides debug methods to inspect repository state,
 * check object presence, and verify pack membership.
 */

import type { RepoStateSchema, Head, UnpackWork } from "./repoState.ts";

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
    queueReasons?: ("post-unpack" | "admin")[];
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
  const hydrationWork = (await store.get("hydrationWork")) as any;
  const hydrationQueue = ((await store.get("hydrationQueue")) as any) || [];

  const looseSample: string[] = [];
  try {
    const it = await ctx.storage.list({ prefix: "obj:", limit: 10 });
    for (const k of it.keys()) looseSample.push(String(k).slice(4));
  } catch {}

  // Get hydration pending counts from SQLite if there's active work
  let hydrPendingCounts = { bases: 0, loose: 0 };
  let hydrBaseSample: string[] = [];
  let hydrLooseSample: string[] = [];
  if (hydrationWork?.workId) {
    try {
      const db = getDb(ctx.storage);
      hydrPendingCounts = await getHydrPendingCounts(db, hydrationWork.workId);
      hydrBaseSample = await getHydrPendingOids(db, hydrationWork.workId, "base", 10);
      hydrLooseSample = await getHydrPendingOids(db, hydrationWork.workId, "loose", 10);
    } catch {}
  }

  // Gather pack statistics
  const packStats: Array<{
    key: string;
    packSize?: number;
    hasIndex: boolean;
    indexSize?: number;
  }> = [];

  // Limit to first N packs for performance and fetch stats with limited parallelism
  const PACK_STATS_LIMIT = 20;
  const CONCURRENCY = 6;
  const packKeys = packList.slice(0, PACK_STATS_LIMIT);
  try {
    const results = await mapLimit(
      packKeys,
      CONCURRENCY,
      async (
        packKey
      ): Promise<{
        key: string;
        packSize?: number;
        hasIndex: boolean;
        indexSize?: number;
      }> => {
        const stat = { key: packKey, hasIndex: false } as {
          key: string;
          packSize?: number;
          hasIndex: boolean;
          indexSize?: number;
        };
        // Get pack file size from R2
        try {
          const packHead = await env.REPO_BUCKET.head(packKey);
          if (packHead) stat.packSize = packHead.size;
        } catch {}
        // Check if index exists and get its size
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
    for (const s of results) packStats.push(s);
  } catch {}

  // Count hydration-generated packs (pack-hydr-*) without sending the full list back to template logic
  let hydrationPackCount = 0;
  try {
    for (const k of packList) {
      const base = k.split("/").pop() || "";
      if (base.startsWith("pack-hydr-")) hydrationPackCount++;
    }
  } catch {}

  const prefix = doPrefix(ctx.id.toString());

  // SQLite DB size (bytes)
  let dbSizeBytes: number | undefined = undefined;
  try {
    dbSizeBytes = ctx.storage.sql?.databaseSize;
    if (typeof dbSizeBytes !== "number") dbSizeBytes = undefined;
  } catch {}

  // Sample R2 loose usage: list first page under loose prefix and sum sizes
  let looseR2SampleBytes: number | undefined = undefined;
  let looseR2SampleCount: number | undefined = undefined;
  let looseR2Truncated: boolean | undefined = undefined;
  try {
    const prefixLoose = r2LooseKey(prefix, "");
    const list = await env.REPO_BUCKET.list({ prefix: prefixLoose, limit: 250 });
    let sum = 0;
    for (const obj of list.objects || []) sum += obj.size || 0;
    looseR2SampleBytes = sum;
    looseR2SampleCount = (list.objects || []).length;
    looseR2Truncated = !!list.truncated;
  } catch {}

  // Sanitize unpackWork (no large arrays stored anymore)
  const sanitizedUnpackWork = unpackWork
    ? {
        packKey: unpackWork.packKey,
        totalCount: (unpackWork as any).totalCount || 0,
        processedCount: unpackWork.processedCount,
        startedAt: unpackWork.startedAt,
      }
    : null;

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
    dbSizeBytes,
    looseR2SampleBytes,
    looseR2SampleCount,
    looseR2Truncated,
    hydration: {
      running: !!hydrationWork,
      stage: hydrationWork?.stage,
      segmentSeq: hydrationWork?.progress?.segmentSeq,
      queued: Array.isArray(hydrationQueue) ? hydrationQueue.length : 0,
      needBasesCount: hydrPendingCounts.bases > 0 ? hydrPendingCounts.bases : undefined,
      needLooseCount: hydrPendingCounts.loose > 0 ? hydrPendingCounts.loose : undefined,
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
      queueReasons: Array.isArray(hydrationQueue) ? hydrationQueue.map((q: any) => q?.reason) : [],
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
