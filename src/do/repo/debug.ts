/**
 * Debug utilities for repository inspection
 *
 * This module provides debug methods to inspect repository state,
 * check object presence, and verify pack membership.
 */

import type { RepoStateSchema, Head, UnpackWork } from "./repoState.ts";

import { asTypedStorage, objKey, packOidsKey } from "./repoState.ts";
import { r2LooseKey, doPrefix, packIndexKey } from "@/keys.ts";
import { isValidOid } from "@/common/index.ts";
import { readCommitFromStore } from "./storage.ts";

/**
 * Get comprehensive debug state of the repository
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @returns Debug state object with repository metadata and statistics
 */
export async function debugState(
  ctx: any,
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
  unpackWork: UnpackWork | null;
  unpackNext: string | null;
  looseSample: string[];
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

  // Gather pack statistics
  const packStats: Array<{
    key: string;
    packSize?: number;
    hasIndex: boolean;
    indexSize?: number;
  }> = [];

  for (const packKey of packList.slice(0, 20)) {
    // Limit to first 20 packs for performance
    try {
      const packStat: {
        key: string;
        packSize?: number;
        hasIndex: boolean;
        indexSize?: number;
      } = {
        key: packKey,
        hasIndex: false,
      };

      // Get pack file size from R2
      try {
        const packHead = await env.REPO_BUCKET.head(packKey);
        if (packHead) {
          packStat.packSize = packHead.size;
        }
      } catch {}

      // Check if index exists and get its size
      const indexKey = packIndexKey(packKey);
      try {
        const indexHead = await env.REPO_BUCKET.head(indexKey);
        if (indexHead) {
          packStat.hasIndex = true;
          packStat.indexSize = indexHead.size;
        }
      } catch {}

      packStats.push(packStat);
    } catch {}
  }

  const prefix = doPrefix(ctx.id.toString());

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
    unpackWork: unpackWork || null,
    unpackNext: unpackNext || null,
    looseSample,
    hydration: {
      running: !!hydrationWork,
      stage: hydrationWork?.stage,
      segmentSeq: hydrationWork?.progress?.segmentSeq,
      queued: Array.isArray(hydrationQueue) ? hydrationQueue.length : 0,
      needBasesCount: Array.isArray(hydrationWork?.pending?.needBases)
        ? hydrationWork.pending.needBases.length
        : undefined,
      needLooseCount: Array.isArray(hydrationWork?.pending?.needLoose)
        ? hydrationWork.pending.needLoose.length
        : undefined,
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
      needBasesSample: Array.isArray(hydrationWork?.pending?.needBases)
        ? hydrationWork.pending.needBases.slice(0, 10)
        : undefined,
      needLooseSample: Array.isArray(hydrationWork?.pending?.needLoose)
        ? hydrationWork.pending.needLoose.slice(0, 10)
        : undefined,
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
  ctx: any,
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
  const packList = (await store.get("packList")) ?? [];
  const membership: Record<string, { hasCommit: boolean; hasTree: boolean }> = {};

  for (const key of packList) {
    try {
      const oids = (await store.get(packOidsKey(key))) ?? [];
      const set = new Set(oids.map((x) => x.toLowerCase()));
      membership[key] = { hasCommit: set.has(q), hasTree: false };
    } catch {}
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

    for (const key of Object.keys(membership)) {
      try {
        const oids = (await store.get(packOidsKey(key))) ?? [];
        const set = new Set(oids.map((x) => x.toLowerCase()));
        membership[key].hasTree = !!tree && set.has(tree);
      } catch {}
    }
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
  ctx: any,
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
  const inPacks: string[] = [];
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const packList = (await store.get("packList")) || [];

  // Check each pack's OID list
  for (const packKey of packList) {
    try {
      const packOids = (await store.get(packOidsKey(packKey))) || [];
      if (packOids.includes(oid)) {
        inPacks.push(packKey);
      }
    } catch {}
  }

  return {
    oid,
    presence: {
      hasLoose,
      hasR2Loose,
    },
    inPacks,
  };
}
