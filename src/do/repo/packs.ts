/**
 * Pack management operations for Git repository
 *
 * This module handles pack metadata and membership tracking,
 * including pack lists, OID memberships, and batch operations.
 */

import type { RepoStateSchema } from "./repoState.ts";
import type { Logger } from "@/common/logger.ts";

import { asTypedStorage } from "./repoState.ts";
import { getConfig } from "./repoConfig.ts";
import {
  getDb,
  getPackOids as getPackOidsHelper,
  getPackOidsBatch as getPackOidsBatchHelper,
  deletePackObjects,
} from "./db/index.ts";

/**
 * Get the latest pack information with its OIDs
 * @param ctx - Durable Object state context
 * @returns Latest pack key and OIDs, or null if no packs exist
 */
export async function getPackLatest(
  ctx: DurableObjectState
): Promise<{ key: string; oids: string[] } | null> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const key = await store.get("lastPackKey");
  if (!key) return null;
  const oids = ((await store.get("lastPackOids")) || []).slice(0, 10000);
  return { key, oids };
}

/**
 * Get list of pack keys (newest first)
 * @param ctx - Durable Object state context
 * @param env - Worker environment for configuration
 * @returns Array of pack keys, limited to configured packListMax
 */
export async function getPacks(ctx: DurableObjectState, env: Env): Promise<string[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const cfg = getConfig(env);
  const list = ((await store.get("packList")) || []).slice(0, cfg.packListMax);
  return list;
}

/**
 * Get OIDs contained in a specific pack
 * @param ctx - Durable Object state context
 * @param key - Pack key to get OIDs for
 * @returns Array of OIDs in the pack
 */
export async function getPackOids(ctx: DurableObjectState, key: string): Promise<string[]> {
  if (!key) return [];
  const db = getDb(ctx.storage);
  return await getPackOidsHelper(db, key);
}

/**
 * Batch API: Retrieve OID membership arrays for multiple pack keys in one call
 * Uses DurableObjectStorage.get([...]) to reduce roundtrips and total subrequests
 * @param ctx - Durable Object state context
 * @param keys - Pack keys to fetch membership for
 * @param logger - Logger instance
 * @returns Map of pack key -> string[] of OIDs (empty array if missing)
 */
export async function getPackOidsBatch(
  ctx: DurableObjectState,
  keys: string[],
  logger?: Logger
): Promise<Map<string, string[]>> {
  try {
    if (!Array.isArray(keys) || keys.length === 0) return new Map();
    const db = getDb(ctx.storage);
    return await getPackOidsBatchHelper(db, keys);
  } catch (e) {
    logger?.debug("getPackOidsBatch:error", { error: String(e), count: keys?.length || 0 });
    return new Map();
  }
}

/**
 * Remove pack from list and clean up its metadata
 * @param ctx - Durable Object state context
 * @param packKey - Pack key to remove
 */
export async function removePackFromList(ctx: DurableObjectState, packKey: string): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);

  // Remove from pack list
  const packList = (await store.get("packList")) || [];
  const newList = packList.filter((k) => k !== packKey);
  await store.put("packList", newList);

  // Clean up pack OIDs
  await deletePackObjects(db, packKey);

  // Update lastPackKey if necessary
  const lastPackKey = await store.get("lastPackKey");
  if (lastPackKey === packKey) {
    if (newList.length > 0) {
      const newest = newList[0];
      await store.put("lastPackKey", newest);
      // Load OIDs from SQLite for the newest pack
      const oids = await getPackOidsHelper(db, newest);
      await store.put("lastPackOids", oids.slice(0, 10000));
    } else {
      await store.delete("lastPackKey");
      await store.delete("lastPackOids");
    }
  }
}
