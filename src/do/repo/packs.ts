/**
 * Pack management operations for Git repository
 *
 * This module handles pack metadata and membership tracking,
 * including pack lists, OID memberships, and batch operations.
 */

import type { RepoStateSchema } from "./repoState.ts";

import { asTypedStorage, packOidsKey } from "./repoState.ts";

/**
 * Get the latest pack information with its OIDs
 * @param ctx - Durable Object state context
 * @returns Latest pack key and OIDs, or null if no packs exist
 */
export async function getPackLatest(ctx: any): Promise<{ key: string; oids: string[] } | null> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const key = await store.get("lastPackKey");
  if (!key) return null;
  const oids = ((await store.get("lastPackOids")) || []).slice(0, 10000);
  return { key, oids };
}

/**
 * Get list of pack keys (newest first)
 * @param ctx - Durable Object state context
 * @returns Array of pack keys, limited to 20 most recent
 */
export async function getPacks(ctx: any): Promise<string[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const list = ((await store.get("packList")) || []).slice(0, 20);
  return list;
}

/**
 * Get OIDs contained in a specific pack
 * @param ctx - Durable Object state context
 * @param key - Pack key to get OIDs for
 * @returns Array of OIDs in the pack
 */
export async function getPackOids(ctx: any, key: string): Promise<string[]> {
  if (!key) return [];
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const oids = (await store.get(packOidsKey(key))) || [];
  return oids;
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
  ctx: any,
  keys: string[],
  logger?: { debug: (msg: string, data?: any) => void }
): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>();
  try {
    if (!Array.isArray(keys) || keys.length === 0) return out;

    // Clamp batch size to a reasonable number to avoid large payloads
    const BATCH = 128;
    for (let i = 0; i < keys.length; i += BATCH) {
      const part = keys.slice(i, i + BATCH);
      const storageKeys = part.map((k) => packOidsKey(k) as unknown as string);
      const fetched = (await ctx.storage.get(storageKeys)) as Map<string, unknown>;

      for (let j = 0; j < part.length; j++) {
        const packKey = part[j];
        const skey = storageKeys[j];
        const val = fetched.get(skey);
        if (Array.isArray(val)) {
          out.set(packKey, val as string[]);
        } else {
          out.set(packKey, []);
        }
      }
    }
  } catch (e) {
    try {
      logger?.debug("getPackOidsBatch:error", { error: String(e), count: keys?.length || 0 });
    } catch {}
  }
  return out;
}

/**
 * Update pack list with a new pack
 * Maintains newest-first ordering and updates lastPackKey/lastPackOids
 * @param ctx - Durable Object state context
 * @param packKey - New pack key to add
 * @param oids - OIDs in the new pack
 */
export async function addPackToList(ctx: any, packKey: string, oids: string[]): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);

  // Store pack OIDs
  await store.put(packOidsKey(packKey), oids);

  // Update pack list (newest first)
  const packList = (await store.get("packList")) || [];
  const newList = [packKey, ...packList.filter((k) => k !== packKey)];
  await store.put("packList", newList);

  // Update last pack references
  await store.put("lastPackKey", packKey);
  await store.put("lastPackOids", oids.slice(0, 10000)); // Cap at 10k for memory
}

/**
 * Remove pack from list and clean up its metadata
 * @param ctx - Durable Object state context
 * @param packKey - Pack key to remove
 */
export async function removePackFromList(ctx: any, packKey: string): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);

  // Remove from pack list
  const packList = (await store.get("packList")) || [];
  const newList = packList.filter((k) => k !== packKey);
  await store.put("packList", newList);

  // Clean up pack OIDs
  await ctx.storage.delete(packOidsKey(packKey));

  // Update lastPackKey if necessary
  const lastPackKey = await store.get("lastPackKey");
  if (lastPackKey === packKey) {
    if (newList.length > 0) {
      const newest = newList[0];
      await store.put("lastPackKey", newest);
      const oids = (await store.get(packOidsKey(newest))) || [];
      await store.put("lastPackOids", oids.slice(0, 10000));
    } else {
      await store.delete("lastPackKey");
      await store.delete("lastPackOids");
    }
  }
}

/**
 * Prune pack list to keep only the specified number of newest packs
 * @param ctx - Durable Object state context
 * @param keepCount - Number of packs to keep
 * @param logger - Logger instance
 * @returns Array of removed pack keys
 */
export async function prunePackList(
  ctx: any,
  keepCount: number,
  logger?: { warn: (msg: string, data?: any) => void }
): Promise<string[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const packList = (await store.get("packList")) || [];

  if (packList.length <= keepCount) {
    return [];
  }

  // packList is maintained newest-first, so keep the first N
  const keep = packList.slice(0, keepCount);
  const keepSet = new Set(keep);
  const removed = packList.filter((k) => !keepSet.has(k));

  // Update pack list
  await store.put("packList", keep);

  // Adjust lastPackKey/lastPackOids if needed
  const lastPackKey = await store.get("lastPackKey");
  if (!lastPackKey || !keepSet.has(lastPackKey)) {
    // Choose the newest kept pack as the latest reference
    const newest = keep[0];
    if (newest) {
      await store.put("lastPackKey", newest);
      const oids = ((await store.get("lastPackOids")) || []).slice(0, 10000);
      // Try to load oids for the newest from packOids:<key> if present
      const alt = await store.get(packOidsKey(newest));
      await store.put("lastPackOids", alt ?? oids);
    } else {
      // No packs remain
      await store.delete("lastPackKey");
      await store.delete("lastPackOids");
    }
  }

  // Delete packOids entries for removed packs
  for (const k of removed) {
    try {
      await ctx.storage.delete(packOidsKey(k));
    } catch (e) {
      logger?.warn("prunePackList:delete-packOids-failed", { key: k, error: String(e) });
    }
  }

  return removed;
}
