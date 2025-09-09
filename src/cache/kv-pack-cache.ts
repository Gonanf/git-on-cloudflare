/**
 * KV-based caching for pack metadata to reduce DO queries.
 * Pack metadata (OID->pack mappings) is immutable once created,
 * making it ideal for KV caching with long TTLs.
 */

import { kvPackListKey, kvOidToPackKey, kvLastPushKey, kvUnpackStatusKey } from "@/keys.ts";

// TTL constants (in seconds)
// Adjust these to tune consistency vs. freshness trade-offs.
const TTL = {
  // OID -> pack mapping may become stale if packs are pruned.
  // 30 days balances longevity with safety.
  OID_PACK_SECONDS: 60 * 60 * 24 * 30, // 30 days

  // Pack list changes on pushes; keep short.
  PACK_LIST_SECONDS: 60 * 5, // 5 minutes

  // After a push, skip KV for a short window to avoid staleness during churn.
  RECENT_PUSH_SECONDS: 60 * 5, // 5 minutes

  // Unpacking should not last long; use a generous upper bound.
  UNPACK_STATUS_SECONDS: 60 * 60, // 1 hour
} as const;

const RECENT_PUSH_MS = TTL.RECENT_PUSH_SECONDS * 1000;

/**
 * Get the pack key containing a specific OID.
 * Returns null if not cached or on error.
 * Note: May return stale data during unpacking due to KV eventual consistency.
 */
export async function getPackForOid(
  kv: KVNamespace,
  repoId: string,
  oid: string
): Promise<string | null> {
  try {
    return await kv.get(kvOidToPackKey(repoId, oid));
  } catch {
    return null;
  }
}

/**
 * Get the list of pack keys for a repository.
 * Returns null if not cached or on error.
 */
export async function getPackList(kv: KVNamespace, repoId: string): Promise<string[] | null> {
  try {
    const json = await kv.get(kvPackListKey(repoId));
    return json ? JSON.parse(json) : null;
  } catch {
    return null;
  }
}

/**
 * Save OID to pack mapping.
 * Long TTL since this mapping is immutable.
 */
export async function saveOidToPackMapping(
  kv: KVNamespace,
  repoId: string,
  oid: string,
  packKey: string
): Promise<void> {
  try {
    await kv.put(kvOidToPackKey(repoId, oid), packKey, { expirationTtl: TTL.OID_PACK_SECONDS });
  } catch {
    // Best effort
  }
}

/**
 * Save pack list for a repository.
 * Short TTL since this changes with pushes.
 */
export async function savePackList(
  kv: KVNamespace,
  repoId: string,
  packKeys: string[]
): Promise<void> {
  try {
    await kv.put(kvPackListKey(repoId), JSON.stringify(packKeys), {
      expirationTtl: TTL.PACK_LIST_SECONDS,
    });
  } catch {
    // Best effort
  }
}

/**
 * Check if we should skip KV cache due to recent push or active unpacking.
 * Returns true if cache should be bypassed.
 */
export async function shouldSkipKVCache(kv: KVNamespace, repoId: string): Promise<boolean> {
  try {
    // Check if actively unpacking
    const unpackStatus = await kv.get(kvUnpackStatusKey(repoId));
    if (unpackStatus === "active") return true;

    // Check if recent push (within 5 minutes)
    const lastPush = await kv.get(kvLastPushKey(repoId));
    if (lastPush) {
      const pushTime = parseInt(lastPush, 10);
      if (!isNaN(pushTime) && Date.now() - pushTime < RECENT_PUSH_MS) {
        return true;
      }
    }

    return false;
  } catch {
    // On error, skip cache to be safe
    return true;
  }
}

/**
 * Mark that a push has occurred.
 * Sets a timestamp that expires after 5 minutes.
 */
export async function markPush(kv: KVNamespace, repoId: string): Promise<void> {
  try {
    await kv.put(kvLastPushKey(repoId), Date.now().toString(), {
      expirationTtl: TTL.RECENT_PUSH_SECONDS,
    });
  } catch {
    // Best effort
  }
}

/**
 * Mark unpacking as active or clear the status.
 */
export async function setUnpackStatus(
  kv: KVNamespace,
  repoId: string,
  active: boolean
): Promise<void> {
  try {
    if (active) {
      await kv.put(kvUnpackStatusKey(repoId), "active", {
        expirationTtl: TTL.UNPACK_STATUS_SECONDS,
      });
    } else {
      await kv.delete(kvUnpackStatusKey(repoId));
    }
  } catch {
    // Best effort
  }
}
