import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";
import type { Logger } from "@/common/logger.ts";

import { packOidsKey, asTypedStorage, type RepoStateSchema } from "../repoState.ts";
import { insertPackOids, getPackObjectCount, normalizePackKeysInPlace } from "./dal.ts";

/**
 * Best-effort one-time migration: backfill pack memberships from KV (packOids:* keys)
 * into SQLite pack_objects, then delete migrated KV keys to avoid 2MB per-key risks.
 */
export async function migrateKvToSql(
  ctx: DurableObjectState,
  db: DrizzleSqliteDODatabase,
  logger?: Logger
) {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const list = (await store.get("packList")) || [];
  if (!Array.isArray(list) || list.length === 0) {
    logger?.debug("kv->sqlite:skip", { reason: "empty packList" });
    return;
  }

  // First normalize any existing rows in place so subsequent counts match by basename
  try {
    await normalizePackKeysInPlace(db, logger);
  } catch (e) {
    logger?.warn("kv->sqlite:normalize-initial:error", { error: String(e) });
  }

  // Fast path: if there are oids for the last (oldest) pack, we already migrated
  const oldestPackKey = list[list.length - 1];
  const count = await getPackObjectCount(db, oldestPackKey);
  if (count > 0) {
    logger?.debug("kv->sqlite:skip", { reason: "oldest packKey contains oids" });
    return;
  }

  // Process each pack; skip if SQL already has rows for this pack
  let migrated = 0;
  for (const packKey of list) {
    try {
      const pc = await getPackObjectCount(db, packKey);
      if (pc > 0) {
        logger?.debug("kv->sqlite:skip", { packKey, reason: "already migrated" });
        continue; // already migrated
      }

      const arr = (await store.get(packOidsKey(packKey))) || [];
      if (!Array.isArray(arr) || arr.length === 0) {
        logger?.debug("kv->sqlite:skip", { packKey, reason: "no oids" });
        continue;
      }

      // Parameter-limit-safe insert via centralized helper (stores basename)
      await insertPackOids(db, packKey, arr);

      // After successful insert, delete KV key to reduce storage
      await store.delete(packOidsKey(packKey));
      migrated++;
    } catch (e) {
      logger?.warn("kv->sqlite:migrate-pack-failed", { packKey, error: String(e) });
    }
  }
  if (migrated > 0) logger?.info("kv->sqlite:migrated", { packs: migrated });

  // Final normalization pass (idempotent)
  try {
    await normalizePackKeysInPlace(db, logger);
  } catch (e) {
    logger?.warn("kv->sqlite:normalize-final:error", { error: String(e) });
  }
}
