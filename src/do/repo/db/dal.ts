/**
 * Data Access Layer (DAL) for repository database operations
 */

import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";

import { eq, inArray, and } from "drizzle-orm";
import { packObjects, hydrCover, hydrPending } from "./schema.ts";

/**
 * Safe maximum bound parameters per SQLite query on the platform.
 * Keep some headroom below the documented 100 to account for potential
 * extra bound params Drizzle may include.
 */
export const SQLITE_PARAM_LIMIT = 100;
const SAFE_ROWS_2COL = 45; // 2 columns per row -> 90 params (< 100)
const SAFE_ROWS_3COL = 30; // 3 columns per row -> 90 params (< 100)

/**
 * Check if an OID exists in any pack
 * Uses the index on oid column for efficient lookup
 */
export async function oidExistsInPacks(db: DrizzleSqliteDODatabase, oid: string): Promise<boolean> {
  const result = await db
    .select({ packKey: packObjects.packKey })
    .from(packObjects)
    .where(eq(packObjects.oid, oid.toLowerCase()))
    .limit(1);
  return result.length > 0;
}

/**
 * Find all packs that contain a specific OID
 * Uses the index on oid column for efficient lookup
 */
export async function findPacksContainingOid(
  db: DrizzleSqliteDODatabase,
  oid: string
): Promise<string[]> {
  const rows = await db
    .select({ packKey: packObjects.packKey })
    .from(packObjects)
    .where(eq(packObjects.oid, oid.toLowerCase()));
  return rows.map((r) => r.packKey);
}

/**
 * Check multiple OIDs and return which ones exist in any pack
 * Efficient batch check using IN clause
 */
export async function filterOidsInPacks(
  db: DrizzleSqliteDODatabase,
  oids: string[]
): Promise<Set<string>> {
  if (oids.length === 0) return new Set();

  // Normalize to lowercase
  const normalizedOids = oids.map((o) => o.toLowerCase());

  // Query in batches to avoid too large IN clauses
  const found = new Set<string>();

  for (let i = 0; i < normalizedOids.length; i += SAFE_ROWS_2COL) {
    const batch = normalizedOids.slice(i, i + SAFE_ROWS_2COL);
    const rows = await db
      .select({ oid: packObjects.oid })
      .from(packObjects)
      .where(inArray(packObjects.oid, batch));

    for (const row of rows) {
      found.add(row.oid);
    }
  }

  return found;
}

/**
 * Get all OIDs for a specific pack
 * Uses the primary key index efficiently
 */
export async function getPackOids(db: DrizzleSqliteDODatabase, packKey: string): Promise<string[]> {
  const rows = await db
    .select({ oid: packObjects.oid })
    .from(packObjects)
    .where(eq(packObjects.packKey, packKey));
  return rows.map((r) => r.oid);
}

/**
 * Get a deterministic slice of OIDs for a specific pack with ordering.
 * Used by the unpacker to page through objects without storing the full list.
 */
export async function getPackOidsSlice(
  db: DrizzleSqliteDODatabase,
  packKey: string,
  offset: number,
  limit: number
): Promise<string[]> {
  if (limit <= 0) return [];
  const rows = await db
    .select({ oid: packObjects.oid })
    .from(packObjects)
    .where(eq(packObjects.packKey, packKey))
    .orderBy(packObjects.oid)
    .limit(limit)
    .offset(offset);
  return rows.map((r) => r.oid);
}

/**
 * Batch get OIDs for multiple packs
 * More efficient than multiple individual queries
 */
export async function getPackOidsBatch(
  db: DrizzleSqliteDODatabase,
  packKeys: string[]
): Promise<Map<string, string[]>> {
  if (packKeys.length === 0) return new Map();

  const result = new Map<string, string[]>();

  // Initialize all keys with empty arrays
  for (const key of packKeys) {
    result.set(key, []);
  }

  // Cloudflare platform limits allow up to 100 bound parameters per query.
  // Since this IN() uses 1 param per key, keep a conservative batch size.
  const BATCH = 80;
  for (let i = 0; i < packKeys.length; i += BATCH) {
    const batch = packKeys.slice(i, i + BATCH);
    const rows = await db
      .select({ packKey: packObjects.packKey, oid: packObjects.oid })
      .from(packObjects)
      .where(inArray(packObjects.packKey, batch));

    // Group by pack key
    for (const row of rows) {
      const existing = result.get(row.packKey) || [];
      existing.push(row.oid);
      result.set(row.packKey, existing);
    }
  }

  return result;
}

/**
 * Insert pack membership rows for a pack key with chunking to respect param limits.
 */
export async function insertPackOids(
  db: DrizzleSqliteDODatabase,
  packKey: string,
  oids: readonly string[]
): Promise<void> {
  if (!oids || oids.length === 0) return;
  for (let i = 0; i < oids.length; i += SAFE_ROWS_2COL) {
    const part = oids
      .slice(i, i + SAFE_ROWS_2COL)
      .map((oid) => ({ packKey, oid: String(oid).toLowerCase() }));
    if (part.length > 0) await db.insert(packObjects).values(part).onConflictDoNothing();
  }
}

/**
 * Delete pack membership data for a specific pack key.
 */
export async function deletePackObjects(
  db: DrizzleSqliteDODatabase,
  packKey: string
): Promise<void> {
  await db.delete(packObjects).where(eq(packObjects.packKey, packKey));
}

/**
 * Insert hydration coverage rows for a work id with chunking to respect param limits.
 */
export async function insertHydrCoverOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  oids: readonly string[]
): Promise<void> {
  if (!oids || oids.length === 0) return;
  for (let i = 0; i < oids.length; i += SAFE_ROWS_2COL) {
    const part = oids
      .slice(i, i + SAFE_ROWS_2COL)
      .map((oid) => ({ workId, oid: String(oid).toLowerCase() }));
    if (part.length > 0) await db.insert(hydrCover).values(part).onConflictDoNothing();
  }
}

/**
 * Insert hydration pending OIDs with chunking to respect param limits.
 */
export async function insertHydrPendingOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  kind: "base" | "loose",
  oids: readonly string[]
): Promise<void> {
  if (!oids || oids.length === 0) return;
  for (let i = 0; i < oids.length; i += SAFE_ROWS_3COL) {
    const part = oids
      .slice(i, i + SAFE_ROWS_3COL)
      .map((oid) => ({ workId, kind, oid: String(oid).toLowerCase() }));
    if (part.length > 0) await db.insert(hydrPending).values(part).onConflictDoNothing();
  }
}

/**
 * Get pending OIDs of a specific kind for a work id.
 */
export async function getHydrPendingOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  kind: "base" | "loose",
  limit?: number
): Promise<string[]> {
  const query = db
    .select({ oid: hydrPending.oid })
    .from(hydrPending)
    .where(and(eq(hydrPending.workId, workId), eq(hydrPending.kind, kind)))
    .orderBy(hydrPending.oid);

  if (limit && limit > 0) {
    query.limit(limit);
  }

  const rows = await query;
  return rows.map((r) => r.oid);
}

/**
 * Check whether hydr_cover has any rows for a given work id.
 * Useful as a cheap existence check to avoid repopulating coverage.
 */
export async function hasHydrCoverForWork(
  db: DrizzleSqliteDODatabase,
  workId: string
): Promise<boolean> {
  const count = await db.$count(hydrCover, eq(hydrCover.workId, workId));
  return count > 0;
}

/**
 * Return the subset of input oids that are NOT present in hydr_cover for this work.
 * Performs batched IN-clause lookups to respect parameter limits.
 */
export async function filterUncoveredAgainstHydrCover(
  db: DrizzleSqliteDODatabase,
  workId: string,
  candidates: string[]
): Promise<string[]> {
  if (!candidates.length) return [];
  const BATCH = 80; // keep well under SQLite param limit (~100)
  const out: string[] = [];
  for (let i = 0; i < candidates.length; i += BATCH) {
    const part = candidates.slice(i, i + BATCH).map((x) => String(x).toLowerCase());
    const rows = await db
      .select({ oid: hydrCover.oid })
      .from(hydrCover)
      .where(and(eq(hydrCover.workId, workId), inArray(hydrCover.oid, part)));
    const covered = new Set(rows.map((r) => r.oid));
    for (const oid of part) if (!covered.has(oid)) out.push(oid);
  }
  return out;
}

/**
 * Get counts of pending OIDs by kind for a work id.
 */
export async function getHydrPendingCounts(
  db: DrizzleSqliteDODatabase,
  workId: string
): Promise<{ bases: number; loose: number }> {
  const basesCount = await db.$count(
    hydrPending,
    and(eq(hydrPending.workId, workId), eq(hydrPending.kind, "base"))
  );
  const looseCount = await db.$count(
    hydrPending,
    and(eq(hydrPending.workId, workId), eq(hydrPending.kind, "loose"))
  );

  return {
    bases: basesCount,
    loose: looseCount,
  };
}

/**
 * Delete specific pending OIDs for a work id.
 */
export async function deleteHydrPendingOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  kind: "base" | "loose",
  oids: string[]
): Promise<void> {
  if (!oids || oids.length === 0) return;

  // Delete in batches to respect parameter limits
  for (let i = 0; i < oids.length; i += SAFE_ROWS_3COL) {
    const batch = oids.slice(i, i + SAFE_ROWS_3COL).map((o) => o.toLowerCase());
    await db
      .delete(hydrPending)
      .where(
        and(
          eq(hydrPending.workId, workId),
          eq(hydrPending.kind, kind),
          inArray(hydrPending.oid, batch)
        )
      );
  }
}

/**
 * Clear all pending OIDs for a work id.
 */
export async function clearHydrPending(db: DrizzleSqliteDODatabase, workId: string): Promise<void> {
  await db.delete(hydrPending).where(eq(hydrPending.workId, workId));
}

/**
 * Clear hydration coverage for a specific work id.
 */
export async function clearHydrCover(db: DrizzleSqliteDODatabase, workId: string): Promise<void> {
  await db.delete(hydrCover).where(eq(hydrCover.workId, workId));
}

/**
 * Get the total number of hydration coverage rows for a given work id.
 * Useful for tests that want to verify insert/dedup behavior without direct SQL.
 */
export async function getHydrCoverCount(
  db: DrizzleSqliteDODatabase,
  workId: string
): Promise<number> {
  const count = await db.$count(hydrCover, eq(hydrCover.workId, workId));
  return count;
}

/**
 * Get all (or a limited number of) OIDs covered for a work id.
 * Results are ordered deterministically by OID to support paging in callers.
 */
export async function getHydrCoverOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  limit?: number
): Promise<string[]> {
  const query = db
    .select({ oid: hydrCover.oid })
    .from(hydrCover)
    .where(eq(hydrCover.workId, workId))
    .orderBy(hydrCover.oid);

  if (limit && limit > 0) {
    query.limit(limit);
  }

  const rows = await query;
  return rows.map((r) => r.oid);
}
