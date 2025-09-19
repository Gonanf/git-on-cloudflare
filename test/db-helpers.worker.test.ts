import type { RepoDurableObject } from "@/index";

import { it, expect } from "vitest";
import { env } from "cloudflare:test";

import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import {
  insertPackOids,
  insertHydrCoverOids,
  getPackOidsBatch,
  insertHydrPendingOids,
  getHydrPendingOids,
  getHydrPendingCounts,
  deleteHydrPendingOids,
  clearHydrPending,
  getDb,
  hydrPending,
  getPackOids,
  getHydrCoverCount,
} from "@/do/repo/db/index.ts";

function oidFor(i: number): string {
  // Generate unique, deterministic 40-hex strings by left-padding base16
  // Avoids collisions like 1 => "111.." and 0x11 => "11.." repeating
  const hex = i.toString(16);
  return hex.padStart(40, "0");
}

it("insertPackOids respects param limits and dedup", async () => {
  const owner = "o";
  const repo = uniqueRepoId("dbh-insert-pack");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (instance: any, state: DurableObjectState) => {
    // Ensure DO is initialized and migrations applied
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);

    const sizes = [0, 1, 45, 46, 100, 101, 300];
    for (const sz of sizes) {
      const pk = `test/${crypto.randomUUID()}/pack-${sz}.pack`;
      const oids = Array.from({ length: sz }, (_, i) => oidFor(i + 1));
      await insertPackOids(db, pk, oids);

      const got1 = await getPackOids(db, pk);
      expect(got1.length).toBe(oids.length);

      // Re-insert same set to confirm onConflictDoNothing prevents duplicates
      await insertPackOids(db, pk, oids);
      const got2 = await getPackOids(db, pk);
      expect(got2.length).toBe(oids.length);
    }
  });
});

it("insertHydrCoverOids respects param limits and dedup", async () => {
  const owner = "o";
  const repo = uniqueRepoId("dbh-insert-hydr");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (instance: any, state: DurableObjectState) => {
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);

    const sizes = [0, 1, 45, 46, 100, 101, 250];
    for (const sz of sizes) {
      const workId = `hydr-${crypto.randomUUID()}`;
      const oids = Array.from({ length: sz }, (_, i) => oidFor(i + 1000));
      await insertHydrCoverOids(db, workId, oids);

      const count1 = await getHydrCoverCount(db, workId);
      expect(count1).toBe(oids.length);

      // Re-insert same set to confirm onConflictDoNothing prevents duplicates
      await insertHydrCoverOids(db, workId, oids);
      const count2 = await getHydrCoverCount(db, workId);
      expect(count2).toBe(oids.length);
    }
  });
});

it("getPackOidsBatch handles >100 keys via chunked IN clause", async () => {
  const owner = "o";
  const repo = uniqueRepoId("dbh-batch-keys");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (instance: any, state: DurableObjectState) => {
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);

    const N = 130; // >100 to ensure chunking occurs
    const prefix = `test/${crypto.randomUUID()}`;
    const keys: string[] = [];
    const expected = new Map<string, string>();

    for (let i = 0; i < N; i++) {
      const key = `${prefix}/pack-${i}.pack`;
      const oid = oidFor(i + 5000);
      keys.push(key);
      expected.set(key, oid);
      await insertPackOids(db, key, [oid]);
    }

    const res = await getPackOidsBatch(db, keys);
    // Verify all keys are present and contain the expected single OID
    for (const key of keys) {
      const arr = res.get(key) || [];
      expect(Array.isArray(arr)).toBe(true);
      expect(arr.length).toBe(1);
      expect(arr[0]).toBe(expected.get(key));
    }
  });
});

it("hydr_pending.kind CHECK constraint rejects invalid values", async () => {
  const owner = "o";
  const repo = uniqueRepoId("dbh-pending-check");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (instance: any, state: DurableObjectState) => {
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);
    const workId = `hydr-${crypto.randomUUID()}`;

    // Valid insert should succeed
    await db.insert(hydrPending).values([{ workId, kind: "base", oid: oidFor(40000) }]);

    // Invalid kind should violate CHECK constraint and reject
    await expect(
      db.insert(hydrPending).values([{ workId, kind: "weird", oid: oidFor(40001) }])
    ).rejects.toThrow();

    // Verify only the valid row exists
    const counts = await getHydrPendingCounts(db, workId);
    expect(counts.bases).toBe(1);
    expect(counts.loose).toBe(0);
  });
});

it("hydr_pending operations: insert, get, count, delete", async () => {
  const owner = "o";
  const repo = uniqueRepoId("dbh-pending");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (instance: any, state: DurableObjectState) => {
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);
    const workId = `hydr-${crypto.randomUUID()}`;

    // Test insertHydrPendingOids with both kinds
    const baseOids = Array.from({ length: 100 }, (_, i) => oidFor(i + 10000));
    const looseOids = Array.from({ length: 75 }, (_, i) => oidFor(i + 20000));

    await insertHydrPendingOids(db, workId, "base", baseOids);
    await insertHydrPendingOids(db, workId, "loose", looseOids);

    // Test getHydrPendingCounts
    const counts = await getHydrPendingCounts(db, workId);
    expect(counts.bases).toBe(100);
    expect(counts.loose).toBe(75);

    // Test getHydrPendingOids with limit
    const firstBases = await getHydrPendingOids(db, workId, "base", 10);
    expect(firstBases.length).toBe(10);
    expect(firstBases[0]).toBe(oidFor(10000));

    const allLoose = await getHydrPendingOids(db, workId, "loose");
    expect(allLoose.length).toBe(75);

    // Test deleteHydrPendingOids
    const toDeleteBases = baseOids.slice(0, 50);
    await deleteHydrPendingOids(db, workId, "base", toDeleteBases);

    const countsAfterDelete = await getHydrPendingCounts(db, workId);
    expect(countsAfterDelete.bases).toBe(50);
    expect(countsAfterDelete.loose).toBe(75);

    // Test clearHydrPending
    await clearHydrPending(db, workId);
    const countsAfterClear = await getHydrPendingCounts(db, workId);
    expect(countsAfterClear.bases).toBe(0);
    expect(countsAfterClear.loose).toBe(0);

    // Verify deduplication (onConflictDoNothing)
    await insertHydrPendingOids(db, workId, "base", [oidFor(30000)]);
    await insertHydrPendingOids(db, workId, "base", [oidFor(30000)]); // duplicate
    const finalCount = await getHydrPendingCounts(db, workId);
    expect(finalCount.bases).toBe(1);
  });
});
