import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";

import { asTypedStorage, type RepoStateSchema, packOidsKey } from "@/do/repo/repoState.ts";
import { doPrefix, r2PackKey } from "@/keys.ts";
import { getDb, packObjects, getPackOids, normalizePackKeysInPlace } from "@/do/repo/db/index.ts";
import { runDOWithRetry, uniqueRepoId } from "./util/test-helpers.ts";
import { migrateKvToSql } from "@/do/repo/db/migrate.ts";

function oid(n: number): string {
  return n.toString(16).padStart(40, "0");
}

it("migrateKvToSql backfills from KV and stores basenames; deletes KV keys", async () => {
  const owner = "o";
  const repo = uniqueRepoId("kv-norm");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (_instance: any, state: DurableObjectState) => {
    // Prepare legacy KV data
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const prefix = doPrefix(state.id.toString());
    const packName = `pack-migrate-${Date.now()}.pack`;
    const fullKey = r2PackKey(prefix, packName);
    const oids = [oid(1), oid(2), oid(3)];

    await store.put("packList", [fullKey]);
    await store.put(packOidsKey(fullKey), oids);

    // Run migration explicitly (constructor may have run earlier with empty list)
    const db = getDb(state.storage);
    await migrateKvToSql(state, db);

    // KV key should be deleted
    const leftover = await store.get(packOidsKey(fullKey));
    expect(leftover).toBeUndefined();

    // Pack membership should be retrievable using either full key or basename
    const gotFull = await getPackOids(db, fullKey);
    const gotBase = await getPackOids(db, packName);
    expect(gotFull.sort()).toEqual(oids.map((x) => x.toLowerCase()).sort());
    expect(gotBase.sort()).toEqual(oids.map((x) => x.toLowerCase()).sort());
  });
});

it("normalizePackKeysInPlace rewrites existing full-path pack_keys to basenames", async () => {
  const owner = "o";
  const repo = uniqueRepoId("sql-norm");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (instance: any, state: DurableObjectState) => {
    // Ensure DB is initialized
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);

    const prefix = doPrefix(state.id.toString());
    const packName = `pack-preexist-${Date.now()}.pack`;
    const fullKey = r2PackKey(prefix, packName);
    const oids = [oid(10), oid(11)];

    // Simulate pre-normalized deployment by inserting full keys directly (bypass DAL)
    await db
      .insert(packObjects)
      .values(oids.map((o) => ({ packKey: fullKey, oid: o.toLowerCase() })) as any);

    // Normalize in place
    await normalizePackKeysInPlace(db);

    // Verify rows can be fetched by either key
    const gotFull = await getPackOids(db, fullKey);
    const gotBase = await getPackOids(db, packName);
    expect(gotFull.sort()).toEqual(oids.map((x) => x.toLowerCase()).sort());
    expect(gotBase.sort()).toEqual(oids.map((x) => x.toLowerCase()).sort());

    // Also verify no rows remain with '/' in pack_key
    const rows = await db
      .select({ pk: packObjects.packKey })
      .from(packObjects)
      .groupBy(packObjects.packKey);
    for (const r of rows) expect((r.pk as string).includes("/")).toBe(false);
  });
});
