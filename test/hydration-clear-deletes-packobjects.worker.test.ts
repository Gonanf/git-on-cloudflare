import { it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { getDb, insertPackOids, getPackOids } from "@/do/repo/db/index.ts";

it("clearHydration deletes pack_objects rows for hydration packs and updates metadata", async () => {
  const owner = "o";
  const repo = `r-hydr-clear-${Math.random().toString(36).slice(2, 8)}`;
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Seed a minimal repo so DO is initialized
  await runInDurableObject(stub, async (instance: RepoDurableObject) => {
    await instance.seedMinimalRepo();
  });

  // Create a fake hydration pack entry in packList and lastPackKey, and insert membership rows in SQLite
  const hydrPack = `do/${id.toString()}/objects/pack/pack-hydr-test.pack`;
  await runInDurableObject(
    stub,
    async (_instance: RepoDurableObject, state: DurableObjectState) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const list = (await store.get("packList")) || [];
      const newList = Array.isArray(list) ? [hydrPack, ...list] : [hydrPack];
      await store.put("packList", newList);
      await store.put("lastPackKey", hydrPack as any);
      await store.put("lastPackOids", ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"] as any);

      const db = getDb(state.storage);
      // Insert a couple of membership rows for the hydration pack via DAL
      await insertPackOids(db, hydrPack, [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      ]);

      const pre = await getPackOids(db, hydrPack);
      expect(pre.length).toBe(2);
    }
  );

  // Call clearHydration via RPC
  const res = await runInDurableObject(stub, async (instance: RepoDurableObject) => {
    return instance.clearHydration();
  });
  expect(res.removedPacks).toBeGreaterThanOrEqual(1);

  // Validate DB rows deleted and metadata updated
  await runInDurableObject(
    stub,
    async (_instance: RepoDurableObject, state: DurableObjectState) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const db = getDb(state.storage);

      const rows = await getPackOids(db, hydrPack);
      expect(rows.length).toBe(0);

      const packList = (await store.get("packList")) || [];
      expect(Array.isArray(packList) ? packList.includes(hydrPack) : false).toBe(false);

      const lastKey = await store.get("lastPackKey");
      const lastOids = await store.get("lastPackOids");
      // Because we set lastPackKey to hydr pack above, clearHydration should have removed it
      expect(lastKey).toBeUndefined();
      expect(lastOids).toBeUndefined();
    }
  );
});
