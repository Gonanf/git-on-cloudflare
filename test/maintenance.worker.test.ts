import { it, expect } from "vitest";
import { env, runDurableObjectAlarm } from "cloudflare:test";
import { asTypedStorage, RepoStateSchema } from "@/do/repo/repoState.ts";
import { getDb, insertPackOids, getPackOids } from "@/do/repo/db/index.ts";
import { runDOWithRetry } from "./util/test-helpers.ts";
import { calculateStableEpochs } from "@/do/repo/packs.ts";
import { getConfig } from "@/do/repo/repoConfig.ts";
import type { RepoDurableObject } from "@/do";

function makeRepoId(suffix: string) {
  return `maint/${suffix}-${Math.random().toString(36).slice(2, 8)}`;
}

it("maintenance: trims packs using epoch-aware keep-set (atomic epochs; preserve last)", async () => {
  const repoId = makeRepoId("packs");
  const id = env.REPO_DO.idFromName(repoId);
  const getStub: () => DurableObjectStub<RepoDurableObject> = () => env.REPO_DO.get(id);

  // Determine DO prefix for R2 keys
  const { prefix } = await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    return { prefix: `do/${state.id.toString()}` };
  });

  // Create 13 synthetic pack+idx files in R2 under this DO prefix
  // (REPO_KEEP_PACKS=10 in wrangler.jsonc, so 3 oldest will be deleted)
  const keys: string[] = [];
  for (let i = 1; i <= 13; i++) {
    const key = `${prefix}/objects/pack/pack-${i}.pack`;
    keys.push(key);
    await env.REPO_BUCKET.put(key, new Uint8Array([i]));
    await env.REPO_BUCKET.put(key.replace(/\.pack$/, ".idx"), new Uint8Array([i, i]));
  }
  // Create epoch-labeled hydration packs and interleave with normal packs
  // Two epochs of 3 packs each to test atomic selection
  const hydras: string[] = [];
  const epochA = "e100";
  const epochB = "e101";
  const hydraNames = [
    `pack-hydr-${epochA}-1`,
    `pack-hydr-${epochA}-2`,
    `pack-hydr-${epochA}-3`,
    `pack-hydr-${epochB}-1`,
    `pack-hydr-${epochB}-2`,
    `pack-hydr-${epochB}-3`,
  ];
  for (const name of hydraNames) {
    const k = `${prefix}/objects/pack/${name}.pack`;
    hydras.push(k);
    await env.REPO_BUCKET.put(k, new Uint8Array([9, 1]));
    await env.REPO_BUCKET.put(k.replace(/\.pack$/, ".idx"), new Uint8Array([9, 1, 1]));
  }

  // Simulate realistic evolution of packList over time:
  // - Each push unshifts the new pack to the front (dedup if already present)
  // - After each of the final `hydras.length` pushes, insert a hydration pack
  //   immediately after the then-current lastPackKey
  const interleaved: string[] = [];
  let last = "";
  const startHydraAfterPush = keys.length - hydras.length; // insert hydra after last K pushes
  let hydraIdx = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    // Simulate receive: place pack at head, removing any previous occurrence
    const filtered = interleaved.filter((x) => x !== k);
    interleaved.length = 0;
    interleaved.push(k, ...filtered);
    last = k;
    // Simulate hydration segment insertion for the last K pushes
    if (i >= startHydraAfterPush && hydraIdx < hydras.length) {
      const h = hydras[hydraIdx++];
      const idx = interleaved.indexOf(last);
      if (idx >= 0) interleaved.splice(idx + 1, 0, h);
      else interleaved.unshift(h);
    }
  }

  // Compute expected keep set using epoch-aware selection with configured KEEP_PACKS
  const cfg = getConfig(env);
  const { keepSet } = calculateStableEpochs(interleaved, cfg.keepPacks, last);
  const expectedKeep = interleaved.filter((k) => keepSet.has(k));

  // Prepare SQLite inserts: choose two kept normals and two removed normals
  const isHydra = (k: string) => k.includes("/objects/pack/pack-hydr-");
  const keptNormals = expectedKeep.filter((k) => !isHydra(k));
  const keptNormalsToInsert = keptNormals.filter((k) => k !== last).slice(0, 2);
  const removedNormals = interleaved.filter((k) => !keepSet.has(k) && !isHydra(k));
  const removedNormalsToInsert = removedNormals.slice(0, 2);

  // Seed DO storage with interleaved packList and lastPackKey, and insert sample pack_objects rows
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", interleaved);
    await store.put("lastPackKey", last);
    // Force maintenance due
    await store.put("lastMaintenanceMs", 0);
    // Add pack_objects entries for older packs in SQLite
    const db = getDb(state.storage);
    if (keptNormalsToInsert[0]) await insertPackOids(db, keptNormalsToInsert[0], ["ka"]);
    if (keptNormalsToInsert[1]) await insertPackOids(db, keptNormalsToInsert[1], ["kb"]);
    if (removedNormalsToInsert[0]) await insertPackOids(db, removedNormalsToInsert[0], ["ra"]);
    if (removedNormalsToInsert[1]) await insertPackOids(db, removedNormalsToInsert[1], ["rb"]);
  });

  // Schedule the alarm slightly in the future so it's considered pending
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    await state.storage.setAlarm(Date.now() + 1_000);
  });
  const ran = await (async () => {
    try {
      return await runDurableObjectAlarm(getStub());
    } catch (e) {
      const msg = String(e || "");
      if (msg.includes("invalidating this Durable Object"))
        return await runDurableObjectAlarm(getStub());
      throw e;
    }
  })();
  expect(ran, "alarm should run").toBe(true);

  // Expect prioritized keep set to remain in R2 (REPO_KEEP_PACKS=10)
  const listed = await env.REPO_BUCKET.list({ prefix: `${prefix}/objects/pack/` });
  const r2Keys = new Set((listed.objects || []).map((o: any) => o.key));
  for (const k of expectedKeep) {
    expect(r2Keys.has(k), "key " + k + " should be kept").toBe(true);
    expect(r2Keys.has(k.replace(/\.pack$/, ".idx")), "key " + k + ".idx should be kept").toBe(true);
  }
  // Any pack not in keepSet should be removed
  for (const k of interleaved) {
    if (keepSet.has(k)) continue;
    expect(r2Keys.has(k), "key " + k + " should be removed").toBe(false);
    expect(r2Keys.has(k.replace(/\.pack$/, ".idx")), "key " + k + ".idx should be removed").toBe(
      false
    );
  }

  // Storage assertions
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const packList = (await store.get("packList")) as string[] | undefined;
    const expectedList = interleaved.filter((k) => keepSet.has(k));
    expect(packList, "packList should be updated").toEqual(expectedList);
    const lastPackKey = (await store.get("lastPackKey")) as string | undefined;
    expect(lastPackKey, "lastPackKey should be preserved").toBe(last);
    // Check SQLite for pack_objects entries
    const db = getDb(state.storage);
    if (removedNormalsToInsert[0]) {
      const pr0 = await getPackOids(db, removedNormalsToInsert[0]);
      expect(
        pr0.length,
        `pack_objects for removed ${removedNormalsToInsert[0]} should be deleted`
      ).toBe(0);
    }
    if (removedNormalsToInsert[1]) {
      const pr1 = await getPackOids(db, removedNormalsToInsert[1]);
      expect(
        pr1.length,
        `pack_objects for removed ${removedNormalsToInsert[1]} should be deleted`
      ).toBe(0);
    }
    if (keptNormalsToInsert[0]) {
      const pk0 = await getPackOids(db, keptNormalsToInsert[0]);
      expect(pk0.length, `pack_objects for kept ${keptNormalsToInsert[0]} should exist`).toBe(1);
      expect(pk0[0], `pack_objects for kept ${keptNormalsToInsert[0]} should have oid 'ka'`).toBe(
        "ka"
      );
    }
    if (keptNormalsToInsert[1]) {
      const pk1 = await getPackOids(db, keptNormalsToInsert[1]);
      expect(pk1.length, `pack_objects for kept ${keptNormalsToInsert[1]} should exist`).toBe(1);
      expect(pk1[0], `pack_objects for kept ${keptNormalsToInsert[1]} should have oid 'kb'`).toBe(
        "kb"
      );
    }
  });
});

it("maintenance: enqueues hydration only when hydration packs were pruned", async () => {
  const repoId = makeRepoId("post-maint");
  const id = env.REPO_DO.idFromName(repoId);
  const getStub: () => DurableObjectStub<RepoDurableObject> = () => env.REPO_DO.get(id);

  // Determine DO prefix for R2 keys
  const { prefix } = await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    return { prefix: `do/${state.id.toString()}` };
  });

  // Create synthetic pack+idx files in R2 (ensure some will be pruned)
  const keys: string[] = [];
  for (let i = 1; i <= 12; i++) {
    const key = `${prefix}/objects/pack/pack-${i}.pack`;
    keys.push(key);
    await env.REPO_BUCKET.put(key, new Uint8Array([i]));
    await env.REPO_BUCKET.put(key.replace(/\.pack$/, ".idx"), new Uint8Array([i, i]));
  }
  // Add multiple hydration packs to satisfy prune safety AND cause some to be pruned.
  // With REPO_KEEP_PACKS=10 and lastPackKey kept, adding 10+ hydras guarantees at least
  // one hydration is pruned, which should enqueue a post-maint hydration task.
  const hydras: string[] = [];
  for (let i = 1; i <= 12; i++) {
    const k = `${prefix}/objects/pack/pack-hydr-${i}.pack`;
    hydras.push(k);
    await env.REPO_BUCKET.put(k, new Uint8Array([9, i]));
    await env.REPO_BUCKET.put(k.replace(/\.pack$/, ".idx"), new Uint8Array([9, i, i]));
  }

  // Simulate interleaving for second test as well
  const interleaved: string[] = [];
  let last = "";
  const startHydraAfterPush = Math.max(0, keys.length - hydras.length);
  let hydraIdx = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    const filtered = interleaved.filter((x) => x !== k);
    interleaved.length = 0;
    interleaved.push(k, ...filtered);
    last = k;
    if (i >= startHydraAfterPush && hydraIdx < hydras.length) {
      const h = hydras[hydraIdx++];
      const idx = interleaved.indexOf(last);
      if (idx >= 0) interleaved.splice(idx + 1, 0, h);
      else interleaved.unshift(h);
    }
  }

  // Seed DO storage: packList interleaved so pruning is allowed and representative of production
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", interleaved);
    await store.put("lastPackKey", last);
    // Force maintenance due
    await store.put("lastMaintenanceMs", 0);
  });

  // Schedule the alarm and run it (will perform maintenance and enqueue hydration)
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    await state.storage.setAlarm(Date.now() + 1_000);
  });
  const ran = await (async () => {
    try {
      return await runDurableObjectAlarm(getStub());
    } catch (e) {
      const msg = String(e || "");
      if (msg.includes("invalidating this Durable Object"))
        return await runDurableObjectAlarm(getStub());
      throw e;
    }
  })();
  expect(ran, "alarm should run").toBe(true);

  // Verify hydration was enqueued with reason post-maint
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const q = (await store.get("hydrationQueue")) as any[] | undefined;
    expect(Array.isArray(q), "hydrationQueue should be an array").toBe(true);
    const hasPostMaint = (q || []).some((t) => t && t.reason === "post-maint");
    expect(hasPostMaint, "should enqueue a post-maint hydration task").toBe(true);
  });
});
