import { it, expect } from "vitest";
import { env, runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { asTypedStorage, RepoStateSchema, packOidsKey } from "@/do/repoState.ts";

function makeRepoId(suffix: string) {
  return `maint/${suffix}-${Math.random().toString(36).slice(2, 8)}`;
}

it("maintenance: trims packList and R2 packs to most recent KEEP_PACKS (default 3)", async () => {
  const repoId = makeRepoId("packs");
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);

  // Determine DO prefix for R2 keys
  const { prefix } = await runInDurableObject(
    stub,
    async (_instance, state: DurableObjectState) => {
      return { prefix: `do/${state.id.toString()}` };
    }
  );

  // Create 5 synthetic pack+idx files in R2 under this DO prefix
  const keys: string[] = [];
  for (let i = 1; i <= 5; i++) {
    const key = `${prefix}/objects/pack/pack-${i}.pack`;
    keys.push(key);
    await env.REPO_BUCKET.put(key, new Uint8Array([i]));
    await env.REPO_BUCKET.put(key.replace(/\.pack$/, ".idx"), new Uint8Array([i, i]));
  }

  // Seed DO storage with packList and lastPackKey, and dummy packOids entries
  await runInDurableObject(stub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", keys);
    await store.put("lastPackKey", keys[keys.length - 1]);
    // Force maintenance due
    await store.put("lastMaintenanceMs", 0);
    // Add packOids entries for older packs
    await state.storage.put(packOidsKey(keys[0]), ["a"]);
    await state.storage.put(packOidsKey(keys[1]), ["b"]);
  });

  // Schedule the alarm slightly in the future so it's considered pending
  await runInDurableObject(stub, async (_instance, state: DurableObjectState) => {
    await state.storage.setAlarm(Date.now() + 1_000);
  });
  const ran = await runDurableObjectAlarm(stub);
  expect(ran).toBe(true);

  // Expect only the first 3 packs (newest) to remain in R2
  const listed = await env.REPO_BUCKET.list({ prefix: `${prefix}/objects/pack/` });
  const r2Keys = new Set((listed.objects || []).map((o: any) => o.key));
  const keep = keys.slice(0, 3); // Keep the first 3 (newest)
  for (const k of keep) {
    expect(r2Keys.has(k)).toBe(true);
    expect(r2Keys.has(k.replace(/\.pack$/, ".idx"))).toBe(true);
  }
  // Older ones should be gone
  for (const k of keys.slice(3)) {
    // Everything after the first 3
    expect(r2Keys.has(k)).toBe(false);
    expect(r2Keys.has(k.replace(/\.pack$/, ".idx"))).toBe(false);
  }

  // Storage assertions
  await runInDurableObject(stub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const packList = (await store.get("packList")) as string[] | undefined;
    expect(packList).toEqual(keep);
    const lastPackKey = (await store.get("lastPackKey")) as string | undefined;
    expect(lastPackKey).toBe(keep[0]); // Should be the newest (first in list)
    // Removed packOids entries should be deleted (keys[3] and keys[4] were removed)
    const p3 = await store.get(packOidsKey(keys[3]) as any);
    const p4 = await store.get(packOidsKey(keys[4]) as any);
    expect(p3).toBeUndefined();
    expect(p4).toBeUndefined();
    // Kept packOids entries should still exist (keys[0], keys[1], keys[2] were kept)
    const p0 = await store.get(packOidsKey(keys[0]) as any);
    const p1 = await store.get(packOidsKey(keys[1]) as any);
    expect(p0).toEqual(["a"]);
    expect(p1).toEqual(["b"]);
  });
});
