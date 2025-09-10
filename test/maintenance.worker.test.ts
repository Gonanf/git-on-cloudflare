import { it, expect } from "vitest";
import { env, runDurableObjectAlarm } from "cloudflare:test";
import { asTypedStorage, RepoStateSchema, packOidsKey } from "@/do/repoState.ts";
import { runDOWithRetry } from "./util/test-helpers.ts";

function makeRepoId(suffix: string) {
  return `maint/${suffix}-${Math.random().toString(36).slice(2, 8)}`;
}

it("maintenance: trims packList and R2 packs to most recent KEEP_PACKS (default 3)", async () => {
  const repoId = makeRepoId("packs");
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id);

  // Determine DO prefix for R2 keys
  const { prefix } = await runDOWithRetry(
    getStub as any,
    async (_instance: any, state: DurableObjectState) => {
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
  await runDOWithRetry(getStub as any, async (_instance: any, state: DurableObjectState) => {
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
  await runDOWithRetry(getStub as any, async (_instance: any, state: DurableObjectState) => {
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

  // Expect only the first 3 packs (newest) to remain in R2
  const listed = await env.REPO_BUCKET.list({ prefix: `${prefix}/objects/pack/` });
  const r2Keys = new Set((listed.objects || []).map((o: any) => o.key));
  const keep = keys.slice(0, 3); // Keep the first 3 (newest)
  for (const k of keep) {
    expect(r2Keys.has(k), "key " + k + " should be kept").toBe(true);
    expect(r2Keys.has(k.replace(/\.pack$/, ".idx")), "key " + k + ".idx should be kept").toBe(true);
  }
  // Older ones should be gone
  for (const k of keys.slice(3)) {
    // Everything after the first 3
    expect(r2Keys.has(k), "key " + k + " should be removed").toBe(false);
    expect(r2Keys.has(k.replace(/\.pack$/, ".idx")), "key " + k + ".idx should be removed").toBe(
      false
    );
  }

  // Storage assertions
  await runDOWithRetry(getStub as any, async (_instance: any, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const packList = (await store.get("packList")) as string[] | undefined;
    expect(packList, "packList should be updated").toEqual(keep);
    const lastPackKey = (await store.get("lastPackKey")) as string | undefined;
    expect(lastPackKey, "lastPackKey should be updated").toBe(keep[0]);
    const p3 = await store.get(packOidsKey(keys[3]) as any);
    const p4 = await store.get(packOidsKey(keys[4]) as any);
    expect(p3, "packOidsKey(keys[3]) should be deleted").toBeUndefined();
    expect(p4, "packOidsKey(keys[4]) should be deleted").toBeUndefined();
    const p0 = await store.get(packOidsKey(keys[0]) as any);
    const p1 = await store.get(packOidsKey(keys[1]) as any);
    expect(p0, "packOidsKey(keys[0]) should exist").toEqual(["a"]);
    expect(p1, "packOidsKey(keys[1]) should exist").toEqual(["b"]);
  });
});
