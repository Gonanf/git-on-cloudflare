import { it, expect } from "vitest";
import { env, runDurableObjectAlarm } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { runDOWithRetry } from "./util/test-helpers.ts";

function makeRepoId(suffix: string) {
  return `alarm/${suffix}-${Math.random().toString(36).slice(2, 8)}`;
}

it("alarm: deletes empty repo storage and R2 objects when idle", async () => {
  const repoId = makeRepoId("empty");
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Discover prefix from the instance and prepare state as empty but with stale access
  const { prefix } = await runDOWithRetry(
    getStub,
    async (_instance: any, state: DurableObjectState) => {
      // Ensure looksEmpty condition: refs=[], head unborn, no lastPackKey
      await state.storage.put("refs", []);
      await state.storage.put("head", { target: "refs/heads/main", unborn: true });
      // Simulate idle long ago
      await state.storage.put("lastAccessMs", Date.now() - 60 * 60 * 1000);
      await state.storage.setAlarm(Date.now() + 60 * 60 * 1000);
      const pfx = `do/${state.id.toString()}`;
      return { prefix: pfx };
    }
  );

  // Place a couple of R2 objects under this DO's namespace to verify deletion
  await env.REPO_BUCKET.put(`${prefix}/objects/pack/tmp.pack`, new Uint8Array([1, 2, 3]));
  await env.REPO_BUCKET.put(`${prefix}/objects/pack/tmp.idx`, new Uint8Array([4, 5, 6]));
  await env.REPO_BUCKET.put(`${prefix}/note.txt`, "hello");

  const ran1 = await (async () => {
    try {
      return await runDurableObjectAlarm(getStub());
    } catch (e) {
      const msg = String(e || "");
      if (msg.includes("invalidating this Durable Object")) {
        return await runDurableObjectAlarm(getStub());
      }
      throw e;
    }
  })();
  expect(ran1).toBe(true);

  // Verify R2 namespace is empty
  const listed = await env.REPO_BUCKET.list({ prefix: `${prefix}/` });
  expect((listed.objects || []).length).toBe(0);

  // Verify known keys are removed from storage
  await runDOWithRetry(getStub, async (_instance: any, state: DurableObjectState) => {
    const refs = await state.storage.get("refs");
    const head = await state.storage.get("head");
    const last = await state.storage.get("lastAccessMs");
    expect(refs).toBeUndefined();
    expect(head).toBeUndefined();
    expect(last).toBeUndefined();
  });
});

it("alarm: does not delete a non-empty repo", async () => {
  const repoId = makeRepoId("nonempty");
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Seed the repo to create refs/head and objects
  await runDOWithRetry(getStub, async (instance: RepoDurableObject) => {
    await instance.seedMinimalRepo();
  });

  // Retrieve prefix
  const { prefix } = await runDOWithRetry(
    getStub,
    async (_instance: any, state: DurableObjectState) => {
      const pfx = `do/${state.id.toString()}`;
      return { prefix: pfx };
    }
  );

  // Add a marker object under this DO's prefix
  await env.REPO_BUCKET.put(`${prefix}/objects/pack/keep.pack`, new Uint8Array([9, 9, 9]));

  // Make it look idle
  await runDOWithRetry(getStub, async (_instance: any, state: DurableObjectState) => {
    await state.storage.put("lastAccessMs", Date.now() - 60 * 60 * 1000);
  });

  const ran2 = await (async () => {
    try {
      return await runDurableObjectAlarm(getStub());
    } catch (e) {
      const msg = String(e || "");
      if (msg.includes("invalidating this Durable Object")) {
        return await runDurableObjectAlarm(getStub());
      }
      throw e;
    }
  })();
  expect(ran2).toBe(true);

  // The repo is non-empty; R2 object should remain
  const listed = await env.REPO_BUCKET.list({ prefix: `${prefix}/objects/pack/` });
  const keys = (listed.objects || []).map((o: any) => o.key);
  expect(keys.some((k: string) => k.endsWith("keep.pack"))).toBe(true);
});
