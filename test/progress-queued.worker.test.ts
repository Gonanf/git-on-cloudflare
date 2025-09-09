import { it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { asTypedStorage, RepoStateSchema } from "@/do/repoState.ts";
import { getUnpackProgress } from "@/common";

it("/unpack-progress reports queued-only state and getUnpackProgress returns it", async () => {
  const repoId = `qonly/${Math.random().toString(36).slice(2, 8)}`;
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);

  // Seed unpackNext only (no unpackWork)
  await runInDurableObject(stub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("unpackNext", `${repoId}/objects/pack/pack-next.pack` as any);
    await store.put("lastAccessMs", Date.now() as any);
  });

  // Call DO endpoint directly
  const res = await stub.fetch("https://do/unpack-progress", { method: "GET" });
  expect(res.status).toBe(200);
  const json = await res.json<any>();
  expect(json.unpacking).toBe(false);
  expect(Number(json.queuedCount || 0)).toBe(1);

  // Verify helper returns non-null when queued-only
  const progress = await getUnpackProgress(env as any, repoId);
  expect(progress).not.toBeNull();
  expect(progress?.queuedCount).toBe(1);
});
