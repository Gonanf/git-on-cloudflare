import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import { asTypedStorage, RepoStateSchema } from "@/do/repo/repoState.ts";
import { getUnpackProgress } from "@/common";
import { runDOWithRetry } from "./util/test-helpers.ts";
import type { RepoDurableObject } from "@/index";

it("/unpack-progress reports queued-only state and getUnpackProgress returns it", async () => {
  const repoId = `qonly/${Math.random().toString(36).slice(2, 8)}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Seed unpackNext only (no unpackWork)
  await runDOWithRetry(getStub, async (_instance: any, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("unpackNext", `${repoId}/objects/pack/pack-next.pack` as any);
    await store.put("lastAccessMs", Date.now() as any);
  });

  // Verify returns non-null when queued-only
  const progress = await getUnpackProgress(env, repoId);
  expect(progress).not.toBeNull();
  expect(progress?.unpacking).toBe(false);
  expect(progress?.queuedCount).toBe(1);
});
