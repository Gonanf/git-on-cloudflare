import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { computeNeeded } from "@/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";

it("computeNeeded includes commit+tree when no haves, and prunes when have commit", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-compute-needed");
  const repoId = `${owner}/${repo}`;

  // Seed a tiny repo via DO
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid, treeOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
    async (instance: RepoDurableObject) => instance.seedMinimalRepo()
  );

  // No haves: expect closure to include commit and tree
  const need1 = await computeNeeded(env, repoId, [commitOid], []);
  expect(need1).toContain(commitOid);
  expect(need1).toContain(treeOid);

  // With have=commit: expect nothing needed
  const need2 = await computeNeeded(env, repoId, [commitOid], [commitOid]);
  expect(need2.length).toBe(0);
});
