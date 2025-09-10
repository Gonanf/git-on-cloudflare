import { it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";

function randomOid() {
  return "deadbeef".repeat(5).slice(0, 40);
}

it("RepoDO.hasLooseBatch returns aligned booleans for present/missing OIDs", async () => {
  const owner = "o";
  const repo = `r-${Math.random().toString(36).slice(2, 10)}`;
  const repoId = `${owner}/${repo}`;

  // Get DO stub
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);

  // Helper: retry once if the DO was invalidated due to module reloads during tests
  async function runWithRetry<T>(fn: (instance: RepoDurableObject) => Promise<T>): Promise<T> {
    try {
      return await runInDurableObject(stub, fn);
    } catch (e) {
      const msg = String(e || "");
      if (msg.includes("invalidating this Durable Object")) {
        const fresh = env.REPO_DO.get(id);
        return await runInDurableObject(fresh, fn);
      }
      throw e;
    }
  }

  // Do seeding and batch check inside a single DO execution to avoid invalidation between calls
  const { arr, commitOid, treeOid } = await runWithRetry(async (instance: RepoDurableObject) => {
    const { commitOid, treeOid } = await instance.seedMinimalRepo();
    const missing = randomOid();
    const arr = await instance.hasLooseBatch([commitOid, treeOid, missing]);
    return { arr, commitOid, treeOid };
  });

  expect(Array.isArray(arr)).toBe(true);
  expect(arr.length).toBe(3);
  expect(arr[0]).toBe(true);
  expect(arr[1]).toBe(true);
  expect(arr[2]).toBe(false);
});
