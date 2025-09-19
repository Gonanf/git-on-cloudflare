import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { uniqueRepoId, runDOWithRetry, callStubWithRetry } from "./util/test-helpers.ts";
import { getDb, insertPackOids } from "@/do/repo/db/index.ts";

it("getPackOidsBatch returns membership for multiple keys and [] for missing", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-batch");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Seed minimal repo to initialize DO storage and schedules
  await runDOWithRetry(getStub, async (instance: RepoDurableObject) => {
    await instance.seedMinimalRepo();
  });

  // Create arbitrary pack keys and OID lists
  const keyA = `test/${crypto.randomUUID()}/pack-a.pack`;
  const keyB = `test/${crypto.randomUUID()}/pack-b.pack`;
  const missing = `test/${crypto.randomUUID()}/pack-missing.pack`;
  const commitA = "a".repeat(40);
  const treeB = "b".repeat(40);

  // Seed DO metadata directly - now using SQLite
  await runDOWithRetry(getStub, async (_instance: any, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", [keyA, keyB]);

    // Insert pack OIDs into SQLite via DAL
    const db = getDb(state.storage);
    await insertPackOids(db, keyA, [commitA]);
    await insertPackOids(db, keyB, [treeB]);
  });

  // Helper to read Map-like results regardless of serialization
  const getFromMapLike = (m: any, k: string): string[] => {
    if (!m) return [];
    if (typeof m.get === "function") return (m.get(k) as string[]) || [];
    if (Array.isArray(m)) {
      const pair = m.find((p: any) => Array.isArray(p) && p[0] === k);
      return pair ? (pair[1] as string[]) : [];
    }
    if (typeof m === "object") return (m[k] as string[]) || [];
    return [];
  };

  const result = await callStubWithRetry(getStub, (s) => s.getPackOidsBatch([keyA, keyB, missing]));

  const a = getFromMapLike(result, keyA);
  const b = getFromMapLike(result, keyB);
  const c = getFromMapLike(result, missing);

  expect(Array.isArray(a)).toBe(true);
  expect(Array.isArray(b)).toBe(true);
  expect(Array.isArray(c)).toBe(true);
  // OIDs are stored in lowercase in SQLite
  expect(a).toContain(commitA.toLowerCase());
  expect(b).toContain(treeB.toLowerCase());
  expect(c.length).toBe(0);
});
