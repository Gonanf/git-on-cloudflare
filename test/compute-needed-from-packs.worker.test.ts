import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import * as git from "isomorphic-git";
import type { RepoDurableObject } from "@/index";
import { computeNeeded } from "@/git";
import { createMemPackFs } from "@/git";
import { asTypedStorage, RepoStateSchema } from "@/do/repo/repoState.ts";
import { uniqueRepoId, runDOWithRetry, callStubWithRetry } from "./util/test-helpers.ts";
import { getDb, insertPackOids } from "@/do/repo/db/index.ts";
import { makeTree, makeCommit, buildPack } from "./util/test-helpers.ts";

it("computeNeeded resolves closure from R2 packs when no loose objects exist", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-needed-from-packs");
  const repoId = `${owner}/${repo}`;

  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Synthesize an empty tree and a commit pointing to it
  const { oid: treeOid, payload: treePayload } = await makeTree();
  const { oid: commitOid, payload: commitPayload } = await makeCommit(treeOid, "msg\n");

  // Build a pack containing both objects
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);

  // Create an index (.idx) for the pack using isomorphic-git
  const files = new Map<string, Uint8Array>();
  const fs = createMemPackFs(files);
  await fs.promises.writeFile("/git/objects/pack/test.pack", pack);
  await git.indexPack({ fs: fs as any, dir: "/git", filepath: "objects/pack/test.pack" } as any);
  const idx = files.get("/git/objects/pack/test.idx");
  if (!idx) throw new Error("failed to create pack idx");

  // Upload pack and idx to R2
  const packKey = `test/${crypto.randomUUID()}/test.pack`;
  await env.REPO_BUCKET.put(packKey, pack);
  await env.REPO_BUCKET.put(packKey.replace(/\.pack$/, ".idx"), idx);

  // Configure DO state: pack list and membership, refs and HEAD
  await runDOWithRetry(getStub, async (_instance: any, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", [packKey]);

    // Insert pack OIDs into SQLite via DAL
    const db = getDb(state.storage);
    await insertPackOids(db, packKey, [commitOid, treeOid]);

    await store.put("refs", [{ name: "refs/heads/main", oid: commitOid }]);
    await store.put("head", { target: "refs/heads/main" });
  });

  // Sanity: ensure DO has no loose objects stored (getObject should return null)
  const hasLooseCommit = await callStubWithRetry(getStub, (s) => s.getObject(commitOid));
  expect(hasLooseCommit).toBeNull();

  // Compute needed from the commit root; should include both commit and tree via pack fallback
  const needed = await computeNeeded(env, repoId, [commitOid], []);
  expect(needed).toContain(commitOid);
  expect(needed).toContain(treeOid);
});
