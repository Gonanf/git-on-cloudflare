import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import { readPath } from "@/git/operations/read.ts";
import { encodeGitObject } from "@/git/core/objects.ts";

it("readPath resolves tag to its target commit tree (tag peel)", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-readpath-tag");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Seed minimal repo: commit pointing to empty tree, HEAD -> refs/heads/main
  const { commitOid, treeOid } = await runDOWithRetry(
    getStub,
    async (instance: RepoDurableObject) => {
      return instance.seedMinimalRepo();
    }
  );

  // Create an annotated tag object pointing to the commit
  const payload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger Test <test@example.com> 0 +0000\n` +
      `\n` +
      `message\n`
  );
  const { oid: tagOid, zdata } = await encodeGitObject("tag", payload);

  // Store tag in DO as loose and add refs/tags/v1 -> tagOid
  await runDOWithRetry(getStub, async (instance: RepoDurableObject, state: DurableObjectState) => {
    await instance.putLooseObject(tagOid, zdata);
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const refs = ((await store.get("refs")) || []) as { name: string; oid: string }[];
    refs.push({ name: "refs/tags/v1", oid: tagOid });
    await store.put("refs", refs);
  });

  // Call readPath with the tag ref; expect a tree result (root tree)
  const result = await readPath(env as unknown as Env, repoId, "refs/tags/v1");
  expect(result.type).toBe("tree");
  // For the minimal repo, tree is empty
  if (result.type === "tree") {
    expect(Array.isArray(result.entries)).toBe(true);
    expect(result.entries.length).toBe(0);
  }
});
