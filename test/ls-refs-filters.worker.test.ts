import { it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { pktLine, delimPkt, flushPkt, concatChunks, encodeGitObjectAndDeflate } from "@/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";

function buildLsRefsBody(args: string[] = []) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=ls-refs\n"));
  chunks.push(delimPkt());
  for (const a of args) chunks.push(pktLine(a + "\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("ls-refs: ref-prefix filters refs and peel adds peeled attribute for annotated tags", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-lsrefs-filters");
  const repoId = `${owner}/${repo}`;

  // Seed minimal repo to create HEAD -> refs/heads/main
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
    async (instance: RepoDurableObject) => instance.seedMinimalRepo()
  );

  // Create an annotated tag pointing to the commit
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const { oid: tagOid, zdata } = await encodeGitObjectAndDeflate("tag", tagPayload);
  await runDOWithRetry(
    () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
    async (instance: RepoDurableObject) => {
      // Store the tag object and add refs/tags/v1
      await instance.putLooseObject(tagOid, zdata);
      const { refs } = await instance.getHeadAndRefs();
      refs.push({ name: "refs/tags/v1", oid: tagOid });
      await instance.setRefs(refs);
    }
  );

  // Ask only for refs/tags/* and request peeling
  const body = buildLsRefsBody(["ref-prefix refs/tags/", "peel"]);
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body,
  } as any);
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const lines = (await import("@/git"))
    .decodePktLines(bytes)
    .filter((i: any) => i.type === "line")
    .map((i: any) => i.text);

  // HEAD should be first and present
  expect(lines[0]?.startsWith("unborn HEAD") || lines[0]?.startsWith(commitOid + " HEAD")).toBe(
    true
  );
  // The tag must be listed and include peeled:<commitOid>
  const tagLine = lines.find((l: string) => l.includes(" refs/tags/v1"));
  expect(tagLine).toBeDefined();
  expect(tagLine!.includes(`peeled:${commitOid}`)).toBe(true);
  // No branch refs (refs/heads/*) beyond HEAD line
  expect(lines.slice(1).some((l: string) => /\srefs\/heads\//.test(l))).toBe(false);
});
