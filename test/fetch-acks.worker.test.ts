import { it, expect } from "vitest";
import { env, SELF, runInDurableObject } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { pktLine, delimPkt, flushPkt, concatChunks } from "@/git";

function buildFetchBody({
  wants,
  haves,
  done,
}: {
  wants: string[];
  haves?: string[];
  done?: boolean;
}) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=fetch\n"));
  chunks.push(delimPkt());
  for (const w of wants) chunks.push(pktLine(`want ${w}\n`));
  for (const h of haves || []) chunks.push(pktLine(`have ${h}\n`));
  if (done) chunks.push(pktLine("done\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("upload-pack fetch returns acknowledgments and packfile", async () => {
  const owner = "o";
  const repo = "r";
  const repoId = `${owner}/${repo}`;
  // Seed tiny repo and get commit OID via DO instance
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);
  const { commitOid } = await runInDurableObject(stub, async (instance: RepoDurableObject) => {
    return instance.seedMinimalRepo();
  });

  const body = buildFetchBody({ wants: [commitOid] });
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
  expect(res.headers.get("Content-Type") || "").toContain("git-upload-pack-result");
  const bytes = new Uint8Array(await res.arrayBuffer());
  // Basic sanity: response should contain acknowledgments, then packfile
  const text = new TextDecoder().decode(bytes);
  expect(text.includes("acknowledgments\n")).toBe(true);
  expect(text.includes("packfile\n")).toBe(true);
});
