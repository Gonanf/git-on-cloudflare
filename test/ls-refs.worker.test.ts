import { it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import { decodePktLines } from "@/git";

function pktLine(s: string | Uint8Array): Uint8Array {
  const enc = typeof s === "string" ? new TextEncoder().encode(s) : s;
  const len = enc.byteLength + 4;
  const hdr = new TextEncoder().encode(len.toString(16).padStart(4, "0"));
  const out = new Uint8Array(hdr.byteLength + enc.byteLength);
  out.set(hdr, 0);
  out.set(enc, hdr.byteLength);
  return out;
}
function delimPkt() {
  return new TextEncoder().encode("0001");
}
function flushPkt() {
  return new TextEncoder().encode("0000");
}
function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((a, c) => a + c.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

function buildLsRefsBody(args: string[] = []) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=ls-refs\n"));
  chunks.push(delimPkt());
  for (const a of args) chunks.push(pktLine(a + "\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("ls-refs: unborn HEAD advertises correctly", async () => {
  const owner = "o";
  const repo = "r-lsrefs-unborn";
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const body = buildLsRefsBody(["ref-prefix refs/heads/"]);
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
  const lines = decodePktLines(bytes)
    .filter((i) => i.type === "line")
    .map((i: any) => i.text);
  // First line should indicate unborn HEAD with symref target
  expect(lines[0]).toBe("unborn HEAD symref-target:refs/heads/main\n");
});

it("ls-refs: resolved HEAD and refs are listed after seeding", async () => {
  const owner = "o";
  const repo = "r-lsrefs-resolved";
  // Seed directly via DO (runInDurableObject)
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);
  const { commitOid } = await runInDurableObject(stub, async (instance) => {
    const res = await instance.fetch(new Request("https://do/seed", { method: "POST" }));
    return res.json<any>();
  });

  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const body = buildLsRefsBody(["ref-prefix refs/heads/"]);
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
  const lines = decodePktLines(bytes)
    .filter((i) => i.type === "line")
    .map((i: any) => i.text);
  // First line should show HEAD resolved with symref
  expect(lines[0]).toBe(`${commitOid} HEAD symref-target:refs/heads/main\n`);
  // There should be a line for refs/heads/main
  expect(lines.some((l) => l === `${commitOid} refs/heads/main\n`)).toBe(true);
});
