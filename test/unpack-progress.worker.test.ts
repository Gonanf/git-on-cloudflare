import { it, expect } from "vitest";
import { env, SELF, runInDurableObject } from "cloudflare:test";
import { decodePktLines, pktLine, flushPkt, concatChunks } from "@/git";

async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const cs: any = new (globalThis as any).CompressionStream("deflate");
  const stream = new Blob([data]).stream().pipeThrough(cs);
  const buf = await new Response(stream).arrayBuffer();
  return new Uint8Array(buf);
}

function encodeObjHeader(type: number, size: number): Uint8Array {
  let first = (type << 4) | (size & 0x0f);
  size >>= 4;
  const bytes: number[] = [];
  if (size > 0) first |= 0x80;
  bytes.push(first);
  while (size > 0) {
    let b = size & 0x7f;
    size >>= 7;
    if (size > 0) b |= 0x80;
    bytes.push(b);
  }
  return new Uint8Array(bytes);
}

async function buildPack(
  objects: { type: "commit" | "tree" | "blob" | "tag"; payload: Uint8Array }[]
) {
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, objects.length);
  const parts: Uint8Array[] = [hdr];
  for (const o of objects) {
    const typeCode = o.type === "commit" ? 1 : o.type === "tree" ? 2 : o.type === "blob" ? 3 : 4;
    parts.push(encodeObjHeader(typeCode, o.payload.byteLength));
    parts.push(await deflateRaw(o.payload));
  }
  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", body));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  return out;
}

function zero40() {
  return "0".repeat(40);
}

it("unpack-progress advances via alarm and finishes", async () => {
  // Tune chunking for test: small chunks and quick reschedule
  env.REPO_UNPACK_CHUNK_SIZE = "1";
  env.REPO_UNPACK_MAX_MS = "50";
  env.REPO_UNPACK_DELAY_MS = "10";
  env.REPO_UNPACK_BACKOFF_MS = "50";
  const owner = "o";
  const repo = "r-unpack-progress";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build empty tree and a commit
  const treePayload = new Uint8Array(0);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "progress test\n";
  const treeHeader = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
  const treeRaw = new Uint8Array(treeHeader.length + treePayload.length);
  treeRaw.set(treeHeader, 0);
  treeRaw.set(treePayload, treeHeader.length);
  const treeOid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", treeRaw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const commitHdr = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
  const commitRaw = new Uint8Array(commitHdr.length + commitPayload.length);
  commitRaw.set(commitHdr, 0);
  commitRaw.set(commitPayload, commitHdr.length);
  const commitOid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", commitRaw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);

  const cmd = `${zero40()} ${commitOid} refs/heads/feat\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  // Push
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(res.status).toBe(200);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => i.text.trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);

  // Call progress via the DO stub
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);

  const progress = async () => {
    const r = await stub.fetch("https://do/unpack-progress", { method: "GET" });
    return r.json<any>();
  };

  let p = await progress();
  expect(p.unpacking).toBe(true);
  expect((p.total || 0) >= 2).toBe(true);

  // Drive alarm until done (bounded loops to avoid flake)
  let guard = 200;
  let lastProcessed = -1;
  while (guard-- > 0) {
    await runInDurableObject(stub, async (instance) => {
      await instance.alarm();
    });
    const cur = await progress();
    if (!cur.unpacking) break;
    // ensure forward progress
    expect(cur.processed).not.toBe(lastProcessed);
    lastProcessed = cur.processed;
  }
  const done = await progress();
  expect(done.unpacking).toBe(false);

  // Object should be readable from DO after unpack completes
  const head = await stub.fetch(`https://do/obj/${treeOid}`, { method: "HEAD" });
  expect(head.status).toBe(200);
});
