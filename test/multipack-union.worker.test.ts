import { it, expect } from "vitest";
import { env, SELF, runInDurableObject } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import * as git from "isomorphic-git";
import { asTypedStorage, RepoStateSchema } from "@/do/repoState.ts";
import { createMemPackFs } from "@/git";

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

async function inflateZlib(z: Uint8Array): Promise<Uint8Array> {
  const ds: any = new (globalThis as any).DecompressionStream("deflate");
  const stream = new Blob([z]).stream().pipeThrough(ds);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function readLoose(
  stub: DurableObjectStub<RepoDurableObject>,
  oid: string
): Promise<{ type: string; payload: Uint8Array }> {
  const obj = await stub.getObject(oid);
  if (!obj) throw new Error("missing loose " + oid);
  const z = new Uint8Array(obj);
  const raw = await inflateZlib(z);
  // parse header: "type size\0"
  let p = 0;
  while (p < raw.length && raw[p] !== 0x20) p++;
  const type = new TextDecoder().decode(raw.subarray(0, p));
  let nul = p + 1;
  while (nul < raw.length && raw[nul] !== 0x00) nul++;
  const payload = raw.subarray(nul + 1);
  return { type, payload };
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
async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const cs: any = new (globalThis as any).CompressionStream("deflate");
  const stream = new Blob([data]).stream().pipeThrough(cs);
  const buf = await new Response(stream).arrayBuffer();
  return new Uint8Array(buf);
}
async function buildPack(objs: { type: string; payload: Uint8Array }[]): Promise<Uint8Array> {
  // PACK header
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, objs.length);
  const parts: Uint8Array[] = [hdr];
  for (const o of objs) {
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

// Use the repo-side mem fs that implements the full interface isomorphic-git expects

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

it("multi-pack union assembles packfile from two R2 packs", async () => {
  const owner = "o";
  const repo = "r-multipack";
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const stub = env.REPO_DO.get(id);

  // Seed DO repo with a commit + empty tree via runInDurableObject
  const { commitOid, treeOid } = await runInDurableObject(
    stub,
    async (instance: RepoDurableObject) => {
      return instance.seedMinimalRepo();
    }
  );

  // Read loose objects
  const commit = await readLoose(stub, commitOid);
  const tree = await readLoose(stub, treeOid);

  // Build two packs: A(commit), B(tree)
  const packA = await buildPack([commit]);
  const packB = await buildPack([tree]);

  // Create idx for both using isomorphic-git indexPack
  const filesA = new Map<string, Uint8Array>();
  const fsA = createMemPackFs(filesA);
  await fsA.promises.writeFile("/git/objects/pack/pack-a.pack", packA);
  await git.indexPack({ fs: fsA as any, dir: "/git", filepath: "objects/pack/pack-a.pack" } as any);
  const idxA = filesA.get("/git/objects/pack/pack-a.idx");
  if (!idxA) throw new Error("failed to create idxA");

  const filesB = new Map<string, Uint8Array>();
  const fsB = createMemPackFs(filesB);
  await fsB.promises.writeFile("/git/objects/pack/pack-b.pack", packB);
  await git.indexPack({ fs: fsB as any, dir: "/git", filepath: "objects/pack/pack-b.pack" } as any);
  const idxB = filesB.get("/git/objects/pack/pack-b.idx");
  if (!idxB) throw new Error("failed to create idxB");

  // Upload pack+idx to R2 under arbitrary keys
  const keyA = `test/${crypto.randomUUID()}/pack-a.pack`;
  const keyB = `test/${crypto.randomUUID()}/pack-b.pack`;
  await env.REPO_BUCKET.put(keyA, packA);
  await env.REPO_BUCKET.put(keyA.replace(/\.pack$/, ".idx"), idxA);
  await env.REPO_BUCKET.put(keyB, packB);
  await env.REPO_BUCKET.put(keyB.replace(/\.pack$/, ".idx"), idxB);

  // Register pack metadata in DO storage
  await runInDurableObject(stub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", [keyA, keyB]);
    await store.put(`packOids:${keyA}`, [commitOid]);
    await store.put(`packOids:${keyB}`, [treeOid]);
  });

  // Issue fetch for the commit; server should assemble from both packs
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
  const bytes = new Uint8Array(await res.arrayBuffer());

  // Extract sideband pack data after the 'packfile' pkt-line
  const td = new TextDecoder();
  let off = 0;
  function readHdr(): number {
    const s = td.decode(bytes.subarray(off, off + 4));
    off += 4;
    return parseInt(s, 16);
  }
  // Skip acknowledgments section (unknown size), find delim 0001 then 'packfile' line
  while (off + 4 <= bytes.length) {
    const hdr = td.decode(bytes.subarray(off, off + 4));
    if (hdr === "0001") {
      off += 4;
      break;
    }
    if (hdr === "0000") {
      off += 4;
    } else {
      const n = parseInt(hdr, 16);
      off += n;
    }
  }
  const len = readHdr();
  const line = td.decode(bytes.subarray(off, off + (len - 4)));
  off += len - 4;
  expect(line).toBe("packfile\n");

  // Gather subsequent band-1 chunks until flush
  const packChunks: Uint8Array[] = [];
  while (off + 4 <= bytes.length) {
    const hdr = td.decode(bytes.subarray(off, off + 4));
    off += 4;
    if (hdr === "0000") break;
    const n = parseInt(hdr, 16);
    const payload = bytes.subarray(off, off + (n - 4));
    off += n - 4;
    if (payload[0] === 0x01) packChunks.push(payload.subarray(1));
  }
  const packOut = concatChunks(packChunks);
  // Basic checks on assembled pack
  expect(td.decode(packOut.subarray(0, 4))).toBe("PACK");
  const dv = new DataView(packOut.buffer, packOut.byteOffset, packOut.byteLength);
  expect(dv.getUint32(4, false)).toBe(2);
  expect(dv.getUint32(8, false)).toBe(2); // commit + tree

  // Strong validation: index the returned pack and assert expected OIDs are present
  const filesC = new Map<string, Uint8Array>();
  const fsC = createMemPackFs(filesC);
  await fsC.promises.writeFile("/git/objects/pack/resp.pack", packOut);
  const { oids: outOids } = await git.indexPack({
    fs: fsC as any,
    dir: "/git",
    filepath: "objects/pack/resp.pack",
  } as any);
  expect(outOids).toContain(commitOid);
  expect(outOids).toContain(treeOid);
});
