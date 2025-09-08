import { it, expect } from "vitest";
import { SELF } from "cloudflare:test";
import { decodePktLines, pktLine, flushPkt, concatChunks } from "../src/pktline.ts";

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

it("receive-pack connectivity: rejects commit whose root tree is missing", async () => {
  const owner = "o";
  const repo = "r-connectivity-missing-tree";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Construct a commit that points to a tree OID that is NOT present anywhere on the server
  const missingTreeOid = "c".repeat(40);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "missing tree\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${missingTreeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );

  // Compute commit oid for assertions (like real git object oid)
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  // Create a PACK containing ONLY the commit (missing its root tree)
  const pack = await buildPack([{ type: "commit", payload: commitPayload }]);

  // Commands section (pkt-lines) followed by flush and then raw pack bytes
  const cmd = `${zero40()} ${commitOid} refs/heads/main\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(res.status).toBe(200);

  // report-status should contain ng for the ref due to missing-objects
  const bytes = new Uint8Array(await res.arrayBuffer());
  const items = decodePktLines(bytes);
  const lines = items.filter((i) => i.type === "line").map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  const ng = lines.find((l) => l.startsWith("ng refs/heads/main"));
  expect(ng && /missing-objects/.test(ng)).toBeTruthy();

  // Verify ref was NOT created
  const refsRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/refs`);
  expect(refsRes.status).toBe(200);
  const refs = await refsRes.json<any>();
  expect(refs.find((r: any) => r.name === "refs/heads/main")).toBeUndefined();
});

it("receive-pack connectivity: accepts annotated tag pointing to commit with present tree", async () => {
  const owner = "o";
  const repo = "r-tag-commit-ok";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build empty tree and commit
  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "tag->commit ok\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  // Annotated tag pointing to the commit
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tagOid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tagPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tagPayload.length);
    raw.set(head, 0);
    raw.set(tagPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
    { type: "tag", payload: tagPayload },
  ]);

  const cmd = `${zero40()} ${tagOid} refs/tags/v1\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/v1");
});

it("receive-pack connectivity: accepts annotated tag pointing to tree present", async () => {
  const owner = "o";
  const repo = "r-tag-tree-ok";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const tagPayload = new TextEncoder().encode(
    `object ${treeOid}\n` +
      `type tree\n` +
      `tag v2\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tagOid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tagPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tagPayload.length);
    raw.set(head, 0);
    raw.set(tagPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "tag", payload: tagPayload },
  ]);
  const cmd = `${zero40()} ${tagOid} refs/tags/v2\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/v2");
});

it("receive-pack connectivity: rejects annotated tag pointing to commit with missing tree", async () => {
  const owner = "o";
  const repo = "r-tag-commit-missing-tree";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;
  const missingTreeOid = "c".repeat(40);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "tag->commit missing tree\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${missingTreeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v3\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tagOid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tagPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tagPayload.length);
    raw.set(head, 0);
    raw.set(tagPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const pack = await buildPack([
    { type: "commit", payload: commitPayload },
    { type: "tag", payload: tagPayload },
  ]);
  const cmd = `${zero40()} ${tagOid} refs/tags/v3\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  const ng = lines.find((l) => l.startsWith("ng refs/tags/v3"));
  expect(ng && /missing-objects/.test(ng)).toBeTruthy();
});

it("receive-pack connectivity: accepts direct ref to tree present", async () => {
  const owner = "o";
  const repo = "r-tree-ref-ok";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build an empty tree
  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  const pack = await buildPack([{ type: "tree", payload: treePayload }]);

  const cmd = `${zero40()} ${treeOid} refs/tags/tree-only\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/tree-only");
});

it("receive-pack connectivity: accepts direct ref to blob present", async () => {
  const owner = "o";
  const repo = "r-blob-ref-ok";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const blobPayload = new TextEncoder().encode("hello\n");
  const blobOid = await (async () => {
    const head = new TextEncoder().encode(`blob ${blobPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + blobPayload.length);
    raw.set(head, 0);
    raw.set(blobPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  const pack = await buildPack([{ type: "blob", payload: blobPayload }]);

  const cmd = `${zero40()} ${blobOid} refs/tags/blob-only\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/blob-only");
});

it("receive-pack connectivity: accepts nested tag->tag->tree present", async () => {
  const owner = "o";
  const repo = "r-nested-tag-tree-ok";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const tag1Payload = new TextEncoder().encode(
    `object ${treeOid}\n` +
      `type tree\n` +
      `tag v1\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tag1Oid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tag1Payload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tag1Payload.length);
    raw.set(head, 0);
    raw.set(tag1Payload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const tag2Payload = new TextEncoder().encode(
    `object ${tag1Oid}\n` +
      `type tag\n` +
      `tag v2\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg2\n`
  );
  const tag2Oid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tag2Payload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tag2Payload.length);
    raw.set(head, 0);
    raw.set(tag2Payload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "tag", payload: tag1Payload },
    { type: "tag", payload: tag2Payload },
  ]);

  const cmd = `${zero40()} ${tag2Oid} refs/tags/v2\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/v2");
});
