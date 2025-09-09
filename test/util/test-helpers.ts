/**
 * Shared test utilities for Git operations
 */

import { deflate, zeroOid } from "@/common/index.ts";
import { concatChunks, encodeObjHeader, GitObjectType, objTypeCode } from "@/git/index.ts";

/**
 * Build a Git pack file from objects
 */
export async function buildPack(
  objects: { type: GitObjectType; payload: Uint8Array }[]
): Promise<Uint8Array> {
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2); // version
  dv.setUint32(8, objects.length); // object count
  const parts: Uint8Array[] = [hdr];

  for (const o of objects) {
    const typeCode = objTypeCode(o.type);
    parts.push(encodeObjHeader(typeCode, o.payload.byteLength));
    parts.push(await deflate(o.payload));
  }

  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", body));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  return out;
}

export async function makeCommit(treeOid: string, msg: string) {
  const author = `You <you@example.com> 0 +0000`;
  const payload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${author}\n\n${msg}`
  );
  const head = new TextEncoder().encode(`commit ${payload.byteLength}\0`);
  const raw = new Uint8Array(head.length + payload.length);
  raw.set(head, 0);
  raw.set(payload, head.length);
  const hash = await crypto.subtle.digest("SHA-1", raw);
  const oid = Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return { oid, payload };
}

export async function makeTree(): Promise<{ oid: string; payload: Uint8Array }> {
  const payload = new Uint8Array(0);
  const header = new TextEncoder().encode(`tree ${payload.byteLength}\0`);
  const raw = new Uint8Array(header.length + payload.length);
  raw.set(header, 0);
  raw.set(payload, header.length);
  const oid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", raw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return { oid, payload };
}

/**
 * Get zero OID (40 zeros)
 */
export function zero40(): string {
  return zeroOid();
}

/**
 * Re-export encodeObjHeader for tests that need it directly
 */
export { encodeObjHeader } from "@/git/index.ts";
