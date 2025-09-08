/**
 * Shared test utilities for Git operations
 */

import { concatChunks } from "../../src/git/pktline.ts";
import { deflate } from "../../src/util/compression.ts";
import { encodeObjHeader, objTypeCode, type GitObjectType } from "../../src/util/git-objects.ts";
import { zeroOid } from "../../src/util/hex.ts";

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

/**
 * Get zero OID (40 zeros)
 */
export function zero40(): string {
  return zeroOid();
}

/**
 * Re-export encodeObjHeader for tests that need it directly
 */
export { encodeObjHeader } from "../../src/util/git-objects.ts";
