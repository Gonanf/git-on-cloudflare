import { pktLine, flushPkt, delimPkt, concatChunks, decodePktLines } from "@/git/core/pktline.ts";
import { getRepoStub } from "@/common/stub.ts";
import { readLooseObjectRaw } from "./read.ts";
import {
  assemblePackFromR2,
  assemblePackFromMultiplePacks,
  encodeOfsDeltaDistance,
} from "@/git/pack/assembler.ts";
export { encodeOfsDeltaDistance };
import { objTypeCode, encodeObjHeader, type GitObjectType } from "@/git/core/objects.ts";
import { deflate } from "@/common/compression.ts";

export function parseFetchArgs(body: Uint8Array) {
  const items = decodePktLines(body);
  const wantSet = new Set<string>();
  const haves: string[] = [];
  let done = false;
  let afterDelim = false;
  for (const it of items) {
    if (it.type === "delim") {
      afterDelim = true;
      continue;
    }
    if (it.type !== "line") continue;
    const line = it.text.replace(/\r?\n$/, "");
    if (!afterDelim) {
      // capability lines ignored for now
      continue;
    }
    if (line.startsWith("want ")) wantSet.add(line.slice(5, 45));
    else if (line.startsWith("have ")) haves.push(line.slice(5, 45));
    else if (line === "done") done = true;
  }
  return { wants: Array.from(wantSet), haves, done };
}

/**
 * Handles Git fetch protocol v2 requests.
 * Implements fast path for initial clones (no haves) by assembling from latest R2 pack.
 * Falls back to computing minimal closure for incremental fetches.
 * @param env - Worker environment
 * @param repoId - Repository identifier (owner/repo)
 * @param body - Raw request body containing fetch arguments
 * @param signal - Optional AbortSignal for request cancellation
 * @returns Response with packfile or acknowledgments
 */
export async function handleFetchV2(
  env: Env,
  repoId: string,
  body: Uint8Array,
  signal?: AbortSignal
) {
  const { wants, haves, done } = parseFetchArgs(body);
  if (signal?.aborted) return new Response("client aborted\n", { status: 499 });
  if (wants.length === 0) {
    // No wants: respond with ack-only
    const chunks = [pktLine("acknowledgments\n"), pktLine("NAK\n"), flushPkt()];
    return new Response(concatChunks(chunks), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  const stub = getRepoStub(env, repoId);

  // Fast path: initial clone (no haves). Assemble from R2 using ALL objects from the latest pack
  // when it contains all wanted tips. This avoids expensive loose object closure traversal and
  // produces a valid, non-thin pack.
  if (haves.length === 0) {
    try {
      const latest = await stub.fetch("https://do/pack-latest", { method: "GET" });
      if (latest.ok) {
        const { key, oids } = await latest.json<any>();
        if (key && Array.isArray(oids) && wants.every((w: string) => oids.includes(w))) {
          if (signal?.aborted) return new Response("client aborted\n", { status: 499 });
          const assembled = await assemblePackFromR2(env, key, oids, signal);
          if (assembled) return respondWithPackfile(assembled, done, [], signal);
        }
      }
    } catch {
      // ignore and fall through to normal path
    }
  }

  // Compute minimal closure of objects needed
  const needed = await computeNeeded(env, repoId, wants, haves);
  // Compute set of common haves we can ACK (limit for perf)
  const ackOids = await findCommonHaves(env, repoId, haves);
  if (signal?.aborted) return new Response("client aborted\n", { status: 499 });

  // Try to serve from R2 using pack+idx range reads (assemble minimal pack)
  try {
    const latest = await stub.fetch("https://do/pack-latest", { method: "GET" });
    if (latest.ok) {
      const { key, oids } = await latest.json<any>();
      if (key && Array.isArray(oids) && needed.every((w: string) => oids.includes(w))) {
        const assembled = await assemblePackFromR2(env, key, needed);
        if (assembled) return respondWithPackfile(assembled, done, ackOids, signal);
      }
    }

    // If latest doesn't cover, try recent packs for a full cover
    const packsRes = await stub.fetch("https://do/packs", { method: "GET" });
    if (packsRes.ok) {
      const { keys } = await packsRes.json<any>();
      if (Array.isArray(keys)) {
        for (const k of keys) {
          const oidsRes = await stub.fetch("https://do/pack-oids?key=" + encodeURIComponent(k), {
            method: "GET",
          });
          if (!oidsRes.ok) continue;
          const { oids } = await oidsRes.json<any>();
          if (Array.isArray(oids) && needed.every((w: string) => oids.includes(w))) {
            const assembled = await assemblePackFromR2(env, k, needed);
            if (assembled) return respondWithPackfile(assembled, done, ackOids);
          }
        }
        // Multi-pack union: try assembling from multiple recent packs
        const mpAssembled = await assemblePackFromMultiplePacks(env, keys.slice(0, 10), needed);
        if (mpAssembled) return respondWithPackfile(mpAssembled, done, ackOids);
      }
    }
  } catch {
    // ignore and fallback to loose objects
  }

  // Fallback: build a minimal pack from loose objects
  const oids = needed;
  const objs: { type: GitObjectType; payload: Uint8Array }[] = [];
  for (const oid of oids) {
    const o = await readLooseObjectRaw(env, repoId, oid);
    if (!o) continue;
    // readLooseObjectRaw returns type as string, but we know it's a valid GitObjectType
    objs.push({ type: o.type as GitObjectType, payload: o.payload });
  }
  if (objs.length === 0) {
    return new Response("server error: no objects found to pack\n", { status: 500 });
  }
  const packfile = await buildPackV2(objs);
  return respondWithPackfile(packfile, done, ackOids, signal);
}

/**
 * Constructs a Git protocol v2 response with packfile data.
 * Formats the response with proper pkt-line encoding and acknowledgments.
 * @param packfile - The assembled pack data
 * @param done - Whether the client sent 'done' (no negotiation needed)
 * @param ackOids - Object IDs to acknowledge as common
 * @param signal - Optional AbortSignal for streaming cancellation
 * @returns Response with properly formatted Git protocol v2 packfile
 */
export function respondWithPackfile(
  packfile: Uint8Array,
  done: boolean,
  ackOids: string[],
  signal?: AbortSignal
) {
  const chunks: Uint8Array[] = [];
  if (!done) {
    chunks.push(pktLine("acknowledgments\n"));
    if (ackOids && ackOids.length > 0) {
      for (let i = 0; i < ackOids.length; i++) {
        const oid = ackOids[i];
        const suffix = i === ackOids.length - 1 ? "ready" : "common";
        chunks.push(pktLine(`ACK ${oid} ${suffix}\n`));
      }
    } else {
      chunks.push(pktLine("NAK\n"));
    }
    chunks.push(delimPkt());
  }
  chunks.push(pktLine("packfile\n"));
  const maxChunk = 65500;
  for (let off = 0; off < packfile.byteLength; off += maxChunk) {
    if (signal?.aborted) return new Response("client aborted\n", { status: 499 });
    const slice = packfile.subarray(off, Math.min(off + maxChunk, packfile.byteLength));
    const banded = new Uint8Array(1 + slice.byteLength);
    banded[0] = 0x01;
    banded.set(slice, 1);
    chunks.push(pktLine(banded));
  }
  chunks.push(flushPkt());
  return new Response(concatChunks(chunks), {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}

/**
 * Collects the complete object closure starting from root commits.
 * Traverses commit trees and includes all reachable objects.
 * @param env - Worker environment
 * @param repoId - Repository identifier
 * @param roots - Starting commit OIDs
 * @returns Array of all reachable object OIDs
 */
async function collectClosure(env: Env, repoId: string, roots: string[]): Promise<string[]> {
  const stub = getRepoStub(env, repoId);
  const seen = new Set<string>();
  const queue = [...roots];
  while (queue.length) {
    const oid = queue.shift()!;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const obj = await readLooseObjectRaw(env, repoId, oid);
    if (!obj) continue;
    if (obj.type === "commit") {
      const text = new TextDecoder().decode(obj.payload);
      const m = text.match(/^tree ([0-9a-f]{40})/m);
      if (m) queue.push(m[1]);
      // include parents if present (helps incremental fetch later)
      for (const pm of text.matchAll(/^parent ([0-9a-f]{40})/gm)) {
        queue.push(pm[1]);
      }
    } else if (obj.type === "tree") {
      // Parse tree entries: [mode ascii] SP [name] NUL [20-byte oid]
      let i = 0;
      const buf = obj.payload;
      while (i < buf.length) {
        // read until space
        let sp = i;
        while (sp < buf.length && buf[sp] !== 0x20) sp++;
        if (sp >= buf.length) break;
        // read until NUL
        let nul = sp + 1;
        while (nul < buf.length && buf[nul] !== 0x00) nul++;
        if (nul + 20 > buf.length) break;
        const oidBytes = buf.subarray(nul + 1, nul + 21);
        const oidHex = [...oidBytes].map((b) => b.toString(16).padStart(2, "0")).join("");
        queue.push(oidHex);
        i = nul + 21;
      }
    }
  }
  return Array.from(seen);
}

/**
 * Computes the minimal set of objects needed for a fetch.
 * Includes all objects reachable from wants but not from haves.
 * @param env - Worker environment
 * @param repoId - Repository identifier
 * @param wants - Requested commit OIDs
 * @param haves - Client's existing commit OIDs
 * @returns Array of object OIDs needed by the client
 */
export async function computeNeeded(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[]
): Promise<string[]> {
  const wantSet = new Set(await collectClosure(env, repoId, wants));
  const haveRoots = haves.slice(0, 128);
  if (haveRoots.length > 0) {
    const haveSet = new Set(await collectClosure(env, repoId, haveRoots));
    for (const oid of haveSet) wantSet.delete(oid);
  }
  return Array.from(wantSet);
}

async function findCommonHaves(env: Env, repoId: string, haves: string[]): Promise<string[]> {
  const stub = getRepoStub(env, repoId);
  const out: string[] = [];
  const limit = 128;
  for (const oid of haves.slice(0, limit)) {
    const res = await stub.fetch(`https://do/obj/${oid}`, { method: "GET" });
    if (res.ok) out.push(oid);
  }
  // De-duplicate while preserving order
  const seen = new Set<string>();
  const uniq: string[] = [];
  for (const o of out) {
    if (!seen.has(o)) {
      seen.add(o);
      uniq.push(o);
    }
  }
  return uniq;
}

async function buildPackV2(
  objs: { type: GitObjectType; payload: Uint8Array }[]
): Promise<Uint8Array> {
  // Header: 'PACK' + version (2) + number of objects (big-endian)
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2); // version 2
  dv.setUint32(8, objs.length);

  const parts: Uint8Array[] = [hdr];
  for (const o of objs) {
    const typeCode = objTypeCode(o.type);
    const head = encodeObjHeader(typeCode, o.payload.byteLength);
    parts.push(head);
    const comp = await deflate(o.payload);
    parts.push(comp);
  }
  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", body));
  const out = new Uint8Array(body.length + 20);
  out.set(body, 0);
  out.set(sha, body.length);
  return out;
}
