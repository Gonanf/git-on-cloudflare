import { packIndexKey } from "@/keys.ts";
import { createLogger, bytesToHex } from "@/common/index.ts";

export type IdxParsed = { oids: string[]; offsets: number[] };

export type PackMeta = {
  oids: string[];
  offsets: number[];
  oidToIndex: Map<string, number>;
  offsetToIndex: Map<number, number>;
  packSize: number;
  nextOffset: Map<number, number>;
};

/**
 * Loads pack metadata (idx + size + offset adjacency) for a given R2 `.pack` key.
 * Centralizes the common computations used by single/multi-pack assemblers and hydration.
 */
export async function loadPackMeta(env: Env, packKey: string): Promise<PackMeta | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackMeta", repoId: packKey });
  const [idxObj, head] = await Promise.all([
    env.REPO_BUCKET.get(packIndexKey(packKey)),
    env.REPO_BUCKET.head(packKey),
  ]);
  if (!idxObj || !head) {
    log.debug("meta:missing", { idx: !!idxObj, head: !!head });
    return undefined;
  }
  const idxBuf = new Uint8Array(await idxObj.arrayBuffer());
  const parsed = parseIdxV2(idxBuf);
  if (!parsed) return undefined;

  const { oids, offsets } = parsed;
  const oidToIndex = new Map<string, number>();
  for (let i = 0; i < oids.length; i++) oidToIndex.set(oids[i], i);
  const offsetToIndex = new Map<number, number>();
  for (let i = 0; i < offsets.length; i++) offsetToIndex.set(offsets[i], i);

  // Build adjacency map for computing payload end positions quickly.
  const sortedOffs = offsets.slice().sort((a, b) => a - b);
  const nextOffset = new Map<number, number>();
  for (let i = 0; i < sortedOffs.length; i++) {
    const cur = sortedOffs[i];
    const nxt = i + 1 < sortedOffs.length ? sortedOffs[i + 1] : head.size - 20;
    nextOffset.set(cur, nxt);
  }

  return { oids, offsets, oidToIndex, offsetToIndex, packSize: head.size, nextOffset };
}

// ---- Low-level helpers migrated from assembler.ts ----

/**
 * Parses a Git pack index v2/v3 file.
 * Extracts object IDs and their offsets within the pack file.
 * Handles both 32-bit and 64-bit offsets.
 * @param buf - Raw index file bytes
 * @returns Parsed index with OIDs and offsets, or undefined if invalid
 */
export function parseIdxV2(buf: Uint8Array): { oids: string[]; offsets: number[] } | undefined {
  if (buf.byteLength < 8) return undefined;
  if (!(buf[0] === 0xff && buf[1] === 0x74 && buf[2] === 0x4f && buf[3] === 0x63)) return undefined;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const version = dv.getUint32(4, false);
  if (version !== 2 && version !== 3) return undefined;
  let pos = 8;
  const fanout: number[] = [];
  for (let i = 0; i < 256; i++) {
    fanout.push(dv.getUint32(pos, false));
    pos += 4;
  }
  const n = fanout[255] || 0;
  const namesStart = pos;
  const namesEnd = namesStart + n * 20;
  const oids: string[] = [];
  for (let i = 0; i < n; i++) {
    const off = namesStart + i * 20;
    const hex = bytesToHex(buf.subarray(off, off + 20));
    oids.push(hex);
  }
  const crcsStart = namesEnd;
  const crcsEnd = crcsStart + n * 4;
  const offsStart = crcsEnd;
  const offsEnd = offsStart + n * 4;
  const largeOffsStart = offsEnd;
  // First pass to count large offsets
  let largeCount = 0;
  for (let i = 0; i < n; i++) {
    const u32 = dv.getUint32(offsStart + i * 4, false);
    if (u32 & 0x80000000) largeCount++;
  }
  const largeTableStart = largeOffsStart;
  const offsets: number[] = [];
  for (let i = 0; i < n; i++) {
    const u32 = dv.getUint32(offsStart + i * 4, false);
    if (u32 & 0x80000000) {
      const li = u32 & 0x7fffffff;
      const off64 = readUint64BE(dv, largeTableStart + li * 8);
      offsets.push(Number(off64));
    } else {
      offsets.push(u32 >>> 0);
    }
  }
  return { oids, offsets };
}

function readUint64BE(dv: DataView, pos: number): bigint {
  const hi = dv.getUint32(pos, false);
  const lo = dv.getUint32(pos + 4, false);
  return (BigInt(hi) << 32n) | BigInt(lo);
}

/**
 * Read and parse a PACK entry header at a given offset.
 * Returns type, header length, size varint bytes, and delta metadata if applicable.
 *
 * @param env Worker environment
 * @param key R2 key of the `.pack`
 * @param offset Byte offset from start of pack where object begins
 */
export async function readPackHeaderEx(
  env: Env,
  key: string,
  offset: number,
  options?: {
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    signal?: AbortSignal;
  }
): Promise<
  | {
      type: number;
      sizeVarBytes: Uint8Array;
      headerLen: number;
      baseOid?: string;
      baseRel?: number;
    }
  | undefined
> {
  if (options?.signal?.aborted) return undefined;
  const head = await readPackRange(env, key, offset, 128, options);
  if (!head) return undefined;
  let p = 0;
  const start = p;
  let c = head[p++];
  const type = (c >> 4) & 0x07;
  // collect size varint bytes
  while (c & 0x80) {
    c = head[p++];
  }
  const sizeVarBytes = head.subarray(start, p);
  if (type === 7) {
    // REF_DELTA
    const baseOid = bytesToHex(head.subarray(p, p + 20));
    const headerLen = sizeVarBytes.length + 20;
    return { type, sizeVarBytes, headerLen, baseOid };
  }
  if (type === 6) {
    // OFS_DELTA
    const ofsStart = p;
    let x = 0;
    let b = head[p++];
    x = b & 0x7f;
    while (b & 0x80) {
      b = head[p++];
      x = ((x + 1) << 7) | (b & 0x7f);
    }
    const headerLen = sizeVarBytes.length + (p - ofsStart);
    return { type, sizeVarBytes, headerLen, baseRel: x };
  }
  return { type, sizeVarBytes, headerLen: sizeVarBytes.length };
}

/**
 * Read a byte range from an R2 `.pack` object.
 */
export async function readPackRange(
  env: Env,
  key: string,
  offset: number,
  length: number,
  options?: {
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    signal?: AbortSignal;
  }
): Promise<Uint8Array | undefined> {
  if (options?.signal?.aborted) return undefined;
  const run = async () => {
    const obj = await env.REPO_BUCKET.get(key, { range: { offset, length } });
    if (!obj) return undefined;
    const ab = await obj.arrayBuffer();
    return new Uint8Array(ab);
  };
  if (options?.limiter) {
    options.countSubrequest?.();
    return await options.limiter.run("r2:get-range", run);
  }
  return await run();
}

/**
 * Parse a PACK entry header from an in-memory pack buffer at the given offset.
 * Mirrors the behavior of readPackHeaderEx but avoids R2 range reads.
 */
export function readPackHeaderExFromBuf(
  buf: Uint8Array,
  offset: number
):
  | {
      type: number;
      sizeVarBytes: Uint8Array;
      headerLen: number;
      baseOid?: string;
      baseRel?: number;
    }
  | undefined {
  let p = offset;
  if (p >= buf.length) return undefined;
  const start = p;
  let c = buf[p++];
  const type = (c >> 4) & 0x07;
  // collect size varint bytes
  while (c & 0x80) {
    if (p >= buf.length) return undefined;
    c = buf[p++];
  }
  const sizeVarBytes = buf.subarray(start, p);
  if (type === 7) {
    // REF_DELTA
    if (p + 20 > buf.length) return undefined;
    const baseOid = bytesToHex(buf.subarray(p, p + 20));
    const headerLen = sizeVarBytes.length + 20;
    return { type, sizeVarBytes, headerLen, baseOid };
  }
  if (type === 6) {
    // OFS_DELTA
    const ofsStart = p;
    if (p >= buf.length) return undefined;
    let x = 0;
    let b = buf[p++];
    x = b & 0x7f;
    while (b & 0x80) {
      if (p >= buf.length) return undefined;
      b = buf[p++];
      x = ((x + 1) << 7) | (b & 0x7f);
    }
    const headerLen = sizeVarBytes.length + (p - ofsStart);
    return { type, sizeVarBytes, headerLen, baseRel: x };
  }
  return { type, sizeVarBytes, headerLen: sizeVarBytes.length };
}

/**
 * Encodes OFS_DELTA distance using Git's varint-with-add-one scheme.
 * Inverse of the decoding implemented in this module.
 * @param rel - Distance from delta object to its base in bytes (newOffset - baseOffset)
 * @returns Varint bytes encoding the relative distance
 */
export function encodeOfsDeltaDistance(rel: number): Uint8Array {
  // Correct inverse of the decoder used above:
  // Given X, produce groups g_k..g_0 such that:
  //   X = (((g_0 + 1) << 7 | g_1) + 1 << 7 | g_2) ... | g_k
  // We compute g_k first by peeling off low 7 bits, then iterate
  // with: prev = ((cur - g) >> 7) - 1 until prev < 0, finally reverse.
  if (rel <= 0) return new Uint8Array([0]);
  let cur = rel >>> 0;
  const groups: number[] = [];
  while (true) {
    const g = cur & 0x7f;
    groups.push(g);
    cur = ((cur - g) >>> 7) - 1;
    if (cur < 0) break;
  }
  // Now groups = [g_k, g_{k-1}, ..., g_0]; emit in order g_0..g_k,
  // setting MSB on all but the final (least-significant) group.
  groups.reverse();
  for (let i = 0; i < groups.length - 1; i++) groups[i] |= 0x80;
  return new Uint8Array(groups);
}

// Utility: simple concurrency-limited mapper
export async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const out: R[] = new Array(items.length) as R[];
  let i = 0;
  const workers = new Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      out[idx] = await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return out;
}
