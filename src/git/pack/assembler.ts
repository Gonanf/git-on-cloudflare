import { packIndexKey } from "@/keys.ts";
import { createLogger } from "@/common/logger.ts";

/**
 * Assemble a minimal PACK from a single R2 pack+idx that covers all needed OIDs.
 * Uses idx to locate object offsets and range-reads payloads from the pack.
 *
 * @param env Worker environment (provides `REPO_BUCKET` and logging level)
 * @param packKey R2 key for the `.pack` file
 * @param neededOids List of object IDs the output pack must contain
 * @returns PACK bytes with trailing SHA-1, or `undefined` if the single pack can't cover all OIDs
 */
export async function assemblePackFromR2(
  env: Env,
  packKey: string,
  neededOids: string[],
  signal?: AbortSignal
): Promise<Uint8Array | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackAssembler", repoId: packKey });
  log.debug("single:start", { needed: neededOids.length, packKey });
  if (signal?.aborted) return undefined;
  // Fetch and parse .idx for this pack
  const idxKey = packIndexKey(packKey);
  const idxObj = await env.REPO_BUCKET.get(idxKey);
  if (!idxObj) {
    log.debug("single:no-idx", { idxKey });
    return undefined;
  }
  const idxBuf = new Uint8Array(await idxObj.arrayBuffer());
  const parsed = parseIdxV2(idxBuf);
  if (!parsed) {
    log.warn("single:idx-parse-failed", { idxKey });
    return undefined;
  }
  const { oids, offsets } = parsed;
  const oidToIndex = new Map<string, number>();
  for (let i = 0; i < oids.length; i++) oidToIndex.set(oids[i], i);
  const offsetToIndex = new Map<number, number>();
  for (let i = 0; i < offsets.length; i++) offsetToIndex.set(offsets[i], i);

  // Ensure all needed objects are present in this pack
  for (const oid of neededOids)
    if (!oidToIndex.has(oid)) {
      log.debug("single:missing-oid", { oid });
      return undefined;
    }

  // Build mapping of entry -> header info and payload length
  const headResp = await env.REPO_BUCKET.head(packKey);
  if (!headResp) {
    log.debug("single:no-pack", { packKey });
    return undefined;
  }
  const packSize = headResp.size;
  // Heuristic: for small packs or when we need many objects, load the entire pack once.
  const shouldLoadWholePack =
    packSize <= 16 * 1024 * 1024 || neededOids.length >= oids.length * 0.25;
  const wholePack: Uint8Array | undefined = shouldLoadWholePack
    ? new Uint8Array(await (await env.REPO_BUCKET.get(packKey))!.arrayBuffer())
    : undefined;
  if (wholePack) log.debug("single:fast-path:whole-pack-loaded", { bytes: wholePack.byteLength });
  const sortedOffs = offsets.slice().sort((a, b) => a - b);
  const nextOffset = new Map<number, number>();
  for (let i = 0; i < sortedOffs.length; i++) {
    const cur = sortedOffs[i];
    // Exclude 20-byte SHA-1 trailer from last object's end
    const nxt = i + 1 < sortedOffs.length ? sortedOffs[i + 1] : packSize - 20;
    nextOffset.set(cur, nxt);
  }

  type Entry = {
    index: number;
    oid: string;
    origOffset: number;
    origHeaderLen: number;
    sizeVarBytes: Uint8Array;
    type: number;
    baseOid?: string;
    baseIndex?: number;
    payloadLen: number;
  };

  const selected = new Set<number>();
  for (const oid of neededOids) selected.add(oidToIndex.get(oid)!);

  // Read headers and include delta bases
  const pending: number[] = Array.from(selected);
  const entries = new Map<number, Entry>();
  while (pending.length) {
    if (signal?.aborted) return undefined;
    const idx = pending.pop()!;
    if (entries.has(idx)) continue;
    const off = offsets[idx];
    const objEnd = nextOffset.get(off)!;
    const header = wholePack
      ? readPackHeaderExFromBuf(wholePack, off)
      : await readPackHeaderEx(env, packKey, off);
    if (!header) {
      log.warn("single:read-header-failed", { off });
      return undefined;
    }
    const payloadLen = objEnd - off - header.headerLen;
    const ent: Entry = {
      index: idx,
      oid: oids[idx],
      origOffset: off,
      origHeaderLen: header.headerLen,
      sizeVarBytes: header.sizeVarBytes,
      type: header.type,
      baseOid: header.baseOid,
      baseIndex: header.baseOid ? oidToIndex.get(header.baseOid) : undefined,
      payloadLen,
    };
    entries.set(idx, ent);
    if (header.type === 6) {
      // OFS_DELTA
      const baseOff = off - (header.baseRel || 0);
      const bIdx = offsetToIndex.get(baseOff);
      // Record base index for OFS deltas so we can compute accurate new distances
      if (bIdx !== undefined) ent.baseIndex = bIdx;
      if (bIdx !== undefined && !selected.has(bIdx)) {
        selected.add(bIdx);
        pending.push(bIdx);
      }
    } else if (header.type === 7) {
      // REF_DELTA
      if (ent.baseIndex !== undefined && !selected.has(ent.baseIndex)) {
        selected.add(ent.baseIndex);
        pending.push(ent.baseIndex);
      }
    }
  }

  // Determine order by original offsets
  const order = Array.from(selected.values()).sort((a, b) => offsets[a] - offsets[b]);

  // First pass: assume OFS varint length stable; compute new offsets
  const newHeaderLen = new Map<number, number>();
  for (const i of order) {
    if (signal?.aborted) return undefined;
    const e = entries.get(i)!;
    if (e.type === 6) {
      // provisional: guess varint length for ofs using original distance
      const origBaseOff = e.baseIndex !== undefined ? offsets[e.baseIndex] : 0;
      const guessRel = offsets[i] - origBaseOff;
      newHeaderLen.set(i, e.sizeVarBytes.length + encodeOfsDeltaDistance(guessRel).length);
    } else if (e.type === 7) {
      newHeaderLen.set(i, e.sizeVarBytes.length + 20);
    } else {
      newHeaderLen.set(i, e.sizeVarBytes.length);
    }
  }

  const newOffsets = new Map<number, number>();
  let cur = 12; // after PACK header
  for (const i of order) {
    newOffsets.set(i, cur);
    cur += newHeaderLen.get(i)! + entries.get(i)!.payloadLen;
  }

  // Second pass: re-evaluate OFS varints with accurate new distances and recompute if length changed
  let changed = false;
  for (const i of order) {
    const e = entries.get(i)!;
    if (e.type !== 6) continue;
    const baseIdx = e.baseIndex!;
    const rel = newOffsets.get(i)! - newOffsets.get(baseIdx)!;
    const newOfsBytes = encodeOfsDeltaDistance(rel);
    const desired = e.sizeVarBytes.length + newOfsBytes.length;
    if (desired !== newHeaderLen.get(i)) {
      newHeaderLen.set(i, desired);
      changed = true;
    }
  }
  if (changed) {
    cur = 12;
    for (const i of order) {
      newOffsets.set(i, cur);
      cur += newHeaderLen.get(i)! + entries.get(i)!.payloadLen;
    }
  }

  // Write output
  const totalLen = cur;
  const body = new Uint8Array(totalLen);
  // PACK header
  body.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(body.buffer);
  dv.setUint32(4, 2); // version
  dv.setUint32(8, order.length);

  // Fill entries
  for (const i of order) {
    const e = entries.get(i)!;
    let p = newOffsets.get(i)!;
    // size varint (includes type bits)
    body.set(e.sizeVarBytes, p);
    p += e.sizeVarBytes.length;
    if (e.type === 6) {
      // OFS_DELTA: write new relative offset to base in new pack
      const rel = newOffsets.get(i)! - newOffsets.get(e.baseIndex!)!;
      const ofsBytes = encodeOfsDeltaDistance(rel);
      body.set(ofsBytes, p);
      p += ofsBytes.length;
    } else if (e.type === 7) {
      // REF_DELTA: write 20-byte base oid
      body.set(hexToBytes(e.baseOid!), p);
      p += 20;
    }
    // Copy compressed payload from original pack
    const payloadStart = offsets[i] + e.origHeaderLen;
    if (wholePack) {
      const payload = wholePack.subarray(payloadStart, payloadStart + e.payloadLen);
      body.set(payload, p);
    } else {
      const payload = await readPackRange(env, packKey, payloadStart, e.payloadLen);
      if (!payload) {
        log.warn("single:read-range-failed", { offset: payloadStart, length: e.payloadLen });
        return undefined;
      }
      body.set(payload, p);
    }
  }

  // Append SHA-1 trailer
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", body));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  log.info("single:assembled", { objects: order.length, bytes: out.byteLength });
  return out;
}

/**
 * Assembles a minimal PACK from multiple source packs that covers all needed OIDs.
 * Handles both OFS_DELTA and REF_DELTA objects, rewriting offsets as needed.
 * Supports topologically valid ordering for delta chains.
 * @param env - Worker environment (provides REPO_BUCKET and logging level)
 * @param packKeys - Array of R2 pack keys to use as sources
 * @param neededOids - List of object IDs the output pack must contain
 * @param signal - Optional AbortSignal for cancellation
 * @returns PACK bytes with trailing SHA-1, or undefined if coverage is not possible
 */
export async function assemblePackFromMultiplePacks(
  env: Env,
  packKeys: string[],
  neededOids: string[],
  signal?: AbortSignal
): Promise<Uint8Array | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackAssemblerMulti" });
  log.debug("multi:start", { packs: packKeys.length, needed: neededOids.length });
  if (signal?.aborted) return undefined;
  type Meta = {
    key: string;
    oids: string[];
    offsets: number[];
    oidToIndex: Map<string, number>;
    offsetToIndex: Map<number, number>;
    packSize: number;
  };
  const metas: Meta[] = [];
  for (const key of packKeys) {
    const idxKey = packIndexKey(key);
    const [idxObj, head] = await Promise.all([
      env.REPO_BUCKET.get(idxKey),
      env.REPO_BUCKET.head(key),
    ]);
    if (!idxObj || !head) {
      log.debug("multi:missing-pack-or-idx", { key, idx: !!idxObj, head: !!head });
      continue;
    }
    const idxBuf = new Uint8Array(await idxObj.arrayBuffer());
    const parsed = parseIdxV2(idxBuf);
    if (!parsed) {
      log.warn("multi:idx-parse-failed", { key });
      continue;
    }
    const oidToIndex = new Map<string, number>();
    for (let i = 0; i < parsed.oids.length; i++) oidToIndex.set(parsed.oids[i], i);
    const offsetToIndex = new Map<number, number>();
    for (let i = 0; i < parsed.offsets.length; i++) offsetToIndex.set(parsed.offsets[i], i);
    metas.push({
      key,
      oids: parsed.oids,
      offsets: parsed.offsets,
      oidToIndex,
      offsetToIndex,
      packSize: head.size,
    });
  }
  if (metas.length === 0) {
    log.debug("multi:no-metas", {});
    return undefined;
  }

  // Selection: map each needed oid to one of the packs containing it (prefer earliest key)
  type Sel = { m: Meta; i: number };
  const selected = new Map<string, Sel>(); // key: `${m.key}#${i}`
  const pending: Sel[] = [];
  const byOid = (oid: string): Sel | undefined => {
    for (const m of metas) {
      const i = m.oidToIndex.get(oid);
      if (i !== undefined) return { m, i };
    }
    return undefined;
  };
  for (const oid of neededOids) {
    const sel = byOid(oid);
    if (!sel) {
      log.debug("multi:cannot-cover", { oid });
      return undefined; // cannot cover
    }
    const key = `${sel.m.key}#${sel.i}`;
    selected.set(key, sel);
    pending.push(sel);
  }

  // Include delta bases
  while (pending.length) {
    if (signal?.aborted) return undefined;
    const { m, i } = pending.pop()!;
    const off = m.offsets[i];
    const header = await readPackHeaderEx(env, m.key, off);
    if (!header) {
      log.warn("multi:read-header-failed", { key: m.key, off });
      return undefined;
    }
    if (header.type === 6) {
      const baseOff = off - (header.baseRel || 0);
      const bIdx = m.offsetToIndex.get(baseOff);
      if (bIdx === undefined) return undefined;
      const key = `${m.key}#${bIdx}`;
      if (!selected.has(key)) {
        selected.set(key, { m, i: bIdx });
        pending.push({ m, i: bIdx });
      }
    } else if (header.type === 7) {
      const base = header.baseOid!;
      const sel = byOid(base);
      if (!sel) return undefined;
      const key = `${sel.m.key}#${sel.i}`;
      if (!selected.has(key)) {
        selected.set(key, sel);
        pending.push(sel);
      }
    }
  }

  // Build dependency graph for topo order
  type Node = Sel & {
    oid: string;
    origHeaderLen: number;
    sizeVarBytes: Uint8Array;
    type: number;
    base?: Sel;
  };
  const nodes: Node[] = [];
  const nodeKey = (s: Sel) => `${s.m.key}#${s.i}`;
  const nodeMap = new Map<string, Node>();
  for (const s of selected.values()) {
    const off = s.m.offsets[s.i];
    const h = await readPackHeaderEx(env, s.m.key, off);
    if (!h) return undefined;
    const n: Node = {
      ...s,
      oid: s.m.oids[s.i],
      origHeaderLen: h.headerLen,
      sizeVarBytes: h.sizeVarBytes,
      type: h.type,
    };
    if (h.type === 6) {
      const baseOff = s.m.offsets[s.i] - (h.baseRel || 0);
      const bi = s.m.offsetToIndex.get(baseOff);
      if (bi === undefined) return undefined;
      n.base = { m: s.m, i: bi };
    } else if (h.type === 7) {
      const sel = byOid(h.baseOid!);
      if (!sel) return undefined;
      n.base = sel;
    }
    nodes.push(n);
    nodeMap.set(nodeKey(s), n);
  }
  const indeg = new Map<string, number>();
  const children = new Map<string, string[]>();
  for (const n of nodes) {
    indeg.set(nodeKey(n), 0);
  }
  for (const n of nodes) {
    if (!n.base) continue;
    const bkey = nodeKey(n.base);
    indeg.set(nodeKey(n), (indeg.get(nodeKey(n)) || 0) + 1);
    const arr = children.get(bkey) || [];
    arr.push(nodeKey(n));
    children.set(bkey, arr);
  }
  // Kahn's algorithm with tie-breaker: by pack key order then offset
  const packOrder = new Map<string, number>();
  for (let pi = 0; pi < metas.length; pi++) packOrder.set(metas[pi].key, pi);
  const ready: Node[] = nodes.filter((n) => (indeg.get(nodeKey(n)) || 0) === 0);
  ready.sort(
    (a, b) =>
      packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
  );
  const order: Node[] = [];
  while (ready.length) {
    const n = ready.shift()!;
    order.push(n);
    const arr = children.get(nodeKey(n)) || [];
    for (const ck of arr) {
      const v = indeg.get(ck)! - 1;
      indeg.set(ck, v);
      if (v === 0) ready.push(nodeMap.get(ck)!);
    }
    ready.sort(
      (a, b) =>
        packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
    );
  }
  if (order.length !== nodes.length) {
    // Fallback: simple order by pack then offset
    order.length = 0;
    const arr = Array.from(nodes);
    arr.sort(
      (a, b) =>
        packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
    );
    order.push(...arr);
  }

  // Two-pass header length and offsets
  const newHeaderLen = new Map<string, number>();
  for (const n of order) {
    if (signal?.aborted) return undefined;
    if (n.type === 6) {
      // Guess using original distance
      const baseOff = n.base!.m.offsets[n.base!.i];
      const guessRel = n.m.offsets[n.i] - baseOff;
      newHeaderLen.set(nodeKey(n), n.sizeVarBytes.length + encodeOfsDeltaDistance(guessRel).length);
    } else if (n.type === 7) {
      newHeaderLen.set(nodeKey(n), n.sizeVarBytes.length + 20);
    } else {
      newHeaderLen.set(nodeKey(n), n.sizeVarBytes.length);
    }
  }
  const newOffsets = new Map<string, number>();
  let cur = 12;
  for (const n of order) {
    newOffsets.set(nodeKey(n), cur);
    const origPayloadLen = n.m.offsets[n.i + 1]
      ? n.m.offsets[n.i + 1] - n.m.offsets[n.i] - n.origHeaderLen
      : n.m.packSize - 20 - n.m.offsets[n.i] - n.origHeaderLen;
    cur += newHeaderLen.get(nodeKey(n))! + origPayloadLen;
  }
  let changed = false;
  for (const n of order) {
    if (n.type !== 6) continue;
    const rel = newOffsets.get(nodeKey(n))! - newOffsets.get(nodeKey(n.base!))!;
    const desired = n.sizeVarBytes.length + encodeOfsDeltaDistance(rel).length;
    if (desired !== newHeaderLen.get(nodeKey(n))) {
      newHeaderLen.set(nodeKey(n), desired);
      changed = true;
    }
  }
  if (changed) {
    cur = 12;
    for (const n of order) {
      newOffsets.set(nodeKey(n), cur);
      const origPayloadLen = n.m.offsets[n.i + 1]
        ? n.m.offsets[n.i + 1] - n.m.offsets[n.i] - n.origHeaderLen
        : n.m.packSize - 20 - n.m.offsets[n.i] - n.origHeaderLen;
      cur += newHeaderLen.get(nodeKey(n))! + origPayloadLen;
    }
  }

  // Compose body
  const body = new Uint8Array(cur);
  body.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(body.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, order.length);
  for (const n of order) {
    if (signal?.aborted) return undefined;
    let p = newOffsets.get(nodeKey(n))!;
    body.set(n.sizeVarBytes, p);
    p += n.sizeVarBytes.length;
    if (n.type === 6) {
      const rel = newOffsets.get(nodeKey(n))! - newOffsets.get(nodeKey(n.base!))!;
      const ofsBytes = encodeOfsDeltaDistance(rel);
      body.set(ofsBytes, p);
      p += ofsBytes.length;
    } else if (n.type === 7) {
      body.set(
        hexToBytes(
          nodeMap.get(nodeKey(n))!.base
            ? nodeMap.get(nodeKey(n))!.base!.m.oids[nodeMap.get(nodeKey(n))!.base!.i]
            : n.base!.m.oids[n.base!.i]
        ),
        p
      );
      p += 20;
    }
    const payloadStart = n.m.offsets[n.i] + n.origHeaderLen;
    const payloadLen = n.m.offsets[n.i + 1]
      ? n.m.offsets[n.i + 1] - n.m.offsets[n.i] - n.origHeaderLen
      : n.m.packSize - 20 - n.m.offsets[n.i] - n.origHeaderLen;
    const payload = await readPackRange(env, n.m.key, payloadStart, payloadLen);
    if (!payload) {
      log.warn("multi:read-range-failed", {
        key: n.m.key,
        offset: payloadStart,
        length: payloadLen,
      });
      return undefined;
    }
    body.set(payload, p);
  }
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", body));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  log.info("multi:assembled", { objects: order.length, bytes: out.byteLength });
  return out;
}

// ---- low-level helpers ----

/**
 * Read and parse a PACK entry header at a given offset.
 * Returns type, header length, size varint bytes, and delta metadata if applicable.
 *
 * @param env Worker environment
 * @param key R2 key of the `.pack`
 * @param offset Byte offset from start of pack where object begins
 */
async function readPackHeaderEx(
  env: Env,
  key: string,
  offset: number
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
  const head = await readPackRange(env, key, offset, 128);
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
    const baseOid = toHex(head.subarray(p, p + 20));
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
 * Parse a PACK entry header from an in-memory pack buffer at the given offset.
 * Mirrors the behavior of readPackHeaderEx but avoids R2 range reads.
 */
function readPackHeaderExFromBuf(
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
    const baseOid = toHex(buf.subarray(p, p + 20));
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
  // Git encodes ofs-delta distance with continuation bytes, MSB acts as flag and add-one trick
  // We implement the inverse of the decoding algorithm used above.
  const bytes: number[] = [];
  let n = rel;
  let stack: number[] = [];
  // Build bytes in reverse then set MSB for all but last
  stack.push(n & 0x7f);
  while ((n >>= 7) > 0) {
    n--; // compensate for the add-one in decoding
    stack.push(0x80 | (n & 0x7f));
  }
  // Reverse stack to get correct order and ensure only last has MSB cleared
  for (let i = stack.length - 1; i >= 0; i--) {
    const val = stack[i];
    if (i === 0) bytes.push(val & 0x7f);
    else bytes.push(val | 0x80);
  }
  return new Uint8Array(bytes);
}

/**
 * Parses a Git pack index v2/v3 file.
 * Extracts object IDs and their offsets within the pack file.
 * Handles both 32-bit and 64-bit offsets.
 * @param buf - Raw index file bytes
 * @returns Parsed index with OIDs and offsets, or undefined if invalid
 */
function parseIdxV2(buf: Uint8Array): { oids: string[]; offsets: number[] } | undefined {
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
    const hex = toHex(buf.subarray(off, off + 20));
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

/**
 * Read an unsigned 64-bit big-endian integer from a DataView.
 */
function readUint64BE(dv: DataView, pos: number): bigint {
  const hi = dv.getUint32(pos, false);
  const lo = dv.getUint32(pos + 4, false);
  return (BigInt(hi) << 32n) | BigInt(lo);
}

/**
 * Read a byte range from an R2 `.pack` object.
 *
 * @param env Worker environment
 * @param key R2 key of the `.pack`
 * @param offset Starting byte offset
 * @param length Number of bytes to fetch
 */
async function readPackRange(
  env: Env,
  key: string,
  offset: number,
  length: number
): Promise<Uint8Array | undefined> {
  // Workers R2 supports ranged GET
  const obj = await env.REPO_BUCKET.get(key, { range: { offset, length } });
  if (!obj) return undefined;
  const ab = await obj.arrayBuffer();
  return new Uint8Array(ab);
}

/**
 * Converts a byte array to a hexadecimal string.
 * @param bytes - Input byte array (typically 20 bytes for SHA-1)
 * @returns Hexadecimal string representation
 */
function toHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Converts a hexadecimal string to a byte array.
 * @param hex - Hexadecimal string (typically 40 chars for SHA-1)
 * @returns Byte array representation (20 bytes)
 */
function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(20);
  for (let i = 0; i < 20; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}
