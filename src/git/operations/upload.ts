import type { GitObjectType } from "@/git/core/index.ts";
import type { Logger } from "@/common/index.ts";
import type { CacheContext } from "@/cache/index.ts";
import type { RepoDurableObject } from "@/do/repo/repoDO.ts";

import { pktLine, flushPkt, delimPkt, concatChunks, decodePktLines } from "@/git/core/index.ts";
import { getRepoStub, createLogger, createInflateStream } from "@/common/index.ts";
import { readLooseObjectRaw } from "./read.ts";
import { assemblePackFromR2, assemblePackFromMultiplePacks } from "@/git/pack/assembler.ts";
import { getPackCandidates } from "./packDiscovery.ts";
import { getLimiter, countSubrequest } from "./limits.ts";
import { beginClosurePhase, endClosurePhase } from "./heavyMode.ts";
import { buildPackV2 } from "@/git/pack/index.ts";

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
 * Builds a capped union of object OIDs across the given pack keys using DO RPCs.
 * Prefers the batch API `getPackOidsBatch(keys)` and falls back to per-key RPCs on error.
 * This union is intended to drive a multi-pack assembly that produces a thick pack
 * without computing object closure (used for initial clone and timeout fallback).
 * @param stub - Repo DO stub exposing getPackOidsBatch/getPackOids
 * @param keys - Recent pack keys to union (already capped by caller)
 * @param limiter - Per-request limiter wrapper
 * @param cacheCtx - Optional CacheContext for subrequest accounting
 * @param log - Logger-like object for debug logging
 */
async function buildUnionNeededForKeys(
  stub: DurableObjectStub<RepoDurableObject>,
  keys: string[],
  limiter: { run<T>(name: string, fn: () => Promise<T>): Promise<T> },
  cacheCtx: CacheContext | undefined,
  log: Logger
): Promise<string[]> {
  const MAX_UNION = 25000;
  const needSet = new Set<string>();
  let usedBatch = false;
  try {
    const batchMap = await limiter.run("do:getPackOidsBatch", async () => {
      countSubrequest(cacheCtx);
      return await stub.getPackOidsBatch(keys);
    });
    for (const k of keys) {
      const oids = (batchMap.get ? batchMap.get(k) : undefined) || [];
      for (const oid of oids) {
        needSet.add(String(oid).toLowerCase());
        if (needSet.size >= MAX_UNION) break;
      }
      if (needSet.size >= MAX_UNION) break;
    }
    usedBatch = true;
  } catch (e) {
    log.debug("fetch:union:getPackOidsBatch:error", { error: String(e) });
  }
  if (!usedBatch) {
    for (const k of keys) {
      try {
        const oids = await limiter.run("do:getPackOids", async () => {
          countSubrequest(cacheCtx);
          return await stub.getPackOids(k);
        });
        for (const oid of oids) {
          needSet.add(String(oid).toLowerCase());
          if (needSet.size >= MAX_UNION) break;
        }
      } catch {}
      if (needSet.size >= MAX_UNION) break;
    }
  }
  return Array.from(needSet);
}

/**
 * Handles Git fetch protocol v2 requests.
 * Optimizations:
 * - Initial clone union-first fast path: directly assemble a thick pack by unioning across recent packs,
 *   avoiding object-by-object closure when haves.length === 0 and hydration packs exist.
 * - Standard incremental path: compute minimal closure and assemble from single or multiple packs.
 * - Timeout fallback: safe multi-pack union when closure times out.
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
  signal?: AbortSignal,
  cacheCtx?: CacheContext
) {
  const { wants, haves, done } = parseFetchArgs(body);
  const log = createLogger(env.LOG_LEVEL, { service: "FetchV2", repoId });
  const limiter = getLimiter(cacheCtx);
  if (signal?.aborted) return new Response("client aborted\n", { status: 499 });
  // Enter heavy closure phase: avoid cache reads and cap loader calls
  beginClosurePhase(cacheCtx, { loaderCap: 400, doBatchBudget: 20 });
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

  // Initial clone fast path: avoid object-by-object closure on first fetch.
  // Try to assemble a full pack directly from existing R2 packs.
  if (haves.length === 0) {
    try {
      const doId = stub.id.toString();
      const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
      const packKeys = await getPackCandidates(env, stub, doId, heavy, cacheCtx);
      if (Array.isArray(packKeys) && packKeys.length >= 2) {
        const MAX_KEYS = Math.min(10, packKeys.length);
        const keys = packKeys.slice(0, MAX_KEYS);
        const unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);
        if (unionNeeded.length > 0) {
          const mp = await assemblePackFromMultiplePacks(env, keys, unionNeeded, signal);
          if (mp) {
            log.info("fetch:path:init-union", { packs: keys.length, union: unionNeeded.length });
            // Exit heavy mode before streaming
            endClosurePhase(cacheCtx);
            // No haves in initial clones: acknowledgments block will emit NAK when done === false
            return respondWithPackfile(mp, done, [], signal);
          }
        }
      }
    } catch (e) {
      log.debug("fetch:init-fastpath-failed", { error: String(e) });
    }
  }

  // Standard path: compute needed objects
  const needed = await computeNeeded(env, repoId, wants, haves, cacheCtx);
  log.debug("fetch:incremental", { closure: needed.length, haves: haves.length });

  // Exit heavy closure phase and prepare for downstream reads
  endClosurePhase(cacheCtx);

  // If closure timed out, avoid using the partial set. Try a safe multi-pack union fallback
  // that assembles a complete, thick pack from recent packs without computing tree closure.
  if (cacheCtx?.memo?.flags?.has("closure-timeout")) {
    log.warn("fetch:closure-timeout", { closure: needed.length });
    try {
      const doId = stub.id.toString();
      const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
      const packKeys = await getPackCandidates(env, stub, doId, heavy, cacheCtx);
      if (packKeys.length > 0) {
        const MAX_KEYS = Math.min(10, packKeys.length);
        const keys = packKeys.slice(0, MAX_KEYS);
        const unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);
        if (keys.length >= 2 && unionNeeded.length > 0) {
          const mp = await assemblePackFromMultiplePacks(env, keys, unionNeeded, signal);
          if (mp) {
            log.info("fetch:path:multi-pack-timeout-fallback", {
              packs: keys.length,
              union: unionNeeded.length,
            });
            const ackOids = done ? [] : await findCommonHaves(env, repoId, haves);
            return respondWithPackfile(mp, done, ackOids, signal);
          }
        } else if (keys.length === 1 && unionNeeded.length > 0) {
          // Only one pack available: try a single-pack assembly as a last attempt
          const k = keys[0];
          try {
            log.info("fetch:timeout-fallback:try-single", { key: k, union: unionNeeded.length });
            const single = await assemblePackFromR2(env, k, unionNeeded, signal);
            if (single) {
              log.info("fetch:timeout-fallback:path-single", { key: k });
              const ackOids = done ? [] : await findCommonHaves(env, repoId, haves);
              return respondWithPackfile(single, done, ackOids, signal);
            }
          } catch (e) {
            log.info("fetch:timeout-fallback:failed", { error: String(e) });
          }
        }
      }
    } catch (e) {
      log.debug("fetch:timeout-fallback-failed", { error: String(e) });
    }
    // Still no luck: ask client to retry shortly
    log.warn("fetch:timeout-fallback-503", { closure: needed.length });
    return new Response("Server busy computing object closure; please retry\n", {
      status: 503,
      headers: { "Retry-After": "3", "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  // Coverage guard for initial clones: ensure root tree(s) of wanted commits are present
  // in the computed closure. If not, skip single-pack assembly (it likely won't cover)
  // but still allow multi-pack union which can include delta bases across packs.
  let skipSinglePack = false;
  if (haves.length === 0) {
    try {
      const neededSet = new Set(needed);
      const CHECK_MAX = Math.min(wants.length, 32);
      let missingRoots = 0;
      for (let i = 0; i < CHECK_MAX; i++) {
        const want = wants[i];
        const obj = await readLooseObjectRaw(env, repoId, want, cacheCtx);
        if (!obj || obj.type !== "commit") continue;
        const text = new TextDecoder().decode(obj.payload);
        const m = text.match(/^tree ([0-9a-f]{40})/m);
        if (!m) continue;
        const tree = m[1];
        if (!neededSet.has(tree)) missingRoots++;
      }
      if (missingRoots > 0) {
        skipSinglePack = true;
        log.warn("fetch:coverage-guard-triggered", { missingRoots, wants: wants.length });
      }
      if (!skipSinglePack) {
        log.info("fetch:coverage-guard-pass", { wants: wants.length });
      }
    } catch (e) {
      // Don't block on guard errors; proceed with normal path
      log.debug("fetch:coverage-guard-error", { error: String(e) });
    }
  }

  // For non-done requests with haves, send only acknowledgments
  if (!done && haves.length > 0) {
    const ackOids = await findCommonHaves(env, repoId, haves);
    log.debug("fetch:negotiation", { haves: haves.length, acks: ackOids.length });

    const chunks: Uint8Array[] = [pktLine("acknowledgments\n")];
    if (ackOids.length > 0) {
      for (let i = 0; i < ackOids.length; i++) {
        const suffix = i === ackOids.length - 1 ? "ready" : "common";
        chunks.push(pktLine(`ACK ${ackOids[i]} ${suffix}\n`));
      }
    } else {
      chunks.push(pktLine("NAK\n"));
    }
    chunks.push(flushPkt()); // Important: send flush after acknowledgments only

    return new Response(concatChunks(chunks), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  // Compute set of common haves we can ACK (limit for perf)
  const ackOids = done ? [] : await findCommonHaves(env, repoId, haves);
  if (signal?.aborted) return new Response("client aborted\n", { status: 499 });

  // Try to assemble from R2 packs (single-pack first, then multi-pack union)
  const doId = stub.id.toString();
  const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  const packKeys: string[] = await getPackCandidates(env, stub, doId, heavy, cacheCtx);

  if (!skipSinglePack) {
    try {
      const firstKey = Array.isArray(packKeys) && packKeys.length > 0 ? packKeys[0] : undefined;
      if (firstKey) {
        log.info("fetch:try:single-pack", { key: firstKey, needed: needed.length });
        const assembled = await assemblePackFromR2(env, firstKey, needed, signal);
        if (assembled) {
          log.info("fetch:path:single-pack", { key: firstKey });
          return respondWithPackfile(assembled, done, ackOids, signal);
        } else {
          log.info("fetch:single-pack-miss", { key: firstKey });
        }
      } else {
        log.warn("fetch:single-pack:missing-meta", {});
      }
      // Try each recent pack individually
      if (Array.isArray(packKeys)) {
        for (const k of packKeys) {
          try {
            log.info("fetch:try:single-pack:any", { key: k, needed: needed.length });
            const assembled = await assemblePackFromR2(env, k, needed, signal);
            if (assembled) {
              log.info("fetch:path:single-pack:any", { key: k });
              return respondWithPackfile(assembled, done, ackOids, signal);
            } else {
              log.info("fetch:single-pack-any-miss", { key: k });
            }
          } catch {}
        }
        log.info("fetch:single-pack-all-miss", {
          attempted: Array.isArray(packKeys) ? packKeys.length : 0,
        });
      }
    } catch (e) {
      log.warn("fetch:single-pack:failed", { error: String(e) });
      // ignore and move on to multi-pack
    }
  } else {
    log.debug("fetch:skip-single-pack", {});
  }

  // Multi-pack union: only makes sense when we have at least 2 packs
  if (Array.isArray(packKeys) && packKeys.length >= 2) {
    try {
      log.info("fetch:try:multi-pack", {
        packs: Math.min(10, packKeys.length),
        needed: needed.length,
      });
      const mpAssembled = await assemblePackFromMultiplePacks(
        env,
        packKeys.slice(0, 10),
        needed,
        signal
      );
      if (mpAssembled) {
        log.info("fetch:path:multi-pack", { packs: Math.min(10, packKeys.length) });
        return respondWithPackfile(mpAssembled, done, ackOids, signal);
      } else {
        log.info("fetch:multi-pack-failed", { packs: Math.min(10, packKeys.length) });
      }
    } catch {
      // fall through
    }
  }

  // Note: avoid streaming raw packs as a last resort because packs may be thin
  // (contain REF_DELTA with bases outside the pack) which breaks clients. We prefer
  // multi-pack assembly or the loose fallback below.

  // Fallback: build a minimal pack from loose objects for non-initial clones.
  // Strategy: If many objects are needed, avoid a huge DO batch (which can exceed subrequest limits
  // inside the DO). Instead, try pack-first via readLooseObjectRaw with a small concurrency so pack
  // files are reused across OIDs within this request.
  log.debug("fetch:fallback-loose-objects", { count: needed.length });
  const oids = needed;
  const objs: { type: GitObjectType; payload: Uint8Array }[] = [];
  const found = new Set<string>();

  if (oids.length <= 200) {
    // Small batch: DO-side reads first (cheap and consistent), then pack path for the rest
    let dataMap: Map<string, Uint8Array | null>;
    try {
      dataMap = await limiter.run("do:getObjectsBatch", async () => {
        countSubrequest(cacheCtx);
        return await stub.getObjectsBatch(oids);
      });
    } catch (e) {
      log.error("fetch:batch-read-error", { error: String(e), batch: oids.length });
      dataMap = new Map(oids.map((oid) => [oid, null] as const));
    }

    // Decompress and parse headers with small concurrency
    const CONC = 6;
    let idx = 0;
    const work: Promise<void>[] = [];
    const parseOne = async () => {
      while (idx < oids.length) {
        const j = idx++;
        const oid = oids[j];
        const z = dataMap.get(oid) || null;
        if (!z) continue; // skip; we'll try pack path later
        try {
          const stream = new Blob([z]).stream().pipeThrough(createInflateStream());
          const raw = new Uint8Array(await new Response(stream).arrayBuffer());
          // header: <type> <len>\0
          let p = 0;
          while (p < raw.length && raw[p] !== 0x20) p++;
          const type = new TextDecoder().decode(raw.subarray(0, p)) as GitObjectType;
          let nul = p + 1;
          while (nul < raw.length && raw[nul] !== 0x00) nul++;
          const payload = raw.subarray(nul + 1);
          objs.push({ type, payload });
          found.add(oid);
        } catch (e) {
          log.warn("fetch:decompress-failed", { oid, error: String(e) });
        }
      }
    };
    for (let c = 0; c < CONC; c++) work.push(parseOne());
    await Promise.all(work);
  }

  // Pack-first for remaining or large batches
  const needPack = oids.filter((oid) => !found.has(oid));
  if (needPack.length > 0) {
    const CONC2 = 4; // keep small to avoid subrequest bursts
    let mIdx = 0;
    const workers: Promise<void>[] = [];
    const runOne = async () => {
      while (mIdx < needPack.length) {
        const k = mIdx++;
        const oid = needPack[k];
        try {
          const o = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
          if (!o) {
            log.warn("fetch:missing-object", { oid });
            continue;
          }
          objs.push({ type: o.type as GitObjectType, payload: o.payload });
          found.add(oid);
        } catch (e) {
          log.warn("fetch:read-pack-missing", { oid, error: String(e) });
        }
      }
    };
    for (let c = 0; c < CONC2; c++) workers.push(runOne());
    await Promise.all(workers);
  }

  // For small leftover only, try one more DO batch to fill gaps
  const leftover = oids.filter((oid) => !found.has(oid));
  if (leftover.length > 0 && leftover.length <= 200) {
    try {
      const more = await limiter.run("do:getObjectsBatch", async () => {
        countSubrequest(cacheCtx);
        return await stub.getObjectsBatch(leftover);
      });
      for (const [oid, z] of more) {
        if (!z) continue;
        try {
          const stream = new Blob([z]).stream().pipeThrough(createInflateStream());
          const raw = new Uint8Array(await new Response(stream).arrayBuffer());
          let p = 0;
          while (p < raw.length && raw[p] !== 0x20) p++;
          const type = new TextDecoder().decode(raw.subarray(0, p)) as GitObjectType;
          let nul = p + 1;
          while (nul < raw.length && raw[nul] !== 0x00) nul++;
          const payload = raw.subarray(nul + 1);
          objs.push({ type, payload });
          found.add(oid);
        } catch {}
      }
    } catch (e) {
      log.error("fetch:batch-read-error", { error: String(e), batch: leftover.length });
    }
  }

  if (found.size !== oids.length) {
    log.error("fetch:loose-incomplete", { have: found.size, need: oids.length });
    log.warn("fetch:loose-incomplete-503", { have: found.size, need: oids.length });
    return new Response("Server busy assembling pack; please retry\n", {
      status: 503,
      headers: {
        "Retry-After": "3",
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }
  const packfile = await buildPackV2(objs);
  log.info("fetch:loose-pack-success", { bytes: packfile.byteLength, objects: objs.length });
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
async function collectClosure(
  env: Env,
  repoId: string,
  roots: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(cacheCtx);
  const seen = new Set<string>();
  const queue = [...roots];
  const log = createLogger(env.LOG_LEVEL, { service: "CollectClosure", repoId });
  // Prevent thousands of Cache API reads during traversal; we will still write results.
  if (cacheCtx) {
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
    cacheCtx.memo.flags.add("no-cache-read");
  }
  const startTime = Date.now();
  const timeout = 49000; // Bump to 49s to avoid premature abort under production wall times
  // DO RPC budget: each getObjectRefsBatch() is a DO subrequest. Cap it per worker request.
  // In heavy mode, avoid DO refs batches entirely and rely on pack-based fallback.
  const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  // Aggregated stats for info-level logging
  let memoRefsHitsTotal = 0;
  let doBatchCalls = 0;
  let doBatchRefsTotal = 0;
  let fallbackReadsTotal = 0;
  let fallbackResolvedTotal = 0;
  let fallbackBlobHints = 0;
  // Initialize per-request shared budget and refs memo
  if (cacheCtx) {
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
  }
  let doBatchBudget = cacheCtx?.memo?.doBatchBudget ?? (heavy ? 16 : 20);
  let doBatchDisabled = cacheCtx?.memo?.doBatchDisabled ?? false;

  // Use batch API for much faster object traversal
  while (queue.length > 0) {
    // If DO-backed loose loader has been capped, stop the closure early.
    if (cacheCtx?.memo?.flags?.has("loader-capped")) {
      log.warn("collectClosure:loader-capped-stop", { seen: seen.size, queued: queue.length });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }
    if (Date.now() - startTime > timeout) {
      log.warn("collectClosure:timeout", { seen: seen.size, queued: queue.length });
      // Record in memo so caller can decide to abort the fetch rather than send partial sets
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    // Process larger batches with the new batch API
    const batchSize = Math.min(256, queue.length);
    const batch = queue.splice(0, batchSize);

    // Filter out already seen objects
    const unseenBatch = batch.filter((oid) => !seen.has(oid));
    if (unseenBatch.length === 0) continue;

    // Mark as seen
    for (const oid of unseenBatch) {
      seen.add(oid);
    }

    try {
      // Seed refsMap from memo.refs for any already known OIDs
      let refsMap: Map<string, string[]> = new Map();
      if (cacheCtx?.memo?.refs) {
        for (const oid of unseenBatch) {
          const lc = oid.toLowerCase();
          const cached = cacheCtx.memo.refs.get(lc);
          if (cached && cached.length > 0) {
            refsMap.set(oid, cached);
            memoRefsHitsTotal++;
          }
        }
      }

      // Probe DO batch only for OIDs missing in memo and budget allows
      const toBatch = unseenBatch.filter((oid) => !refsMap.has(oid));
      if (toBatch.length > 0 && !doBatchDisabled && doBatchBudget > 0) {
        try {
          const t0 = Date.now();
          const batchMap = await limiter.run("do:getObjectRefsBatch", async () => {
            countSubrequest(cacheCtx);
            return await stub.getObjectRefsBatch(toBatch);
          });
          doBatchBudget--;
          doBatchCalls++;
          log.info("collectClosure:do-batch", {
            count: toBatch.length,
            timeMs: Date.now() - t0,
          });
          for (const [oid, refs] of batchMap) {
            const lc = oid.toLowerCase();
            const memoArr = cacheCtx?.memo?.refs?.get(lc);
            if (refs && refs.length > 0) {
              // Normal case: DO parsed commit/tree and returned refs
              refsMap.set(oid, refs);
              doBatchRefsTotal += refs.length;
              if (cacheCtx?.memo) {
                cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                cacheCtx.memo.refs.set(lc, refs);
              }
            } else if (Array.isArray(memoArr) && memoArr.length === 0) {
              // We previously pre-marked this OID as a blob via tree parsing; treat as resolved leaf
              refsMap.set(oid, []);
              if (cacheCtx?.memo) {
                cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                cacheCtx.memo.refs.set(lc, []);
              }
            } else {
              // Empty without prior blob hint: leave unresolved so fallback can parse commit/tree from packs
              // Do not set refsMap here; it will be included in `missing` below.
            }
          }
        } catch (e) {
          log.error("collectClosure:batch-error", {
            error: String(e),
            batchSize: toBatch.length,
            seen: seen.size,
          });
          // Disable further DO batches for this request and fall back
          doBatchDisabled = true;
        }
      }

      // Identify objects we still need to resolve client-side
      const missing: string[] = [];
      for (const oid of unseenBatch) {
        if (!refsMap.has(oid)) missing.push(oid);
      }

      // Worker-side fallback: resolve refs via readLooseObjectRaw for missing items
      if (missing.length > 0) {
        log.debug("collectClosure:fallback-missing", { count: missing.length });
        fallbackReadsTotal += missing.length;
        const CONC = heavy ? 4 : 6;
        let mIdx = 0;
        const workers: Promise<void>[] = [];
        const runOne = async () => {
          const td = new TextDecoder();
          while (mIdx < missing.length) {
            if (cacheCtx?.memo?.flags?.has("loader-capped")) return;
            const i = mIdx++;
            const oid = missing[i];
            try {
              const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
              if (!obj) continue;
              const outRefs: string[] = [];
              if (obj.type === "commit") {
                const text = td.decode(obj.payload);
                const m = text.match(/^tree ([0-9a-f]{40})/m);
                if (m) outRefs.push(m[1]);
                for (const pm of text.matchAll(/^parent ([0-9a-f]{40})/gm)) outRefs.push(pm[1]);
              } else if (obj.type === "tree") {
                let i2 = 0;
                const buf = obj.payload;
                while (i2 < buf.length) {
                  let sp = i2;
                  while (sp < buf.length && buf[sp] !== 0x20) sp++;
                  if (sp >= buf.length) break;
                  let nul = sp + 1;
                  while (nul < buf.length && buf[nul] !== 0x00) nul++;
                  if (nul + 20 > buf.length) break;
                  // mode is ascii before the space; tree mode is "40000"
                  const mode = td.decode(buf.subarray(i2, sp));
                  const oidBytes = buf.subarray(nul + 1, nul + 21);
                  const oidHex = [...oidBytes].map((b) => b.toString(16).padStart(2, "0")).join("");
                  outRefs.push(oidHex);
                  // Pre-mark blobs (non-tree) as resolved (no refs) to avoid fallback reads later
                  if (mode !== "40000" && cacheCtx?.memo) {
                    const lc = oidHex.toLowerCase();
                    cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                    if (!cacheCtx.memo.refs.has(lc)) {
                      cacheCtx.memo.refs.set(lc, []);
                      fallbackBlobHints++;
                    }
                  }
                  i2 = nul + 21;
                }
              }
              if (outRefs.length > 0) {
                refsMap.set(oid, outRefs);
                fallbackResolvedTotal++;
                // Persist parsed refs to memo for reuse across closures
                if (cacheCtx?.memo) {
                  const lc = oid.toLowerCase();
                  cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                  cacheCtx.memo.refs.set(lc, outRefs);
                }
              }
            } catch {}
          }
        };
        for (let c = 0; c < CONC; c++) workers.push(runOne());
        await Promise.all(workers);
      }

      // Queue all referenced objects (including those resolved via fallback)
      for (const [oid, refs] of refsMap) {
        for (const ref of refs) {
          if (!seen.has(ref)) {
            queue.push(ref);
          }
        }
      }

      // Yield control periodically to avoid blocking
      if (seen.size % 500 === 0) {
        await new Promise((r) => setTimeout(r, 0));
      }
      // Persist DO batch state back to memo for subsequent closures in this request
      if (cacheCtx?.memo) {
        cacheCtx.memo.doBatchBudget = doBatchBudget;
        cacheCtx.memo.doBatchDisabled = doBatchDisabled;
      }
    } catch {}
  }

  log.info("collectClosure:complete", {
    objects: seen.size,
    timeMs: Date.now() - startTime,
    heavy,
    doBatchCalls,
    doBatchRefs: doBatchRefsTotal,
    doBatchBudget,
    doBatchDisabled,
    memoRefsHits: memoRefsHitsTotal,
    fallbackReads: fallbackReadsTotal,
    fallbackResolved: fallbackResolvedTotal,
    fallbackBlobHints,
    loaderCalls: cacheCtx?.memo?.loaderCalls,
    timedOut: cacheCtx?.memo?.flags?.has("closure-timeout") === true,
  });
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
  haves: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const wantSet = new Set(await collectClosure(env, repoId, wants, cacheCtx));
  const haveRoots = haves.slice(0, 128);
  if (haveRoots.length > 0) {
    const haveSet = new Set(await collectClosure(env, repoId, haveRoots, cacheCtx));
    for (const oid of haveSet) wantSet.delete(oid);
  }
  return Array.from(wantSet);
}

async function findCommonHaves(env: Env, repoId: string, haves: string[]): Promise<string[]> {
  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(undefined);
  const limit = 128;
  const sample = haves.slice(0, limit);

  // Try batch RPC if available
  try {
    const batch: boolean[] = await limiter.run("do:hasLooseBatch", async () => {
      countSubrequest();
      return await stub.hasLooseBatch(sample);
    });
    if (Array.isArray(batch) && batch.length === sample.length) {
      const out: string[] = [];
      for (let i = 0; i < sample.length; i++) if (batch[i]) out.push(sample[i]);
      // De-duplicate while preserving order
      const seen = new Set<string>();
      const uniq: string[] = [];
      for (const o of out)
        if (!seen.has(o)) {
          seen.add(o);
          uniq.push(o);
        }
      return uniq;
    }
  } catch {}

  // Fallback: reduce RPCs using batched probes
  const out: string[] = [];
  const CHUNK = 64;
  for (let i = 0; i < sample.length; i += CHUNK) {
    const part = sample.slice(i, i + CHUNK);
    try {
      const flags = await limiter.run("do:hasLooseBatch", async () => {
        countSubrequest();
        return await stub.hasLooseBatch(part);
      });
      for (let j = 0; j < flags.length; j++) if (flags[j]) out.push(part[j]);
    } catch {
      // If even batch fails, fallback to a few individual calls with tiny concurrency
      const conc = 4;
      let idx = 0;
      const f = new Array(part.length).fill(false) as boolean[];
      const workers: Promise<void>[] = [];
      const run = async () => {
        while (idx < part.length) {
          const k = idx++;
          try {
            f[k] = await limiter.run("do:hasLoose", async () => {
              countSubrequest();
              return await stub.hasLoose(part[k]);
            });
          } catch (e) {
            // Minimal debug log to track failures without spamming
            const log = createLogger(env.LOG_LEVEL, { service: "FindCommonHaves", repoId });
            log.debug("findCommonHaves:hasLoose:error", { error: String(e) });
            f[k] = false;
          }
        }
      };
      for (let c = 0; c < conc; c++) workers.push(run());
      await Promise.all(workers);
      for (let j = 0; j < f.length; j++) if (f[j]) out.push(part[j]);
    }
  }
  const seen = new Set<string>();
  const uniq: string[] = [];
  for (const o of out)
    if (!seen.has(o)) {
      seen.add(o);
      uniq.push(o);
    }
  return uniq;
}

// buildPackV2 is shared from src/git/pack/build.ts via pack/index.ts
