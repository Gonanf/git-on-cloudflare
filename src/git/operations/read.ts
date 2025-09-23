import type { HeadInfo, Ref } from "./types.ts";
import type { CacheContext } from "@/cache/index.ts";

import { parseCommitText } from "@/git/core/commitParse.ts";
import { packIndexKey } from "@/keys.ts";
import { getPackCandidates } from "./packDiscovery.ts";
import { getLimiter, countSubrequest, MAX_SIMULTANEOUS_CONNECTIONS } from "./limits.ts";
import { createMemPackFs, createStubLooseLoader } from "@/git/pack/index.ts";
import { buildObjectCacheKey, cacheOrLoadObject, cachePutObject } from "@/cache/index.ts";
import { createLogger, createInflateStream, getRepoStub, BinaryHeap } from "@/common/index.ts";
import * as git from "isomorphic-git";
import { inflateAndParseHeader, parseTagTarget } from "@/git/core/index.ts";

const LOADER_CAP = 400; // cap DO loose-loader calls per request in heavy mode (prod subrequest budget ~1000)

/**
 * Fetch HEAD and refs for a repository from its Durable Object.
 *
 * - HEAD: `{ target: string; oid?: string; unborn?: boolean }`
 *   - `unborn` means the branch exists but has no commits yet.
 *   - `oid` may be omitted for unborn HEADs; callers should resolve the target from `refs`.
 * - Refs: array of `{ name, oid }` including branches (`refs/heads/*`) and tags (`refs/tags/*`).
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @returns `{ head, refs }` where `head` may be undefined if not initialized
 * @example
 * const { head, refs } = await getHeadAndRefs(env, "owner/repo");
 * // head: { target: "refs/heads/main", oid: "abc123..." }
 * // refs: [{ name: "refs/heads/main", oid: "abc123..." }]
 */
export async function getHeadAndRefs(
  env: Env,
  repoId: string
): Promise<{ head: HeadInfo | undefined; refs: Ref[] }> {
  const stub = getRepoStub(env, repoId);
  const logger = createLogger(env.LOG_LEVEL, { service: "getHeadAndRefs", repoId });
  try {
    return await stub.getHeadAndRefs();
  } catch (e) {
    logger.debug("getHeadAndRefs:error", { error: String(e) });
    return { head: undefined, refs: [] };
  }
}

/**
 * Resolve a ref-ish to a 40-char commit OID.
 *
 * Resolution order:
 * 1. If already a 40-hex OID, return normalized (lowercase)
 * 2. If "HEAD", resolve to current branch's OID
 * 3. If fully qualified ref (starts with "refs/"), exact match
 * 4. For short names: try branches first, then tags
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param refOrOid - Reference, short name, or 40-char OID
 * @returns Resolved commit OID (lowercase) or undefined if not found
 * @example
 * await resolveRef(env, "owner/repo", "HEAD");           // "abc123..."
 * await resolveRef(env, "owner/repo", "main");           // "def456..."
 * await resolveRef(env, "owner/repo", "refs/tags/v1.0"); // "789abc..."
 */
export async function resolveRef(
  env: Env,
  repoId: string,
  refOrOid: string
): Promise<string | undefined> {
  if (/^[0-9a-f]{40}$/i.test(refOrOid)) return refOrOid.toLowerCase();
  const { head, refs } = await getHeadAndRefs(env, repoId);
  if (refOrOid === "HEAD" && head?.target) {
    const r = refs.find((x) => x.name === head.target);
    return r?.oid;
  }
  if (refOrOid.startsWith("refs/")) {
    const r = refs.find((x) => x.name === refOrOid);
    return r?.oid;
  }
  // Try branches first, then tags
  const candidates = [`refs/heads/${refOrOid}`, `refs/tags/${refOrOid}`];
  for (const name of candidates) {
    const r = refs.find((x) => x.name === name);
    if (r) return r.oid;
  }
  return undefined;
}

/**
 * Read and parse a commit object.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Commit object ID (SHA-1 hash)
 * @param cacheCtx - Optional cache context for object caching
 * @returns Parsed commit with tree, parents, and message
 * @throws {Error} If object not found or not a commit
 */
export async function readCommit(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ tree: string; parents: string[]; message: string }> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "commit") throw new Error("Not a commit");
  const text = new TextDecoder().decode(obj.payload);
  const parsed = parseCommitText(text);
  return { tree: parsed.tree, parents: parsed.parents, message: parsed.message };
}

/**
 * Complete commit information including author and committer details.
 */
export interface CommitInfo {
  oid: string;
  tree: string;
  parents: string[];
  author?: { name: string; email: string; when: number; tz: string };
  committer?: { name: string; email: string; when: number; tz: string };
  message: string;
}

/**
 * Read and parse a commit object with full metadata.
 *
 * Similar to readCommit but includes author/committer information.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Commit object ID (SHA-1 hash)
 * @param cacheCtx - Optional cache context for object caching
 * @returns Full commit information including author and committer
 * @throws {Error} If object not found or not a commit
 */
export async function readCommitInfo(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<CommitInfo> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "commit") throw new Error("Not a commit");
  const text = new TextDecoder().decode(obj.payload);
  const parsed = parseCommitText(text);
  const { tree, parents, author, committer, message } = parsed;
  return { oid, tree, parents, author, committer, message };
}

/**
 * First-parent pagination: return `limit` commits starting at absolute `offset` from HEAD
 * along the first-parent chain of `start`.
 *
 * Used by the commits page for pagination. Walks only the first-parent chain
 * (no merge traversal), making it predictable for pagination.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param start - Starting ref or commit OID
 * @param offset - Number of commits to skip from start
 * @param limit - Maximum number of commits to return
 * @param cacheCtx - Optional cache context for object caching
 * @returns Array of commits in the range
 * @example
 * // Get commits 20-40 (page 2 with 20 per page)
 * await listCommitsFirstParentRange(env, "owner/repo", "main", 20, 20);
 */
export async function listCommitsFirstParentRange(
  env: Env,
  repoId: string,
  start: string,
  offset: number,
  limit: number,
  cacheCtx?: CacheContext
): Promise<CommitInfo[]> {
  let oid = await resolveRef(env, repoId, start);
  if (!oid && /^[0-9a-f]{40}$/i.test(start)) oid = start.toLowerCase();
  // Peel annotated tag
  if (oid) {
    const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
    if (obj && obj.type === "tag") {
      const text = new TextDecoder().decode(obj.payload);
      const m = text.match(/^object ([0-9a-f]{40})/m);
      if (m) oid = m[1];
    }
  }
  if (!oid) throw new Error("Ref not found");
  const seen = new Set<string>();

  // Phase 1: Walk first-parent chain just to collect the target OIDs for the requested window
  // We minimize per-step parsing by using readCommit to extract parents and avoid formatting work.
  // This remains inherently sequential because the parent OID of each commit is discovered from
  // the previous object. The heavy object reads and formatting are deferred to Phase 2 below.
  const targetOids: string[] = [];
  let index = 0;
  while (oid && !seen.has(oid) && targetOids.length < limit) {
    seen.add(oid);
    if (index >= offset) {
      targetOids.push(oid);
    }
    // Read minimal commit header to advance along first-parent chain.
    // This will leverage cacheCtx.memo/object cache and reuse any pack files discovered
    // by earlier reads inside this request.
    const c = await readCommit(env, repoId, oid, cacheCtx);
    index++;
    oid = c.parents[0];
  }
  if (targetOids.length === 0) return [];

  // Phase 2: Fetch full CommitInfo for the collected OIDs with bounded concurrency.
  // Downstream DO/R2 calls already use a per-request SubrequestLimiter (MAX_SIMULTANEOUS_CONNECTIONS),
  // and pack discovery uses RequestMemo to coalesce. We still chunk here to avoid creating too many
  // simultaneous promise chains and to align with platform connection limits.
  const out: CommitInfo[] = [];
  const CONCURRENCY = Math.max(1, Math.min(MAX_SIMULTANEOUS_CONNECTIONS, 6));
  for (let i = 0; i < targetOids.length; i += CONCURRENCY) {
    const batch = targetOids.slice(i, i + CONCURRENCY);
    const infos = await Promise.all(batch.map((q) => readCommitInfo(env, repoId, q, cacheCtx)));
    out.push(...infos);
  }
  return out;
}

/**
 * Options for controlling merge side traversal behavior.
 */
export interface MergeSideOptions {
  /** Maximum commits to scan before stopping (default: limit * 3) */
  scanLimit?: number;
  /** Time budget in milliseconds before stopping (default: 150ms) */
  timeBudgetMs?: number;
  /** Number of mainline commits to probe for early stop (default: 300) */
  mainlineProbe?: number;
}

/**
 * Return up to `limit` commits drawn from the non-first-parent sides of a merge commit.
 *
 * Algorithm:
 * 1. Probe mainline (parents[0]) to build a stop set
 * 2. Initialize frontier with side parents (parents[1..])
 * 3. Priority queue traversal by author date (newest first)
 * 4. Stop when: reached limit, hit mainline, timeout, or scan limit
 *
 * Guardrails prevent runaway traversal:
 * - scanLimit: max commits to examine
 * - timeBudgetMs: max time to spend
 * - mainlineProbe: how far to look ahead on mainline
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param mergeOid - OID of the merge commit
 * @param limit - Maximum commits to return (default: 20)
 * @param options - Traversal options for performance tuning
 * @param cacheCtx - Optional cache context for object caching
 * @returns Array of commits from merge side branches
 */
export async function listMergeSideFirstParent(
  env: Env,
  repoId: string,
  mergeOid: string,
  limit = 20,
  options: MergeSideOptions = {},
  cacheCtx?: CacheContext
): Promise<CommitInfo[]> {
  const logger = createLogger(env.LOG_LEVEL, { service: "listMergeSideFirstParent", repoId });
  const scanLimit = Math.min(400, Math.max(limit * 3, options.scanLimit ?? 120));
  const timeBudgetMs = Math.max(50, Math.min(10000, options.timeBudgetMs ?? 150)); // Allow up to 10s for production
  const mainlineProbe = Math.min(1000, Math.max(50, options.mainlineProbe ?? 100)); // Reduced default from 300 to 100
  const started = Date.now();

  // Load the merge commit
  const merge = await readCommitInfo(env, repoId, mergeOid, cacheCtx);
  const parents = merge.parents || [];
  if (parents.length < 2) return [];

  // Probe a window of mainline (parents[0]) to stop when a side reaches it
  const mainlineSet = new Set<string>();
  try {
    let cur: string | undefined = parents[0];
    let seen = 0;
    const visited = new Set<string>();
    const probeStarted = Date.now();
    // Limit probe time to 1/3 of budget to leave time for actual traversal
    const probeTimeBudget = Math.min(1500, timeBudgetMs / 3);

    while (
      cur &&
      seen < mainlineProbe &&
      !visited.has(cur) &&
      Date.now() - probeStarted < probeTimeBudget
    ) {
      visited.add(cur);
      mainlineSet.add(cur);
      const info = await readCommitInfo(env, repoId, cur, cacheCtx);
      cur = info.parents?.[0];
      seen++;
    }

    // Log probe performance
    logger.info("Mainline probe completed", {
      commits: seen,
      timeMs: Date.now() - probeStarted,
      mergeOid,
    });
  } catch {}

  // Helper to compare by date desc then oid desc
  const newerFirst = (a: CommitInfo, b: CommitInfo) => {
    const aw = a.author?.when ?? 0;
    const bw = b.author?.when ?? 0;
    if (aw !== bw) return bw - aw;
    return b.oid.localeCompare(a.oid);
  };

  // Initialize frontier with side parents (parents[1..])
  const visited = new Set<string>();
  const frontier: CommitInfo[] = [];
  for (let i = 1; i < parents.length; i++) {
    const p = parents[i];
    try {
      const info = await readCommitInfo(env, repoId, p, cacheCtx);
      frontier.push(info);
    } catch {}
  }
  // Build initial heap using generic BinaryHeap
  const heap = new BinaryHeap<CommitInfo>(newerFirst, frontier);

  const out: CommitInfo[] = [];
  let scanned = 0;

  while (
    out.length < limit &&
    !heap.isEmpty() &&
    scanned < scanLimit &&
    Date.now() - started < timeBudgetMs
  ) {
    const current = heap.pop()!;
    scanned++;
    if (visited.has(current.oid)) continue;
    visited.add(current.oid);

    // Stop the branch if we reached the mainline (approximate merge-base boundary)
    if (mainlineSet.has(current.oid)) {
      continue;
    }

    out.push(current);
    if (out.length >= limit) break;

    // Advance along first-parent for this branch
    const next = current.parents?.[0];
    if (next && !visited.has(next)) {
      try {
        const ni = await readCommitInfo(env, repoId, next, cacheCtx);
        heap.push(ni);
      } catch {}
    }
  }

  return out;
}

/**
 * Git tree entry representing a file or directory.
 */
export interface TreeEntry {
  /** File mode (e.g., "100644" for regular file, "40000" for directory) */
  mode: string;
  /** Entry name (filename or directory name) */
  name: string;
  /** Object ID of the blob (file) or tree (directory) */
  oid: string;
}

/**
 * Read and parse a tree object.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Tree object ID
 * @param cacheCtx - Optional cache context for object caching
 * @returns Array of tree entries
 * @throws {Error} If object not found or not a tree
 */
export async function readTree(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<TreeEntry[]> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "tree") {
    // If we can't find it in loose objects, it might still be in a pack
    // Let's try to get it through the DO endpoint which might trigger pack assembly
    throw new Error("Not a tree");
  }
  return parseTree(obj.payload);
}

/**
 * Read a tree or blob by ref and path.
 *
 * Versatile function that navigates the Git object tree from a ref to a path.
 * Handles various starting points (commit, tree, tag, blob) and returns
 * appropriate data based on whether the path points to a file or directory.
 *
 * For large files (>5MB), returns metadata only to avoid memory issues.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param ref - Starting point: branch, tag, commit SHA, tree OID, or blob OID
 * @param path - File or directory path (optional, defaults to root)
 * @param cacheCtx - Optional cache context for object caching
 * @returns Union type based on path target:
 *   - Directory: `{ type: 'tree', entries, base }`
 *   - File: `{ type: 'blob', oid, content, base, size?, tooLarge? }`
 * @throws {Error} If ref not found, path not found, or path not a directory when expected
 * @example
 * // Get root directory listing
 * await readPath(env, "owner/repo", "main");
 * // Get specific file
 * await readPath(env, "owner/repo", "main", "README.md");
 * // Navigate to subdirectory
 * await readPath(env, "owner/repo", "main", "src/utils");
 */
export async function readPath(
  env: Env,
  repoId: string,
  ref: string,
  path?: string,
  cacheCtx?: CacheContext
): Promise<
  | { type: "tree"; entries: TreeEntry[]; base: string }
  | {
      type: "blob";
      oid: string;
      content: Uint8Array;
      base: string;
      size?: number;
      tooLarge?: boolean;
    }
> {
  // Determine starting point: ref name, commit OID, tree OID, or blob OID
  let startOid: string | undefined = await resolveRef(env, repoId, ref);
  if (!startOid && /^[0-9a-f]{40}$/i.test(ref)) startOid = ref.toLowerCase();
  if (!startOid) throw new Error("Ref not found");

  const startObj = await readLooseObjectRaw(env, repoId, startOid, cacheCtx);
  if (!startObj) throw new Error("Object not found");

  let currentTreeOid: string | undefined;
  if (startObj.type === "commit") {
    const { tree } = await readCommit(env, repoId, startOid, cacheCtx);
    currentTreeOid = tree;
  } else if (startObj.type === "tree") {
    currentTreeOid = startOid;
  } else if (startObj.type === "tag") {
    const t = parseTagTarget(startObj.payload);
    if (!t || !t.targetOid) throw new Error("Unsupported object type");
    const target = t.targetOid;
    const { tree } = await readCommit(env, repoId, target, cacheCtx);
    currentTreeOid = tree;
  } else if (startObj.type === "blob") {
    // If the ref points to a blob directly, only valid when no path is given
    if (path && path !== "") throw new Error("Path not a directory");
    return { type: "blob", oid: startOid, content: startObj.payload, base: "" };
  } else {
    throw new Error("Unsupported object type");
  }

  const parts = (path || "").split("/").filter(Boolean);
  let base = "";
  for (let i = 0; i < parts.length; i++) {
    const entries = await readTree(env, repoId, currentTreeOid, cacheCtx);
    const ent = entries.find((e) => e.name === parts[i]);
    if (!ent) throw new Error("Path not found");
    base = parts.slice(0, i + 1).join("/");
    if (ent.mode.startsWith("40000")) {
      currentTreeOid = ent.oid;
      if (i === parts.length - 1) {
        const finalEntries = await readTree(env, repoId, currentTreeOid, cacheCtx);
        return { type: "tree", entries: finalEntries, base };
      }
    } else {
      if (i !== parts.length - 1) throw new Error("Path not a directory");

      // Check blob size first to avoid loading large files
      const stub = getRepoStub(env, repoId);
      const log = createLogger(env.LOG_LEVEL, { service: "readPath", repoId });
      const limiter = getLimiter(cacheCtx);
      const size = await limiter.run("do:getObjectSize", async () => {
        if (!countSubrequest(cacheCtx)) {
          log.warn("soft-budget-exhausted", { op: "do:getObjectSize", oid: ent.oid });
          return null;
        }
        try {
          return await stub.getObjectSize(ent.oid);
        } catch (e) {
          log.debug("do:getObjectSize:error", { error: String(e), oid: ent.oid });
          return null;
        }
      });
      if (size === null) throw new Error("Blob not found");

      // Content-Length (from DO/R2) is the compressed size, but we need decompressed size
      // For now, we'll use a conservative estimate (compressed * 10)
      const compressedSize = size;
      const MAX_SIZE = 5 * 1024 * 1024;
      const estimatedSize = compressedSize * 10;

      if (estimatedSize > MAX_SIZE) {
        // Return metadata only for large files
        return {
          type: "blob",
          oid: ent.oid,
          content: new Uint8Array(0),
          base,
          size: estimatedSize,
          tooLarge: true,
        };
      }

      // Load the blob if it's small enough
      const blob = await readLooseObjectRaw(env, repoId, ent.oid, cacheCtx);
      if (!blob || blob.type !== "blob") throw new Error("Not a blob");

      // Double-check actual size after decompression
      const actualSize = blob.payload.byteLength;
      if (actualSize > MAX_SIZE) {
        return {
          type: "blob",
          oid: ent.oid,
          content: new Uint8Array(0),
          base,
          size: actualSize,
          tooLarge: true,
        };
      }

      return { type: "blob", oid: ent.oid, content: blob.payload, base };
    }
  }
  // root tree (or descended directory)
  const rootEntries = await readTree(env, repoId, currentTreeOid, cacheCtx);
  return { type: "tree", entries: rootEntries, base };
}

/**
 * Parse binary tree object format into structured entries.
 *
 * Git tree format: `<mode> <name>\0<20-byte-oid>`
 * Repeated for each entry.
 *
 * @param buf - Raw tree object payload
 * @returns Parsed tree entries
 */
function parseTree(buf: Uint8Array): TreeEntry[] {
  const td = new TextDecoder();
  const out: TreeEntry[] = [];
  let i = 0;
  while (i < buf.length) {
    let sp = i;
    while (sp < buf.length && buf[sp] !== 0x20) sp++;
    if (sp >= buf.length) break;
    const mode = td.decode(buf.subarray(i, sp));
    let nul = sp + 1;
    while (nul < buf.length && buf[nul] !== 0x00) nul++;
    if (nul + 20 > buf.length) break;
    const name = td.decode(buf.subarray(sp + 1, nul));
    const oidBytes = buf.subarray(nul + 1, nul + 21);
    const oid = [...oidBytes].map((b) => b.toString(16).padStart(2, "0")).join("");
    out.push({ mode, name, oid });
    i = nul + 21;
  }
  return out;
}

/**
 * Read a blob object and return its content.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Blob object ID
 * @param cacheCtx - Optional cache context for object caching
 * @returns Blob content and type, or null if not found
 */
export async function readBlob(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ content: Uint8Array | null; type: string | null }> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj) return { content: null, type: null };
  return { content: obj.payload, type: obj.type };
}

/**
 * Stream a blob without buffering the entire object in memory.
 *
 * Ideal for large files. The response streams directly from DO/R2 through
 * decompression, stripping the Git header on the fly.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Object ID (SHA-1 hash) of the blob
 * @returns Response with streaming body or null if not found
 * @example
 * const response = await readBlobStream(env, "owner/repo", "abc123...");
 * // Stream directly to client: return response;
 */
export async function readBlobStream(
  env: Env,
  repoId: string,
  oid: string
): Promise<Response | null> {
  const stub = getRepoStub(env, repoId);
  const objStream = await stub.getObjectStream(oid);
  if (!objStream) return null;

  // State for header parsing
  let headerParsed = false;
  let buffer = new Uint8Array(0);

  // Create a TransformStream to parse Git object header and stream payload
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk: Uint8Array, controller) {
      if (!headerParsed) {
        // Accumulate chunks until we find the header end
        const combined = new Uint8Array(buffer.length + chunk.length);
        combined.set(buffer);
        combined.set(chunk, buffer.length);

        // Look for null byte that ends the header
        const nullIndex = combined.indexOf(0);
        if (nullIndex !== -1) {
          headerParsed = true;
          // Skip header and stream the rest
          if (nullIndex + 1 < combined.length) {
            controller.enqueue(combined.slice(nullIndex + 1));
          }
        } else {
          buffer = combined;
        }
      } else {
        // Header already parsed, just pass through
        controller.enqueue(chunk);
      }
    },
  });

  // Decompress and parse in a streaming fashion
  const decompressed = objStream
    .pipeThrough(createInflateStream())
    .pipeThrough({ readable, writable });

  return new Response(decompressed, {
    headers: {
      "Content-Type": "application/octet-stream",
      "Cache-Control": "public, max-age=31536000, immutable",
      ETag: `"${oid}"`,
    },
  });
}

/**
 * Read and fully buffer a raw Git object from storage.
 *
 * Storage hierarchy (in order):
 * 1. Cache API - Edge cache, ~5-20ms latency
 * 2. Durable Object state - Recent/active objects, ~30-50ms
 * 3. R2 packfiles - Cold storage, ~100-300ms
 *
 * Optimizations and hints:
 * - Per-request memoization caches pack lists and object results
 * - DO pack metadata (`getPackLatest()`, `getPacks()`) seeds candidate packs
 * - R2 listing under the DO pack prefix provides a last-resort discovery path
 *
 * Objects are immutable, so aggressive object caching (1 year TTL) is used by the Cache API
 * layer. For large blobs, prefer `readBlobStream()` to avoid memory buffering.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Object ID (SHA-1 hash)
 * @param cacheCtx - Optional cache context for Cache API (enables object caching)
 * @returns Decompressed object with type and payload, or undefined if not found
 */
export async function readLooseObjectRaw(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ type: string; payload: Uint8Array } | undefined> {
  const oidLc = oid.toLowerCase();
  const stub = getRepoStub(env, repoId);
  // Repository DO ID used for R2 prefix scoping and pack discovery; also included in logs for tracing.
  const doId = stub.id.toString();
  const logger = createLogger(env.LOG_LEVEL, {
    service: "readLooseObjectRaw",
    repoId,
    doId,
  });

  // Ensure per-request memo exists and is pinned to this repo to avoid cross-repo contamination
  if (cacheCtx) {
    if (!cacheCtx.memo || (cacheCtx.memo.repoId && cacheCtx.memo.repoId !== repoId)) {
      cacheCtx.memo = { repoId };
    } else if (!cacheCtx.memo.repoId) {
      cacheCtx.memo.repoId = repoId;
    }
  }

  // Per-request memo: short-circuit if we already loaded this OID in this request
  if (cacheCtx?.memo?.objects?.has(oidLc)) {
    return cacheCtx.memo.objects.get(oidLc);
  }

  // In heavy phases, avoid certain DO-backed paths to preserve subrequest budget.
  const heavyNoCache = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  // Global per-request limiter for all upstream calls (DO, R2)
  const limiter = getLimiter(cacheCtx);

  /**
   * Load a pack and its index from R2 into an in-memory virtual FS map.
   * Returns true on success, false if either file is missing.
   */
  async function addPackToFiles(
    env: Env,
    packKey: string,
    files: Map<string, Uint8Array>
  ): Promise<boolean> {
    const [p, i] = await Promise.all([
      limiter.run("r2:get-pack", async () => {
        if (!countSubrequest(cacheCtx)) {
          logger.warn("soft-budget-exhausted", { op: "r2:get-pack", key: packKey });
          return null;
        }
        return await env.REPO_BUCKET.get(packKey);
      }),
      limiter.run("r2:get-idx", async () => {
        if (!countSubrequest(cacheCtx)) {
          logger.warn("soft-budget-exhausted", { op: "r2:get-idx", key: packKey });
          return null;
        }
        return await env.REPO_BUCKET.get(packIndexKey(packKey));
      }),
    ]);
    if (!p || !i) return false;

    const [packArrayBuf, idxArrayBuf] = await Promise.all([p.arrayBuffer(), i.arrayBuffer()]);
    const packBuf = new Uint8Array(packArrayBuf);
    const idxBuf = new Uint8Array(idxArrayBuf);
    const base = packKey.split("/").pop()!;
    const idxBase = base.replace(/\.pack$/i, ".idx");
    files.set(`/git/objects/pack/${base}`, packBuf);
    files.set(`/git/objects/pack/${idxBase}`, idxBuf);
    return true;
  }

  // Note: previously a KV-backed fast path loaded a single known pack; that path has been removed.

  /**
   * Build a candidate `packList` used for multi-pack reads:
   * - Ensure DO `/pack-latest` is present to cover very recent pushes
   * - Fallback to DO `/packs` (and finally `/pack-latest`) if still empty
   * - As a last resort, list R2 under the DO pack prefix
   */

  /**
   * Multi-pack path: identify a candidate pack via DO `/pack-oids` probes and load a
   * small set of packs from R2 (helps delta resolution across packs).
   */
  const loadFromPacks = async () => {
    try {
      // Step 1: Build candidate pack list
      // Per-request memo: reuse pack list within the same request
      const packListRaw = await getPackCandidates(env, stub, doId, heavyNoCache, cacheCtx);
      let packList: string[] = packListRaw;
      // Reduce probe set in heavy phases to limit DO RPCs and R2 downloads
      const PROBE_MAX = heavyNoCache ? 10 : packList.length;
      if (packList.length > PROBE_MAX) packList = packList.slice(0, PROBE_MAX);
      if (cacheCtx?.memo) {
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
        if (!cacheCtx.memo.flags.has("pack-list-candidates-logged")) {
          logger.debug("pack-list-candidates", { count: packList.length });
          cacheCtx.memo.flags.add("pack-list-candidates-logged");
        }
      } else {
        logger.debug("pack-list-candidates", { count: packList.length });
      }
      if (packList.length === 0) {
        // Throttle noisy warning to once per request
        const alreadyWarned = cacheCtx?.memo?.flags?.has("pack-list-empty");
        if (!alreadyWarned) {
          logger.warn("pack-list-empty", { oid: oidLc, afterFallbacks: true });
          if (cacheCtx?.memo) {
            cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
            cacheCtx.memo.flags.add("pack-list-empty");
          }
        }
        return undefined;
      }

      // Step 2: Find which pack contains our target OID by querying pack indexes
      let chosenPackKey: string | undefined;
      const contains: Record<string, boolean> = {};
      for (const key of packList) {
        try {
          // Per-request memo: cache pack OIDs per pack key
          let set: Set<string>;
          if (cacheCtx?.memo?.packOids?.has(key)) {
            set = cacheCtx.memo.packOids.get(key)!;
          } else {
            const dataOids = await limiter.run("do:getPackOids", async () => {
              if (!countSubrequest(cacheCtx)) {
                logger.warn("soft-budget-exhausted", { op: "do:getPackOids", key });
                return [] as string[];
              }
              return await stub.getPackOids(key);
            });
            set = new Set((dataOids || []).map((x: string) => x.toLowerCase()));
            if (cacheCtx?.memo) {
              cacheCtx.memo.packOids = cacheCtx.memo.packOids || new Map();
              cacheCtx.memo.packOids.set(key, set);
            }
          }
          const has = set.has(oidLc);
          contains[key] = has;
          if (!chosenPackKey && has) chosenPackKey = key;
        } catch {}
      }
      if (!chosenPackKey) chosenPackKey = packList[0];
      if (cacheCtx?.memo) {
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
        if (!cacheCtx.memo.flags.has("chosen-pack-logged")) {
          logger.debug("chosen-pack", { chosenPackKey, hasDirectHit: !!contains[chosenPackKey] });
          cacheCtx.memo.flags.add("chosen-pack-logged");
        }
      } else {
        logger.debug("chosen-pack", { chosenPackKey, hasDirectHit: !!contains[chosenPackKey] });
      }

      // Note: previously we persisted KV hints (pack list and OID->pack). With KV removed,
      // we rely solely on DO metadata and R2 listing.

      // Step 3: Iteratively load packs from R2 until we can resolve the object (to handle
      // cross-pack delta chains). Always prioritize the chosen pack, then load remaining
      // packs in small batches. Cap the total to avoid excessive memory/time.
      const order: string[] = (() => {
        const arr = packList.slice(0);
        if (chosenPackKey) {
          const i = arr.indexOf(chosenPackKey);
          if (i > 0) {
            arr.splice(i, 1);
            arr.unshift(chosenPackKey);
          } else if (i < 0) {
            arr.unshift(chosenPackKey);
          }
        }
        // Cap total packs to load to reduce R2 GETs during heavy phases
        const LOAD_MAX = heavyNoCache ? 12 : 20;
        if (arr.length > LOAD_MAX) arr.length = LOAD_MAX;
        return arr;
      })();

      // Reuse pack files across OIDs within the same request to avoid re-downloading packs
      let files: Map<string, Uint8Array>;
      if (cacheCtx?.memo?.packFiles) {
        files = cacheCtx.memo.packFiles;
      } else {
        files = new Map<string, Uint8Array>();
        if (cacheCtx?.memo) cacheCtx.memo.packFiles = files;
      }
      const loaded = new Set<string>();
      const BATCH = 5;
      const dir = "/git";
      const baseLoader = createStubLooseLoader(stub);
      const looseLoader = async (oid: string) => {
        if (cacheCtx?.memo) {
          const next = (cacheCtx.memo.loaderCalls ?? 0) + 1;
          cacheCtx.memo.loaderCalls = next;
          const cap = cacheCtx.memo.loaderCap ?? LOADER_CAP;
          if (heavyNoCache && next > cap) {
            cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
            if (!cacheCtx.memo.flags.has("loader-capped")) {
              logger.warn("read:loader-calls-capped", { cap });
              cacheCtx.memo.flags.add("loader-capped");
              cacheCtx.memo.flags.add("closure-timeout");
            }
            return undefined;
          }
        }
        return await limiter.run("do:getObject", async () => {
          countSubrequest(cacheCtx);
          return await baseLoader(oid);
        });
      };
      const fs = createMemPackFs(files, { looseLoader });

      for (let idx = 0; idx < order.length; idx += BATCH) {
        const batch = order.slice(idx, idx + BATCH).filter((k) => !loaded.has(k));
        await Promise.all(
          batch.map(async (key) => {
            try {
              // Skip download if already present in the shared files map
              const base = key.split("/").pop()!;
              const idxBase = base.replace(/\.pack$/i, ".idx");
              if (
                files.has(`/git/objects/pack/${base}`) &&
                files.has(`/git/objects/pack/${idxBase}`)
              ) {
                loaded.add(key);
                return;
              }
              const ok = await addPackToFiles(env, key, files);
              if (ok) loaded.add(key);
            } catch {}
          })
        );
        if (files.size === 0) continue;
        try {
          const result = (await git.readObject({ fs, dir, oid: oidLc, format: "content" })) as {
            object: Uint8Array;
            type: "blob" | "tree" | "commit" | "tag";
          };
          if (cacheCtx?.memo) {
            cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
            if (!cacheCtx.memo.flags.has("object-read-logged")) {
              logger.debug("object-read", {
                source: "r2-packs",
                chosenPackKey,
                packsLoaded: files.size,
                type: result.type,
              });
              cacheCtx.memo.flags.add("object-read-logged");
            }
          } else {
            logger.debug("object-read", {
              source: "r2-packs",
              chosenPackKey,
              packsLoaded: files.size,
              type: result.type,
            });
          }
          // Store in per-request memo for subsequent reads within the same request
          if (cacheCtx?.memo) {
            cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
            cacheCtx.memo.objects.set(oidLc, { type: result.type, payload: result.object });
          }
          return { type: result.type, payload: result.object };
        } catch (e) {
          logger.debug("git-readObject-miss", {
            error: String(e),
            oid: oidLc,
            packsTried: files.size,
          });
          // continue to load next batch
        }
      }
      return undefined;
    } catch (e) {
      logger.debug("loadFromPacks:error", { error: String(e) });
      return undefined;
    }
  };

  // Helper function to load from Durable Object state (preferred for recent objects)
  const loadFromState = async (): Promise<{ type: string; payload: Uint8Array } | undefined> => {
    try {
      const z = await limiter.run("do:getObject", async () => {
        if (!countSubrequest(cacheCtx)) {
          logger.warn("soft-budget-exhausted", { op: "do:getObject", oid: oidLc });
          return null;
        }
        return await stub.getObject(oidLc);
      });
      if (z) {
        const parsed = await inflateAndParseHeader(z instanceof Uint8Array ? z : new Uint8Array(z));
        if (parsed) {
          logger.debug("object-read", { source: "do-state", type: parsed.type });
          return { type: parsed.type, payload: parsed.payload };
        }
      } else {
        logger.debug("do-state-miss", { oid: oidLc });
      }
    } catch (e) {
      logger.debug("do:getObject:error", { error: String(e), oid: oidLc });
      return undefined;
    }
  };

  // Main flow: Use Cache API wrapper if context available, otherwise direct load
  if (cacheCtx) {
    const cacheKey = buildObjectCacheKey(cacheCtx.req, repoId, oidLc);
    const bypassCacheRead = cacheCtx.memo?.flags?.has("no-cache-read") === true;
    const doLoad = async (): Promise<{ type: string; payload: Uint8Array } | undefined> => {
      // In heavy phases, avoid per-oid DO state reads which can exceed subrequest budgets.
      if (heavyNoCache) {
        // Go straight to pack path.
        const res = await loadFromPacks();
        if (res && cacheCtx?.memo) {
          cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
          cacheCtx.memo.objects.set(oidLc, res);
        }
        return res;
      }

      // Normal phase: Try loose object via DO first (preferred)
      const stateResult = await loadFromState();
      if (stateResult) {
        if (cacheCtx?.memo) {
          cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
          cacheCtx.memo.objects.set(oidLc, stateResult);
        }
        return stateResult;
      }

      // If DO fetch fails, try R2 packs
      const res = await loadFromPacks();
      if (res && cacheCtx?.memo) {
        cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
        cacheCtx.memo.objects.set(oidLc, res);
      }
      return res;
    };

    if (!bypassCacheRead) {
      const loaded = await cacheOrLoadObject(cacheKey, doLoad, cacheCtx.ctx);
      if (loaded && cacheCtx?.memo) {
        cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
        cacheCtx.memo.objects.set(oidLc, loaded);
      }
      return loaded;
    }

    // Bypass Cache API read: perform load and optionally write to cache in background
    const loaded = await doLoad();
    if (loaded && !heavyNoCache) {
      try {
        const savePromise = cachePutObject(cacheKey, loaded.type, loaded.payload);
        cacheCtx.ctx?.waitUntil?.(savePromise);
      } catch {}
    }
    return loaded;
  }

  // No cache context: Try DO state first, then fall back to R2 packs
  {
    const stateResult = await loadFromState();
    if (stateResult) return stateResult;
    return await loadFromPacks();
  }
}
