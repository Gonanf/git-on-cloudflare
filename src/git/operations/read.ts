import type { HeadInfo, Ref } from "./engine.ts";
import { parseCommitText } from "@/git/core/commitParse.ts";
import { getRepoStub } from "@/common/stub.ts";
import { packIndexKey } from "@/keys.ts";
import { createMemPackFs } from "@/git/pack/unpack.ts";
import { createStubLooseLoader } from "@/git/pack/loose-loader.ts";
import { createInflateStream } from "@/common/compression.ts";
import { buildObjectCacheKey, cacheOrLoadObject, type CacheContext } from "@/cache/cache.ts";
import * as git from "isomorphic-git";

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
 */
export async function getHeadAndRefs(
  env: Env,
  repoId: string
): Promise<{ head: HeadInfo | undefined; refs: Ref[] }> {
  const stub = getRepoStub(env, repoId);
  const [headRes, refsRes] = await Promise.all([
    stub.fetch("https://do/head", { method: "GET" }),
    stub.fetch("https://do/refs", { method: "GET" }),
  ]);
  const head = headRes.ok ? ((await headRes.json()) as HeadInfo) : undefined;
  const refs = refsRes.ok ? ((await refsRes.json()) as Ref[]) : [];
  return { head, refs };
}

/**
 * Resolve a ref-ish to a 40-char commit OID.
 *
 * - Accepts: `HEAD`, fully qualified refs (e.g., `refs/heads/main`), short names (e.g., `main` or `v1.0`),
 *   or a 40-hex OID (in which case it is normalized to lowercase).
 * - Resolution order for short names: branches first (`refs/heads/*`), then tags (`refs/tags/*`).
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param refOrOid - Reference, short name, or 40-char OID
 * @returns Resolved commit OID (lowercase) or undefined if not found
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

export interface CommitInfo {
  oid: string;
  tree: string;
  parents: string[];
  author?: { name: string; email: string; when: number; tz: string };
  committer?: { name: string; email: string; when: number; tz: string };
  message: string;
}

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

// Signature parsing moved to ./git/commitParse and used via parseCommitText

export async function listCommits(
  env: Env,
  repoId: string,
  start: string,
  max = 50,
  cacheCtx?: CacheContext
): Promise<CommitInfo[]> {
  let oid = await resolveRef(env, repoId, start);
  if (!oid && /^[0-9a-f]{40}$/i.test(start)) oid = start.toLowerCase();
  // Peel annotated tags to their target commit
  if (oid) {
    const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
    if (obj && obj.type === "tag") {
      const text = new TextDecoder().decode(obj.payload);
      const m = text.match(/^object ([0-9a-f]{40})/m);
      if (m) oid = m[1];
    }
  }
  if (!oid) throw new Error("Ref not found");
  const commits: CommitInfo[] = [];
  const seen = new Set<string>();
  while (commits.length < max && oid && !seen.has(oid)) {
    seen.add(oid);
    const info = await readCommitInfo(env, repoId, oid, cacheCtx);
    commits.push(info);
    // Follow first parent chain for now
    oid = info.parents[0];
  }
  return commits;
}

export interface TreeEntry {
  mode: string;
  name: string;
  oid: string;
}

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
 * Behavior
 * - If `path` resolves to a directory: returns `{ type: 'tree', entries, base }` where `base` is the path prefix.
 * - If `path` resolves to a file: returns `{ type: 'blob', oid, content, base }`.
 * - If the blob appears too large (estimated from compressed size via HEAD), returns `{ tooLarge: true, size }` and empty content.
 * - Supports starting from a commit ref, tree OID, tag (peeled to commit), or a blob OID (only valid with empty `path`).
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param ref - Git reference (branch, tag, or commit SHA)
 * @param path - File or directory path (optional, defaults to root)
 * @returns Union: tree entries for directories or blob content/metadata for files
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
    const text = new TextDecoder().decode(startObj.payload);
    const m = text.match(/^object ([0-9a-f]{40})/m);
    if (!m) throw new Error("Unsupported object type");
    const target = m[1];
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
      const headRes = await stub.fetch(`https://do/obj/${ent.oid}`, { method: "HEAD" });
      if (!headRes.ok) throw new Error("Blob not found");

      // Content-Length is the compressed size, but we need decompressed size
      // For now, we'll use a conservative estimate (compressed * 10)
      const compressedSize = parseInt(headRes.headers.get("Content-Length") || "0", 10);
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
 * Implementation detail
 * - Performs `GET /obj/:oid` to the repo DO, then pipes through a `DecompressionStream('deflate')`.
 * - Parses and strips the Git object header (`<type> <len>\0`) on the fly.
 * - Sets a long-lived `Cache-Control` and `ETag` header for efficient caching.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Object ID (SHA-1 hash) of the blob
 * @returns Response with streaming body or null if not found
 */
export async function readBlobStream(
  env: Env,
  repoId: string,
  oid: string
): Promise<Response | null> {
  const stub = getRepoStub(env, repoId);
  const res = await stub.fetch(`https://do/obj/${oid}`, { method: "GET" });
  if (!res.ok) return null;

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
  const decompressed = res
    .body!.pipeThrough(createInflateStream())
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
 * Read and fully buffer a raw Git object (header + payload) from storage.
 *
 * - Fetches `GET /obj/:oid` from the repo DO, inflates (zlib/deflate), and parses the header
 *   (`<type> <len>\0`). Returns `{ type, payload }`.
 * - For large files, prefer `readBlobStream()` to avoid buffering in memory.
 *
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (format: "owner/repo")
 * @param oid - Object ID (SHA-1 hash)
 * @returns Decompressed object with type and payload, or undefined if not found
 */
export async function readLooseObjectRaw(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ type: string; payload: Uint8Array } | undefined> {
  const oidLc = oid.toLowerCase();

  // Helper function to load from packs
  const loadFromPacks = async () => {
    try {
      const stub = getRepoStub(env, repoId);
      // Get recent packs recorded by the DO
      const packsRes = await stub.fetch("https://do/packs", { method: "GET" });
      const packList: string[] = packsRes.ok
        ? ((await packsRes.json()) as { keys: string[] }).keys || []
        : [];
      // Fallback to just latest if packs list is unavailable
      if (packList.length === 0) {
        const metaRes = await stub.fetch("https://do/pack-latest", { method: "GET" });
        if (!metaRes.ok) return undefined;
        const meta = (await metaRes.json()) as { key: string } | null;
        if (!meta?.key) return undefined;
        packList.push(meta.key);
      }

      // Prefer the pack that contains the OID, but load multiple packs to satisfy deltas
      let chosenPackKey: string | undefined;
      const contains: Record<string, boolean> = {};
      for (const key of packList) {
        try {
          const oidsRes = await stub.fetch(`https://do/pack-oids?key=${encodeURIComponent(key)}`);
          if (!oidsRes.ok) continue;
          const data = (await oidsRes.json()) as { key: string; oids: string[] };
          const set = new Set((data.oids || []).map((x) => x.toLowerCase()));
          const has = set.has(oidLc);
          contains[key] = has;
          if (!chosenPackKey && has) chosenPackKey = key;
        } catch {}
      }
      if (!chosenPackKey) chosenPackKey = packList[0];

      // Load up to the first 5 packs (newest-first) into the in-memory fs
      const toLoad = packList.slice(0, Math.min(5, packList.length));
      if (!toLoad.includes(chosenPackKey)) toLoad.unshift(chosenPackKey);
      const seenKeys = new Set<string>();
      const uniqueLoad = toLoad.filter((k) => (seenKeys.has(k) ? false : (seenKeys.add(k), true)));
      const files = new Map<string, Uint8Array>();
      await Promise.all(
        uniqueLoad.map(async (key) => {
          try {
            const [p, i] = await Promise.all([
              env.REPO_BUCKET.get(key),
              env.REPO_BUCKET.get(packIndexKey(key)),
            ]);
            if (!p || !i) return;
            const packBuf = new Uint8Array(await p.arrayBuffer());
            const idxBuf = new Uint8Array(await i.arrayBuffer());
            const base = key.split("/").pop()!;
            const idxBase = base.replace(/\.pack$/i, ".idx");
            files.set(`/git/objects/pack/${base}`, packBuf);
            files.set(`/git/objects/pack/${idxBase}`, idxBuf);
          } catch {}
        })
      );
      if (files.size === 0) return undefined;

      // Create a loader for existing loose objects (needed for thin packs)
      const looseLoader = createStubLooseLoader(stub);

      const fs = createMemPackFs(files, { looseLoader });
      const dir = "/git";
      const result = (await git.readObject({ fs, dir, oid: oidLc, format: "content" })) as {
        object: Uint8Array;
        type: "blob" | "tree" | "commit" | "tag";
      };
      return { type: result.type, payload: result.object };
    } catch {
      return undefined;
    }
  };

  // Helper function to load from Durable Object state
  const loadFromState = async (): Promise<{ type: string; payload: Uint8Array } | undefined> => {
    try {
      const stub = getRepoStub(env, repoId);
      const res = await stub.fetch(`https://do/obj/${oidLc}`, { method: "GET" });
      if (res.ok) {
        const z = new Uint8Array(await res.arrayBuffer());
        const ds = createInflateStream();
        const stream = new Blob([z]).stream().pipeThrough(ds);
        const raw = new Uint8Array(await new Response(stream).arrayBuffer());
        // header: <type> <len>\0
        let p = 0;
        let sp = p;
        while (sp < raw.length && raw[sp] !== 0x20) sp++;
        const type = new TextDecoder().decode(raw.subarray(p, sp));
        let nul = sp + 1;
        while (nul < raw.length && raw[nul] !== 0x00) nul++;
        const payload = raw.subarray(nul + 1);
        return { type, payload };
      }
    } catch {}
    return undefined;
  };

  // Use cache helper if cache context is available
  if (cacheCtx) {
    const cacheKey = buildObjectCacheKey(cacheCtx.req, repoId, oidLc);
    return cacheOrLoadObject(
      cacheKey,
      async () => {
        // Try loose object via DO first (preferred)
        const stateResult = await loadFromState();
        if (stateResult) return stateResult;

        // If DO fetch fails, try R2 packs (fallback logic below)
        return await loadFromPacks();
      },
      cacheCtx.ctx
    );
  }

  // No request available, load without caching
  const stateResult = await loadFromState();
  if (stateResult) return stateResult;

  // Fallback: read directly from R2 packs
  return await loadFromPacks();
}
