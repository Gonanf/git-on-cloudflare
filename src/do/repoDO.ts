import { DurableObject } from "cloudflare:workers";
import {
  asTypedStorage,
  RepoStateSchema,
  objKey,
  packOidsKey,
  Head,
  UnpackWork,
} from "./repoState.ts";
import {
  doPrefix,
  r2LooseKey,
  r2PackDirPrefix,
  isPackKey,
  isIdxKey,
  packKeyFromIndexKey,
} from "@/keys.ts";
import {
  encodeGitObjectAndDeflate,
  unpackPackToLoose,
  unpackOidsChunkFromPackBytes,
  receivePack,
  parseCommitText,
} from "@/git/index.ts";
import {
  json,
  text,
  badRequest,
  createLogger,
  createInflateStream,
  UnpackProgress,
} from "@/common/index.ts";
import { setUnpackStatus } from "@/cache/index.ts";

/**
 * Repository Durable Object (per-repo authority)
 *
 * Responsibilities
 * - Acts as the strongly consistent source of truth for a single repository
 * - Stores refs and HEAD in DO storage
 * - Caches loose objects (zlib-compressed) in DO storage
 * - Mirrors loose objects to R2 under `do/<id>/objects/loose/<oid>` for cheap reads
 * - Writes received packfiles to R2 under `do/<id>/objects/pack/*.pack` (and .idx)
 * - Exposes focused internal HTTP endpoints:
 *   - `POST /receive` — receive-pack implementation (delegates to do/receivePack.ts)
 *   - `POST /reindex` — reindex latest pack (diagnostic)
 * - All other operations are provided as typed RPC methods on the class.
 *
 * Read Path (RPC)
 * - Loose object reads are exposed via RPC methods such as `getObjectStream()` and `getObject()`.
 * - Reads prefer R2 (range-friendly and cheap) and fall back to DO storage if missing.
 * - There is no public HTTP endpoint for object reads; this reduces the attack surface and
 *   keeps all internal state access typed and testable.
 *
 * Write Path
 * - Loose object writes: DO storage first, then mirror to R2 via `r2LooseKey()`.
 * - Pushes: `POST /receive` stores the raw `.pack` to R2 (under the DO prefix) and performs a
 *   fast index-only step to produce `.idx`. It then queues asynchronous unpack work which runs in
 *   small time-budgeted chunks under the DO `alarm()` (mirrors loose objects to R2 as it goes).
 *   Pack metadata is maintained to enable efficient fetch assembly.
 *
 * Maintenance & Background Work
 * - `alarm()` combines three duties:
 *   1) Unpack work: Process pending pack objects in time-limited chunks to avoid long blocking.
 *   2) Idle cleanup: If a repo looks empty and idle long enough, purge DO storage and its R2 prefix.
 *   3) Pack maintenance: Periodically prune old pack files in R2 and their metadata in DO.
 * - Listing/sweeping uses helpers from `keys.ts` to avoid path mismatches.
 */
export class RepoDurableObject extends DurableObject {
  declare env: Env;
  // Throttle lastAccessMs writes to storage to reduce per-request write amplification
  private lastAccessMemMs: number | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.lastAccessMemMs = await ctx.storage.get("lastAccessMs");
      await this.ensureAccessAndAlarm();
    });
  }

  // Thin request router: delegates to focused handlers below.
  // Keep this mapping explicit and small so behavior is easy to audit.
  async fetch(request: Request): Promise<Response> {
    // Touch access and (re)schedule an idle cleanup alarm
    try {
      await this.touchAndMaybeSchedule();
    } catch {}
    const url = new URL(request.url);
    this.logger.debug("fetch", { path: url.pathname, method: request.method });
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);

    // Dev: reindex the latest pack from R2 into loose objects
    // Note: kept as an HTTP endpoint for convenience during development and
    // manual debugging. Production flows should rely on background unpack
    // work scheduled via receive-pack and the DO alarm loop.
    if (url.pathname === "/reindex" && request.method === "POST") {
      return this.handleReindexPost();
    }

    // Receive-pack: parse update commands section and packfile, store pack to R2,
    // update refs atomically if valid, and respond with report-status. This remains
    // on HTTP (instead of RPC) to preserve streaming semantics end-to-end without
    // buffering the pack in memory.
    if (url.pathname === "/receive" && request.method === "POST") {
      return this.handleReceive(request);
    }

    return text("Not found\n", 404);
  }

  public async listRefs(): Promise<{ name: string; oid: string }[]> {
    await this.ensureAccessAndAlarm();
    return await this.getRefs();
  }

  public async setRefs(refs: { name: string; oid: string }[]): Promise<void> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    await store.put("refs", refs);
  }

  public async getHead(): Promise<Head> {
    await this.ensureAccessAndAlarm();
    return await this.resolveHead();
  }

  public async setHead(head: Head): Promise<void> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    await store.put("head", head);
  }

  public async getHeadAndRefs(): Promise<{ head: Head; refs: { name: string; oid: string }[] }> {
    await this.ensureAccessAndAlarm();
    const [head, refs] = await Promise.all([this.resolveHead(), this.getRefs()]);
    return { head, refs };
  }

  public async getObjectStream(oid: string): Promise<ReadableStream | null> {
    await this.ensureAccessAndAlarm();
    if (!this.isValidOid(oid)) return null;
    // Try R2 first
    try {
      const obj = await this.env.REPO_BUCKET.get(r2LooseKey(this.prefix(), oid));
      if (obj) return obj.body;
    } catch {}
    // Fallback to DO storage
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const data = await store.get(objKey(oid));
    if (!data) return null;
    // IMPORTANT: This stream contains the Git object in its zlib-compressed form
    // including the Git header. Callers that want the raw payload should pipe
    // through `createInflateStream()` and strip the header ("<type> <len>\0").
    return new ReadableStream({
      start(controller) {
        controller.enqueue(data);
        controller.close();
      },
    });
  }

  public async getObject(oid: string): Promise<ArrayBuffer | Uint8Array | null> {
    await this.ensureAccessAndAlarm();
    if (!this.isValidOid(oid)) return null;
    // Try R2 first
    try {
      const obj = await this.env.REPO_BUCKET.get(r2LooseKey(this.prefix(), oid));
      if (obj) return new Uint8Array(await obj.arrayBuffer());
    } catch {}
    // Fallback to DO storage
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const data = await store.get(objKey(oid));
    return data || null;
  }

  public async hasLoose(oid: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    if (!this.isValidOid(oid)) return false;
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const data = await store.get(objKey(oid));
    if (data) return true;
    try {
      const head = await this.env.REPO_BUCKET.head(r2LooseKey(this.prefix(), oid));
      return !!head;
    } catch {}
    return false;
  }

  /**
   * Batch membership check for loose objects. Returns an array of booleans aligned with input OIDs.
   * Uses small concurrency for R2 HEADs; DO storage checks are performed directly.
   */
  public async hasLooseBatch(oids: string[]): Promise<boolean[]> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const prefix = this.prefix();
    const env = this.env;

    // Short-circuit using recent pack membership to avoid R2 HEADs
    // Build a small set of OIDs from the newest packs we know about.
    const packSet = new Set<string>();
    try {
      const last = ((await store.get("lastPackOids")) || []) as string[];
      for (const x of last) packSet.add(x.toLowerCase());
      // Include a couple more recent packs if available
      const list = (((await store.get("packList")) || []) as string[]).slice(0, 2);
      for (const k of list) {
        try {
          const arr = ((await store.get(packOidsKey(k))) || []) as string[];
          for (const x of arr) packSet.add(x.toLowerCase());
        } catch {}
      }
    } catch {}

    const checkOne = async (oid: string): Promise<boolean> => {
      if (!/^[0-9a-f]{40}$/i.test(oid)) return false;
      // 1) Fast-path: known to be present in a recent pack
      if (packSet.size > 0 && packSet.has(oid.toLowerCase())) return true;
      // 2) DO state (loose) lookup
      const data = await store.get(objKey(oid));
      if (data) return true;
      try {
        // 3) R2 loose HEAD fallback
        const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, oid));
        return !!head;
      } catch {
        return false;
      }
    };

    const MAX = 16;
    const out: boolean[] = [];
    for (let i = 0; i < oids.length; i += MAX) {
      const part = oids.slice(i, i + MAX);
      const res = await Promise.all(part.map((oid) => checkOne(oid)));
      out.push(...res);
    }
    return out;
  }

  public async getPackLatest(): Promise<{ key: string; oids: string[] } | null> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const key = await store.get("lastPackKey");
    if (!key) return null;
    const oids = ((await store.get("lastPackOids")) || []).slice(0, 10000);
    return { key, oids };
  }

  public async getPacks(): Promise<string[]> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const list = ((await store.get("packList")) || []).slice(0, 20);
    return list;
  }

  public async getPackOids(key: string): Promise<string[]> {
    await this.ensureAccessAndAlarm();
    if (!key) return [];
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const oids = (await store.get(packOidsKey(key))) || [];
    return oids;
  }

  public async getUnpackProgress(): Promise<UnpackProgress> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const work = await store.get("unpackWork");
    const nextKey = await store.get("unpackNext");
    if (!work) return { unpacking: false, queuedCount: nextKey ? 1 : 0 } as UnpackProgress;
    return {
      unpacking: true,
      processed: work.processedCount,
      total: work.oids.length,
      percent: Math.round((work.processedCount / work.oids.length) * 100),
      currentPackKey: work.packKey,
      queuedCount: nextKey ? 1 : 0,
    } as UnpackProgress;
  }

  public async debugState(): Promise<{
    meta: { doId: string; prefix: string };
    head?: Head;
    refsCount: number;
    refs: { name: string; oid: string }[];
    lastPackKey: string | null;
    lastPackOidsCount: number;
    packListCount: number;
    packList: string[];
    unpackWork: UnpackWork | null;
    unpackNext: string | null;
    looseSample: string[];
  }> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const refs = (await store.get("refs")) ?? [];
    const head = await store.get("head");
    const lastPackKey = await store.get("lastPackKey");
    const lastPackOids = (await store.get("lastPackOids")) ?? [];
    const packList = (await store.get("packList")) ?? [];
    const unpackWork = await store.get("unpackWork");
    const unpackNext = await store.get("unpackNext");

    const looseSample: string[] = [];
    try {
      const it = await this.ctx.storage.list({ prefix: "obj:", limit: 10 });
      for (const k of it.keys()) looseSample.push(String(k).slice(4));
    } catch {}

    return {
      meta: { doId: this.ctx.id.toString(), prefix: this.prefix() },
      head,
      refsCount: refs.length,
      refs: refs.slice(0, 20),
      lastPackKey: lastPackKey || null,
      lastPackOidsCount: lastPackOids.length,
      packListCount: packList.length,
      packList,
      unpackWork: unpackWork || null,
      unpackNext: unpackNext || null,
      looseSample,
    };
  }

  public async debugCheckCommit(commit: string): Promise<{
    commit: { oid: string; parents: string[]; tree?: string };
    presence: { hasLooseCommit: boolean; hasLooseTree: boolean; hasR2LooseTree: boolean };
    membership: Record<string, { hasCommit: boolean; hasTree: boolean }>;
  }> {
    await this.ensureAccessAndAlarm();
    const q = (commit || "").toLowerCase();
    if (!/^[0-9a-f]{40}$/.test(q)) {
      throw new Error("Invalid commit");
    }

    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const packList = (await store.get("packList")) ?? [];
    const membership: Record<string, { hasCommit: boolean; hasTree: boolean }> = {};
    for (const key of packList) {
      try {
        const oids = (await store.get(packOidsKey(key))) ?? [];
        const set = new Set(oids.map((x) => x.toLowerCase()));
        membership[key] = { hasCommit: set.has(q), hasTree: false };
      } catch {}
    }

    let tree: string | undefined = undefined;
    let parents: string[] = [];
    try {
      const info = await this.readCommitFromStore(q);
      if (info) {
        tree = info.tree.toLowerCase();
        parents = info.parents;
      }
    } catch {}

    const hasLooseCommit = !!(await this.ctx.storage.get(objKey(q)));
    let hasLooseTree = false;
    let hasR2LooseTree = false;
    if (tree) {
      hasLooseTree = !!(await this.ctx.storage.get(objKey(tree)));
      try {
        const head = await this.env.REPO_BUCKET.head(r2LooseKey(this.prefix(), tree));
        hasR2LooseTree = !!head;
      } catch {}
      for (const key of Object.keys(membership)) {
        try {
          const oids = (await store.get(packOidsKey(key))) ?? [];
          const set = new Set(oids.map((x) => x.toLowerCase()));
          membership[key].hasTree = !!tree && set.has(tree);
        } catch {}
      }
    }

    return {
      commit: { oid: q, parents, tree },
      presence: { hasLooseCommit, hasLooseTree, hasR2LooseTree },
      membership,
    };
  }

  /**
   * RPC: Seed a minimal repository with an empty tree and a single commit pointing to it.
   * Used by tests to initialize a valid repo state without using HTTP fetch routes.
   */
  public async seedMinimalRepo(): Promise<{ commitOid: string; treeOid: string }> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    // Build empty tree object (content is empty)
    const treeContent = new Uint8Array(0);
    const { oid: treeOid, zdata: treeZ } = await encodeGitObjectAndDeflate("tree", treeContent);

    // Build a simple commit pointing to empty tree
    const author = `You <you@example.com> 0 +0000`;
    const committer = author;
    const msg = "initial\n";
    const commitPayload =
      `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n` + `\n${msg}`;
    const { oid: commitOid, zdata: commitZ } = await encodeGitObjectAndDeflate(
      "commit",
      new TextEncoder().encode(commitPayload)
    );

    // Store objects and update refs
    await store.put(objKey(treeOid), treeZ);
    await store.put(objKey(commitOid), commitZ);
    await store.put("refs", [{ name: "refs/heads/main", oid: commitOid }]);
    await store.put("head", { target: "refs/heads/main" });

    return { treeOid, commitOid };
  }

  /**
   * RPC: Store a loose object (zlib-compressed with Git header) by its OID.
   * Mirrors to R2 best-effort. Throws on invalid OID.
   */
  public async putLooseObject(oid: string, zdata: Uint8Array): Promise<void> {
    await this.ensureAccessAndAlarm();
    if (!this.isValidOid(oid)) throw new Error("Bad oid");
    await this.storeObject(oid, zdata);
  }

  // --- Alarm-based tasks ---
  // Combines three responsibilities:
  // 1) Unpack work: Process pending pack objects in chunks to avoid blocking
  // 2) Idle cleanup: If the DO remains idle beyond IDLE_MS and appears empty/unused, purge storage and R2 mirror.
  // 3) Maintenance: Periodically prune stale packs and metadata even for active repos.
  async alarm(): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    this.logger.debug("alarm:start", {});

    // Priority 1: Handle pending unpack work
    if (await this.handleUnpackWork(store)) {
      return; // Exit early to let unpack continue
    }

    // Priority 2: Check for idle cleanup or maintenance needs
    await this.handleIdleAndMaintenance(store);

    this.logger.debug("alarm:end", {});
  }

  /**
   * Processes pending unpack work from the queue.
   * @param store - The typed storage instance
   * @returns true if unpack work was found and processed, false otherwise
   */
  private async handleUnpackWork(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>
  ): Promise<boolean> {
    const unpackWork = await store.get("unpackWork");
    if (!unpackWork) return false;

    try {
      await this.processUnpackChunk(unpackWork);
    } catch (e) {
      this.logger.error("alarm:process-unpack-error", { error: String(e) });
      // Best-effort reschedule so we don't get stuck
      try {
        await this.ctx.storage.setAlarm(Date.now() + 1000);
      } catch {}
    }
    return true;
  }

  /**
   * Handles idle cleanup and periodic maintenance tasks.
   * Checks if the repository should be cleaned up due to idleness,
   * and performs periodic maintenance (pack pruning) if due.
   * @param store - The typed storage instance
   */
  private async handleIdleAndMaintenance(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>
  ): Promise<void> {
    try {
      const cfg = this.getConfig();
      const now = Date.now();
      const lastAccess = await store.get("lastAccessMs");
      const lastMaint = await store.get("lastMaintenanceMs");

      // Check if idle cleanup is needed
      if (await this.shouldCleanupIdle(store, cfg.idleMs, lastAccess)) {
        await this.performIdleCleanup();
        return;
      }

      // Check if maintenance is due
      if (this.isMaintenanceDue(lastMaint, now, cfg.maintMs)) {
        await this.performMaintenance(store, cfg.keepPacks, now);
      }

      // Schedule next alarm
      await this.scheduleNextAlarm(lastAccess, lastMaint, now, cfg.idleMs, cfg.maintMs);
    } catch (e) {
      this.logger.error("alarm:error", { error: String(e) });
    }
  }

  /**
   * Determines if the repository should be cleaned up due to idleness.
   * A repo is considered for cleanup if it's been idle beyond the threshold
   * AND appears empty (no refs, unborn/missing HEAD, no packs).
   * @param store - The typed storage instance
   * @param idleMs - Idle threshold in milliseconds
   * @param lastAccess - Last access timestamp
   * @returns true if cleanup should proceed
   */
  private async shouldCleanupIdle(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
    idleMs: number,
    lastAccess: number | undefined
  ): Promise<boolean> {
    const now = Date.now();
    const idleExceeded = !lastAccess || now - lastAccess >= idleMs;
    if (!idleExceeded) return false;

    // Check if repo looks empty
    const refs = (await store.get("refs")) ?? [];
    const head = await store.get("head");
    const lastPackKey = await store.get("lastPackKey");

    return refs.length === 0 && (!head || head.unborn || !head.target) && !lastPackKey;
  }

  /**
   * Performs complete cleanup of an idle repository.
   * Deletes all DO storage and purges the R2 mirror.
   */
  private async performIdleCleanup(): Promise<void> {
    const storage = this.ctx.storage;

    // Purge DO storage
    try {
      await storage.deleteAll();
    } catch (e) {
      this.logger.error("cleanup:delete-storage-failed", { error: String(e) });
    }

    // Purge R2 mirror
    await this.purgeR2Mirror();

    // Clear the alarm after cleanup
    try {
      await storage.deleteAlarm();
    } catch (e) {
      this.logger.warn("cleanup:delete-alarm-failed", { error: String(e) });
    }
  }

  /**
   * Purges all R2 objects under this DO's prefix.
   * Continues even if individual deletes fail.
   */
  private async purgeR2Mirror(): Promise<void> {
    try {
      const prefix = this.prefix();
      const pfx = `${prefix}/`;
      let cursor: string | undefined = undefined;

      do {
        const res: R2Objects = await this.env.REPO_BUCKET.list({ prefix: pfx, cursor });
        const objects: R2Object[] = (res && res.objects) || [];

        for (const obj of objects) {
          try {
            await this.env.REPO_BUCKET.delete(obj.key);
          } catch (e) {
            this.logger.warn("cleanup:delete-r2-object-failed", {
              key: obj.key,
              error: String(e),
            });
          }
        }

        cursor = res.truncated ? res.cursor : undefined;
      } while (cursor);
    } catch (e) {
      this.logger.error("cleanup:purge-r2-failed", { error: String(e) });
    }
  }

  private isMaintenanceDue(lastMaint: number | undefined, now: number, maintMs: number): boolean {
    return !lastMaint || now - lastMaint >= maintMs;
  }

  private async performMaintenance(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
    keepPacks: number,
    now: number
  ): Promise<void> {
    try {
      // Deletes older packs beyond the keep-window from both DO metadata and R2,
      // and keeps `lastPackKey/lastPackOids` consistent.
      await this.runMaintenance(keepPacks);
      await store.put("lastMaintenanceMs", now);
    } catch (e) {
      this.logger.error("maintenance:failed", { error: String(e) });
    }
  }

  private async scheduleNextAlarm(
    lastAccess: number | undefined,
    lastMaint: number | undefined,
    now: number,
    idleMs: number,
    maintMs: number
  ): Promise<void> {
    try {
      const nextIdleAt = (lastAccess ?? now) + idleMs;
      const nextMaintAt = (lastMaint ?? now) + maintMs;
      const next = Math.min(nextIdleAt, nextMaintAt);
      await this.ctx.storage.setAlarm(next);
      this.logger.debug("alarm:scheduled", { next });
    } catch (e) {
      this.logger.error("alarm:set-failed", { error: String(e) });
    }
  }

  private async touchAndMaybeSchedule(): Promise<void> {
    const cfg = this.getConfig();
    const now = Date.now();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);

    // Update last access time with throttling (max once per 60s)
    try {
      if (!this.lastAccessMemMs || now - this.lastAccessMemMs >= 60_000) {
        await store.put("lastAccessMs", now);
        this.lastAccessMemMs = now;
      }
    } catch {}

    try {
      // Check if we need to prioritize unpack work
      if (await this.scheduleUnpackAlarmIfNeeded(store, cfg, now)) {
        return;
      }

      // Otherwise schedule regular idle/maintenance alarm
      await this.scheduleRegularAlarm(store, cfg, now);
    } catch (e) {
      this.logger.error("alarm:schedule-failed", { error: String(e) });
    }
  }

  private async scheduleUnpackAlarmIfNeeded(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
    cfg: ReturnType<typeof this.getConfig>,
    now: number
  ): Promise<boolean> {
    const work = await store.get("unpackWork");
    const next = await store.get("unpackNext");
    if (!work && !next) return false;

    const existing = (await this.ctx.storage.getAlarm()) as number | null | undefined;
    const soon = now + cfg.unpackDelayMs;

    if (!existing || existing < now || existing > soon) {
      await this.ctx.storage.setAlarm(soon);
    }
    return true;
  }

  private async scheduleRegularAlarm(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
    cfg: ReturnType<typeof this.getConfig>,
    now: number
  ): Promise<void> {
    const lastMaint = await store.get("lastMaintenanceMs");
    const existing = await this.ctx.storage.getAlarm();

    const nextIdle = now + cfg.idleMs;
    const nextMaint = (lastMaint ?? now) + cfg.maintMs;
    const next = Math.min(nextIdle, nextMaint);

    if (!existing || existing < now || existing > next) {
      await this.ctx.storage.setAlarm(next);
    }
  }

  /**
   * Safe wrapper around touchAndMaybeSchedule() for RPC and internal entrypoints.
   * Ensures last access time is updated and an alarm is scheduled without
   * forcing every method to duplicate try/catch.
   */
  private async ensureAccessAndAlarm(): Promise<void> {
    try {
      await this.touchAndMaybeSchedule();
    } catch (e) {
      try {
        this.logger.warn("touch:schedule:failed", { error: String(e) });
      } catch {}
    }
  }

  // Read and parse a commit object directly from DO storage (fallback to R2 if needed)
  private async readCommitFromStore(oid: string): Promise<{
    oid: string;
    tree: string;
    parents: string[];
    author?: { name: string; email: string; when: number; tz: string };
    committer?: { name: string; email: string; when: number; tz: string };
    message: string;
  } | null> {
    // Prefer DO-stored loose object to avoid R2/HTTP hops
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    let data = await store.get(objKey(oid));

    if (!data) {
      // Fallback: try R2-stored loose copy
      try {
        const obj = await this.env.REPO_BUCKET.get(r2LooseKey(this.prefix(), oid));
        if (obj) data = await obj.arrayBuffer();
      } catch {}
    }
    if (!data) return null;

    const z = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
    // Decompress (zlib/deflate) and parse git header
    const ds = createInflateStream();
    const stream = new Blob([z]).stream().pipeThrough(ds);
    const raw = new Uint8Array(await new Response(stream).arrayBuffer());
    // header: <type> <len>\0
    let p = 0;
    let sp = p;
    while (sp < raw.length && raw[sp] !== 0x20) sp++;
    const type = new TextDecoder().decode(raw.subarray(p, sp));
    if (type !== "commit") return null;
    let nul = sp + 1;
    while (nul < raw.length && raw[nul] !== 0x00) nul++;
    const payload = raw.subarray(nul + 1);
    const text = new TextDecoder().decode(payload);

    const parsed = parseCommitText(text);
    return { oid, ...parsed };
  }

  /**
   * Retrieves all refs from storage.
   * @returns Array of ref objects with name and oid, or empty array if none exist
   */
  private async getRefs(): Promise<{ name: string; oid: string }[]> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    return (await store.get("refs")) ?? [];
  }

  /**
   * Resolves the current HEAD state by looking up the target ref.
   * @returns The resolved HEAD object with target and either oid or unborn flag
   */
  private async resolveHead(): Promise<Head> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const stored = await store.get("head");
    const refs = await this.getRefs();

    // Determine target (default to main)
    const target = stored?.target || "refs/heads/main";
    const match = refs.find((r) => r.name === target);
    const resolved = match
      ? ({ target, oid: match.oid } as Head)
      : ({ target, unborn: true } as Head);

    // Persist resolved head only if it changed
    await this.updateHeadIfChanged(store, stored, resolved);

    return resolved;
  }

  /**
   * Updates HEAD in storage only if the resolved value differs semantically.
   * Handles normalization of legacy HEAD shapes (e.g., both oid and unborn present).
   * @param store - The typed storage instance
   * @param stored - The currently stored HEAD value
   * @param resolved - The newly resolved HEAD value
   */
  private async updateHeadIfChanged(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
    stored: Head | undefined,
    resolved: Head
  ): Promise<void> {
    try {
      const storedOid = stored?.oid ?? undefined;
      const resolvedOid = resolved.oid ?? undefined;
      const sameTarget = !!stored && stored.target === resolved.target;
      const sameOid = storedOid === resolvedOid;
      const sameUnborn =
        storedOid || resolvedOid ? true : (stored?.unborn === true) === (resolved.unborn === true);
      const same = !!stored && sameTarget && sameOid && sameUnborn;

      if (!same) {
        await store.put("head", resolved);
      }
    } catch {}
  }

  private async handleReindexPost(): Promise<Response> {
    await this.ensureAccessAndAlarm();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const key = await store.get("lastPackKey");
    if (!key) return text("No pack to reindex\n", 404);
    const obj = await this.env.REPO_BUCKET.get(key);
    if (!obj) return text("Pack not found in R2\n", 404);
    const bytes = new Uint8Array(await obj.arrayBuffer());
    this.logger.info("reindex:start", { key });
    await unpackPackToLoose(bytes, this.ctx, this.env, this.prefix(), key);
    this.logger.info("reindex:done", { key });
    return text("OK\n");
  }

  private isValidOid(oid: string): boolean {
    return /^[0-9a-f]{40}$/i.test(oid);
  }

  public async getObjectSize(oid: string): Promise<number | null> {
    await this.ensureAccessAndAlarm();
    const prefix = this.prefix();
    const r2key = r2LooseKey(prefix, oid);

    // Try R2 first
    try {
      const obj = await this.env.REPO_BUCKET.head(r2key);
      if (obj) return obj.size;
    } catch {}

    // Fallback to DO storage
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const data = await store.get(objKey(oid));

    if (!data) return null;
    return data.byteLength;
  }

  public async getObjectContent(oid: string): Promise<BodyInit | null> {
    await this.ensureAccessAndAlarm();
    const prefix = this.prefix();
    const r2key = r2LooseKey(prefix, oid);

    // Try R2 first
    try {
      const obj = await this.env.REPO_BUCKET.get(r2key);
      if (obj) return obj.body;
    } catch {}

    // Fallback to DO storage
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const data = await store.get(objKey(oid));

    return data || null;
  }

  private async storeObject(oid: string, bytes: Uint8Array): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);

    // Store in DO storage
    await store.put(objKey(oid), bytes);

    // Best-effort mirror to R2
    await this.mirrorObjectToR2(oid, bytes);
  }

  private async mirrorObjectToR2(oid: string, bytes: Uint8Array): Promise<void> {
    const r2key = r2LooseKey(this.prefix(), oid);
    try {
      // Mirrors the compressed loose object to R2 for low-latency reads.
      // We do not fail the write if mirroring fails; DO storage remains the
      // source of truth and R2 will be filled by subsequent writes.
      await this.env.REPO_BUCKET.put(r2key, bytes);
    } catch {}
  }

  private async handleReceive(request: Request) {
    // Delegate to extracted implementation for clarity and testability.
    this.logger.info("receive:start", {});
    // Pre-body guard: block when current unpack is running and a next pack is already queued
    try {
      const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
      const work = await store.get("unpackWork");
      const next = await store.get("unpackNext");
      if (work && next) {
        this.logger.warn("receive:block-busy", { retryAfter: 10 });
        return new Response("Repository is busy unpacking; please retry shortly.\n", {
          status: 503,
          headers: {
            "Retry-After": "10",
            "Content-Type": "text/plain; charset=utf-8",
          },
        });
      }
    } catch {}
    const res = await receivePack(this.ctx, this.env, this.prefix(), request);
    this.logger.info("receive:end", { status: res.status });
    return res;
  }

  private prefix() {
    // Tests and R2 layout expect Durable Object data under the 'do/<id>' prefix
    return doPrefix(this.ctx.id.toString());
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "RepoDO",
      doId: this.ctx.id.toString(),
    });
  }

  private getConfig() {
    // Parse configuration from env vars with sensible defaults.
    // All values are validated and clamped to safe ranges.
    const idleMins = Number(this.env.REPO_DO_IDLE_MINUTES ?? 30);
    const maintMins = Number(this.env.REPO_DO_MAINT_MINUTES ?? 60 * 24);
    const keepPacks = Number(this.env.REPO_KEEP_PACKS ?? 3);
    const packListMaxRaw = Number(this.env.REPO_PACKLIST_MAX ?? 20);
    const unpackChunkSize = Number(this.env.REPO_UNPACK_CHUNK_SIZE ?? 50);
    const unpackMaxMs = Number(this.env.REPO_UNPACK_MAX_MS ?? 5000);
    const unpackDelayMs = Number(this.env.REPO_UNPACK_DELAY_MS ?? 100);
    const unpackBackoffMs = Number(this.env.REPO_UNPACK_BACKOFF_MS ?? 1000);
    const clamp = (n: number, min: number, max: number) =>
      Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
    const packListMax = clamp(packListMaxRaw, 1, 100);
    return {
      idleMs: clamp(idleMins, 1, 60 * 24 * 7) * 60 * 1000,
      maintMs: clamp(maintMins, 5, 60 * 24 * 30) * 60 * 1000,
      // keepPacks cannot exceed the recent-list window (packListMax)
      keepPacks: clamp(keepPacks, 1, packListMax),
      packListMax,
      unpackChunkSize: clamp(unpackChunkSize, 1, 1000),
      unpackMaxMs: clamp(unpackMaxMs, 50, 30_000),
      unpackDelayMs: clamp(unpackDelayMs, 10, 10_000),
      unpackBackoffMs: clamp(unpackBackoffMs, 100, 60_000),
    };
  }

  private async runMaintenance(keepPacks: number) {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    // Ensure packList exists
    const packList = (await store.get("packList")) ?? [];
    if (packList.length === 0) return;
    // Determine which packs to keep.
    // packList is maintained newest-first (most recent at index 0), so keep the first N.
    const keep = packList.slice(0, keepPacks);
    const keepSet = new Set(keep);
    const removed = packList.filter((k) => !keepSet.has(k));
    const newList = packList.filter((k) => keepSet.has(k));
    // Trim packList in storage while preserving additional kept keys
    if (removed.length > 0) await store.put("packList", newList);
    // Adjust lastPackKey/lastPackOids if needed
    const lastPackKey = await store.get("lastPackKey");
    if (!lastPackKey || !keepSet.has(lastPackKey)) {
      // Choose the newest kept pack as the latest reference
      const newest = keep[0];
      if (newest) {
        await store.put("lastPackKey", newest);
        const oids = ((await store.get("lastPackOids")) || []).slice(0, 10000);
        // Try to load oids for the newest from packOids:<key> if present
        const alt = await store.get(packOidsKey(newest));
        await store.put("lastPackOids", alt ?? oids);
      } else {
        // No packs remain
        await store.delete("lastPackKey");
        await store.delete("lastPackOids");
      }
    }
    // Delete packOids entries for removed packs
    for (const k of removed) {
      try {
        await this.ctx.storage.delete(packOidsKey(k));
      } catch (e) {
        this.logger.warn("maintenance:delete-packOids-failed", { key: k, error: String(e) });
      }
    }
    // Sweep R2 pack files not in keep set
    try {
      const prefix = this.prefix();
      const pfx = r2PackDirPrefix(prefix);
      let cursor: string | undefined = undefined;
      do {
        const res: any = await this.env.REPO_BUCKET.list({ prefix: pfx, cursor });
        const objects: any[] = (res && res.objects) || [];
        for (const obj of objects) {
          const key: string = obj.key;
          if (!(isPackKey(key) || isIdxKey(key))) continue;
          const base = isIdxKey(key) ? packKeyFromIndexKey(key) : key;
          if (!keepSet.has(base)) {
            try {
              await this.env.REPO_BUCKET.delete(key);
            } catch {}
          }
        }
        cursor = res && res.truncated ? res.cursor : undefined;
      } while (cursor);
    } catch {}
  }

  private async processUnpackChunk(work: UnpackWork): Promise<void> {
    const packBytes = await this.loadPackBytes(work.packKey);
    if (!packBytes) {
      await this.abortUnpackWork(work.packKey);
      return;
    }

    const result = await this.processUnpackBatch(work, packBytes);
    await this.updateUnpackProgress(work, result);
  }

  private async loadPackBytes(packKey: string): Promise<Uint8Array | null> {
    const packObj = await this.env.REPO_BUCKET.get(packKey);
    if (!packObj) {
      this.logger.warn("unpack:pack-missing", { packKey });
      return null;
    }
    return new Uint8Array(await packObj.arrayBuffer());
  }

  private async abortUnpackWork(packKey: string): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    await store.delete("unpackWork");
  }

  private async processUnpackBatch(
    work: UnpackWork,
    packBytes: Uint8Array
  ): Promise<{ processed: number; processedInRun: number; exceededBudget: boolean }> {
    const cfg = this.getConfig();
    const startTime = Date.now();
    let processed = work.processedCount;
    let processedInRunTotal = 0;
    let loops = 0;
    let exceededBudget = false;

    this.logger.debug("unpack:begin", {
      packKey: work.packKey,
      processed,
      total: work.oids.length,
      budgetMs: cfg.unpackMaxMs,
    });

    while (processed < work.oids.length && !this.isTimeExceeded(startTime, cfg.unpackMaxMs)) {
      const oidsToProcess = work.oids.slice(processed, processed + cfg.unpackChunkSize);
      if (oidsToProcess.length === 0) break;

      this.logger.debug("unpack:chunk", {
        packKey: work.packKey,
        from: processed,
        to: processed + oidsToProcess.length,
        total: work.oids.length,
        loop: loops,
      });

      const processedInRun = await this.unpackChunk(packBytes, work.packKey, oidsToProcess);
      if (processedInRun === 0) break;

      processed += processedInRun;
      processedInRunTotal += processedInRun;
      loops++;

      this.logger.debug("unpack:chunk-result", {
        packKey: work.packKey,
        processedInRun,
        processed,
        total: work.oids.length,
      });
    }

    if (this.isTimeExceeded(startTime, cfg.unpackMaxMs)) {
      exceededBudget = true;
      this.logger.debug("unpack:budget-exceeded", {
        packKey: work.packKey,
        timeMs: Date.now() - startTime,
        processed,
        total: work.oids.length,
        loops,
      });
    }

    return { processed, processedInRun: processedInRunTotal, exceededBudget };
  }

  private isTimeExceeded(startTime: number, maxDuration: number): boolean {
    return Date.now() - startTime >= maxDuration;
  }

  private async unpackChunk(
    packBytes: Uint8Array,
    packKey: string,
    oids: string[]
  ): Promise<number> {
    try {
      return await unpackOidsChunkFromPackBytes(
        packBytes,
        this.ctx,
        this.env,
        this.prefix(),
        packKey,
        oids
      );
    } catch (e) {
      this.logger.error("unpack:chunk-error", { error: String(e), packKey });
      return 0;
    }
  }

  private async updateUnpackProgress(
    work: UnpackWork,
    result: { processed: number; processedInRun: number; exceededBudget: boolean }
  ): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const cfg = this.getConfig();

    if (result.processed >= work.oids.length) {
      // Done unpacking current pack
      await store.delete("unpackWork");
      this.logger.info("unpack:done", { packKey: work.packKey, total: work.oids.length });

      // If a next pack is queued, promote it and continue without clearing status
      const nextKey = await store.get("unpackNext");
      if (nextKey) {
        const nextOids = (await store.get(packOidsKey(nextKey))) ?? [];
        if (nextOids.length > 0) {
          await store.put("unpackWork", {
            packKey: nextKey,
            oids: nextOids,
            processedCount: 0,
            startedAt: Date.now(),
          });
          await store.delete("unpackNext");
          // Keep unpacking status set to true in KV
          await setUnpackStatus(this.env.PACK_METADATA_CACHE, this.ctx.id.toString(), true);
          // Schedule next alarm soon to continue unpacking
          await this.ctx.storage.setAlarm(Date.now() + cfg.unpackDelayMs);
          this.logger.info("queue:promote-next", { packKey: nextKey, total: nextOids.length });
          return;
        } else {
          // No oids recorded for next pack (unexpected); drop it
          await store.delete("unpackNext");
          this.logger.warn("queue:next-missing-oids", { packKey: nextKey });
        }
      }

      // No queued work remains; clear unpacking status
      await setUnpackStatus(this.env.PACK_METADATA_CACHE, this.ctx.id.toString(), false);
    } else {
      // Update progress and reschedule
      await store.put("unpackWork", {
        ...work,
        processedCount: result.processed,
      });

      const delay = result.processedInRun === 0 ? cfg.unpackBackoffMs : cfg.unpackDelayMs;
      await this.ctx.storage.setAlarm(Date.now() + delay);

      const percent = Math.round((result.processed / work.oids.length) * 100);
      this.logger.debug("unpack:progress", {
        packKey: work.packKey,
        processed: result.processed,
        total: work.oids.length,
        percent,
        timeMs: Date.now(),
        exceededBudget: result.exceededBudget,
      });

      this.logger.debug("unpack:reschedule", {
        packKey: work.packKey,
        processed: result.processed,
        total: work.oids.length,
        delay,
      });
    }
  }
}
