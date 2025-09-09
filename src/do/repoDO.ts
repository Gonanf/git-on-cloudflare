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
} from "@/git";
import {
  json,
  text,
  badRequest,
  createLogger,
  createInflateStream,
  UnpackProgress,
} from "@/common";
import { setUnpackStatus } from "@/cache";

/**
 * Repository Durable Object (per-repo authority)
 *
 * Responsibilities
 * - Acts as the strongly consistent source of truth for a single repository
 * - Stores refs and HEAD in DO storage
 * - Caches loose objects (zlib-compressed) in DO storage
 * - Mirrors loose objects to R2 under `do/<id>/objects/loose/<oid>` for cheap reads
 * - Writes received packfiles to R2 under `do/<id>/objects/pack/*.pack` (and .idx)
 * - Exposes small internal endpoints consumed by the worker/router and tests:
 *   - `GET /refs`, `PUT /refs` — list/update refs
 *   - `GET /head`, `PUT /head` — get/update HEAD
 *   - `GET|HEAD /obj/<oid>` — fetch loose object (R2 first, fallback DO)
 *   - `PUT /obj/<oid>` — put loose object (write DO + mirror to R2)
 *   - `GET /pack-latest`, `GET /packs`, `GET /pack-oids?key=...` — metadata for pack assembly
 *   - `POST /receive` — receive-pack implementation (delegates to do/receivePack.ts)
 *   - `GET /debug-state` — diagnostic state dump (refs, head, packs, sample loose)
 *   - `GET /debug-check?commit=<oid>` — check commit/tree presence across storage and packs
 *
 * Read Path
 * - Loose object reads hit `GET /obj/<oid>`.
 * - The handler prefers R2 (range-friendly and cheap) and falls back to DO storage if missing.
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
export class RepoDurableObject implements DurableObject {
  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  // Thin request router: delegates to focused handlers below.
  // Keep this mapping explicit and small so behavior is easy to audit.
  async fetch(request: Request): Promise<Response> {
    // Touch access and (re)schedule an idle cleanup alarm
    try {
      await this.touchAndMaybeSchedule();
    } catch {}
    const url = new URL(request.url);
    this.logger.debug("fetch", { path: url.pathname, method: request.method });
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);

    // Admin: seed a tiny repo (empty tree + 1 commit).
    // Used by tests to create a minimal, valid repository state quickly.
    if (url.pathname === "/seed" && request.method === "POST") {
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

      return json({ treeOid, commitOid });
    }

    // Refs storage (JSON array of { name, oid })
    if (url.pathname === "/refs" && request.method === "GET") {
      return this.handleRefsGet();
    }

    if (url.pathname === "/refs" && request.method === "PUT") {
      return this.handleRefsPut(request);
    }

    // HEAD storage (JSON object { target, oid?, unborn? })
    if (url.pathname === "/head" && request.method === "GET") {
      return this.handleHeadGet();
    }

    if (url.pathname === "/head" && request.method === "PUT") {
      return this.handleHeadPut(request);
    }

    // Loose object storage (zlib-compressed) endpoints
    if (
      url.pathname.startsWith("/obj/") &&
      (request.method === "GET" || request.method === "HEAD")
    ) {
      const oid = url.pathname.slice("/obj/".length);
      return this.handleObjGet(oid, request.method);
    }

    if (url.pathname.startsWith("/obj/") && request.method === "PUT") {
      const oid = url.pathname.slice("/obj/".length);
      return this.handleObjPut(oid, request);
    }

    // Return latest pack key (if any)
    if (url.pathname === "/pack-latest" && request.method === "GET") {
      return this.handlePackLatestGet();
    }

    // List recent pack keys (maintained during unpack)
    if (url.pathname === "/packs" && request.method === "GET") {
      return this.handlePacksGet();
    }

    // Get oids for a specific pack key (recorded at index time)
    if (url.pathname === "/pack-oids" && request.method === "GET") {
      return this.handlePackOidsGet(url);
    }

    // Get unpacking progress
    if (url.pathname === "/unpack-progress" && request.method === "GET") {
      const work = await store.get("unpackWork");
      const nextKey = await store.get("unpackNext");
      if (!work) return json({ unpacking: false, queuedCount: nextKey ? 1 : 0 });
      return json({
        unpacking: true,
        processed: work.processedCount,
        total: work.oids.length,
        percent: Math.round((work.processedCount / work.oids.length) * 100),
        currentPackKey: work.packKey,
        queuedCount: nextKey ? 1 : 0,
      } as UnpackProgress);
    }

    // Debug: dump durable object state
    if (url.pathname === "/debug-state" && request.method === "GET") {
      return this.handleDebugState();
    }

    // Debug: check a specific commit/tree presence
    if (url.pathname === "/debug-check" && request.method === "GET") {
      return this.handleDebugCheck(url);
    }

    // Batched commits listing (walk first-parent chain)
    // GET /commits?start=<oid>&ref=<ref>&max=<n>
    // Either provide a 40-hex `start` OID or a ref/branch/tag via `ref`.
    if (url.pathname === "/commits" && request.method === "GET") {
      return this.handleCommitsGet(url);
    }

    // Admin: reindex the latest pack from R2 into loose objects
    if (url.pathname === "/reindex" && request.method === "POST") {
      return this.handleReindexPost();
    }

    // Receive-pack: parse update commands section and packfile, store pack to R2,
    // update refs atomically if valid, and respond with report-status
    if (url.pathname === "/receive" && request.method === "POST") {
      return this.handleReceive(request);
    }

    return text("Not found\n", 404);
  }

  // --- Alarm-based tasks ---
  // Combines three responsibilities:
  // 1) Unpack work: Process pending pack objects in chunks to avoid blocking
  // 2) Idle cleanup: If the DO remains idle beyond IDLE_MS and appears empty/unused, purge storage and R2 mirror.
  // 3) Maintenance: Periodically prune stale packs and metadata even for active repos.
  async alarm(): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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
        await this.state.storage.setAlarm(Date.now() + 1000);
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
    const storage = this.state.storage;

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
        const res: any = await this.env.REPO_BUCKET.list({ prefix: pfx, cursor });
        const objects: any[] = (res && res.objects) || [];

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

        cursor = res && res.truncated ? res.cursor : undefined;
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
      await this.state.storage.setAlarm(next);
      this.logger.debug("alarm:scheduled", { next });
    } catch (e) {
      this.logger.error("alarm:set-failed", { error: String(e) });
    }
  }

  private async touchAndMaybeSchedule(): Promise<void> {
    const cfg = this.getConfig();
    const now = Date.now();
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);

    // Update last access time
    await store.put("lastAccessMs", now);

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

    const existing = (await this.state.storage.getAlarm()) as number | null | undefined;
    const soon = now + cfg.unpackDelayMs;

    if (!existing || existing < now || existing > soon) {
      await this.state.storage.setAlarm(soon);
    }
    return true;
  }

  private async scheduleRegularAlarm(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
    cfg: ReturnType<typeof this.getConfig>,
    now: number
  ): Promise<void> {
    const lastMaint = await store.get("lastMaintenanceMs");
    const existing = await this.state.storage.getAlarm();

    const nextIdle = now + cfg.idleMs;
    const nextMaint = (lastMaint ?? now) + cfg.maintMs;
    const next = Math.min(nextIdle, nextMaint);

    if (!existing || existing < now || existing > next) {
      await this.state.storage.setAlarm(next);
    }
  }

  // ---- Small route handlers (extracted for readability) ----

  private async handleRefsGet(): Promise<Response> {
    const refs = await this.getRefs();
    return json(refs);
  }

  private async handleRefsPut(request: Request): Promise<Response> {
    const payload = await request.json<any>();
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    await store.put("refs", payload);
    return text("OK\n");
  }

  /**
   * GET /commits?start=<oid>&ref=<ref>&max=<n>&cursor=<oid>
   * Returns up to `max` commits (default 25) starting from the provided commit OID/ref.
   * If `cursor` is provided, it supersedes start/ref and is used as the starting commit.
   * Walks the first-parent chain. Reads commit objects directly from DO storage when possible.
   * Response shape: { items: CommitInfo[], next?: string }
   */
  private async handleCommitsGet(url: URL): Promise<Response> {
    const maxRaw = Number(url.searchParams.get("max") || "25");
    const clamp = (n: number, min: number, max: number) =>
      Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
    const max = clamp(maxRaw, 1, 100);

    // Resolve starting commit OID from either explicit `start` or a `ref` name
    const cursor = url.searchParams.get("cursor");
    let start = url.searchParams.get("start");
    const ref = url.searchParams.get("ref");
    if (cursor && /^[0-9a-f]{40}$/i.test(cursor)) {
      start = cursor;
    } else {
      if ((!start || start === "") && ref) {
        const resolved = await this.resolveRefLocal(ref);
        start = resolved ?? null;
      }
      if (!start || !/^[0-9a-f]{40}$/i.test(start)) {
        return badRequest("Missing or invalid start/ref\n");
      }
    }

    const commits: Array<{
      oid: string;
      tree: string;
      parents: string[];
      author?: { name: string; email: string; when: number; tz: string };
      committer?: { name: string; email: string; when: number; tz: string };
      message: string;
    }> = [];

    const seen = new Set<string>();
    let oid: string | undefined = (start as string).toLowerCase();
    while (commits.length < max && oid && !seen.has(oid)) {
      seen.add(oid);
      const info = await this.readCommitFromStore(oid);
      if (!info) break; // stop if object missing/unavailable yet
      commits.push(info);
      oid = info.parents[0]; // first-parent chain
    }

    return json({ items: commits, next: oid });
  }

  // Resolve a ref-ish locally using DO-stored refs without crossing the worker boundary
  private async resolveRefLocal(refOrOid: string): Promise<string | undefined> {
    if (/^[0-9a-f]{40}$/i.test(refOrOid)) return refOrOid.toLowerCase();
    const refs = await this.getRefs();
    // Fully qualified ref
    if (refOrOid.startsWith("refs/")) {
      const r = refs.find((x) => x.name === refOrOid);
      return r?.oid?.toLowerCase();
    }
    // Try branches then tags
    const c1 = refs.find((x) => x.name === `refs/heads/${refOrOid}`);
    if (c1) return c1.oid.toLowerCase();
    const c2 = refs.find((x) => x.name === `refs/tags/${refOrOid}`);
    if (c2) return c2.oid.toLowerCase();
    // Fall back to HEAD target
    const head = (await asTypedStorage<RepoStateSchema>(this.state.storage).get("head")) as
      | Head
      | undefined;
    if (head?.oid) return head.oid.toLowerCase();
    return undefined;
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
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    return (await store.get("refs")) ?? [];
  }

  /**
   * GET /head
   * Returns the repository HEAD as `{ target, oid? }` or `{ target, unborn: true }`.
   * The handler resolves the current `oid` by looking up the target ref in stored refs.
   * If the target ref does not exist, it returns `{ target, unborn: true }`.
   *
   * Note: The resolved value is persisted back to DO storage only if it differs
   * from the stored value (semantic equality check).
   */
  private async handleHeadGet(): Promise<Response> {
    const resolved = await this.resolveHead();
    return json(resolved);
  }

  /**
   * Resolves the current HEAD state by looking up the target ref.
   * @returns The resolved HEAD object with target and either oid or unborn flag
   */
  private async resolveHead(): Promise<Head> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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

  private async handleHeadPut(request: Request): Promise<Response> {
    const payload = await request.json<any>();
    if (!payload || typeof payload.target !== "string") {
      return badRequest("Invalid head payload\n");
    }
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    await store.put("head", payload);
    return new Response("OK\n", { status: 200 });
  }

  private async handlePackLatestGet(): Promise<Response> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const key = await store.get("lastPackKey");
    if (!key) return text("Not found\n", 404);
    const oids = ((await store.get("lastPackOids")) || []).slice(0, 10000);
    return json({ key, oids });
  }

  private async handlePacksGet(): Promise<Response> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const list = ((await store.get("packList")) || []).slice(0, 20);
    return json({ keys: list });
  }

  private async handlePackOidsGet(url: URL): Promise<Response> {
    const key = url.searchParams.get("key");
    if (!key) return badRequest("Missing key\n");
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const oids = (await store.get(packOidsKey(key))) || [];
    return json({ key, oids });
  }

  private async handleReindexPost(): Promise<Response> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const key = await store.get("lastPackKey");
    if (!key) return text("No pack to reindex\n", 404);
    const obj = await this.env.REPO_BUCKET.get(key);
    if (!obj) return text("Pack not found in R2\n", 404);
    const bytes = new Uint8Array(await obj.arrayBuffer());
    this.logger.info("reindex:start", { key });
    await unpackPackToLoose(bytes, this.state, this.env, this.prefix(), key);
    this.logger.info("reindex:done", { key });
    return text("OK\n");
  }

  // ---- Debug helpers ----
  private async handleDebugState(): Promise<Response> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const refs = (await store.get("refs")) ?? [];
    const head = await store.get("head");
    const lastPackKey = await store.get("lastPackKey");
    const lastPackOids = (await store.get("lastPackOids")) ?? [];
    const packList = (await store.get("packList")) ?? [];
    const unpackWork = await store.get("unpackWork");
    const unpackNext = await store.get("unpackNext");

    // Sample a few loose object keys to avoid large payloads
    const looseSample: string[] = [];
    try {
      const it = await this.state.storage.list({ prefix: "obj:", limit: 10 });
      for (const k of it.keys()) looseSample.push(String(k).slice(4));
    } catch {}

    return json({
      meta: { doId: this.state.id.toString(), prefix: this.prefix() },
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
    });
  }

  private async handleDebugCheck(url: URL): Promise<Response> {
    const q = url.searchParams;
    const commit = (q.get("commit") || "").toLowerCase();
    if (!/^[0-9a-f]{40}$/.test(commit)) return badRequest("Missing or invalid commit\n");

    // Check pack membership via stored indices first
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const packList = (await store.get("packList")) ?? [];
    const membership: Record<string, { hasCommit: boolean; hasTree: boolean }> = {};
    for (const key of packList) {
      try {
        const oids = (await store.get(packOidsKey(key))) ?? [];
        const set = new Set(oids.map((x) => x.toLowerCase()));
        membership[key] = { hasCommit: set.has(commit), hasTree: false };
      } catch {}
    }

    // Try to read commit to discover its tree; ok if it fails
    let tree: string | undefined = undefined;
    let parents: string[] = [];
    try {
      const info = await this.readCommitFromStore(commit);
      if (info) {
        tree = info.tree.toLowerCase();
        parents = info.parents;
      }
    } catch {}

    // Presence checks for loose objects
    const hasLooseCommit = !!(await this.state.storage.get(objKey(commit)));
    let hasLooseTree = false;
    let hasR2LooseTree = false;
    if (tree) {
      hasLooseTree = !!(await this.state.storage.get(objKey(tree)));
      try {
        const head = await this.env.REPO_BUCKET.head(r2LooseKey(this.prefix(), tree));
        hasR2LooseTree = !!head;
      } catch {}
      // Update membership.hasTree using discovered tree
      for (const key of Object.keys(membership)) {
        try {
          const oids = (await store.get(packOidsKey(key))) ?? [];
          const set = new Set(oids.map((x) => x.toLowerCase()));
          membership[key].hasTree = !!tree && set.has(tree);
        } catch {}
      }
    }

    return json({
      commit: { oid: commit, parents, tree },
      presence: { hasLooseCommit, hasLooseTree, hasR2LooseTree },
      membership,
    });
  }

  private async handleObjGet(oid: string, method: string): Promise<Response> {
    // Validate OID format
    if (!this.isValidOid(oid)) {
      return badRequest("Bad oid\n");
    }

    if (method === "HEAD") {
      return this.handleObjHead(oid);
    }

    return this.handleObjGetContent(oid);
  }

  private isValidOid(oid: string): boolean {
    return /^[0-9a-f]{40}$/i.test(oid);
  }

  private async handleObjHead(oid: string): Promise<Response> {
    const size = await this.getObjectSize(oid);

    if (size === null) {
      return new Response("Not found\n", { status: 404 });
    }

    return new Response(null, {
      status: 200,
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(size),
      },
    });
  }

  private async handleObjGetContent(oid: string): Promise<Response> {
    const content = await this.getObjectContent(oid);

    if (!content) {
      return new Response("Not found\n", { status: 404 });
    }

    return new Response(content, {
      status: 200,
      headers: { "Content-Type": "application/octet-stream" },
    });
  }

  private async getObjectSize(oid: string): Promise<number | null> {
    const prefix = this.prefix();
    const r2key = r2LooseKey(prefix, oid);

    // Try R2 first
    try {
      const obj = await this.env.REPO_BUCKET.head(r2key);
      if (obj) return obj.size;
    } catch {}

    // Fallback to DO storage
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const data = await store.get(objKey(oid));

    if (!data) return null;
    return data instanceof ArrayBuffer ? data.byteLength : data.byteLength;
  }

  private async getObjectContent(oid: string): Promise<BodyInit | null> {
    const prefix = this.prefix();
    const r2key = r2LooseKey(prefix, oid);

    // Try R2 first
    try {
      const obj = await this.env.REPO_BUCKET.get(r2key);
      if (obj) return obj.body;
    } catch {}

    // Fallback to DO storage
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
    const data = await store.get(objKey(oid));

    return data || null;
  }

  private async handleObjPut(oid: string, request: Request): Promise<Response> {
    // Validate OID format
    if (!this.isValidOid(oid)) {
      return badRequest("Bad oid\n");
    }

    const bytes = new Uint8Array(await request.arrayBuffer());
    await this.storeObject(oid, bytes);

    return text("OK\n");
  }

  private async storeObject(oid: string, bytes: Uint8Array): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);

    // Store in DO storage
    await store.put(objKey(oid), bytes);

    // Best-effort mirror to R2
    await this.mirrorObjectToR2(oid, bytes);
  }

  private async mirrorObjectToR2(oid: string, bytes: Uint8Array): Promise<void> {
    const r2key = r2LooseKey(this.prefix(), oid);
    try {
      await this.env.REPO_BUCKET.put(r2key, bytes);
    } catch {}
  }

  private async handleReceive(request: Request) {
    // Delegate to extracted implementation for clarity and testability.
    this.logger.info("receive:start", {});
    // Pre-body guard: block when current unpack is running and a next pack is already queued
    try {
      const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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
    const res = await receivePack(this.state, this.env, this.prefix(), request);
    this.logger.info("receive:end", { status: res.status });
    return res;
  }

  private prefix() {
    // Tests and R2 layout expect Durable Object data under the 'do/<id>' prefix
    return doPrefix(this.state.id.toString());
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "RepoDO",
      doId: this.state.id.toString(),
    });
  }

  private getConfig() {
    // Parse configuration from env vars with sensible defaults
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
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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
        await this.state.storage.delete(packOidsKey(k));
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
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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
        this.state,
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
    const store = asTypedStorage<RepoStateSchema>(this.state.storage);
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
          await setUnpackStatus(this.env.PACK_METADATA_CACHE, this.state.id.toString(), true);
          // Schedule next alarm soon to continue unpacking
          await this.state.storage.setAlarm(Date.now() + cfg.unpackDelayMs);
          this.logger.info("queue:promote-next", { packKey: nextKey, total: nextOids.length });
          return;
        } else {
          // No oids recorded for next pack (unexpected); drop it
          await store.delete("unpackNext");
          this.logger.warn("queue:next-missing-oids", { packKey: nextKey });
        }
      }

      // No queued work remains; clear unpacking status
      await setUnpackStatus(this.env.PACK_METADATA_CACHE, this.state.id.toString(), false);
    } else {
      // Update progress and reschedule
      await store.put("unpackWork", {
        ...work,
        processedCount: result.processed,
      });

      const delay = result.processedInRun === 0 ? cfg.unpackBackoffMs : cfg.unpackDelayMs;
      await this.state.storage.setAlarm(Date.now() + delay);

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
