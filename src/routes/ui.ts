import { AutoRouter } from "itty-router";
import {
  getHeadAndRefs,
  readPath,
  listCommits,
  readCommitInfo,
  readBlobStream,
  type CommitInfo,
  type TreeEntry,
} from "@/git";
import { getRepoStub } from "@/common";
import { repoKey } from "@/keys";
import {
  escapeHtml,
  detectBinary,
  formatSize,
  bytesToText,
  formatWhen,
  renderPage,
  getUnpackProgress,
  renderView,
} from "@/web";
import { listReposForOwner } from "@/registry";
import {
  buildCacheKeyFrom,
  cacheGetJSON,
  cachePutJSON,
  cacheOrLoadJSON,
  CacheContext,
} from "@/cache";

export function registerUiRoutes(router: ReturnType<typeof AutoRouter>) {
  // Owner repos list
  router.get(`/:owner`, async (request, env: Env) => {
    const { owner } = request.params as { owner: string };
    const repos = await listReposForOwner(env, owner);
    // Prefer Liquid template rendering (auto-escaped, loops/conditionals)
    const page = await renderView(env, "owner", {
      title: `${owner} · Repositories`,
      owner,
      repos,
    });
    if (!page) throw new Error("Failed to render owner template");
    return new Response(page, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-layout",
      },
    });
  });
  // Repo overview page
  router.get(`/:owner/:repo`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    const repoId = repoKey(owner, repo);

    // Cache HEAD and refs for 60 seconds for branches, longer for tags
    const cacheKeyRefs = buildCacheKeyFrom(request, "/_cache/refs", {
      repo: repoId,
    });

    let head: any;
    let refs: any[] = [];

    // Try cache first
    try {
      const cached = await cacheGetJSON<{ head: any; refs: any[] }>(cacheKeyRefs);
      if (cached) {
        head = cached.head;
        refs = cached.refs;
      }
    } catch {}

    // Fetch if not cached
    if (!refs || refs.length === 0) {
      const result = await getHeadAndRefs(env, repoId);
      head = result.head;
      refs = result.refs;
      // Cache for 60 seconds
      await cachePutJSON(cacheKeyRefs, { head, refs }, 60);
    }

    const defaultRef = head?.target || (refs[0]?.name ?? "refs/heads/main");
    const refShort = defaultRef.replace(/^refs\/(heads|tags)\//, "");
    const refEnc = encodeURIComponent(refShort);
    // Format branches and tags as structured data
    const branchesData = refs
      .filter((r) => r.name.startsWith("refs/heads/"))
      .map((b) => {
        const short = b.name.replace("refs/heads/", "");
        return {
          name: encodeURIComponent(short),
          displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
        };
      });
    const tagsData = refs
      .filter((r) => r.name.startsWith("refs/tags/"))
      .map((t) => {
        const short = t.name.replace("refs/tags/", "");
        return {
          name: encodeURIComponent(short),
          displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
        };
      });

    // Try to load README at repo root on default branch with caching
    let readmeHtml = "";
    const cacheKeyReadme = buildCacheKeyFrom(request, "/_cache/readme", {
      repo: repoId,
      ref: refShort,
    });

    // Try cache first for README
    try {
      const cachedReadme = await cacheGetJSON<{ html: string }>(cacheKeyReadme);
      if (cachedReadme) {
        readmeHtml = cachedReadme.html;
      }
    } catch {}

    if (!readmeHtml) {
      try {
        // Load all candidates in parallel for better performance
        const candidates = ["README.md", "README.MD", "Readme.md", "README", "readme.md"];
        const readmePromises = candidates.map(async (name) => {
          try {
            const cacheCtx: CacheContext = { req: request, ctx };
            const res = await readPath(env, repoId, refShort, name, cacheCtx);
            if (res.type === "blob") {
              return { name, content: res.content };
            }
          } catch {}
          return null;
        });

        const results = await Promise.all(readmePromises);
        const found = results.find((r) => r !== null);

        if (found) {
          const text = bytesToText(found.content);
          readmeHtml = `<h3>README</h3><div data-markdown="1" data-md-owner="${escapeHtml(
            owner
          )}" data-md-repo="${escapeHtml(repo)}" data-md-ref="${escapeHtml(
            refShort
          )}" data-md-base=""><pre class="md-src">${escapeHtml(text)}</pre></div>`;
          // Cache README for 5 minutes
          await cachePutJSON(cacheKeyReadme, { html: readmeHtml }, 300);
        }
      } catch {}
    }

    // Check unpacking progress (shared helper)
    const progress = await getUnpackProgress(env, repoId);

    const page = await renderView(env, "overview", {
      title: `${owner}/${repo}`,
      owner,
      repo,
      refShort,
      refEnc: refEnc,
      branches: branchesData,
      tags: tagsData,
      readme: readmeHtml,
      progress,
    });
    if (!page) throw new Error("Failed to render overview template");
    return new Response(page, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-layout",
      },
    });
  });

  // Tree/Blob browser using query params: ?ref=<branch|tag|oid>&path=<path>
  router.get(`/:owner/:repo/tree`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const path = u.searchParams.get("path") || "";

    // Build cache key for tree content
    const cacheKeyTree = buildCacheKeyFrom(request, "/_cache/tree", {
      repo: repoId,
      ref,
      path,
    });

    // Try cache first
    let result: any = null;
    try {
      const cached = await cacheGetJSON<any>(cacheKeyTree);
      if (cached) {
        result = cached;
      }
    } catch {}

    try {
      // Fetch if not cached
      if (!result) {
        const cacheCtx: CacheContext = { req: request, ctx };
        result = await readPath(env, repoId, ref, path, cacheCtx);
        // Cache tree listings for 60 seconds, blob metadata for 5 minutes
        const ttl = result.type === "tree" ? 60 : 300;
        await cachePutJSON(cacheKeyTree, result, ttl);
      }
      if (result.type === "tree") {
        // Format tree entries as structured data
        let entries: Array<{
          name: string;
          href: string;
          isDir: boolean;
          shortOid: string;
          size: string;
        }> = [];
        if (result.type === "tree" && result.entries) {
          // Helper to determine if entry is a directory based on git mode
          const isDirectory = (mode: string) => mode.startsWith("40000");

          const sorted = result.entries.sort((a: TreeEntry, b: TreeEntry) => {
            const aIsDir = isDirectory(a.mode);
            const bIsDir = isDirectory(b.mode);
            if (aIsDir !== bIsDir) return aIsDir ? -1 : 1;
            return a.name.localeCompare(b.name);
          });
          entries = sorted.map((e: TreeEntry) => {
            const isDir = isDirectory(e.mode);
            return {
              name: e.name,
              href: isDir
                ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(
                    (path ? path + "/" : "") + e.name
                  )}`
                : `/${owner}/${repo}/blob?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(
                    (path ? path + "/" : "") + e.name
                  )}`,
              isDir,
              shortOid: e.oid ? e.oid.slice(0, 7) : "",
              size: "", // Size not available in tree entries, would need separate lookup
            };
          });
        }
        // Generate breadcrumbs and parent link
        const parts = (path || "").split("/").filter(Boolean);
        // Truncate ref if too long (e.g., commit hashes)
        const refDisplay = ref.length > 20 ? ref.slice(0, 7) + "..." : ref;
        const breadcrumbs = [
          {
            name: refDisplay,
            href: parts.length > 0 ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}` : null,
          },
          ...parts.map((part, i) => {
            const subPath = parts.slice(0, i + 1).join("/");
            const isLast = i === parts.length - 1;
            return {
              name: part,
              href: isLast
                ? null
                : `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(subPath)}`,
            };
          }),
        ];
        const parentHref =
          parts.length > 0
            ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(parts.slice(0, -1).join("/"))}`
            : null;
        const progress = await getUnpackProgress(env, repoId);
        const page = await renderView(env, "tree", {
          title: `${path || "root"} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          progress,
          breadcrumbs,
          parentHref,
          entries,
        });
        if (!page) throw new Error("Failed to render tree template");
        return new Response(page, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-layout",
          },
        });
      } else {
        const raw = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}`;
        const text = bytesToText(result.content);
        const title = path || result.oid;
        const page = await renderView(env, "blob", {
          title: `${title} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName: title,
          viewRawHref: `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(title)}`,
          rawHref: raw,
          contentHtml: `<pre>${escapeHtml(text)}</pre>`,
        });
        if (!page) throw new Error("Failed to render blob template");
        return new Response(page, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-layout",
          },
        });
      }
    } catch (e: any) {
      // Error path: include unpack progress banner so users understand transient states
      const refEnc = encodeURIComponent(ref);
      const progress = await getUnpackProgress(env, repoId);
      const progressHtml = progress
        ? `<div class="alert warn">📦 Unpacking objects: ${progress.processed}/${progress.total} (${progress.percent}%)</div>`
        : "";
      const body = `<nav><a href="/${owner}/${repo}"><strong>${owner}/${repo}</strong></a> <a href="/${owner}/${repo}/tree?ref=${refEnc}">Browse</a> <a href="/${owner}/${repo}/commits?ref=${refEnc}">Commits</a></nav>
      <h2>Tree</h2>
      ${progressHtml}
      <div class="alert warn">Unable to browse${path ? `: ${escapeHtml(path)}` : ""}</div>
      <pre>${escapeHtml(String(e?.message || e))}</pre>`;
      return renderPage(env, request, `${owner}/${repo} · Tree`, body);
    }
  });

  /**
   * Blob preview endpoint - renders file content with syntax highlighting
   * @route GET /:owner/:repo/blob
   * @param ref - Git reference (branch/tag/commit)
   * @param path - File path in repository
   * @note Large files (>1MB) show size info instead of content
   * @note Binary files are detected and show download link
   */
  router.get(`/:owner/:repo/blob`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const path = u.searchParams.get("path") || "";
    try {
      const cacheCtx: CacheContext = { req: request, ctx };
      const result = await readPath(env, repoId, ref, path, cacheCtx);
      if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
      const fileName = path || result.oid;

      // Check if already marked as too large
      if (result.tooLarge) {
        const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(
          result.oid
        )}&view=1&name=${encodeURIComponent(fileName)}`;
        const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(
          result.oid
        )}&download=1&name=${encodeURIComponent(fileName)}`;
        const contentHtml = `<div class="muted">File too large to preview (${formatSize(
          result.size || 0
        )}). <a href="${viewRawHref}">View raw</a></div>`;
        const title = fileName;
        const page = await renderView(env, "blob", {
          title: `${fileName} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName: fileName,
          viewRawHref,
          rawHref,
          contentHtml,
        });
        if (!page) throw new Error("Failed to render blob template");
        return new Response(page, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-layout",
          },
        });
      }

      // Size and binary checks for loaded content
      const size = result.content.byteLength;
      const isBinary = detectBinary(result.content);
      const tooLarge = false; // Already checked server-side

      const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(
        result.oid
      )}&view=1&name=${encodeURIComponent(fileName)}`;
      const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(
        result.oid
      )}&download=1&name=${encodeURIComponent(fileName)}`;

      let contentHtml: string;
      if (tooLarge) {
        contentHtml = `<div class="muted">File too large to preview (${formatSize(
          size
        )}). <a href="${viewRawHref}">View raw</a></div>`;
      } else if (isBinary) {
        contentHtml = `<div class="muted">Binary file (${formatSize(size)}). <a href="${rawHref}">Download</a></div>`;
      } else {
        const text = bytesToText(result.content);
        const isMd =
          fileName.toLowerCase().endsWith(".md") || fileName.toLowerCase().endsWith(".markdown");
        const baseDir = (path || "").split("/").filter(Boolean).slice(0, -1).join("/");
        contentHtml = isMd
          ? `<div data-markdown="1" data-md-owner="${escapeHtml(owner)}" data-md-repo="${escapeHtml(
              repo
            )}" data-md-ref="${escapeHtml(ref)}" data-md-base="${escapeHtml(
              baseDir
            )}"><pre class="md-src">${escapeHtml(text)}</pre></div>`
          : `<pre><code>${escapeHtml(text)}</code></pre>`;
      }
      const title = fileName;
      const page = await renderView(env, "blob", {
        title: `${fileName} · ${owner}/${repo}`,
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        fileName: fileName,
        viewRawHref,
        rawHref,
        contentHtml,
      });
      if (!page) throw new Error("Failed to render blob template");
      return new Response(page, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-layout",
        },
      });
    } catch (e: any) {
      return new Response(`<h2>Error</h2><pre>${escapeHtml(String(e?.message || e))}</pre>`, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
        status: 500,
      });
    }
  });

  // Commit list
  router.get(`/:owner/:repo/commits`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const cursor = u.searchParams.get("cursor") || "";
    const trailParam = u.searchParams.get("trail") || "";
    const trail = trailParam
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter((s) => /^[0-9a-f]{40}$/i.test(s));
    const perRaw = Number(u.searchParams.get("per_page") || "25");
    const clamp = (n: number, min: number, max: number) =>
      Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
    const perPage = clamp(perRaw, 10, 200);
    // Fast path: ask the repo DO for a batched commit list to avoid many round-trips
    let commits: CommitInfo[] = [];
    let nextCursor: string | undefined;
    const isFirstPage = !cursor;
    // Attempt to serve from cache first
    let headOid: string | undefined;
    let isTagRef = false;
    try {
      if (isFirstPage) {
        const { head, refs } = await getHeadAndRefs(env, repoId);
        if (/^[0-9a-f]{40}$/i.test(ref)) {
          headOid = ref.toLowerCase();
        } else if (ref === "HEAD" && head?.target) {
          const r = refs.find((x) => x.name === head.target);
          headOid = r?.oid;
          // treat HEAD as branch-like for TTL
        } else if (ref.startsWith("refs/")) {
          const r = refs.find((x) => x.name === ref);
          headOid = r?.oid;
          isTagRef = ref.startsWith("refs/tags/");
        } else {
          const rb = refs.find((x) => x.name === `refs/heads/${ref}`);
          if (rb) {
            headOid = rb.oid;
          } else {
            const rt = refs.find((x) => x.name === `refs/tags/${ref}`);
            if (rt) {
              headOid = rt.oid;
              isTagRef = true;
            }
          }
        }
      }
    } catch {}

    const keyReq = buildCacheKeyFrom(request, "/_cache/commits", {
      repo: repoId,
      ref,
      cursor: cursor || undefined,
      per: String(perPage),
      head: isFirstPage ? headOid : undefined,
    });

    // Calculate TTL based on ref type
    const ttl = cursor ? 3600 : isTagRef || /^[0-9a-f]{40}$/i.test(ref) ? 3600 : 60;

    // Use cache helper for cleaner code
    const result = await cacheOrLoadJSON<{ items: CommitInfo[]; next?: string }>(
      keyReq,
      async () => {
        try {
          const stub = getRepoStub(env, repoId);
          const qp = new URLSearchParams({ ref, max: String(perPage) });
          if (cursor) qp.set("cursor", cursor);
          const doRes = await stub.fetch(`https://do/commits?${qp.toString()}`, { method: "GET" });
          if (doRes.ok) {
            const parsed = (await doRes.json()) as { items: CommitInfo[]; next?: string };
            return { items: parsed.items || [], next: parsed.next };
          } else {
            const cacheCtx: CacheContext = { req: request, ctx };
            const items = await listCommits(env, repoId, ref, perPage, cacheCtx);
            return { items, next: undefined };
          }
        } catch {
          // Fallback to existing path on any error
          const cacheCtx: CacheContext = { req: request, ctx };
          const items = await listCommits(env, repoId, ref, perPage, cacheCtx);
          return { items, next: undefined };
        }
      },
      ttl,
      ctx
    );

    commits = result?.items || [];
    nextCursor = result?.next;
    // Transform commits to structured data for template
    const commitsData = commits.map((c) => ({
      oid: c.oid,
      shortOid: c.oid.slice(0, 7),
      firstLine: c.message.split("\n")[0],
      authorName: c.author?.name || "",
      when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
    }));
    // Build pager data structure
    const baseQs = new URLSearchParams({ ref, per_page: String(perPage) });
    const currentStart = commits.length > 0 ? commits[0].oid : "";

    let newerHref: string | null = null;
    if (trail.length > 0) {
      const prevCursor = trail[trail.length - 1];
      const remainingTrail = trail.slice(0, -1);
      if (remainingTrail.length === 0) {
        const qs = new URLSearchParams(baseQs);
        newerHref = `/${owner}/${repo}/commits?${qs.toString()}`;
      } else {
        const qs = new URLSearchParams(baseQs);
        qs.set("cursor", prevCursor);
        qs.set("trail", remainingTrail.join(","));
        newerHref = `/${owner}/${repo}/commits?${qs.toString()}`;
      }
    }

    let olderHref: string | null = null;
    if (nextCursor && currentStart) {
      const nextQs = new URLSearchParams(baseQs);
      nextQs.set("cursor", nextCursor);
      const nextTrail = trail.concat([currentStart]).join(",");
      if (nextTrail) nextQs.set("trail", nextTrail);
      olderHref = `/${owner}/${repo}/commits?${nextQs.toString()}`;
    }

    const pager = {
      perPageLinks: [
        {
          href: `/${owner}/${repo}/commits?${new URLSearchParams({ ref, per_page: "25" }).toString()}`,
          text: "25",
        },
        {
          href: `/${owner}/${repo}/commits?${new URLSearchParams({ ref, per_page: "50" }).toString()}`,
          text: "50",
        },
        {
          href: `/${owner}/${repo}/commits?${new URLSearchParams({ ref, per_page: "100" }).toString()}`,
          text: "100",
        },
      ],
      newerHref,
      olderHref,
    };
    const progress = await getUnpackProgress(env, repoId);
    const page = await renderView(env, "commits", {
      title: `Commits · ${owner}/${repo}`,
      owner,
      repo,
      ref,
      refEnc: encodeURIComponent(ref),
      commits: commitsData,
      pager,
      progress,
    });
    if (!page) throw new Error("Failed to render commits template");
    return new Response(page, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-layout",
      },
    });
  });

  // Commit details
  router.get(`/:owner/:repo/commit/:oid`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo, oid } = request.params;
    const repoId = repoKey(owner, repo);
    try {
      const cacheCtx: CacheContext = { req: request, ctx };
      const c = await readCommitInfo(env, repoId, oid, cacheCtx);
      const when = c.author ? formatWhen(c.author.when, c.author.tz) : "";
      const parents = c.parents
        .map((p) => `<a href="/${owner}/${repo}/commit/${p}">${p.slice(0, 7)}</a>`)
        .join(", ");
      const page = await renderView(env, "commit", {
        title: `${c.oid.slice(0, 7)} · ${owner}/${repo}`,
        owner,
        repo,
        refEnc: encodeURIComponent(c.oid),
        commitShort: c.oid.slice(0, 7),
        authorName: c.author?.name || "",
        authorEmail: c.author?.email || "",
        when: when,
        parents: parents || '<span class="muted">(none)</span>',
        treeShort: c.tree.slice(0, 7),
        message: c.message,
      });
      if (!page) throw new Error("Failed to render commit template");
      return new Response(page, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-layout",
        },
      });
    } catch (e: any) {
      return new Response(`<h2>Error</h2><pre>${escapeHtml(String(e?.message || e))}</pre>`, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
        status: 500,
      });
    }
  });

  // Raw blob endpoint - streams file content without buffering
  router.get(`/:owner/:repo/raw`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    const url = new URL(request.url);
    const oid = url.searchParams.get("oid") || "";
    const fileName = url.searchParams.get("name") || oid;
    const download = url.searchParams.get("download") === "1";

    if (!oid) return new Response("Missing oid\n", { status: 400 });

    // Use streaming version to avoid buffering entire file in memory
    const streamResponse = await readBlobStream(env, repoKey(owner, repo), oid);
    if (!streamResponse) return new Response("Not found\n", { status: 404 });

    // Use text/plain for all files (like GitHub's raw view)
    // This prevents browser from executing HTML/JS and ensures consistent display
    const headers = new Headers(streamResponse.headers);
    headers.set("Content-Type", "text/plain; charset=utf-8");

    if (download) {
      headers.set("Content-Disposition", `attachment; filename="${fileName}"`);
    } else {
      headers.set("Content-Disposition", `inline; filename="${fileName}"`);
    }

    return new Response(streamResponse.body, {
      status: streamResponse.status,
      headers,
    });
  });

  // Raw blob by ref+path (used for images in Markdown)
  router.get(`/:owner/:repo/rawpath`, async (request: any, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    const url = new URL(request.url);
    const ref = url.searchParams.get("ref") || "main";
    const path = url.searchParams.get("path") || "";
    const name = url.searchParams.get("name") || path.split("/").pop() || "file";
    const download = url.searchParams.get("download") === "1";

    try {
      const repoId = repoKey(owner, repo);
      const cacheCtx: CacheContext = { req: request, ctx };
      const result = await readPath(env, repoId, ref, path, cacheCtx);
      if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
      const streamResponse = await readBlobStream(env, repoId, result.oid);
      if (!streamResponse) return new Response("Not found\n", { status: 404 });

      const headers = new Headers(streamResponse.headers);
      // Best-effort content type based on extension for inline rendering in <img>
      const ext = (name.split(".").pop() || "").toLowerCase();
      const ct =
        ext === "png"
          ? "image/png"
          : ext === "jpg" || ext === "jpeg"
            ? "image/jpeg"
            : ext === "gif"
              ? "image/gif"
              : ext === "webp"
                ? "image/webp"
                : ext === "bmp"
                  ? "image/bmp"
                  : ext === "ico"
                    ? "image/x-icon"
                    : ext === "svg"
                      ? "image/svg+xml"
                      : "application/octet-stream";
      headers.set("Content-Type", ct);
      if (download) headers.set("Content-Disposition", `attachment; filename="${name}"`);
      else headers.set("Content-Disposition", `inline; filename="${name}"`);
      return new Response(streamResponse.body, { status: streamResponse.status, headers });
    } catch (e: any) {
      return new Response("Not found\n", { status: 404 });
    }
  });
}
