import { AutoRouter } from "itty-router";
import {
  getHeadAndRefs,
  readPath,
  listCommitsFirstParentRange,
  listMergeSideFirstParent,
  readCommitInfo,
  readBlobStream,
  type TreeEntry,
} from "@/git";
import { repoKey } from "@/keys";
import {
  detectBinary,
  formatSize,
  bytesToText,
  formatWhen,
  renderView,
  renderViewStream,
  getMarkdownHighlightLangs,
  getHighlightLangsForBlobSmart,
  isValidOwnerRepo,
  isValidRef,
  isValidPath,
  OID_RE,
  getContentTypeFromName,
} from "@/web";
import { HttpError } from "@/web";
import { listReposForOwner } from "@/registry";
import { buildCacheKeyFrom, cacheOrLoadJSON, cacheOrLoadJSONWithTTL, CacheContext } from "@/cache";
import { handleError } from "@/web/templates";
import { getUnpackProgress } from "@/common";

// Shorthand for 400 Bad Request using the shared error handler
async function badRequest(
  env: Env,
  title: string,
  message: string,
  extra?: { owner?: string; repo?: string; refEnc?: string; path?: string }
): Promise<Response> {
  return handleError(env, new HttpError(400, message, { expose: true }), title, extra);
}

export function registerUiRoutes(router: ReturnType<typeof AutoRouter>) {
  // Owner repos list
  router.get(`/:owner`, async (request, env: Env) => {
    const { owner } = request.params as { owner: string };
    if (!isValidOwnerRepo(owner)) {
      return badRequest(env, "Invalid owner", "Owner contains invalid characters or length");
    }
    const repos = await listReposForOwner(env, owner);
    const stream = await renderViewStream(env, "owner", {
      title: `${owner} · Repositories`,
      owner,
      repos,
    });
    return new Response(stream, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-stream",
      },
    });
  });
  // Repo overview page
  router.get(`/:owner/:repo`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);

    // Cache HEAD and refs for 60 seconds for branches, longer for tags
    const cacheKeyRefs = buildCacheKeyFrom(request, "/_cache/refs", {
      repo: repoId,
    });

    const refsData = await cacheOrLoadJSON<{ head: any; refs: any[] }>(
      cacheKeyRefs,
      async () => {
        try {
          const result = await getHeadAndRefs(env, repoId);
          return { head: result.head, refs: result.refs };
        } catch {
          return null;
        }
      },
      60,
      ctx
    );
    const head: any = refsData?.head;
    const refs: any[] = refsData?.refs || [];

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

    // Try to load README at repo root on default branch with caching (5 minutes)
    const cacheKeyReadme = buildCacheKeyFrom(request, "/_cache/readme", {
      repo: repoId,
      ref: refShort,
    });
    const readmeData = await cacheOrLoadJSON<{ md: string }>(
      cacheKeyReadme,
      async () => {
        try {
          // Load all candidates in parallel for better performance
          const candidates = ["README.md", "README.MD", "Readme.md", "README", "readme.md"];
          const cacheCtx: CacheContext = { req: request, ctx };
          const results = await Promise.all(
            candidates.map(async (name) => {
              try {
                const res = await readPath(env, repoId, refShort, name, cacheCtx);
                if (res.type === "blob") {
                  return { name, content: res.content };
                }
              } catch {}
              return null;
            })
          );
          const found = results.find((r) => r !== null) as {
            name: string;
            content: Uint8Array;
          } | null;
          if (!found) return null;
          const text = bytesToText(found.content);
          return { md: text };
        } catch {
          return null;
        }
      },
      300,
      ctx
    );
    const readmeMd = readmeData?.md || "";

    // Check unpacking progress (shared helper)
    const progress = await getUnpackProgress(env, repoId);

    const stream = await renderViewStream(env, "overview", {
      title: `${owner}/${repo}`,
      owner,
      repo,
      refShort,
      refEnc: refEnc,
      branches: branchesData,
      tags: tagsData,
      readmeMd,
      // Load markdown + syntax highlighting assets only if README is present
      needsMarkdown: Boolean(readmeMd),
      needsHighlight: Boolean(readmeMd),
      highlightLangs: readmeMd ? getMarkdownHighlightLangs() : [],
      progress,
    });
    return new Response(stream, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-stream",
      },
    });
  });

  // Tree/Blob browser using query params: ?ref=<branch|tag|oid>&path=<path>
  router.get(`/:owner/:repo/tree`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const path = u.searchParams.get("path") || "";
    if (!isValidRef(ref)) {
      return badRequest(env, "Invalid ref", "Ref format not allowed", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
    if (path && !isValidPath(path)) {
      return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }

    // Build cache key for tree content
    const cacheKeyTree = buildCacheKeyFrom(request, "/_cache/tree", {
      repo: repoId,
      ref,
      path,
    });

    const result = await cacheOrLoadJSONWithTTL<any>(
      cacheKeyTree,
      async () => {
        try {
          const cacheCtx: CacheContext = { req: request, ctx };
          return await readPath(env, repoId, ref, path, cacheCtx);
        } catch {
          return null;
        }
      },
      (value) => (value && value.type === "tree" ? 60 : 300),
      ctx
    );

    // Handle missing tree/blob result gracefully (e.g., non-existent repo or path)
    if (!result) {
      try {
        const errHtml = await renderView(env, "error", {
          title: `${owner}/${repo} · Tree`,
          message: "Not found",
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          path,
        });
        if (errHtml) {
          return new Response(errHtml, {
            status: 404,
            headers: { "Content-Type": "text/html; charset=utf-8" },
          });
        }
      } catch {}
      return new Response("Not found\n", { status: 404 });
    }

    try {
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
        const stream = await renderViewStream(env, "tree", {
          title: `${path || "root"} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          progress,
          breadcrumbs,
          parentHref,
          entries,
        });
        return new Response(stream, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-stream",
          },
        });
      } else {
        const raw = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}`;
        const text = bytesToText(result.content);
        const lineCount = text === "" ? 0 : text.split(/\r?\n/).length;
        const title = path || result.oid;
        // Infer language and load only what we need (use smart inference with content)
        const langs = getHighlightLangsForBlobSmart(title, text);
        const codeLang = langs[0] || null;
        const stream = await renderViewStream(env, "blob", {
          title: `${title} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName: title,
          viewRawHref: `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(title)}`,
          rawHref: raw,
          // Structured fields instead of HTML
          codeText: text,
          codeLang,
          lineCount,
          contentClass: "markdown-content",
          needsHighlight: true,
          highlightLangs: langs,
        });
        return new Response(stream, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-stream",
          },
        });
      }
    } catch (e: any) {
      return handleError(env, e, `${owner}/${repo} · Tree`, {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
  });

  // Blob preview endpoint - renders file content with syntax highlighting and media previews
  router.get(`/:owner/:repo/blob`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const path = u.searchParams.get("path") || "";
    if (!isValidRef(ref)) {
      return badRequest(env, "Invalid ref", "Ref format not allowed", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
    if (path && !isValidPath(path)) {
      return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
    try {
      const cacheCtx: CacheContext = { req: request, ctx };
      const result = await readPath(env, repoId, ref, path, cacheCtx);
      if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
      const fileName = path || result.oid;

      // Too large to render inline
      if (result.tooLarge) {
        const sizeStr = formatSize(result.size || 0);
        const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(fileName)}`;
        const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&download=1&name=${encodeURIComponent(fileName)}`;
        const stream = await renderViewStream(env, "blob", {
          title: `${fileName} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName,
          tooLarge: true,
          sizeStr,
          viewRawHref,
          rawHref,
        });
        return new Response(stream, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-stream",
          },
        });
      }

      // Binary vs text
      const isBinary = detectBinary(result.content);
      const size = result.content.byteLength;
      const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(fileName)}`;
      const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&download=1&name=${encodeURIComponent(fileName)}`;
      const templateData: Record<string, any> = {
        title: `${fileName} · ${owner}/${repo}`,
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        fileName,
        viewRawHref,
        rawHref,
        contentClass: !isBinary ? "markdown-content" : undefined,
      };

      if (isBinary) {
        const ext = (fileName.split(".").pop() || "").toLowerCase();
        const isImage = ["png", "jpg", "jpeg", "gif", "webp", "bmp", "ico", "svg"].includes(ext);
        const isPdf = ext === "pdf";
        if ((isImage || isPdf) && path) {
          const name = encodeURIComponent(fileName);
          const mediaSrc = `/${owner}/${repo}/rawpath?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(path)}&name=${name}`;
          templateData.isImage = isImage;
          templateData.isPdf = isPdf;
          templateData.mediaSrc = mediaSrc;
          templateData.sizeStr = formatSize(size);
        } else {
          templateData.isBinary = true;
          templateData.sizeStr = formatSize(size);
        }
      } else {
        const text = bytesToText(result.content);
        const lineCount = text === "" ? 0 : text.split(/\r?\n/).length;
        const isMd =
          fileName.toLowerCase().endsWith(".md") || fileName.toLowerCase().endsWith(".markdown");
        if (isMd) {
          const baseDir = (path || "").split("/").filter(Boolean).slice(0, -1).join("/");
          templateData.isMarkdown = true;
          templateData.markdownRaw = text;
          templateData.lineCount = lineCount;
          templateData.mdOwner = owner;
          templateData.mdRepo = repo;
          templateData.mdRef = ref;
          templateData.mdBase = baseDir;
          // Markdown rendering will handle highlighting after sanitize
          templateData.needsMarkdown = true;
        } else {
          const langs = getHighlightLangsForBlobSmart(fileName, text);
          const codeLang = langs[0] || null;
          templateData.codeText = text;
          templateData.codeLang = codeLang;
          templateData.lineCount = lineCount;
          templateData.needsHighlight = true;
          templateData.highlightLangs = langs;
          if (!codeLang) {
            templateData.sizeStr = formatSize(size);
          }
        }
      }

      const stream = await renderViewStream(env, "blob", templateData);
      return new Response(stream, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-stream",
        },
      });
    } catch (e: any) {
      return handleError(env, e, `Error · ${owner}/${repo}`, {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
  });

  // Commit list
  router.get(`/:owner/:repo/commits`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    if (!isValidRef(ref)) {
      return badRequest(env, "Invalid ref", "Ref format not allowed", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
      });
    }
    const pageRaw = u.searchParams.get("page") || "";
    const perRaw = Number(u.searchParams.get("per_page") || "25");
    const perPage = Number.isFinite(perRaw) ? Math.max(5, Math.min(100, Math.floor(perRaw))) : 25;
    try {
      const cacheCtx: CacheContext = { req: request, ctx };
      let page = Number(pageRaw);
      if (!Number.isFinite(page) || page < 0) page = 0;
      let offset = page * perPage;

      const cacheKey = buildCacheKeyFrom(request, "/_cache/commits", {
        repo: repoId,
        ref,
        per_page: String(perPage),
        page: String(page),
        offset: String(offset),
      });

      const commitsView = await cacheOrLoadJSONWithTTL<
        Array<{
          oid: string;
          shortOid: string;
          firstLine: string;
          authorName: string;
          when: string;
        }>
      >(
        cacheKey,
        async () => {
          const commits = await listCommitsFirstParentRange(
            env,
            repoId,
            ref,
            offset,
            perPage,
            cacheCtx
          );
          return commits.map((c) => ({
            oid: c.oid,
            shortOid: c.oid.slice(0, 7),
            firstLine: (c.message || "").split(/\r?\n/, 1)[0],
            authorName: c.author?.name || "",
            when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
            isMerge: Array.isArray(c.parents) && c.parents.length > 1,
          }));
        },
        () => {
          const isOid = OID_RE.test(ref);
          const isTag = ref.startsWith("refs/tags/");
          return isOid || isTag ? 3600 : 60;
        },
        ctx
      );
      const list = commitsView || [];
      const last = list[list.length - 1]?.oid || "";
      const refEnc = encodeURIComponent(ref);
      const pager = {
        perPageLinks: [10, 25, 50].map((n) => ({
          text: String(n),
          href: `/${owner}/${repo}/commits?ref=${refEnc}&page=${page}&per_page=${n}`,
        })),
        newerHref:
          page > 0
            ? `/${owner}/${repo}/commits?ref=${refEnc}&page=${page - 1}&per_page=${perPage}`
            : undefined,
        olderHref:
          last && list.length === perPage
            ? `/${owner}/${repo}/commits?ref=${refEnc}&page=${page + 1}&per_page=${perPage}`
            : undefined,
      };
      const progress = await getUnpackProgress(env, repoId);
      const stream = await renderViewStream(env, "commits", {
        title: `Commits on ${ref} · ${owner}/${repo}`,
        owner,
        repo,
        ref,
        refEnc,
        commits: list,
        pager,
        progress,
      });
      return new Response(stream, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-stream",
        },
      });
    } catch (e: any) {
      return handleError(env, e, `Error · ${owner}/${repo}`, {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
      });
    }
  });

  // Merge expansion fragment endpoint: returns HTML <tr> rows for side-branch commits of a merge
  // Example: /:owner/:repo/commits/fragments/:oid?limit=20
  router.get(
    `/:owner/:repo/commits/fragments/:oid`,
    async (request, env: Env, ctx: ExecutionContext) => {
      const { owner, repo, oid } = request.params as { owner: string; repo: string; oid: string };
      if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
        return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
      }
      if (!OID_RE.test(oid)) {
        return badRequest(env, "Invalid OID", "OID must be 40 hex", {
          owner,
          repo,
          refEnc: encodeURIComponent(oid),
        });
      }
      const u = new URL(request.url);
      const limitRaw = Number(u.searchParams.get("limit") || "20");
      const limit = Number.isFinite(limitRaw)
        ? Math.max(1, Math.min(100, Math.floor(limitRaw)))
        : 20;
      const repoId = repoKey(owner, repo);
      try {
        const cacheCtx: CacheContext = { req: request, ctx };
        const side = await listMergeSideFirstParent(
          env,
          repoId,
          oid,
          limit,
          {
            scanLimit: Math.min(400, limit * 5),
            timeBudgetMs: 5000, // Increased for production R2 latency
            mainlineProbe: 50, // Reduced to speed up initial probe
          },
          cacheCtx
        );
        const commits = (side || []).map((c) => ({
          oid: c.oid,
          shortOid: c.oid.slice(0, 7),
          firstLine: (c.message || "").split(/\r?\n/, 1)[0],
          authorName: c.author?.name || "",
          when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
        }));
        const stream = await renderViewStream(env, "commit_rows", {
          owner,
          repo,
          commits,
          compact: true,
          mergeOf: oid,
        });
        return new Response(stream, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Page-Renderer": "liquid-fragment",
          },
        });
      } catch (e: any) {
        return handleError(env, e, `Error · ${owner}/${repo}`, {
          owner,
          repo,
          refEnc: encodeURIComponent(oid),
        });
      }
    }
  );

  // Commit details
  router.get(`/:owner/:repo/commit/:oid`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo, oid } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    if (!OID_RE.test(oid)) {
      return badRequest(env, "Invalid commit OID", "Commit id must be 40-hex", {
        owner,
        repo,
        refEnc: encodeURIComponent(oid),
      });
    }
    const repoId = repoKey(owner, repo);
    try {
      const cacheCtx: CacheContext = { req: request, ctx };
      const c = await readCommitInfo(env, repoId, oid, cacheCtx);
      const when = c.author ? formatWhen(c.author.when, c.author.tz) : "";
      const parents = (c.parents || []).map((p) => ({ oid: p, short: p.slice(0, 7) }));
      const stream = await renderViewStream(env, "commit", {
        title: `${c.oid.slice(0, 7)} · ${owner}/${repo}`,
        owner,
        repo,
        refEnc: encodeURIComponent(c.oid),
        commitShort: c.oid.slice(0, 7),
        authorName: c.author?.name || "",
        authorEmail: c.author?.email || "",
        when,
        parents,
        treeShort: (c.tree || "").slice(0, 7),
        message: c.message || "",
      });
      return new Response(stream, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-stream",
        },
      });
    } catch (e: any) {
      return handleError(env, e, `Error · ${owner}/${repo}`, {
        owner,
        repo,
        refEnc: encodeURIComponent(oid),
        path: "",
      });
    }
  });

  // Raw blob endpoint - streams file content without buffering
  router.get(`/:owner/:repo/raw`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return new Response("Bad Request\n", { status: 400 });
    }
    const url = new URL(request.url);
    const oid = url.searchParams.get("oid") || "";
    if (!OID_RE.test(oid)) return new Response("Bad Request\n", { status: 400 });
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
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return new Response("Bad Request\n", { status: 400 });
    }
    const url = new URL(request.url);
    const ref = url.searchParams.get("ref") || "main";
    const path = url.searchParams.get("path") || "";
    const name = url.searchParams.get("name") || path.split("/").pop() || "file";
    if (!isValidRef(ref) || !isValidPath(path))
      return new Response("Bad Request\n", { status: 400 });
    const download = url.searchParams.get("download") === "1";

    // Basic hotlink protection: require same-origin Referer
    try {
      const referer = request.headers.get("referer") || "";
      const allowed = (() => {
        try {
          const r = new URL(referer);
          return r.host === url.host;
        } catch {
          return false;
        }
      })();
      if (!allowed) {
        return new Response("Hotlinking not allowed\n", { status: 403 });
      }
    } catch {}

    try {
      const repoId = repoKey(owner, repo);
      const cacheCtx: CacheContext = { req: request, ctx };
      const result = await readPath(env, repoId, ref, path, cacheCtx);
      if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
      const streamResponse = await readBlobStream(env, repoId, result.oid);
      if (!streamResponse) return new Response("Not found\n", { status: 404 });

      const headers = new Headers(streamResponse.headers);
      headers.set("Content-Type", getContentTypeFromName(name));
      if (download) headers.set("Content-Disposition", `attachment; filename="${name}"`);
      else headers.set("Content-Disposition", `inline; filename="${name}"`);
      return new Response(streamResponse.body, { status: streamResponse.status, headers });
    } catch (e: any) {
      return new Response("Not found\n", { status: 404 });
    }
  });

  // Async refs API for repo_nav dropdown
  router.get(`/:owner/:repo/api/refs`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return new Response(JSON.stringify({ branches: [], tags: [] }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    const repoId = repoKey(owner, repo);
    try {
      const cacheKey = buildCacheKeyFrom(request, "/_cache/refs", { repo: repoId });
      const refsData = await cacheOrLoadJSON<{ refs: any[] }>(
        cacheKey,
        async () => {
          try {
            const result = await getHeadAndRefs(env, repoId);
            return { refs: result.refs };
          } catch {
            return null;
          }
        },
        60,
        ctx
      );
      const refs = refsData?.refs || [];
      const branches = refs
        .filter((r: any) => r.name && r.name.startsWith("refs/heads/"))
        .map((b: any) => {
          const short = b.name.replace("refs/heads/", "");
          return {
            name: encodeURIComponent(short),
            displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
          };
        });
      const tags = refs
        .filter((r: any) => r.name && r.name.startsWith("refs/tags/"))
        .map((t: any) => {
          const short = t.name.replace("refs/tags/", "");
          return {
            name: encodeURIComponent(short),
            displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
          };
        });
      return new Response(JSON.stringify({ branches, tags }), {
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "public, max-age=60",
        },
      });
    } catch (e: any) {
      return new Response(
        JSON.stringify({ branches: [], tags: [], error: String(e?.message || e) }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  });
}
