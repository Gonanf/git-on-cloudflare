import type { FS } from "liquidjs";

import { Liquid } from "liquidjs";
import { escapeHtml } from "./format";
import { isHttpError } from "./http";

/**
 * LiquidJS engine configured to load templates from Wrangler ASSETS.
 * - Root directories map to `src/assets/templates/` and its `partials/` subdir.
 * - HTML output is escaped by default via outputEscape: "escape".
 * - We provide a minimal FS adapter that fetches template text via env.ASSETS.
 */
let cachedEngine: Liquid | null = null;

function normalize(p: string): string {
  const parts = p.replace(/\\/g, "/").split("/");
  const stack: string[] = [];
  for (const seg of parts) {
    if (!seg || seg === ".") continue;
    if (seg === "..") stack.pop();
    else stack.push(seg);
  }
  return stack.join("/");
}

function join(a: string, b: string): string {
  if (!a) return normalize(b);
  if (!b) return normalize(a);
  return normalize(a.replace(/\/$/, "") + "/" + b.replace(/^\//, ""));
}

function withExt(file: string, ext: string): string {
  if (!ext) return file;
  if (/\.[^/.]+$/.test(file)) return file;
  return file + ext;
}

function createAssetFs(env: Env): NonNullable<FS> {
  const roots = ["templates", "templates/partials"]; // must match Liquid options below
  function isContained(root: string, file: string) {
    const rp = normalize(root);
    const fp = normalize(file);
    return fp === rp || fp.startsWith(rp + "/");
  }
  const fsImpl: FS = {
    // Resolve a file path against a root with default ext
    resolve(root: string, file: string, ext: string) {
      const f = withExt(file, ext);
      return join(root || "", f);
    },
    // Directory name for a path, used by relative references
    dirname(file: string) {
      const s = normalize(file);
      const i = s.lastIndexOf("/");
      return i >= 0 ? s.slice(0, i) : "";
    },
    // Path separator
    sep: "/",
    // Existence checks: rely on contains guard; readFile will fail if missing
    async exists(file: string) {
      // Only allow files within our allowed roots
      return roots.some((r) => isContained(r, file));
    },
    existsSync(file: string) {
      return roots.some((r) => isContained(r, file));
    },
    contains(root: string, file: string) {
      // Prevent directory traversal outside configured roots
      const resolved = normalize(join(root || "", file));
      return roots.some((r) => isContained(r, resolved));
    },
    async readFile(file: string) {
      const path = "/" + normalize(file);
      if (!env.ASSETS) throw new Error("ASSETS binding not configured");
      const url = new URL(path, "https://assets.local");
      const res = await env.ASSETS.fetch(new Request(url.toString()));
      if (!res || !res.ok) throw new Error(`ENOENT: ${file}`);
      return await res.text();
    },
    readFileSync(_file: string) {
      // Not used in our async rendering path
      throw new Error("readFileSync not supported in this environment");
    },
  };
  return fsImpl;
}

function getEngine(env: Env): Liquid {
  if (cachedEngine) {
    return cachedEngine;
  }
  const engine = new Liquid({
    extname: ".liquid",
    root: ["templates", "templates/partials"],
    layouts: ["templates"],
    partials: ["templates/partials"],
    relativeReference: true,
    cache: true, // enable LRU parse cache
    jsTruthy: true,
    dynamicPartials: true,
    outputEscape: "escape", // escape {{ var }} by default
    fs: createAssetFs(env),
  });
  cachedEngine = engine;
  return engine;
}

/**
 * Render a body view (no DOCTYPE), e.g. "owner", "overview", etc.
 * Returns null if the template cannot be loaded/rendered.
 *
 * Usage guidance:
 * - Used for all template rendering in the application
 * - Returns the complete rendered HTML as a string
 * - Exceptions are caught before sending any bytes
 */
export async function renderView(
  env: Env,
  name: string,
  data: Record<string, unknown>
): Promise<string | null> {
  const engine = getEngine(env);
  // We expect templates under src/assets/templates/<name>.liquid
  return await engine.renderFile(name, data);
}

/**
 * Handle errors by returning a formatted HTTP response.
 * If the error is an HttpError, use its status and message; otherwise, default to 500.
 * This intentionally uses non-stream rendering to avoid partial output if
 * templates or assets fail during the render.
 *
 * @param env - Cloudflare environment bindings
 * @param e - Error object or unknown to handle
 * @param fallbackTitle - Title to use for the error page if not an HttpError
 * @param extra - Optional additional context for the error
 * @returns A Promise resolving to an HTTP Response object
 */
export async function handleError(
  env: Env,
  e: unknown,
  fallbackTitle: string,
  extra?: {
    owner?: string;
    repo?: string;
    refEnc?: string;
    path?: string;
  }
): Promise<Response> {
  const debug = String(env.LOG_LEVEL || "").toLowerCase() === "debug";
  const http = isHttpError(e) ? e : undefined;
  const status = http?.status ?? (/not found/i.test(String((e as any)?.message)) ? 404 : 500);
  try {
    const html = await renderView(env, "error", {
      title: fallbackTitle,
      message: String((e as any)?.message || e),
      owner: extra?.owner,
      repo: extra?.repo,
      refEnc: extra?.refEnc,
      path: extra?.path,
      stack: debug ? String((e as any)?.stack || "") : undefined,
    });
    if (html) {
      return new Response(html, {
        status,
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }
  } catch {}
  return new Response(`<h2>Error</h2><pre>${escapeHtml(String((e as any)?.message || e))}</pre>`, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}
