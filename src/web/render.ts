import { escapeHtml } from "./format";
import { renderView } from "./templates";
export { renderView };

/**
 * Load a static asset via Wrangler [assets]
 * @param env - Cloudflare environment bindings
 * @param relPath - Relative path under the assets directory
 * @param req - Optional request, used to build a base URL for fetch
 */
export async function loadAsset(env: Env, relPath: string, req?: Request): Promise<string | null> {
  if (!env.ASSETS) return null;
  try {
    const base = req ? req.url : "https://assets.local";
    const u = new URL("/" + relPath.replace(/^\/+/, ""), base);
    const res = await env.ASSETS.fetch(new Request(u.toString()));
    if (!res || !res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

/**
 * Escape a string for use in RegExp
 */
export function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Render an HTML template by replacing {{ keys }} with values from `data`
 * @param env - Cloudflare environment bindings
 * @param req - Optional request for asset base resolution
 * @param name - Template path (e.g., templates/layout.html)
 * @param data - Key/value map to substitute
 */
export async function renderTemplate(
  env: Env,
  req: Request | undefined,
  name: string,
  data: Record<string, string>
): Promise<string | null> {
  const tpl = await loadAsset(env, name, req);
  if (!tpl) return null;
  let out = tpl;
  for (const [k, v] of Object.entries(data)) {
    out = out.replace(new RegExp("{{\\s*" + escapeRegExp(k) + "\\s*}}", "g"), v);
  }
  return out;
}

/**
 * Render a full HTML page using the layout template; falls back to inline wrapper
 */
export async function renderPage(
  env: Env,
  req: Request | undefined,
  title: string,
  bodyHtml: string
): Promise<Response> {
  // Render Liquid layout, injecting content as raw HTML
  const doc = await renderView(env, "layout", {
    title: escapeHtml(title),
    content: bodyHtml,
  });
  if (doc)
    return new Response(doc, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "layout",
      },
    });
  // Fallback to built-in wrapper
  return new Response(
    `<!DOCTYPE html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>git-on-cloudflare</title><link rel="stylesheet" href="/base.css"></head><body>${bodyHtml}</body></html>`,
    {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "inline",
      },
    }
  );
}
