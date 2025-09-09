import { renderView } from "./templates";
export { renderView };

/**
 * Render a full HTML page using the inline wrapper
 */
export async function renderPage(
  env: Env,
  req: Request | undefined,
  title: string,
  bodyHtml: string
): Promise<Response> {
  // Only use an inline wrapper as a last-resort fallback.
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
