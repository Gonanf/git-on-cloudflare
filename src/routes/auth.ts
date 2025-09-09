import { AutoRouter } from "itty-router";
import { renderView, renderPage } from "@/web";
import { getAuthStub } from "@/common";

export function registerAuthRoutes(router: ReturnType<typeof AutoRouter>) {
  // Auth UI page
  router.get(`/auth`, async (request, env: Env) => {
    const html = await renderView(env, "auth", {});
    if (html) {
      return new Response(html, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-layout",
        },
      });
    }
    const body = `<h1>Auth</h1><p>Auth page asset missing.</p>`;
    return renderPage(env, request, "Auth · git-on-cloudflare", body);
  });

  // Trailing slash alias
  router.get(`/auth/`, async (request, env: Env) => {
    const html = await renderView(env, "auth", {});
    if (html) {
      return new Response(html, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "liquid-layout",
        },
      });
    }
    const body = `<h1>Auth</h1><p>Auth page asset missing.</p>`;
    return renderPage(env, request, "Auth · git-on-cloudflare", body);
  });

  // List users
  router.get(`/auth/api/users`, async (request, env: Env) => {
    const stub = getAuthStub(env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    return stub.fetch("https://auth/users", {
      method: "GET",
      headers: { Authorization: request.headers.get("Authorization") || "" },
    });
  });

  // Create user
  router.post(`/auth/api/users`, async (request, env: Env) => {
    const stub = getAuthStub(env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const body = await request.text();
    const h = {
      "Content-Type": "application/json",
      Authorization: request.headers.get("Authorization") || "",
    };
    return stub.fetch("https://auth/users", { method: "POST", body, headers: h });
  });

  // Delete user
  router.delete(`/auth/api/users`, async (request, env: Env) => {
    const stub = getAuthStub(env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const body = await request.text();
    const h = {
      "Content-Type": "application/json",
      Authorization: request.headers.get("Authorization") || "",
    };
    return stub.fetch("https://auth/users", { method: "DELETE", body, headers: h });
  });
}
