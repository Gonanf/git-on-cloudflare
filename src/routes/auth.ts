import { AutoRouter } from "itty-router";
import { loadAsset, renderPage } from "@/web";
import { getAuthStub } from "@/common";

export function registerAuthRoutes(router: ReturnType<typeof AutoRouter>) {
  // Auth UI page
  router.get(`/auth`, async (request: any, env: Env) => {
    const body =
      (await loadAsset(env, "templates/auth.html", request as Request)) ||
      `<h1>Auth</h1><p>Auth page asset missing.</p>`;
    return renderPage(env, request as Request, "Auth · git-on-cloudflare", body);
  });

  // Trailing slash alias
  router.get(`/auth/`, async (request: any, env: Env) => {
    const body =
      (await loadAsset(env, "templates/auth.html", request as Request)) ||
      `<h1>Auth</h1><p>Auth page asset missing.</p>`;
    return renderPage(env, request as Request, "Auth · git-on-cloudflare", body);
  });

  // List users
  router.get(`/auth/api/users`, async (request: any, env: Env) => {
    const stub = getAuthStub(env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    return stub.fetch("https://auth/users", {
      method: "GET",
      headers: { Authorization: (request as Request).headers.get("Authorization") || "" },
    });
  });

  // Create user
  router.post(`/auth/api/users`, async (request: any, env: Env) => {
    const stub = getAuthStub(env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const body = await (request as Request).text();
    const h = {
      "Content-Type": "application/json",
      Authorization: (request as Request).headers.get("Authorization") || "",
    };
    return stub.fetch("https://auth/users", { method: "POST", body, headers: h });
  });

  // Delete user
  router.delete(`/auth/api/users`, async (request: any, env: Env) => {
    const stub = getAuthStub(env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const body = await (request as Request).text();
    const h = {
      "Content-Type": "application/json",
      Authorization: (request as Request).headers.get("Authorization") || "",
    };
    return stub.fetch("https://auth/users", { method: "DELETE", body, headers: h });
  });
}
