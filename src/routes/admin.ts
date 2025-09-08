import { AutoRouter } from "itty-router";
import { getRepoStub } from "@/common";
import { repoKey } from "@/keys";
import { verifyAuth } from "@/auth";
import { listReposForOwner, addRepoToOwner, removeRepoFromOwner } from "@/registry";

export function registerAdminRoutes(router: ReturnType<typeof AutoRouter>) {
  // Owner registry: list current repos from KV
  router.get(`/:owner/admin/registry`, async (request, env: Env) => {
    const { owner } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const repos = await listReposForOwner(env, owner);
    return new Response(JSON.stringify({ owner, repos }), {
      headers: { "Content-Type": "application/json" },
    });
  });

  // Owner registry: backfill/sync membership
  // POST body: { repos?: string[] } — if provided, (re)validate those; otherwise, revalidate existing KV entries
  router.post(`/:owner/admin/registry/sync`, async (request, env: Env) => {
    const { owner } = request.params as { owner: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    let input: { repos?: string[] } = {};
    try {
      input = await request.json();
    } catch {}
    let targets = input?.repos?.filter(Boolean) || [];
    if (targets.length === 0) {
      // revalidate existing KV entries only
      targets = await listReposForOwner(env, owner);
    }
    const updated: { added: string[]; removed: string[]; unchanged: string[] } = {
      added: [],
      removed: [],
      unchanged: [],
    };
    for (const repo of targets) {
      const stub = getRepoStub(env, repoKey(owner, repo));
      // consider present if refs has entries
      let present = false;
      try {
        const refsRes = await stub.fetch("https://do/refs", { method: "GET" });
        if (refsRes.ok) {
          const refs = (await refsRes.json<any>()) as { name: string; oid: string }[];
          present = Array.isArray(refs) && refs.length > 0;
        }
      } catch {}
      if (present) {
        await addRepoToOwner(env, owner, repo);
        updated.added.push(repo);
      } else {
        await removeRepoFromOwner(env, owner, repo);
        updated.removed.push(repo);
      }
    }
    return new Response(JSON.stringify({ owner, ...updated }), {
      headers: { "Content-Type": "application/json" },
    });
  });

  // Admin refs
  router.get(`/:owner/:repo/admin/refs`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    return stub.fetch("https://do/refs", { method: "GET" });
  });

  router.put(`/:owner/:repo/admin/refs`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    const body = await request.text();
    const ct = request.headers.get("Content-Type") || "application/json";
    return stub.fetch("https://do/refs", { method: "PUT", body, headers: { "Content-Type": ct } });
  });

  // Admin head
  router.get(`/:owner/:repo/admin/head`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    return stub.fetch("https://do/head", { method: "GET" });
  });

  router.put(`/:owner/:repo/admin/head`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    const body = await (request as Request).text();
    const ct = (request as Request).headers.get("Content-Type") || "application/json";
    return stub.fetch("https://do/head", { method: "PUT", body, headers: { "Content-Type": ct } });
  });

  // Debug: dump DO state (JSON)
  router.get(`/:owner/:repo/admin/debug-state`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    return stub.fetch("https://do/debug-state", { method: "GET" });
  });

  // Debug: check a specific commit's tree presence
  // GET param: ?commit=<40-hex>
  router.get(`/:owner/:repo/admin/debug-check`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const url = new URL((request as Request).url);
    const commit = url.searchParams.get("commit") || "";
    const stub = getRepoStub(env, repoKey(owner, repo));
    return stub.fetch(`https://do/debug-check?commit=${encodeURIComponent(commit)}`, {
      method: "GET",
    });
  });
}
