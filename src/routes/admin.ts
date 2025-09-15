import { AutoRouter } from "itty-router";
import { getRepoStub, isValidOid } from "@/common";
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

  // Admin: clear hydration state and hydration-generated packs
  router.delete(`/:owner/:repo/admin/hydrate`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const res = await stub.clearHydration();
      return new Response(JSON.stringify({ ok: true, ...res }), {
        headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ ok: false, error: String(e) }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  });

  // Admin: trigger hydration (dry-run by default)
  // POST body: { dryRun?: boolean }
  router.post(`/:owner/:repo/admin/hydrate`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    let body: { dryRun?: boolean } = {};
    try {
      body = await (request as Request).json();
    } catch {}
    const dryRun = body?.dryRun !== false; // default to true
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const res = await stub.startHydration({ dryRun });
      const json = JSON.stringify(res);
      return new Response(json, {
        status: dryRun ? 200 : 202,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: String(e) }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
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
        const refs = await stub.listRefs();
        present = Array.isArray(refs) && refs.length > 0;
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
    try {
      const refs = await stub.listRefs();
      return new Response(JSON.stringify(refs), {
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response("[]", { headers: { "Content-Type": "application/json" } });
    }
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
    try {
      const refs = JSON.parse(body);
      await stub.setRefs(refs);
      return new Response("OK\n");
    } catch {
      return new Response("Invalid refs payload\n", { status: 400 });
    }
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
    try {
      const head = await stub.getHead();
      return new Response(JSON.stringify(head), {
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response("Not found\n", { status: 404 });
    }
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
    try {
      const head = JSON.parse(body);
      await stub.setHead(head);
      return new Response("OK\n");
    } catch {
      return new Response("Invalid head payload\n", { status: 400 });
    }
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
    try {
      const state = await stub.debugState();
      return new Response(JSON.stringify(state), {
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response("{}", { headers: { "Content-Type": "application/json" } });
    }
  });

  // Debug: check a specific commit's tree presence
  router.get(`/:owner/:repo/admin/debug-commit/:commit`, async (request, env: Env) => {
    const { owner, repo, commit } = request.params as {
      owner: string;
      repo: string;
      commit: string;
    };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    if (!isValidOid(commit)) {
      return new Response("Invalid commit\n", { status: 400 });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.debugCheckCommit(commit);
      return new Response(JSON.stringify(result), {
        headers: { "Content-Type": "application/json" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: String(e) }), {
        headers: { "Content-Type": "application/json" },
        status: 500,
      });
    }
  });
}
