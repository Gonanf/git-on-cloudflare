import { AutoRouter } from "itty-router";
import { getRepoStub, isValidOid, json } from "@/common";
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
    return json({ owner, repos });
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
      return json({ ok: true, ...res }, 200, { "Cache-Control": "no-cache" });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
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
      return json(res, dryRun ? 200 : 202, { "Cache-Control": "no-cache" });
    } catch (e) {
      return json({ error: String(e) }, 500);
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
    return json({ owner, ...updated });
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
      return json(refs);
    } catch {
      return json([]);
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
      return json(head);
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
      return json(state);
    } catch {
      return json({});
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
      return json(result);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  });

  // Debug: check if an OID exists in loose, R2 loose, and/or packs
  router.get(`/:owner/:repo/admin/debug-oid/:oid`, async (request, env: Env) => {
    const { owner, repo, oid } = request.params as {
      owner: string;
      repo: string;
      oid: string;
    };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    if (!isValidOid(oid)) {
      return new Response("Invalid OID\n", { status: 400 });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.debugCheckOid(oid);
      return json(result);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  });

  // Admin: Remove a specific pack file
  router.delete(`/:owner/:repo/admin/pack/:packKey`, async (request, env: Env) => {
    const { owner, repo, packKey } = request.params as {
      owner: string;
      repo: string;
      packKey: string;
    };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }

    if (!packKey) {
      return json({ error: "Pack key is required" }, 400);
    }

    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.removePack(packKey);
      return json({ ok: result.removed, ...result });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  });

  // Admin: DANGEROUS - completely purge repo (all R2 objects + DO storage)
  router.delete(`/:owner/:repo/admin/purge`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }

    // Require explicit confirmation
    const body: any = await request.json().catch(() => ({}));
    if (body.confirm !== `purge-${owner}/${repo}`) {
      return json(
        {
          error: "Confirmation required",
          hint: `Set confirm to "purge-${owner}/${repo}"`,
        },
        400
      );
    }

    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.purgeRepo();

      // Remove from owner registry
      await removeRepoFromOwner(env, owner, repo);

      return json({ ok: true, ...result });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  });
}
