import { AutoRouter } from "itty-router";
import { capabilityAdvertisement, parseV2Command } from "../git/protocol";
import { handleFetchV2 } from "../git/uploadPack";
import { JsRepoEngine } from "../git/repoEngine";
import { getRepoStub } from "../util/stub";
import { repoKey } from "../keys";
import { verifyAuth } from "../util/auth";
import { addRepoToOwner, removeRepoFromOwner } from "../util/ownerRegistry";

/**
 * Handles Git upload-pack (fetch) POST requests.
 * Supports both protocol v2 and legacy protocol based on Git-Protocol header.
 * @param env - Worker environment
 * @param repoId - Repository identifier (owner/repo)
 * @param request - Incoming HTTP request
 * @returns Response with pack data or error
 */
async function handleUploadPackPOST(env: Env, repoId: string, request: Request) {
  const body = new Uint8Array(await request.arrayBuffer());
  const gitProto = request.headers.get("Git-Protocol") || "";
  const { command } = parseV2Command(body);
  // Accept either explicit v2 header or a v2-formatted body (contains command=...)
  if (!/version=2/.test(gitProto) && !command) {
    return new Response("Expected Git protocol v2 (set Git-Protocol: version=2)\n", {
      status: 400,
    });
  }

  const engine = new JsRepoEngine(env, repoId);

  if (command === "ls-refs") {
    const [head, refs] = await Promise.all([
      engine.getHead().catch(() => undefined),
      engine.listRefs().catch(() => []),
    ]);
    const chunks: Uint8Array[] = [];
    // Reuse pkt-line helpers via dynamic import to avoid direct dependency here
    const { pktLine, flushPkt, concatChunks } = await import("../git/pktline");
    // Per spec, HEAD should be first when available
    if (head && head.target) {
      const t = (refs as { name: string; oid: string }[]).find((r) => r.name === head.target);
      const headOid = head.oid ?? t?.oid;
      // Treat HEAD as unborn only if the target ref doesn't exist
      if (headOid) {
        chunks.push(pktLine(`${headOid} HEAD symref-target:${head.target}\n`));
      } else {
        chunks.push(pktLine(`unborn HEAD symref-target:${head.target}\n`));
      }
    }
    for (const r of refs) {
      chunks.push(pktLine(`${r.oid} ${r.name}\n`));
    }
    chunks.push(flushPkt());
    return new Response(concatChunks(chunks), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  if (command === "fetch") {
    return handleFetchV2(env, repoId, body, request.signal);
  }

  return new Response("Unsupported command or malformed request\n", { status: 400 });
}

/**
 * Handles Git receive-pack (push) POST requests.
 * Forwards the request to the repository Durable Object and updates owner registry.
 * @param env - Worker environment
 * @param repoId - Repository identifier (owner/repo)
 * @param request - Incoming HTTP request with push data
 * @returns Response with receive-pack result
 */
async function handleReceivePackPOST(env: Env, repoId: string, request: Request) {
  // Forward raw body to the Durable Object /receive endpoint
  const stub = getRepoStub(env, repoId);
  const ct = request.headers.get("Content-Type") || "application/x-git-receive-pack-request";
  const res = await stub.fetch("https://do/receive", {
    method: "POST",
    body: request.body,
    headers: { "Content-Type": ct },
    signal: request.signal,
  });
  // Proxy DO response through
  const headers = new Headers(res.headers);
  if (!headers.has("Content-Type"))
    headers.set("Content-Type", "application/x-git-receive-pack-result");
  headers.set("Cache-Control", "no-cache");
  // Update owner registry on change signal
  try {
    const changed = res.headers.get("X-Repo-Changed") === "1";
    if (changed) {
      const empty = res.headers.get("X-Repo-Empty") === "1";
      const [owner, repo] = repoId.split("/", 2);
      if (owner && repo) {
        if (empty) await removeRepoFromOwner(env, owner, repo);
        else await addRepoToOwner(env, owner, repo);
      }
    }
  } catch {}
  return new Response(res.body, { status: res.status, headers });
}

/**
 * Registers Git Smart HTTP v2 routes on the router.
 * Sets up handlers for info/refs, upload-pack, and receive-pack endpoints.
 * @param router - The application router instance
 */
export function registerGitRoutes(router: ReturnType<typeof AutoRouter>) {
  // Git info/refs
  router.get(`/:owner/:repo/info/refs`, async (request, env: Env) => {
    const u = new URL(request.url);
    const service = u.searchParams.get("service");
    if (service === "git-upload-pack" || service === "git-receive-pack") {
      const { owner, repo } = request.params;
      return await capabilityAdvertisement(env, service, repoKey(owner, repo));
    }
    return new Response("Missing or unsupported service\n", { status: 400 });
  });

  // git-upload-pack (POST)
  router.post(`/:owner/:repo/git-upload-pack`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    return handleUploadPackPOST(env, repoKey(owner, repo), request);
  });

  // git-receive-pack (POST)
  router.post(`/:owner/:repo/git-receive-pack`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, false))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    return handleReceivePackPOST(env, repoKey(owner, repo), request);
  });
}
