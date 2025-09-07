import { parsePktSection, pktLine, flushPkt, concatChunks } from "../pktline";
import { asTypedStorage, packOidsKey, RepoStateSchema, Head } from "../doState";
import { indexPackOnly } from "../pack/unpack.ts";
import { r2PackKey } from "../keys.ts";
import { createLogger } from "../util/logger";

/**
 * Handle git-receive-pack POST inside the Durable Object.
 *
 * @param state Durable Object state
 * @param env Worker environment (R2 bucket, vars)
 * @param prefix DO prefix for R2 keys, e.g., `do/<id>`
 * @param request Request from the Worker containing push data
 * @returns Response with `application/x-git-receive-pack-result` body (pkt-line `report-status`)
 */
export async function receivePack(
  state: DurableObjectState,
  env: Env,
  prefix: string,
  request: Request
): Promise<Response> {
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const log = createLogger(env.LOG_LEVEL, { service: "ReceivePack", repoId: prefix });

  try {
    const body = new Uint8Array(await request.arrayBuffer());
    const section = parsePktSection(body);
    if (!section) {
      log.warn("parse:malformed", {});
      return new Response("malformed receive-pack\n", { status: 400 });
    }
    const { lines, offset } = section;

    // Parse commands: "<old> <new> <ref>" (first line may include NUL + capabilities)
    const cmds: { oldOid: string; newOid: string; ref: string }[] = [];
    for (let i = 0; i < lines.length; i++) {
      let line = lines[i];
      if (i === 0) {
        const nul = line.indexOf("\0");
        if (nul !== -1) line = line.slice(0, nul);
      }
      const parts = line.trim().split(/\s+/);
      if (parts.length >= 3) {
        cmds.push({ oldOid: parts[0], newOid: parts[1], ref: parts.slice(2).join(" ") });
      }
    }

    const pack = body.subarray(offset);
    log.debug("receive:parsed", { commands: cmds.length, packBytes: pack.byteLength });

    // Determine if this push contains any creates/updates (non-zero new OIDs)
    const hasNonDelete = cmds.some((c) => !/^0{40}$/i.test(c.newOid));

    // Handle pack storage/index only if there are non-delete updates
    let unpackOk = true;
    let unpackErr = "";
    let packKey: string | undefined = undefined;
    if (hasNonDelete) {
      // Store pack in R2 under per-DO prefix
      packKey = r2PackKey(prefix, `pack-${Date.now()}.pack`);
      try {
        await env.REPO_BUCKET.put(packKey, pack);
        await store.put("lastPackKey", packKey);
        log.info("pack:stored", { packKey, bytes: pack.byteLength });
      } catch (e) {
        unpackOk = false;
        unpackErr = `store-pack-failed`;
        log.error("pack:store-failed", { error: String(e) });
      }

      // Quick index-only (no unpacking yet)
      try {
        const oids = await indexPackOnly(new Uint8Array(pack), env, packKey);

        // Store pack metadata
        await store.put("lastPackOids", oids);
        await store.put(packOidsKey(packKey), oids);
        log.info("index:ok", { packKey, oids: oids.length });

        // Update pack list
        const list = ((await store.get("packList")) || []).filter((k: string) => k !== packKey);
        list.unshift(packKey);
        if (list.length > 20) list.length = 20;
        await store.put("packList", list);

        // Queue unpack work for alarm
        await store.put("unpackWork", {
          packKey,
          oids,
          processedCount: 0,
          startedAt: Date.now(),
        });

        // Schedule immediate alarm to start unpacking
        await state.storage.setAlarm(Date.now() + 100);
        log.debug("unpack:scheduled", { packKey });
      } catch (e: any) {
        unpackOk = false;
        unpackErr = e?.message || String(e);
        log.error("index:error", { error: unpackErr });
      }
    } else {
      // Delete-only push: no pack is expected/required
      unpackOk = true;
    }

    // Load current refs state
    const refs = ((await store.get("refs")) as { name: string; oid: string }[] | undefined) ?? [];
    const refMap = new Map(refs.map((r) => [r.name, r.oid] as const));

    // Validate commands against current refs
    const statuses: { ref: string; ok: boolean; msg?: string }[] = [];
    if (!unpackOk) {
      for (const c of cmds) statuses.push({ ref: c.ref, ok: false, msg: `unpack-failed` });
    } else {
      for (const c of cmds) {
        const cur = refMap.get(c.ref);
        const isZeroOld = /^0{40}$/i.test(c.oldOid);
        const isZeroNew = /^0{40}$/i.test(c.newOid);
        // Delete ref
        if (isZeroNew) {
          if (!cur) {
            statuses.push({ ref: c.ref, ok: false, msg: `no such ref` });
            continue;
          }
          if (cur.toLowerCase() !== c.oldOid.toLowerCase()) {
            statuses.push({ ref: c.ref, ok: false, msg: `stale old-oid` });
            continue;
          }
          statuses.push({ ref: c.ref, ok: true });
          continue;
        }
        // Create new ref
        if (!cur) {
          if (!isZeroOld) {
            statuses.push({ ref: c.ref, ok: false, msg: `expected zero old-oid` });
            continue;
          }
          statuses.push({ ref: c.ref, ok: true });
          continue;
        }
        // Update existing ref: require old matches current; allow non-fast-forward (force)
        if (cur.toLowerCase() !== c.oldOid.toLowerCase()) {
          statuses.push({ ref: c.ref, ok: false, msg: `stale old-oid` });
          continue;
        }
        statuses.push({ ref: c.ref, ok: true });
      }
    }

    // Apply updates atomically if all commands are valid
    const allOk = statuses.length === cmds.length && statuses.every((s) => s.ok);
    log.debug("commands:validated", {
      total: cmds.length,
      ok: statuses.filter((s) => s.ok).length,
    });
    let newRefs: { name: string; oid: string }[] | undefined = undefined;
    if (allOk) {
      for (let i = 0; i < cmds.length; i++) {
        const c = cmds[i];
        if (/^0{40}$/i.test(c.newOid)) refMap.delete(c.ref);
        else refMap.set(c.ref, c.newOid);
      }
      newRefs = Array.from(refMap, ([name, oid]) => ({ name, oid }));
      try {
        await store.put("refs", newRefs);
        log.info("refs:updated", { count: newRefs.length });
      } catch (e) {
        log.error("refs:update-failed", { error: String(e) });
      }
      // Refresh HEAD resolution based on updated refs
      try {
        const curHead = (await store.get("head")) as Head | undefined;
        const target = curHead?.target || "refs/heads/main";
        const match = newRefs.find((r) => r.name === target);
        const resolved: Head = match ? { target, oid: match.oid } : { target, unborn: true };
        await store.put("head", resolved);
        log.debug("head:resolved", { target, oid: resolved.oid, unborn: resolved.unborn === true });
      } catch (e) {
        log.warn("head:resolve-failed", { error: String(e) });
      }
    }

    // Assemble report-status response
    const chunks: Uint8Array[] = [];
    chunks.push(pktLine(unpackOk ? "unpack ok\n" : `unpack error ${unpackErr || "failed"}\n`));
    for (let i = 0; i < cmds.length; i++) {
      const st = statuses[i];
      const c = cmds[i];
      if (st?.ok) chunks.push(pktLine(`ok ${c.ref}\n`));
      else chunks.push(pktLine(`ng ${c.ref} ${st?.msg || "rejected"}\n`));
    }
    chunks.push(flushPkt());
    // Signal to the outer router if repository changed and whether it's now empty
    const changed = allOk && cmds.length > 0;
    const empty = !!newRefs && newRefs.length === 0;

    const resHeaders: Record<string, string> = {
      "Content-Type": "application/x-git-receive-pack-result",
      "Cache-Control": "no-cache",
      "X-Repo-Changed": changed ? "1" : "0",
      "X-Repo-Empty": empty ? "1" : "0",
    };

    const resp = new Response(concatChunks(chunks), {
      status: 200,
      headers: resHeaders,
    });
    log.info("receive:done", { changed, empty, allOk });
    return resp;
  } finally {
    // No-op: rely on Durable Object's single-concurrency to serialize pushes.
  }
}
