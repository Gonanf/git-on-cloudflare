import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import { asTypedStorage, RepoStateSchema } from "@/do/repo/repoState.ts";
import { runDOWithRetry } from "./util/test-helpers.ts";
import { getDb, insertPackOids, getHydrCoverOids } from "@/do/repo/db/index.ts";
import { enqueueHydrationTask, processHydrationSlice } from "@/do/repo/hydration.ts";
import { calculateStableEpochs } from "@/do/repo/packs.ts";
import type { RepoDurableObject } from "@/do";

it("hydration coverage: seeds only from stable epochs", async () => {
  const repoId = `cov/${Math.random().toString(36).slice(2, 8)}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub: () => DurableObjectStub<RepoDurableObject> = () => env.REPO_DO.get(id);

  // Prepare state: interleave last, epoch-labeled hydras, normals
  const { prefix } = await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    return { prefix: `do/${state.id.toString()}` };
  });

  const last = `${prefix}/objects/pack/pack-90.pack`;
  await env.REPO_BUCKET.put(last, new Uint8Array([1]));
  await env.REPO_BUCKET.put(last.replace(/\.pack$/, ".idx"), new Uint8Array([1, 1]));

  const eNew = [
    `${prefix}/objects/pack/pack-hydr-e900-1.pack`,
    `${prefix}/objects/pack/pack-hydr-e900-2.pack`,
  ];
  const eOld = [
    `${prefix}/objects/pack/pack-hydr-e800-1.pack`,
    `${prefix}/objects/pack/pack-hydr-e800-2.pack`,
  ];
  for (const k of [...eNew, ...eOld]) {
    await env.REPO_BUCKET.put(k, new Uint8Array([9]));
    await env.REPO_BUCKET.put(k.replace(/\.pack$/, ".idx"), new Uint8Array([9, 9]));
  }
  const normals = [`${prefix}/objects/pack/pack-89.pack`, `${prefix}/objects/pack/pack-88.pack`];
  for (const k of normals) {
    await env.REPO_BUCKET.put(k, new Uint8Array([2]));
    await env.REPO_BUCKET.put(k.replace(/\.pack$/, ".idx"), new Uint8Array([2, 2]));
  }

  // Seed storage and pack_objects rows
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", [last, ...eNew, normals[0], ...eOld, normals[1]]);
    await store.put("lastPackKey", last);
    const db = getDb(state.storage);
    // Insert unique OIDs: e900-* => x1/x2; e800-* => y1/y2
    await insertPackOids(db, eNew[0], ["x1"]);
    await insertPackOids(db, eNew[1], ["x2"]);
    await insertPackOids(db, eOld[0], ["y1"]);
    await insertPackOids(db, eOld[1], ["y2"]);
  });

  // Queue hydration and run plan stage to seed hydr_cover
  const { workId } = await runDOWithRetry(getStub, async (_i, state: DurableObjectState) => {
    const res = await enqueueHydrationTask(state, env, { dryRun: false, reason: "admin" });
    return res;
  });
  expect(workId).toBeTypeOf("string");

  // Run slices until after planning to ensure hydr_cover is populated
  // First slice creates work and moves to plan (retry-safe wrapper)
  await runDOWithRetry(getStub, async (_i, state: DurableObjectState) => {
    await processHydrationSlice(state, env, `do/${state.id.toString()}`);
  });
  // Second slice runs plan and seeds hydr_cover (separate retry-safe call)
  await runDOWithRetry(getStub, async (_i, state: DurableObjectState) => {
    await processHydrationSlice(state, env, `do/${state.id.toString()}`);
  });

  // Read current hydrationWork to get the actual workId used for hydr_cover
  let curWorkId = "";
  await runDOWithRetry(getStub, async (_i, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const work = (await store.get("hydrationWork")) as unknown as { workId?: string } | undefined;
    curWorkId = work?.workId || "";
  });

  // Verify hydr_cover contains only OIDs from stable epochs per calculateStableEpochs()
  await runDOWithRetry(getStub, async (_i, state: DurableObjectState) => {
    const db = getDb(state.storage);
    const oids = await getHydrCoverOids(db, curWorkId || (workId as string));
    // Compute stable epochs for the same packList and ensure only those are present
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const lastPackKey = (await store.get("lastPackKey")) || undefined;
    const packList = ((await store.get("packList")) || []) as string[];
    const { stableEpochs } = calculateStableEpochs(packList, 10, lastPackKey);
    const isStable = new Set(stableEpochs);
    const expectX = isStable.has("e900");
    const expectY = isStable.has("e800");
    expect(oids.includes("x1")).toBe(expectX);
    expect(oids.includes("x2")).toBe(expectX);
    expect(oids.includes("y1")).toBe(expectY);
    expect(oids.includes("y2")).toBe(expectY);
  });
});
