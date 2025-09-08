import test from "ava";
import { createMemPackFs } from "@/git/index.ts";

function u8(...xs: number[]) {
  return new Uint8Array(xs);
}

test("createMemPackFs: maps /work paths to /git/objects/pack", async (t) => {
  const files = new Map<string, Uint8Array>();
  files.set("/git/objects/pack/pack-input.pack", u8(1, 2, 3));
  const fs = createMemPackFs(files);

  const buf = await fs.promises.readFile("/work/pack-input.pack");
  t.deepEqual(buf, u8(1, 2, 3));
});

test("createMemPackFs: lazy-loads loose object via looseLoader and caches it", async (t) => {
  const files = new Map<string, Uint8Array>();
  const calls: string[] = [];
  const LOADED = u8(9, 9, 9);
  const fs = createMemPackFs(files, {
    looseLoader: async (oid: string) => {
      calls.push(oid);
      if (oid === "ab" + "0".repeat(38)) return LOADED;
      return undefined;
    },
  });

  const path = "/git/objects/ab/" + "0".repeat(38);
  const buf = await fs.promises.readFile(path);
  t.deepEqual(buf, LOADED, "readFile should return bytes loaded via looseLoader");
  t.deepEqual(calls, ["ab" + "0".repeat(38)], "looseLoader called with expected OID");
  t.true(files.has(path), "resolved loose object should be cached in files map");
});

test("createMemPackFs: stat uses same resolution logic and returns file size", async (t) => {
  const files = new Map<string, Uint8Array>();
  const LOADED = u8(7, 7);
  const fs = createMemPackFs(files, {
    looseLoader: async (oid: string) => (oid === "aa" + "0".repeat(38) ? LOADED : undefined),
  });
  const path = "/git/objects/aa/" + "0".repeat(38);
  const st = await fs.promises.stat(path);
  t.is(st.size, LOADED.byteLength);
});

test("createMemPackFs: readdir lists pack entries under /git/objects/pack", async (t) => {
  const files = new Map<string, Uint8Array>();
  files.set("/git/objects/pack/a.pack", u8(1));
  files.set("/git/objects/pack/b.idx", u8(2));
  files.set("/git/objects/pack/c.pack", u8(3));
  const fs = createMemPackFs(files);

  const names = await fs.promises.readdir("/git/objects/pack");
  // Order not guaranteed; compare as a set
  t.deepEqual(new Set(names), new Set(["a.pack", "b.idx", "c.pack"]));
});
