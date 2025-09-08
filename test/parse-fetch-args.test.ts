import test from "ava";
import { pktLine, delimPkt, flushPkt, parseFetchArgs } from "@/git/index.ts";

function buildFetchBody({
  wants,
  haves,
  done,
}: {
  wants: string[];
  haves: string[];
  done?: boolean;
}): Uint8Array {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=fetch\n"));
  chunks.push(delimPkt());
  for (const w of wants) chunks.push(pktLine(`want ${w}\n`));
  for (const h of haves) chunks.push(pktLine(`have ${h}\n`));
  if (done) chunks.push(pktLine("done\n"));
  chunks.push(flushPkt());
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

const O1 = "0123456789abcdef0123456789abcdef01234567";
const O2 = "89abcdef0123456789abcdef0123456789abcdef";

test("parseFetchArgs extracts wants/haves/done after delim", (t) => {
  const body = buildFetchBody({ wants: [O1], haves: [O2], done: true });
  const res = parseFetchArgs(body);
  t.deepEqual(res.wants, [O1]);
  t.deepEqual(res.haves, [O2]);
  t.true(res.done);
});

test("parseFetchArgs handles multiple wants/haves and missing done", (t) => {
  const body = buildFetchBody({ wants: [O1, O2], haves: [O2, O1], done: false });
  const res = parseFetchArgs(body);
  t.deepEqual(new Set(res.wants), new Set([O1, O2]));
  t.deepEqual(res.haves, [O2, O1]);
  t.false(res.done);
});
