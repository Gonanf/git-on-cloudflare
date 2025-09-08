import test from "ava";
import { decodePktLines, respondWithPackfile } from "@/git/index.ts";

async function getBytes(r: Response): Promise<Uint8Array> {
  const ab = await r.arrayBuffer();
  return new Uint8Array(ab);
}

function findLine(items: ReturnType<typeof decodePktLines>, text: string): number {
  return items.findIndex((it) => it.type === "line" && (it as any).text === text);
}

test("respondWithPackfile emits NAK when no common haves and done=false", async (t) => {
  const fakePack = new Uint8Array([0x50, 0x41, 0x43, 0x4b]); // arbitrary bytes
  const resp = respondWithPackfile(fakePack, /*done=*/ false, /*ackOids=*/ []);
  t.is(resp.status, 200);
  const bytes = await getBytes(resp);
  const items = decodePktLines(bytes);

  const iAck = findLine(items, "acknowledgments\n");
  t.true(iAck >= 0);
  t.is((items[iAck + 1] as any).text, "NAK\n");
  t.is(items[iAck + 2].type, "delim");
  t.is((items[iAck + 3] as any).text, "packfile\n");
  // Next item should be sideband data (band 0x01)
  const bandLine = items[iAck + 4];
  t.is(bandLine.type, "line");
  const raw = (bandLine as any).raw as Uint8Array;
  t.true(raw.length >= 1);
  t.is(raw[0], 0x01);
});

test("respondWithPackfile emits ACK <oid> common... and last ACK ready", async (t) => {
  const fakePack = new Uint8Array([0xaa]);
  const o1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const o2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  const resp = respondWithPackfile(fakePack, /*done=*/ false, /*ackOids=*/ [o1, o2]);
  const items = decodePktLines(await getBytes(resp));

  const iAck = findLine(items, "acknowledgments\n");
  t.true(iAck >= 0);
  t.is((items[iAck + 1] as any).text, `ACK ${o1} common\n`);
  t.is((items[iAck + 2] as any).text, `ACK ${o2} ready\n`);
  t.is(items[iAck + 3].type, "delim");
});

test("respondWithPackfile omits acknowledgments when done=true", async (t) => {
  const fakePack = new Uint8Array([0xff, 0x00]);
  const resp = respondWithPackfile(fakePack, /*done=*/ true, /*ackOids=*/ []);
  const items = decodePktLines(await getBytes(resp));
  // First meaningful line should be packfile
  const iPack = findLine(items, "packfile\n");
  t.true(iPack >= 0);
  // There must not be an "acknowledgments" line
  const iAck = findLine(items, "acknowledgments\n");
  t.is(iAck, -1);
});
