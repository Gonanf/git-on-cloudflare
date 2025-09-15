// Lightweight Bloom filter for string membership checks
// - Uses double hashing with FNV-1a 32-bit variants
// - Serializable via toJSON()/fromJSON() for Durable Object storage

export type BloomExport = {
  m: number; // number of bits
  k: number; // number of hash functions
  bits: string; // base64-encoded bit array
};

export class BloomFilter {
  private m: number;
  private k: number;
  private bits: Uint8Array;

  private constructor(m: number, k: number, bits?: Uint8Array) {
    if (!Number.isFinite(m) || m <= 0) throw new Error("BloomFilter: m must be > 0");
    if (!Number.isFinite(k) || k <= 0) throw new Error("BloomFilter: k must be > 0");
    this.m = Math.floor(m);
    this.k = Math.floor(k);
    const bytes = Math.ceil(this.m / 8);
    this.bits = bits ? bits : new Uint8Array(bytes);
  }

  // Create from expected item count and desired false-positive rate
  static create(expectedItems: number, falsePositiveRate = 0.01): BloomFilter {
    const n = Math.max(1, Math.floor(expectedItems));
    const p = Math.min(Math.max(falsePositiveRate, 1e-9), 0.5);
    // m = -(n * ln p) / (ln 2)^2
    const m = Math.max(8, Math.ceil(-(n * Math.log(p)) / Math.LN2 ** 2));
    // k = (m/n) * ln 2
    const k = Math.max(1, Math.round((m / n) * Math.LN2));
    return new BloomFilter(m, k);
  }

  static fromJSON(obj: BloomExport): BloomFilter {
    const bits = fromBase64(obj.bits);
    return new BloomFilter(obj.m, obj.k, bits);
  }

  toJSON(): BloomExport {
    return { m: this.m, k: this.k, bits: toBase64(this.bits) };
  }

  add(s: string) {
    const [h1, h2] = this.hashPair(s);
    for (let i = 0; i < this.k; i++) {
      // Double hashing: h_i(x) = h1 + i*h2
      const idx = (h1 + i * h2 + i * i) % this.m;
      this.setBit(idx);
    }
  }

  has(s: string): boolean {
    const [h1, h2] = this.hashPair(s);
    for (let i = 0; i < this.k; i++) {
      const idx = (h1 + i * h2 + i * i) % this.m;
      if (!this.getBit(idx)) return false;
    }
    return true;
  }

  // --- internals ---

  private setBit(i: number) {
    const idx = Math.floor(i);
    const byte = idx >> 3;
    const bit = idx & 7;
    this.bits[byte] |= 1 << bit;
  }

  private getBit(i: number): boolean {
    const idx = Math.floor(i);
    const byte = idx >> 3;
    const bit = idx & 7;
    return (this.bits[byte] & (1 << bit)) !== 0;
  }

  private hashPair(s: string): [number, number] {
    // Use two FNV-1a variants with different seeds to produce h1, h2
    const h1 = fnv1a32(s, 0x811c9dc5) >>> 0;
    const h2raw = fnv1a32(s, 0x811c9dc5 ^ 0x9e3779b9) >>> 0; // golden ratio xor
    // Guard against h2 = 0 which would collapse to repeated h1
    const h2 = h2raw % this.m || 1;
    return [h1 % this.m, h2];
  }
}

function fnv1a32(str: string, seed = 0x811c9dc5): number {
  let hash = seed >>> 0;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i) & 0xff;
    // multiply by FNV prime 0x01000193
    hash = (hash + ((hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24))) >>> 0;
  }
  return hash >>> 0;
}

function toBase64(bytes: Uint8Array): string {
  // Prefer Buffer in Node/test, fallback to btoa in Workers
  const g: any = globalThis as any;
  if (g.Buffer && typeof g.Buffer.from === "function") {
    return g.Buffer.from(bytes).toString("base64");
  }
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  // btoa expects binary string
  // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
  return g.btoa ? g.btoa(binary) : binary;
}

function fromBase64(b64: string): Uint8Array {
  const g: any = globalThis as any;
  if (g.Buffer && typeof g.Buffer.from === "function") {
    return new Uint8Array(g.Buffer.from(b64, "base64"));
  }
  const bin = g.atob ? g.atob(b64) : b64;
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i) & 0xff;
  return out;
}
