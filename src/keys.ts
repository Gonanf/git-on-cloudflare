// Centralized helpers for building repo IDs and R2/DO key paths
// Keep all key/prefix formats here to avoid divergence between tests and runtime.

// Build a repository ID from owner + repo. Tests and routing expect "owner/repo".
export function repoKey(owner: string, repo: string): string {
  return `${owner}/${repo}`;
}

// Compute the Durable Object R2 prefix for a given DO instance ID.
// DO-backed data in R2 is stored under: "do/<durable-object-id>/..."
export function doPrefix(doId: string): string {
  return `do/${doId}`;
}

// R2 key for a loose object under a given DO prefix
export function r2LooseKey(prefix: string, oid: string): string {
  return `${prefix}/objects/loose/${oid}`;
}

// R2 key for a pack file under a given DO prefix
export function r2PackKey(prefix: string, name: string): string {
  return `${prefix}/objects/pack/${name}`;
}

// Given a .pack key, return its matching .idx key
export function packIndexKey(packKey: string): string {
  return packKey.replace(/\.pack$/, ".idx");
}

// Directory prefix for pack objects under a given DO prefix
export function r2PackDirPrefix(prefix: string): string {
  return `${prefix}/objects/pack/`;
}

// Return true if the key ends with .pack
export function isPackKey(key: string): boolean {
  return key.endsWith(".pack");
}

// Return true if the key ends with .idx
export function isIdxKey(key: string): boolean {
  return key.endsWith(".idx");
}

// Given an .idx key, return the corresponding .pack key
export function packKeyFromIndexKey(idxKey: string): string {
  return idxKey.replace(/\.idx$/, ".pack");
}

// KV keys for pack metadata caching

// Pack list for a repository (short TTL)
export function kvPackListKey(repoId: string): string {
  return `packs:${repoId}`;
}

// OID to pack mapping (immutable, long TTL)
export function kvOidToPackKey(repoId: string, oid: string): string {
  return `oid:${repoId}:${oid.toLowerCase()}`;
}

// Track recent pushes/unpacking (short TTL)
export function kvLastPushKey(repoId: string): string {
  return `push:${repoId}`;
}

// Track active unpacking status
export function kvUnpackStatusKey(repoId: string): string {
  return `unpack:${repoId}`;
}
