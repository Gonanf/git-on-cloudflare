# Storage Model (DO + R2)

This project uses a hybrid storage approach to balance strong consistency for refs and hot objects with cheap, scalable storage for large data:

## Durable Objects (DO) storage

- Per-repo, strongly consistent state
- Stores:
  - `refs` (array of `{ name, oid }`)
  - `head` (object with `target`, optional `oid`, `unborn`)
  - Loose objects (zlib-compressed, raw Git format `type size\0payload`)
- Access patterns:
  - Always consistent; great for writes and cache-like reads

## R2 storage

- Large, cheap object store
- Stores under a per-DO prefix: `do/<do-id>/...`
- Objects:
  - Pack files: `do/<id>/objects/pack/<name>.pack`
  - Pack indexes: `do/<id>/objects/pack/<name>.idx`
  - Mirrored loose objects: `do/<id>/objects/loose/<oid>`
- Access patterns:
  - Range reads for packfile assembly (cheap and efficient)
  - HEAD requests to get object sizes without transferring data

## Key conventions (src/keys.ts)

- `repoKey(owner, repo)` → `owner/repo`
- `doPrefix(doId)` → `do/<do-id>`
- `r2LooseKey(prefix, oid)` → `do/<id>/objects/loose/<oid>`
- `r2PackKey(prefix, name)` → `do/<id>/objects/pack/<name>.pack`
- `packIndexKey(packKey)` maps `.pack` → `.idx`
- `packKeyFromIndexKey(idxKey)` maps `.idx` → `.pack`
- `r2PackDirPrefix(prefix)` → `do/<id>/objects/pack/`

## Why this design

- DO provides strong consistency for refs and state transitions (e.g., atomic ref updates during push)
- R2 provides cheap, scalable storage for large packfiles, with range-read support ideal for fetch assembly
- Mirroring loose objects to R2 reduces DO CPU during reads and enables HEAD size checks for the web UI
