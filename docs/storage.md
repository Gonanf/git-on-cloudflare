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

### SQLite metadata in Durable Objects

- A small SQLite database is embedded in each Repository DO using `drizzle-orm/durable-sqlite`.
- Purpose: store indexed metadata that benefits fetch and hydration flows.
- Tables:
  - `pack_objects(pack_key, oid)` — exact per-pack membership; indexed by `oid` for fast lookups.
  - `hydr_cover(work_id, oid)` — coverage set for hydration segments.
  - `hydr_pending(work_id, kind, oid)` — pending OIDs for hydration work; `kind` ∈ {`base`, `loose`}; primary key `(work_id, kind, oid)` and index on `(work_id, kind)`.
- Migrations run during DO initialization via `migrate(db, migrations)` and Wrangler `new_sqlite_classes` (see `wrangler.jsonc` and `drizzle.config.ts`).
- Enables efficient batch queries such as `getPackOidsBatch()` and robust coverage checks.

Note: All SQLite access goes through the data access layer (DAL) in `src/do/repo/db/dal.ts`. Avoid raw drizzle queries outside the DAL.

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
