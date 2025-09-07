# Architecture Overview

This project implements a minimal Git Smart HTTP v2 server on Cloudflare Workers using a hybrid of Durable Objects (DO) and R2.

- Worker entry: `src/index.ts`
  - Routes for Git endpoints, admin JSON, and the web UI
  - Template loading and `renderPage()` for HTML
- Repository DO: `src/repoDO.ts`
  - Owns refs/HEAD and loose objects for a single repo
  - Internal endpoints consumed by the Worker and tests:
    - `GET/PUT /refs` — list/update refs
    - `GET/PUT /head` — get/update HEAD
    - `GET|HEAD /obj/:oid` — read loose object (R2-first, fallback DO)
    - `PUT /obj/:oid` — write loose object (DO then mirror to R2)
    - `GET /pack-latest`, `GET /packs`, `GET /pack-oids?key=...` — pack metadata
    - `POST /receive` — receive-pack (push) handler
  - Push flow: raw `.pack` is written to R2, a fast index-only step writes `.idx`, and unpack is queued for background processing on the DO alarm in small time-budgeted chunks.
  - Maintains pack metadata (`lastPackKey`, `lastPackOids`, `packList`, `packOids:<key>`) used by fetch assembly.
- Auth DO: `src/authDO.ts`
  - Stores owners → token hashes
  - `/verify` for Worker auth checks; `/users` for admin UI/API
- Git protocol helpers: `src/protocol.ts`, `src/uploadPack.ts`, `src/pktline.ts`
  - Supports request cancellation via AbortSignal
  - Fast path for initial clones to avoid expensive closure computation
- Pack helpers: `src/pack/packAssembler.ts`, `src/pack/unpack.ts`
  - Fast path for initial clones: assembles from latest R2 pack using all OIDs
  - In-memory pack assembly for small packs (<16MB) or when needing many objects (≥25%)
  - Multi-pack union support for incremental fetches
- Web UI readers: `src/gitRead.ts`
- Key helpers (single-source-of-truth): `src/keys.ts`

## Background processing and alarms

- The repo DO `alarm()` performs three duties: (1) unpack chunks within a time budget; (2) idle cleanup for empty, idle repos; (3) periodic pack maintenance (prune old packs + metadata).
  - `handleUnpackWork()` - Processes pending unpack work
  - `handleIdleAndMaintenance()` - Manages idle cleanup and maintenance
  - `shouldCleanupIdle()` - Determines if cleanup is needed
  - `performIdleCleanup()` - Executes cleanup
  - `purgeR2Mirror()` - Handles R2 cleanup
- Unpack chunking is controlled via env vars: `REPO_UNPACK_CHUNK_SIZE`, `REPO_UNPACK_MAX_MS`, `REPO_UNPACK_DELAY_MS`, `REPO_UNPACK_BACKOFF_MS`.

## Logging

- Structured JSON logs are emitted with a minimal logger. Set `LOG_LEVEL` to `debug|info|warn|error` to control verbosity.

See also:

- [Storage model](./storage.md)
- [Data flows](./data-flows.md)
- For troubleshooting, see the "How to debug" section in the top-level README.
