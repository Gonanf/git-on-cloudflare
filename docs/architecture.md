# Architecture Overview

This project implements a Git Smart HTTP v2 server on Cloudflare Workers using a hybrid of Durable Objects (DO) and R2.

## Module Structure

The codebase is organized into focused modules with `index.ts` export files:

- **`/git`** - Core Git functionality
  - `operations/` - Git operations (upload-pack, receive-pack)
  - `core/` - Protocol handling, pkt-line, readers
  - `pack/` - Pack assembly, unpacking, indexing
- **`/do`** - Durable Objects
  - `repoDO.ts` - Repository Durable Object (per-repo authority)
- **`/auth`** - Authentication module
  - `authDO.ts` - Auth Durable Object
  - `verify.ts` - Token verification
- **`/cache`** - Two-tier caching system
  - UI layer caching (JSON responses)
  - Git object caching (immutable objects)
- **`/web`** - Web UI utilities
  - `format.ts` - Content formatting helpers
  - `render.ts` - Page rendering
  - `templates.ts` - Liquid template engine
- **`/common`** - Shared utilities
  - `compression.ts`, `hex.ts`, `logger.ts`, `response.ts`, `stub.ts`, `progress.ts`
- **`/registry`** - Owner/repo registry management
- **`/routes`** - HTTP route handlers
  - `git.ts` - Git protocol endpoints (upload-pack, receive-pack)
  - `ui.ts` - Web UI routes for browsing repos
  - `auth.ts` - Authentication UI and API endpoints
  - `admin.ts` - Admin routes for registry management

## Core Components

### Worker Entry (`src/index.ts`)

- Routes for Git endpoints, admin JSON, and the web UI
- Integrates all route handlers via AutoRouter

### Repository DO (`src/do/repoDO.ts`)

- Owns refs/HEAD and loose objects for a single repo
- HTTP endpoints (minimal):
  - `POST /receive` — receive-pack (push) handler
  - `POST /reindex` — reindex latest pack (dev/diagnostic)
- Typed RPC methods (selected):
  - `listRefs()`, `setRefs()`, `getHead()`, `setHead()`, `getHeadAndRefs()`
  - `getObjectStream()`, `getObject()`, `getObjectSize()`
  - `getPackLatest()`, `getPacks()`, `getPackOids()`
  - `getUnpackProgress()` — unpacking status/progress for UI gating (includes `queuedCount` and `currentPackKey`)
- Push flow: raw `.pack` is written to R2, a fast index-only step writes `.idx`, and unpack is queued for background processing on the DO alarm in small time-budgeted chunks.
- Maintains pack metadata (`lastPackKey`, `lastPackOids`, `packList`, `packOids:<key>`) used by fetch assembly.

#### Receive-pack queueing

- The DO maintains at most one active unpack (`unpackWork`) and a one-deep next slot (`unpackNext`).
- When a push arrives while unpacking is active:
  - If `unpackNext` is empty, the new pack is staged as `unpackNext`.
  - If `unpackNext` is already occupied, the DO returns HTTP 503 with `Retry-After` pre-body.
- The Worker performs a preflight call to the DO RPC `getUnpackProgress()` and returns 503 early when a next pack is already queued, avoiding unnecessary upload.

### Auth DO (`src/auth/authDO.ts`)

- Stores owners → token hashes
- `/verify` for Worker auth checks; `/users` for admin UI/API
- PBKDF2-SHA256 with 100k iterations for password hashing

### Caching Layer (`src/cache/`)

- **UI Cache**: 60s for HEAD/refs, 5min for README, 1hr for tag commits
- **Object Cache**: Immutable Git objects cached for 1 year
- **Pack discovery and memoization**: `src/git/operations/packDiscovery.ts#getPackCandidates()` coalesces per-request discovery using DO metadata (latest + list) with a best‑effort R2 listing fallback. Results are memoized in `RequestMemo`.
- **Per-request limiter and soft budget**: All DO/R2 calls in read and upload paths use a concurrency limiter and a soft subrequest budget to avoid hitting platform limits.

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
