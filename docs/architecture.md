# Architecture Overview

This project implements a Git Smart HTTP v2 server on Cloudflare Workers using a hybrid of Durable Objects (DO) and R2.

## Module Structure

The codebase is organized into focused modules with `index.ts` export files:

- **`/git`** - Core Git functionality
  - `repoDO.ts` - Repository Durable Object
  - `operations/` - Git operations (upload-pack, receive-pack)
  - `core/` - Protocol handling, pkt-line, readers
  - `pack/` - Pack assembly, unpacking, indexing
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
- Internal endpoints consumed by the Worker and tests:
  - `GET/PUT /refs` — list/update refs
  - `GET/PUT /head` — get/update HEAD
  - `GET|HEAD /obj/:oid` — read loose object (R2-first, fallback DO)
  - `PUT /obj/:oid` — write loose object (DO then mirror to R2)
  - `GET /pack-latest`, `GET /packs`, `GET /pack-oids?key=...` — pack metadata
  - `GET /unpack-progress` — unpacking status/progress for gating KV writes and UI. Includes `queuedCount` (0|1) and `currentPackKey`.
  - `POST /receive` — receive-pack (push) handler
- Push flow: raw `.pack` is written to R2, a fast index-only step writes `.idx`, and unpack is queued for background processing on the DO alarm in small time-budgeted chunks.
- Maintains pack metadata (`lastPackKey`, `lastPackOids`, `packList`, `packOids:<key>`) used by fetch assembly.

#### Receive-pack queueing

- The DO maintains at most one active unpack (`unpackWork`) and a one-deep next slot (`unpackNext`).
- When a push arrives while unpacking is active:
  - If `unpackNext` is empty, the new pack is staged as `unpackNext`.
  - If `unpackNext` is already occupied, the DO returns HTTP 503 with `Retry-After` pre-body.
- The Worker performs a preflight call to `GET /unpack-progress` and returns 503 early when a next pack is already queued, avoiding unnecessary upload.

### Auth DO (`src/auth/authDO.ts`)

- Stores owners → token hashes
- `/verify` for Worker auth checks; `/users` for admin UI/API
- PBKDF2-SHA256 with 100k iterations for password hashing

### Caching Layer (`src/cache/`)

- **UI Cache**: 60s for HEAD/refs, 5min for README, 1hr for tag commits
- **Object Cache**: Immutable Git objects cached for 1 year
- **KV-backed pack metadata hints**: recent pack list and OID→pack mapping to reduce DO calls during reads; writes are gated by `shouldSkipKVCache()` and DO `/unpack-progress` to avoid staleness during churn
- Reduces DO/R2 calls significantly, improves response times to <50ms

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
