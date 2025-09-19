# Data Flows

This document describes the primary data flows of the server: pushing (receive-pack), fetching (upload-pack), and the Web UI blob views.

## Push (git-receive-pack)

1. Client sends `POST /:owner/:repo/git-receive-pack` (Smart HTTP v0 style body)
2. Worker preflights the DO via RPC: `getUnpackProgress()`. If an unpack is active and a next pack is already queued, the Worker returns `503 Service Unavailable` with `Retry-After: 10` without uploading the pack body.
3. Otherwise, Worker forwards the raw body to the repository DO internal endpoint: `POST https://do/receive`
4. DO `receivePack()`:
   - Parses pkt-line commands and the packfile payload
   - Writes the `.pack` to R2 under `do/<id>/objects/pack/pack-<ts>.pack`
   - Performs a fast index-only step to produce `.idx` in R2
   - Queues unpack work; the DO `alarm()` processes objects in small time-budgeted chunks and mirrors loose objects to R2 as it goes
   - Validates and atomically updates refs if all commands are valid
   - Returns a pkt-line `report-status` response

Concurrency: relies on Durable Object single-threaded execution, plus a one-deep receive-pack queue on the DO (`unpackWork` + `unpackNext`). A third concurrent push is rejected with HTTP 503 (also guarded within the DO pre-body) to avoid unbounded queuing.

The DO maintains metadata to help fetch:

- DO state: `lastPackKey` and `packList` (recent packs)
- SQLite tables (embedded in the DO):
  - `pack_objects(pack_key, oid)` — exact pack membership, indexed by `oid`
  - `hydr_cover(work_id, oid)` — coverage set for hydration segments
  - `hydr_pending(work_id, kind, oid)` — pending OIDs to build hydration segments; `kind` ∈ {`base`, `loose`}

Pack membership is persisted immediately during hydration/segment builds and referenced during fetch to compute union coverage efficiently. Hydration is resumable across alarms; pending sets and coverage live in SQLite to survive restarts.

## Fetch (git-upload-pack v2)

1. Client sends capability advertisement request: `GET /:owner/:repo/info/refs?service=git-upload-pack`
2. For `POST /:owner/:repo/git-upload-pack` with a v2 body:
   - `ls-refs` command: reads the DO via RPC (`getHead()` and `listRefs()`) and responds with HEAD + refs
   - `fetch` command (now using streaming by default):
     - Negotiation phase (`done=false`): server returns an acknowledgments block only (ACK/NAK), no `packfile` section
     - Parses wants/haves and computes minimal closure using frontier-subtract approach with stop sets
     - Discovers candidate packs via `src/git/operations/packDiscovery.ts#getPackCandidates()` (DO metadata first, then best‑effort R2 listing), memoized per request with limiter + soft budget
     - **Streaming pack assembly** (no buffering):
       - Single-pack: `streamPackFromR2()` streams directly from R2 with backpressure
       - Multi-pack union: `streamPackFromMultiplePacks()` with proper delta resolution
       - Uses `crypto.DigestStream` for incremental SHA-1 computation
       - Emits sideband-64k with progress messages on channel 2
     - If repository has no packs (loose-only), returns `503` with `Retry-After: 5` and message about objects being packed
     - If closure traversal times out, tries a safe multi-pack union based on recent packs
     - Legacy buffered mode still available with `X-Git-Streaming: false` header (deprecated)

## Web UI blob views

- `GET /:owner/:repo/blob?ref=...&path=...` (preview)
  - Resolves path to an OID via DO RPC reads
  - Uses a DO RPC (`getObjectSize()`) which performs an R2 `HEAD` to get size without transferring data
  - If the file is "too large" (configurable threshold), shows a friendly message and links to raw
  - If not too large, fetches the object and renders text (with simple binary detection)

- `GET /:owner/:repo/raw?oid=...&name=...` (raw)
  - Streams the object via DO RPC (`getObjectStream()`), piping through a decompression stream and stripping the Git header on the fly
  - Uses `Content-Disposition: inline` by default (add `&download=1` to force attachment)
  - Uses `text/plain; charset=utf-8` for safety (prevents HTML/JS execution)

## Merge commit exploration

- `GET /:owner/:repo/commits` (main page)
  - Displays commit history with expandable merge commits
  - Merge commits show a badge and are clickable to expand side branch history
- `GET /:owner/:repo/commits/fragments/:oid` (AJAX fragment)
  - Called when user clicks a merge commit row
  - Uses `listMergeSideFirstParent()` to traverse non-mainline parents
  - Algorithm:
    1. Probe mainline (parents[0]) to build a stop set
    2. Initialize frontier with side parents (parents[1..])
    3. Priority queue traversal by author date (newest first)
    4. Stop when: reached limit, hit mainline, timeout, or scan limit
  - Returns HTML fragment with commit rows for dynamic insertion
  - No caching at UI level (dynamic content)
