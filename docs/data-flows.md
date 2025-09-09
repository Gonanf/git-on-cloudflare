# Data Flows

This document describes the primary data flows of the server: pushing (receive-pack), fetching (upload-pack), and the Web UI blob views.

## Push (git-receive-pack)

1. Client sends `POST /:owner/:repo/git-receive-pack` (Smart HTTP v0 style body)
2. Worker preflights `GET https://do/unpack-progress` on the repo DO. If an unpack is active and a next pack is already queued, the Worker returns `503 Service Unavailable` with `Retry-After: 10` without uploading the pack body.
3. Otherwise, Worker forwards the raw body to the repository DO: `POST https://do/receive`
4. DO `receivePack()`:
   - Parses pkt-line commands and the packfile payload
   - Writes the `.pack` to R2 under `do/<id>/objects/pack/pack-<ts>.pack`
   - Performs a fast index-only step to produce `.idx` in R2
   - Queues unpack work; the DO `alarm()` processes objects in small time-budgeted chunks and mirrors loose objects to R2 as it goes
   - Validates and atomically updates refs if all commands are valid
   - Returns a pkt-line `report-status` response

Concurrency: relies on Durable Object single-threaded execution, plus a one-deep receive-pack queue on the DO (`unpackWork` + `unpackNext`). A third concurrent push is rejected with HTTP 503 (also guarded within the DO pre-body) to avoid unbounded queuing.

The DO also records metadata to help fetch:

- `lastPackKey` and `lastPackOids`
- `packList` (recent packs)
- `packOids:<packKey>` oids per pack

## Fetch (git-upload-pack v2)

1. Client sends capability advertisement request: `GET /:owner/:repo/info/refs?service=git-upload-pack`
2. For `POST /:owner/:repo/git-upload-pack` with a v2 body:
   - `ls-refs` command: reads DO `/head` and `/refs` and responds with HEAD + refs
   - `fetch` command:
     - Parses wants/haves and computes a minimal closure of needed oids
     - Tries to assemble a minimal pack from R2 using `.idx` range reads
       - Single-pack: `assemblePackFromR2()`
       - Multi-pack union: `assemblePackFromMultiplePacks()`
     - Uses KV-backed pack metadata hints (OID→pack, recent pack list) to avoid extra DO metadata calls when safe (see Caching Strategy)
     - If packs can’t fully cover, falls back to DO loose objects and builds a minimal pack on the fly
     - Responds with sidebanded `packfile` chunks

## Web UI blob views

- `GET /:owner/:repo/blob?ref=...&path=...` (preview)
  - Resolves path to an oid via DO reads
  - Uses `HEAD` against DO `/obj/<oid>` to get size (no body)
  - If the file is "too large" (configurable threshold), shows a friendly message and links to raw
  - If not too large, fetches the object and renders text (with simple binary detection)

- `GET /:owner/:repo/raw?oid=...&name=...` (raw)
  - Streams the object by piping the compressed DO response through a decompression stream
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
