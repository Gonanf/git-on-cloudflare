# git-on-cloudflare

[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-f38020?logo=cloudflare&logoColor=white)](https://developers.cloudflare.com/workers/)
[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zllovesuki/git-on-cloudflare)

**A Git Smart HTTP v2 server running entirely on Cloudflare Workers** — no VMs, no containers, just Durable Objects and R2.

Host unlimited private Git repositories at the edge with <50ms response times globally. Full Git compatibility, modern web UI, and usage-based pricing that actually makes sense.

## Key Features

- **Complete Git Smart HTTP v2 implementation** with pack protocol support
- **Strong consistency** via Durable Objects for refs/HEAD (the hard part of distributed Git)
- **Two-tier caching** reducing latency from 200ms to <50ms for hot paths
- **Streaming pack assembly** from R2 with range reads for efficient clones
- **Time-budgeted background unpacking** handles large pushes without blocking
- **Modern web UI** with Tailwind CSS v4, Liquid templates, and dark mode (default)
- **Interactive merge commit exploration** - expand merge commits to see side branch history
- **Safer raw views**: `text/plain` for `/raw` by default and same‑origin Referer check for `/rawpath` to prevent hotlinking

## Quick Demo

```bash
# Clone the project
git clone https://github.com/zllovesuki/git-on-cloudflare
cd git-on-cloudflare
npm install

# Build CSS (one‑time) and start locally (no Docker required)
npm run build:css
npm run dev

# Push any repo to it
cd /your/existing/repo
git push http://localhost:8787/test/myrepo main
```

Visit `http://localhost:8787/test/myrepo` to browse your code. That's it — you now have a fully functional Git server.

Tip: during active UI work, run `npm run watch:css` in another terminal to rebuild styles on change.

## Technical Architecture

This is a complete Git Smart HTTP v2 server built on Cloudflare's edge primitives:

### Core Design

- **Durable Objects** provide linearizable consistency for refs/HEAD without coordination
- **R2 storage** for pack files and objects with range-read support for streaming
- **Workers** handle the Git protocol, pack negotiation, and smart HTTP transport
- **Two-tier caching**: UI responses (60s-1hr TTL), Git objects (1 year, immutable)

### Performance Characteristics

- **Clone speeds**: 10-50 MB/s from any edge location
- **Push processing**: <5s for typical commits, large pushes handled incrementally
- **Response times**: <50ms for cached paths, <100ms globally for cold requests
- **Pack assembly**: Streaming from R2 using `.idx` range reads, no full pack loads
- **KV-backed pack metadata hints**: OID→pack and recent pack list cached in KV (gated during push/unpack) to reduce DO roundtrips during reads
- **Memory efficiency**: Multi-pack-index support avoids expensive repack operations

### Implementation Details

- Complete Git pack protocol v2 with `ls-refs` and `fetch` commands
- Time-budgeted unpacking keeps pushes under 30s CPU limit
- PBKDF2-SHA256 (100k iterations) for auth tokens
- Background object mirroring from DO to R2 for read scaling
- Modern web UI with Tailwind CSS v4 and Liquid templates
- Structured JSON logging with `LOG_LEVEL` (debug/info/warn/error)

## Deploy to Production

```bash
# Configure Cloudflare account
wrangler login

# Set admin token for auth UI
wrangler secret put AUTH_ADMIN_TOKEN

# Deploy to Workers
npm run deploy
```

Your Git server is now live at `https://your-subdomain.workers.dev`. Push repos, browse code, manage auth — all from the edge.

## Authentication

By default, repos are **completely open** — anyone can push and pull without authentication.

To enable push protection, set `AUTH_ADMIN_TOKEN`:

```bash
# Development
echo "AUTH_ADMIN_TOKEN=secret123" > .dev.vars

# Production
wrangler secret put AUTH_ADMIN_TOKEN
```

With auth enabled:

- **Reads remain public** (clone/pull/browse)
- **Pushes require authentication** (per-owner tokens)
- Manage tokens at `/auth` or via API
- Tokens use PBKDF2-SHA256 with 100k iterations

## Configuration

Environment variables control pack management and unpacking behavior:

```bash
REPO_DO_IDLE_MINUTES=30      # Cleanup idle repos after 30 min
REPO_DO_MAINT_MINUTES=1440   # Run maintenance daily
REPO_KEEP_PACKS=3            # Long-term pack retention
REPO_UNPACK_CHUNK_SIZE=25    # Objects per chunk
REPO_UNPACK_MAX_MS=1000      # Max time per unpack operation
```

See `wrangler.jsonc` for the complete configuration.

## Documentation

- [API Endpoints](docs/api-endpoints.md) - Complete HTTP API reference
- [Architecture Overview](docs/architecture.md) - Module structure and components
- [Storage Model](docs/storage.md) - Hybrid DO + R2 storage design
- [Data Flows](docs/data-flows.md) - Push, fetch, and web UI flows
- [Caching Strategy](docs/caching.md) - Two-tier caching implementation

## Limitations

- Pushes over 128MB are processed in chunks
- 30s CPU limit per request (Workers platform limit)
- HTTP(S) only, no SSH protocol support
- No server-side hooks yet

## Development

```bash
npm install
npm run dev             # Start local server
npm run test:workers    # Run Vitest tests
npm run test            # Run AVA tests
```

## License

MIT
