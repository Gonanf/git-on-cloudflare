# git-on-cloudflare

> _Vibe‑coded on the edge._ ⚡🌀 _Ship fast, keep it readable, iterate with taste._

[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-f38020?logo=cloudflare&logoColor=white)](https://developers.cloudflare.com/workers/)
[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zllovesuki/git-on-cloudflare)

**A fully-functional Git server that runs entirely on Cloudflare's edge infrastructure.** No VMs, no containers, no Kubernetes. Just Durable Objects and R2.

Host unlimited private Git repositories at the edge with predictable, usage-based pricing. Clone, push, and browse repos from anywhere in the world with sub-100ms latency.

## Why?

GitHub/GitLab are great, but sometimes you want:

- **Complete control** over your code without the hassle of self-hosting
- **Global edge performance** without managing servers in multiple regions
- **Predictable pricing** that scales with storage, not seats
- **Zero-ops hosting** that actually means zero ops

## Quick Start

```bash
# Clone and install
git clone https://github.com/zllovesuki/git-on-cloudflare
cd git-on-cloudflare
npm install

# Run locally
npm run dev

# Create a repo and push
git push http://localhost:8787/you/my-repo main
```

That's it. No Docker, no k8s, no config files. Visit `http://localhost:8787/you/my-repo` to browse your code.

## How It Works

**The interesting bit:** This is a complete Git Smart HTTP v2 server implemented using Cloudflare's edge primitives:

- **Durable Objects** provide strong consistency for refs and HEAD (the tricky part of distributed Git hosting)
- **R2** stores pack files and loose objects (unlimited storage with usage-based pricing)
- **Workers** handle HTTP, pack negotiation, and object assembly
- **Zero cold starts** - Durable Objects keep repositories "warm" in memory

### Technical Highlights

- **Streaming pack generation** with multi-pack-index support for efficient fetches
- **Time-budgeted unpacking** - large pushes are processed incrementally without blocking
- **PBKDF2-SHA256** auth with 100k iterations (stored as `salt:iterations:hash`)
- **10MB/s+ clone speeds** from any Cloudflare edge location
- **Hybrid storage model** - hot objects in Durable Object memory, cold in R2

## Features

✅ **Full Git compatibility** - Works with any Git client  
✅ **Web UI** - Browse code, commits, and files without cloning  
✅ **Per-owner auth** - Separate tokens for each namespace  
✅ **Global edge hosting** - Repos served from 300+ locations  
✅ **Unlimited repos** - Pay only for storage used  
✅ **Simple operations** - No databases to manage, no servers to patch

## API Reference

### Git Endpoints (Git Smart HTTP v2)

```bash
# Clone a repository
git clone https://your-domain.com/owner/repo

# Push with authentication
git push https://owner:token@your-domain.com/owner/repo main
```

- `GET /:owner/:repo/info/refs?service=git-upload-pack` - Capability advertisement
- `POST /:owner/:repo/git-upload-pack` - Fetch objects (clone/pull)
- `POST /:owner/:repo/git-receive-pack` - Push objects

### Web UI Routes

- `GET /:owner/:repo` - Repository overview
- `GET /:owner/:repo/tree` - Browse files
- `GET /:owner/:repo/commits` - Commit history
- `GET /:owner/:repo/blob` - View file contents

### Admin API

- `GET /:owner/:repo/admin/refs` - List all refs (JSON)
- `PUT /:owner/:repo/admin/refs` - Update refs
- `GET /:owner/:repo/admin/head` - Get HEAD

## Local Development

```bash
# Install dependencies
npm install

# Start local server
npm run dev

# In another terminal - push a repo
git push http://localhost:8787/test/my-repo main
```

### With Authentication

```bash
# Set admin token
echo "AUTH_ADMIN_TOKEN=secret123" > .dev.vars

# Restart dev server
npm run dev

# Add a user via the web UI
open http://localhost:8787/auth
# Or via API:
curl -X POST http://localhost:8787/auth/api/owners/alice/tokens \
  -H "Authorization: Bearer secret123"

# Push with auth
git push http://alice:token@localhost:8787/alice/my-repo main
```

## Production Deployment

### Deploy to Cloudflare

```bash
# Configure your Cloudflare account
wrangler login

# Set admin token (for auth UI)
wrangler secret put AUTH_ADMIN_TOKEN

# Deploy
npm run deploy
```

Or use the one-click deploy button above.

### Configuration

Key settings in `wrangler.jsonc`:

```jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "REPO_DO", "class_name": "RepoDurableObject" },
      { "name": "AUTH_DO", "class_name": "AuthDurableObject" },
    ],
  },
  "r2_buckets": [{ "binding": "REPO_BUCKET", "bucket_name": "git-repos" }],
  "vars": {
    // Maintenance windows (minutes)
    "REPO_DO_IDLE_MINUTES": "30", // Cleanup idle repos after 30 min
    "REPO_DO_MAINT_MINUTES": "1440", // Run maintenance daily

    // Pack management
    "REPO_KEEP_PACKS": "3", // Long-term pack retention
    "REPO_PACKLIST_MAX": "20", // Recent pack window

    // Background unpacking (tune for your workload)
    "REPO_UNPACK_CHUNK_SIZE": "25", // Objects per chunk
    "REPO_UNPACK_MAX_MS": "1000", // Max time per unpack
    "REPO_UNPACK_DELAY_MS": "500", // Delay between chunks

    "LOG_LEVEL": "info", // debug|info|warn|error
  },
}
```

## Authentication

**Default behavior**: Reads (clone/pull) and web UI viewing are always public, no auth required.

**To protect pushes**: Set `AUTH_ADMIN_TOKEN` to enable authentication:

- Only pushes require owner tokens (HTTP Basic auth)
- Clones, pulls, and web browsing remain public
- Tokens are hashed with PBKDF2-SHA256 (100k iterations)
- Manage tokens via web UI at `/auth` or API

Note: This is intentional - the philosophy is "read open, write protected"

## Architecture

This implementation makes several interesting technical decisions:

1. **Durable Objects for consistency** - Each repository gets its own Durable Object, providing linearizable consistency for refs without coordination.

2. **R2 for bulk storage** - Pack files and objects are stored in R2, providing unlimited storage with pay-per-GB pricing.

3. **Hybrid caching** - Hot objects are cached in Durable Object memory (fast), cold objects streamed from R2 (cheap).

4. **Incremental unpacking** - Large pushes are unpacked in time-sliced chunks to stay within CPU limits.

5. **Pack reuse** - Multi-pack-index support enables efficient clones without full repack operations.

For detailed architecture docs, see:

- [Architecture Overview](docs/architecture.md)
- [Storage Model](docs/storage.md)
- [Data Flows](docs/data-flows.md)

## Performance

- **Clone speed**: 10-50 MB/s from edge locations
- **Push processing**: <5s for typical commits
- **Global latency**: <100ms from any location
- **Storage cost**: Pay only for what you use (R2 pricing)
- **Request cost**: Usage-based pricing per million requests

## Limitations

- **Pack size**: Pushes over 128MB need chunked processing
- **CPU time**: 30s limit per request (configurable with paid plans)
- **No SSH**: HTTP(S) only (use tokens for auth)
- **No hooks**: No server-side hooks (yet)

## Testing

```bash
npm run test:workers  # Vitest for Worker tests
npm run test          # AVA for Node tests
```

## Contributing

PRs welcome! The codebase is vibe-coded:

- Ship fast, then refactor
- Explicit over clever
- Types over any (but any is fine when prototyping)
- Comments where the vibes need explaining
- Dark mode is not optional

## License

MIT
