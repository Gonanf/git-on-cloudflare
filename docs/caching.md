# Caching Strategy

git-on-cloudflare implements a two-tier caching strategy to optimize performance and reduce load on Durable Objects and R2.

## Two-Tier Cache Architecture

### 1. UI Layer Cache (`git-on-cf:json`)

Caches JSON responses for web UI operations. Keys are synthetic same-origin GET requests under a private `/_cache/*` path, with parameters encoded as query strings:

- **HEAD/refs**: 60 seconds
  - Repository refs and HEAD state
  - Key path: `/_cache/refs?repo=<owner/repo>`
- **Tree listings / blob metadata**: 60s for trees, 300s for blob metadata (TTL depends on result type)
  - Directory contents (trees) and small blob previews
  - Key path: `/_cache/tree?repo=<owner/repo>&ref=<ref>&path=<path>`
- **README content**: 5 minutes
  - Rendered README markdown (raw markdown is rendered client-side)
  - Key path: `/_cache/readme?repo=<owner/repo>&ref=<ref>`
- **Commits**: Variable TTL
  - Branch commits: 60 seconds; Tag/OID commits: 1 hour (immutable)
  - Key path: `/_cache/commits?repo=<owner/repo>&ref=<ref>&page=<n>`

### 2. Git Object Cache (`git-on-cf:objects`)

Caches immutable Git objects:

- **Separate namespace** for binary Git objects
- **1 year TTL** with immutable headers
- **Objects cached**: commits, trees, blobs
- **Key format**: `/_cache/obj/:owner/:repo/:oid`

## Implementation Details

### Cache Namespaces

- **`git-on-cf:json`** - For JSON payloads (UI responses)
- **`git-on-cf:objects`** - For binary git objects

### Cache Key Functions

```typescript
// UI cache keys - builds same-origin GET request
buildCacheKeyFrom(req: Request, pathname: string, params: Record<string, string>)
// Example: buildCacheKeyFrom(req, "/_cache/commits", { ref: "main" })

// Object cache keys
buildObjectCacheKey(req: Request, repoId: string, oid: string)
// Returns: "/_cache/obj/:repoId/:oid"
```

### Cache Operations

```typescript
// JSON caching
await cacheGetJSON<T>(keyReq: Request): Promise<T | null>
await cachePutJSON(keyReq: Request, payload: unknown, ttlSeconds: number)

// Object caching
await cacheGetObject(keyReq: Request): Promise<{ type: string; payload: Uint8Array } | null>
await cachePutObject(keyReq: Request, type: string, payload: Uint8Array)

// Helper patterns with ctx.waitUntil support
await cacheOrLoad<T>(cacheKey, loader, ctx?: ExecutionContext)
await cacheOrLoadJSON<T>(cacheKey, loader, ttl, ctx?: ExecutionContext)
await cacheOrLoadJSONWithTTL<T>(cacheKey, loader, ttlResolver: (v: T) => number, ctx?: ExecutionContext)
```

### CacheContext Interface

```typescript
interface CacheContext {
  req: Request;
  ctx: ExecutionContext;
}
```

The CacheContext interface combines Request and ExecutionContext for operations that need both. While the cache functions themselves take Request directly, the CacheContext is used in higher-level code (like UI routes) to pass both values together when calling git functions that may cache results.

### Pack Metadata Hints (KV)

In addition to the two cache namespaces above, we use KV to persist small pieces of pack metadata that help avoid extra Durable Object (DO) metadata calls during reads:

- OID → pack mapping (immutable hint)
- Recent pack list for a repository (newest-first)

These values are stored under keys derived from the repository's DO ID (not owner/repo) to ensure consistency across DO and Worker contexts. See `src/keys.ts` for:

- `kvOidToPackKey(repoId, oid)`
- `kvPackListKey(repoId)`

TTLs and gating (defined in `src/cache/kv-pack-cache.ts`):

- OID → pack mapping TTL: 30 days
- Pack list TTL: 5 minutes
- Recent push window: 5 minutes (skip KV during churn)
- Unpack status TTL: up to 1 hour (skip KV while unpacking)

Writes are conservative to avoid staleness during pushes/unpacking:

1. Skip writes whenever `shouldSkipKVCache()` indicates a recent push or active unpack (based on KV markers)
2. Additionally, the Worker consults the DO RPC `getUnpackProgress()` before writing, to avoid relying solely on KV's eventual consistency

Reads treat KV as a performance hint only. If a hint is stale (e.g., a pack has been pruned), the R2 fetch will fail and the system falls back to the DO path without affecting correctness.

## Performance Impact

- **Latency reduction**: From 200-500ms to <50ms for cached objects
- **DO/R2 calls**: Massive reduction in backend calls
- **Edge performance**: Content served from Cloudflare's global cache
- **Cost savings**: Fewer Durable Object invocations and R2 reads

## Cache Invalidation

### UI Cache

- Short TTLs (60s-5min) ensure freshness
- Branch/HEAD changes visible within 1 minute
- README updates visible within 5 minutes

### Object Cache

- Git objects are immutable by design
- No invalidation needed
- 1 year TTL with proper cache headers

## Best Practices

1. **Always pass CacheContext** when available in route handlers
2. **Use appropriate TTLs** based on data mutability
3. **Separate namespaces** prevent key collisions
4. **Include repo scope** in cache keys for isolation
