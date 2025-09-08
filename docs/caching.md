# Caching Strategy

git-on-cloudflare implements a two-tier caching strategy to optimize performance and reduce load on Durable Objects and R2.

## Two-Tier Cache Architecture

### 1. UI Layer Cache (`git-on-cf:json`)

Caches JSON responses for web UI operations:

- **HEAD/refs**: 60 seconds
  - Repository refs and HEAD state
  - Key: `/:owner/:repo/refs`
- **Tree listings**: 60 seconds
  - Directory contents
  - Key: `/:owner/:repo/tree/:ref/:path`
- **README content**: 5 minutes
  - Rendered README HTML
  - Key: `/:owner/:repo/readme/:ref`
- **Commits**: Variable TTL
  - Branch commits: 60 seconds
  - Tag/OID commits: 1 hour (immutable)
  - Key: `/:owner/:repo/commits/:ref/:page`

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
```

### CacheContext Interface

```typescript
interface CacheContext {
  req: Request;
  ctx: ExecutionContext;
}
```

The CacheContext interface combines Request and ExecutionContext for operations that need both. While the cache functions themselves take Request directly, the CacheContext is used in higher-level code (like UI routes) to pass both values together when calling git functions that may cache results.

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
