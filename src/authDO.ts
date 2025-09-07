import { createLogger } from "./util/logger";
import { asTypedStorage } from "./doState";
import {
  AuthStateSchema,
  AuthUsers,
  RateLimitEntry,
  makeOwnerRateLimitKey,
  makeAdminRateLimitKey,
} from "./authState";
function json(data: unknown, status = 200, headers: HeadersInit = {}) {
  const h = new Headers(headers);
  if (!h.has("Content-Type")) h.set("Content-Type", "application/json");
  return new Response(JSON.stringify(data), { status, headers: h });
}

/**
 * Generates a cryptographically secure random salt
 * @returns 16-byte salt for password hashing
 */
function generateSalt(): Uint8Array {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return bytes;
}

/**
 * Hash token with salt using native PBKDF2
 * @param token - Plain text token to hash
 * @param salt - Salt bytes for hashing
 * @param iterations - Number of PBKDF2 iterations (default: 100000, Cloudflare's limit)
 * @returns String in format "salt:iterations:hash" for storage
 */
const PBKDF2_ITERATIONS = 100000; // Cloudflare's current limit

async function hashTokenWithPBKDF2(
  token: string,
  salt: Uint8Array,
  iterations: number = PBKDF2_ITERATIONS
): Promise<string> {
  const encoder = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey("raw", encoder.encode(token), "PBKDF2", false, [
    "deriveBits",
  ]);

  const derivedBits = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt: salt,
      iterations: iterations,
      hash: "SHA-256",
    },
    keyMaterial,
    256 // 32 bytes * 8 bits
  );

  const hashArray = new Uint8Array(derivedBits);
  const saltHex = Array.from(salt)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const hashHex = Array.from(hashArray)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `${saltHex}:${iterations}:${hashHex}`; // Store salt:iterations:hash format
}

/**
 * Verifies a token against its stored hash
 * @param token - Plain text token to verify
 * @param storedHash - Stored hash in format "salt:iterations:hash"
 * @returns True if token matches the stored hash
 */
async function verifyToken(token: string, storedHash: string): Promise<boolean> {
  const parts = storedHash.split(":");
  if (parts.length !== 3) return false;

  // Format: salt:iterations:hash
  const [saltHex, iterStr, hashHex] = parts;
  const salt = new Uint8Array(saltHex.match(/.{2}/g)!.map((byte) => parseInt(byte, 16)));
  const iterations = parseInt(iterStr, 10);
  const computed = await hashTokenWithPBKDF2(token, salt, iterations);
  return computed === storedHash;
}

// Rate limiting configuration
const RATE_LIMIT = {
  maxAttempts: 5,
  windowMs: 60 * 1000, // 1 minute
  blockDurationMs: 5 * 60 * 1000, // 5 minutes
};

/**
 * Durable Object for managing repository authentication and rate limiting
 * Handles token verification, admin operations, and automatic cleanup
 */
export class AuthDurableObject implements DurableObject {
  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  /**
   * Alarm handler that cleans up expired rate limit entries
   * Runs hourly to prevent unbounded storage growth
   */
  async alarm() {
    // Clean up old rate limit entries (older than 1 hour)
    const now = Date.now();
    const ONE_HOUR = 60 * 60 * 1000;
    const log = this.logger;
    try {
      const keys = await this.state.storage.list({ prefix: "ratelimit:" });
      let removed = 0;
      for (const [key, value] of keys) {
        const entry = value as RateLimitEntry;
        if (entry && entry.lastAttempt && now - entry.lastAttempt > ONE_HOUR) {
          try {
            await this.state.storage.delete(key);
            removed++;
          } catch (e) {
            log.warn("ratelimit:cleanup-failed", { key, error: String(e) });
          }
        }
      }
      if (removed > 0) log.info("ratelimit:cleanup", { removed });
    } catch (e) {
      log.error("alarm:error", { error: String(e) });
    }
  }

  private async getStore(): Promise<AuthUsers> {
    const store = asTypedStorage<AuthStateSchema>(this.state.storage);
    return (await store.get("users")) ?? {};
  }
  private async putStore(obj: AuthUsers) {
    const store = asTypedStorage<AuthStateSchema>(this.state.storage);
    await store.put("users", obj);
  }

  private isAdmin(req: Request): boolean {
    const tok = (req.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
    const admin = this.env.AUTH_ADMIN_TOKEN || "";
    return admin.length > 0 && tok === admin;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method.toUpperCase();
    const log = this.logger;
    log.debug("fetch", { path: url.pathname, method });

    if (url.pathname === "/verify" && method === "POST") {
      const body = await request.json<any>().catch(() => ({}));
      const owner = String(body.owner || "").trim();
      const token = String(body.token || "").trim();
      if (!owner || !token) return json({ ok: false }, 200);

      // Rate limiting check
      const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
      const rateLimitKey = makeOwnerRateLimitKey(owner, clientIp);
      const now = Date.now();
      const astore = asTypedStorage<AuthStateSchema>(this.state.storage);
      const rateLimit = await astore.get(rateLimitKey);

      // Schedule cleanup if not already scheduled
      const alarmTime = await this.state.storage.getAlarm();
      if (!alarmTime) {
        // Schedule cleanup in 1 hour
        await this.state.storage.setAlarm(Date.now() + 60 * 60 * 1000);
      }

      if (rateLimit) {
        // Check if blocked
        if (rateLimit.blockedUntil && now < rateLimit.blockedUntil) {
          log.warn("verify:blocked", { owner, clientIp });
          return json({ ok: false, error: "Too many attempts. Please try again later." }, 429);
        }

        // Check if within rate limit window
        if (now - rateLimit.lastAttempt < RATE_LIMIT.windowMs) {
          if (rateLimit.attempts >= RATE_LIMIT.maxAttempts) {
            // Block the user
            rateLimit.blockedUntil = now + RATE_LIMIT.blockDurationMs;
            await astore.put(rateLimitKey, rateLimit);
            log.warn("verify:block", { owner, clientIp });
            return json({ ok: false, error: "Too many attempts. Please try again later." }, 429);
          }
        } else {
          // Reset counter if outside window
          rateLimit.attempts = 0;
        }
      }

      const users = await this.getStore();
      const list = users[owner] || [];
      if (list.length === 0) {
        // Update rate limit on failed attempt
        await astore.put(rateLimitKey, {
          attempts: (rateLimit?.attempts || 0) + 1,
          lastAttempt: now,
          blockedUntil: rateLimit?.blockedUntil,
        });
        log.info("verify:unknown-owner", { owner, clientIp });
        return json({ ok: false }, 200);
      }

      // Check against all stored hashes (supports both legacy and new format)
      for (const storedHash of list) {
        if (await verifyToken(token, storedHash)) {
          // Clear rate limit on successful auth
          await astore.delete(rateLimitKey);
          log.info("verify:ok", { owner, clientIp });
          return json({ ok: true }, 200);
        }
      }

      // Update rate limit on failed attempt
      await astore.put(rateLimitKey, {
        attempts: (rateLimit?.attempts || 0) + 1,
        lastAttempt: now,
        blockedUntil: rateLimit?.blockedUntil,
      });
      log.info("verify:fail", { owner, clientIp });
      return json({ ok: false }, 200);
    }

    // Admin-only endpoints below
    if (!this.isAdmin(request)) {
      // Rate limit admin auth attempts
      const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
      const adminRateLimitKey = makeAdminRateLimitKey(clientIp);
      const now = Date.now();
      const astore = asTypedStorage<AuthStateSchema>(this.state.storage);
      const adminRateLimit = await astore.get(adminRateLimitKey);
      if (adminRateLimit) {
        if (adminRateLimit.blockedUntil && now < adminRateLimit.blockedUntil) {
          return new Response("Too many attempts\n", {
            status: 429,
            headers: {
              "Retry-After": String(Math.ceil((adminRateLimit.blockedUntil - now) / 1000)),
            },
          });
        }

        if (now - adminRateLimit.lastAttempt < RATE_LIMIT.windowMs) {
          if (adminRateLimit.attempts >= RATE_LIMIT.maxAttempts) {
            adminRateLimit.blockedUntil = now + RATE_LIMIT.blockDurationMs;
            await astore.put(adminRateLimitKey, adminRateLimit);
            this.logger.warn("admin:block", { clientIp });
            return new Response("Too many attempts\n", {
              status: 429,
              headers: { "Retry-After": String(RATE_LIMIT.blockDurationMs / 1000) },
            });
          }
        } else {
          adminRateLimit.attempts = 0;
        }
      }

      // Update rate limit on failed admin auth
      await astore.put(adminRateLimitKey, {
        attempts: (adminRateLimit?.attempts || 0) + 1,
        lastAttempt: now,
        blockedUntil: adminRateLimit?.blockedUntil,
      });

      this.logger.info("admin:unauthorized", { clientIp, path: url.pathname, method });
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": "Bearer" },
      });
    }

    // Clear admin rate limit on successful auth
    const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
    const adminRateLimitKey = makeAdminRateLimitKey(clientIp);
    const astore = asTypedStorage<AuthStateSchema>(this.state.storage);
    await astore.delete(adminRateLimitKey);

    if (url.pathname === "/users" && method === "GET") {
      const users = await this.getStore();
      const data = Object.entries(users).map(([owner, hashes]) => ({ owner, tokens: hashes }));
      this.logger.debug("users:list", { count: data.length });
      return json({ users: data });
    }

    if (url.pathname === "/users" && method === "POST") {
      const body = await request.json<any>().catch(() => ({}));
      const owner = String(body.owner || "").trim();
      const token: string | undefined = body.token ? String(body.token) : undefined;
      const tokens: string[] | undefined = Array.isArray(body.tokens)
        ? body.tokens.map(String)
        : undefined;
      if (!owner || (!token && !tokens)) return json({ error: "owner and token(s) required" }, 400);
      const users = await this.getStore();
      const cur = new Set<string>(users[owner] || []);
      const toAdd: string[] = [];
      if (token) toAdd.push(token);
      if (tokens) toAdd.push(...tokens);
      for (const t of toAdd) {
        const salt = generateSalt();
        const h = await hashTokenWithPBKDF2(t, salt);
        cur.add(h);
      }
      users[owner] = Array.from(cur);
      await this.putStore(users);
      this.logger.info("users:added", { owner, count: users[owner].length });
      return json({ ok: true, owner, count: users[owner].length });
    }

    if (url.pathname === "/users" && method === "DELETE") {
      const body = await request.json<any>().catch(() => ({}));
      const owner = String(body.owner || "").trim();
      const token: string | undefined = body.token ? String(body.token) : undefined;
      const tokenHash: string | undefined = body.tokenHash ? String(body.tokenHash) : undefined;
      if (!owner) return json({ error: "owner required" }, 400);
      const users = await this.getStore();
      if (!(owner in users)) return json({ ok: true });
      if (!token && !tokenHash) {
        // No specific token provided, delete entire owner
        delete users[owner];
        await this.putStore(users);
        this.logger.info("users:owner-deleted", { owner });
        return json({ ok: true });
      }
      if (tokenHash) {
        // Direct hash removal
        users[owner] = (users[owner] || []).filter((x) => x !== tokenHash);
        if (users[owner].length === 0) delete users[owner];
        await this.putStore(users);
        this.logger.info("users:tokenhash-deleted", { owner });
      } else if (token) {
        // Find and remove matching token (check all stored hashes)
        const remaining: string[] = [];
        for (const storedHash of users[owner] || []) {
          if (!(await verifyToken(token, storedHash))) {
            remaining.push(storedHash);
          }
        }
        if (remaining.length < (users[owner] || []).length) {
          users[owner] = remaining;
          if (users[owner].length === 0) delete users[owner];
          await this.putStore(users);
          this.logger.info("users:token-deleted", { owner });
        }
      }
      return json({ ok: true });
    }

    return new Response("Not found\n", { status: 404 });
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "AuthDO",
      doId: this.state.id.toString(),
    });
  }
}
