import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
  server: { deps: { inline: ["isomorphic-git", "@noble/hashes"] } },
  test: {
    include: ["test/auth.worker.test.ts"],
    poolOptions: {
      workers: {
        main: "./src/index.ts",
        wrangler: {
          configPath: "./wrangler.jsonc",
        },
        isolatedStorage: false,
        singleWorker: true,
        miniflare: {
          durableObjectsPersist: false,
          kvPersist: false,
          r2Persist: false,
          cachePersist: false,
          compatibilityDate: "2025-09-02",
          // Enable centralized auth in this auth test suite
          bindings: { AUTH_ADMIN_TOKEN: "admin", LOG_LEVEL: "warn" },
        },
      },
    },
  },
} as any);
