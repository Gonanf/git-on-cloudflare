// @ts-nocheck
import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";
import path from "path";
import { BASE_TEST_BINDINGS } from "./test/vitest.bindings.ts";

export default defineWorkersConfig({
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  // Inline deps at the vite-node server level to avoid SSR optimizer resolution issues
  server: { deps: { inline: ["isomorphic-git", "@noble/hashes"] } },
  test: {
    include: ["test/**/*.worker.test.ts"],
    exclude: ["test/auth.worker.test.ts"],
    poolOptions: {
      workers: {
        main: "./src/index.ts",
        wrangler: {
          configPath: "./wrangler.jsonc",
        },

        // Windows workaround: disable isolatedStorage to avoid EBUSY teardown failures
        // Avoid filesystem persistence to prevent SQLite CANTOPEN/EBUSY on Windows
        singleWorker: true,
        isolatedStorage: false,

        miniflare: {
          durableObjectsPersist: false,
          kvPersist: false,
          r2Persist: false,
          cachePersist: false,
          // Silence compatibility date warnings by matching installed runtime
          compatibilityDate: "2025-09-02",
          // Ensure centralized auth is disabled in tests (no admin token)
          bindings: { ...BASE_TEST_BINDINGS, AUTH_ADMIN_TOKEN: "" },
        },
      },
    },
  },
} as any);
