// Shared stable environment variables for Vitest Workers runs
// These override Wrangler vars via poolOptions.workers.miniflare.bindings
// Miniflare values take precedence over wrangler.jsonc.

export const BASE_TEST_BINDINGS = {
  // Repo DO maintenance windows (minutes)
  REPO_DO_IDLE_MINUTES: "30",
  REPO_DO_MAINT_MINUTES: "1440",

  // Pack retention and recent window
  REPO_KEEP_PACKS: "10",
  REPO_PACKLIST_MAX: "50",

  // Background unpack chunking and timing
  REPO_UNPACK_CHUNK_SIZE: "100",
  REPO_UNPACK_MAX_MS: "2000",
  REPO_UNPACK_DELAY_MS: "500",
  REPO_UNPACK_BACKOFF_MS: "1000",

  // Logging level lowered for tests to reduce noise
  LOG_LEVEL: "warn",
} as const;
