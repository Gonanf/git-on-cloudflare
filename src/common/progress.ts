import { getRepoStub } from "./stub.ts";

export interface UnpackProgress {
  unpacking: boolean;
  processed?: number;
  total?: number;
  percent?: number;
  queuedCount?: number; // 0 or 1 (one-deep queue)
  currentPackKey?: string;
}

/**
 * Fetch unpacking progress data for a repository.
 * Returns null when no unpack is in progress.
 */
export async function getUnpackProgress(env: Env, repoId: string): Promise<UnpackProgress | null> {
  try {
    const stub = getRepoStub(env, repoId);
    const res = await stub.fetch("https://do/unpack-progress", { method: "GET" });
    if (!res.ok) return null;
    const progress = (await res.json()) as UnpackProgress;
    if ((progress.unpacking && progress.total) || Number(progress.queuedCount || 0) > 0) {
      return progress;
    }
  } catch {}
  return null;
}
