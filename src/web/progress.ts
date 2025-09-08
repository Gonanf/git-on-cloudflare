import { getRepoStub } from "@/common/stub";

export interface UnpackProgress {
  unpacking: boolean;
  processed?: number;
  total?: number;
  percent?: number;
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
    if (progress.unpacking && progress.total) {
      return progress;
    }
  } catch {}
  return null;
}
