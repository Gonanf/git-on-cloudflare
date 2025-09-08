import { getRepoStub } from "../doUtil";

/**
 * Fetch unpacking progress banner HTML for a repository.
 * Returns an empty string when no unpack is in progress.
 */
export async function getUnpackProgressHtml(env: Env, repoId: string): Promise<string> {
  try {
    const stub = getRepoStub(env, repoId);
    const res = await stub.fetch("https://do/unpack-progress", { method: "GET" });
    if (!res.ok) return "";
    const progress = (await res.json()) as {
      unpacking: boolean;
      processed?: number;
      total?: number;
      percent?: number;
    };
    if (progress.unpacking && progress.total) {
      const processed = progress.processed || 0;
      const total = progress.total || 0;
      const percent = progress.percent || 0;
      return `<div class="alert warn">📦 Unpacking objects: ${processed}/${total} (${percent}%)</div>`;
    }
  } catch {}
  return "";
}
