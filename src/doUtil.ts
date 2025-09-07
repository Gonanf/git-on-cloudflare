import { makeRepoId } from "./keys.ts";

export function getRepoStub(env: Env, repoId: string) {
  const id = env.REPO_DO.idFromName(repoId);
  return env.REPO_DO.get(id);
}

export function repoKey(owner: string, repo: string): string {
  // Use slash-separated IDs to match tests and Durable Object naming
  // e.g. "owner/repo" (NOT colon). Many tests and routes assume this shape.
  return makeRepoId(owner, repo);
}
