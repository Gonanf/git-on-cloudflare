import { getRepoStub } from "@/common/stub.ts";

export interface Ref {
  name: string;
  oid: string;
}

export interface HeadInfo {
  target: string; // e.g., "refs/heads/main"
  oid?: string; // optional resolved OID if known
  unborn?: boolean; // true if the target ref is unborn
}

export interface RepoEngine {
  // Return HEAD and refs/* as available
  listRefs(): Promise<Ref[]>;
  getHead(): Promise<HeadInfo | undefined>;
}

// Placeholder JS engine implementation (to be replaced with real logic)
export class JsRepoEngine implements RepoEngine {
  private env: Env;
  private repoId: string;

  constructor(env: Env, repoId: string) {
    this.env = env;
    this.repoId = repoId;
  }

  async listRefs(): Promise<Ref[]> {
    const stub = getRepoStub(this.env, this.repoId);
    try {
      const refs = await stub.listRefs();
      return Array.isArray(refs) ? (refs as Ref[]) : [];
    } catch {
      return [];
    }
  }

  async getHead(): Promise<HeadInfo | undefined> {
    const stub = getRepoStub(this.env, this.repoId);
    try {
      const head = await stub.getHead();
      return head as HeadInfo;
    } catch {
      return undefined;
    }
  }
}
