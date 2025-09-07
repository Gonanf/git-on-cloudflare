import { getRepoStub } from "./doUtil";

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
  constructor(
    private env: Env,
    private repoId: string
  ) {}

  async listRefs(): Promise<Ref[]> {
    const stub = getRepoStub(this.env, this.repoId);
    const res = await stub.fetch("https://do/refs", { method: "GET" });
    if (!res.ok) return [];
    const data = await res.json();
    if (Array.isArray(data)) return data as Ref[];
    if (data && typeof data === "object" && "name" in data && "oid" in data) {
      return [data as Ref];
    }
    return [];
  }

  async getHead(): Promise<HeadInfo | undefined> {
    const stub = getRepoStub(this.env, this.repoId);
    const res = await stub.fetch("https://do/head", { method: "GET" });
    if (!res.ok) return undefined;
    const data = await res.json();
    if (data && typeof data === "object" && "target" in data) {
      return data as HeadInfo;
    }
    return undefined;
  }
}
