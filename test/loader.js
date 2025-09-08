import { resolve as resolvePath } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";

const baseURL = pathToFileURL(resolvePath(fileURLToPath(import.meta.url), "../../src")).href;

export async function resolve(specifier, context, nextResolve) {
  // Handle @ alias
  if (specifier.startsWith("@/")) {
    const newSpecifier = specifier.replace("@/", baseURL + "/");
    return nextResolve(newSpecifier, context);
  }

  return nextResolve(specifier, context);
}
