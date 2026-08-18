import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Same origin by DEFAULT, absolute URL only on request.
 *
 * This used to default to a hardcoded Azure Container Apps hostname. That subscription expired
 * and the host went dead, and because the value was a committed BUILD DEFAULT rather than a
 * deployment input, the next frontend build would have pointed a working site at a dead backend.
 * The live site was unaffected only by luck: it had been deployed before the value changed.
 *
 * Empty means "call the origin that served this page" -- `backendPath()` in src/config.ts
 * returns a relative path when BACKEND_URL is empty. The API and the SPA are now served by one
 * Azure Container App, so relative is not merely adequate, it is correct: it cannot drift, it
 * needs no CORS allowlist, and it survives the host being renamed or moved.
 *
 * A split deployment is still possible: set BACKEND_URL in the environment to an absolute URL.
 * That is an explicit deployment decision, which is where a hostname belongs.
 */
const DEFAULT_BACKEND_URL = "";
const scriptDir = dirname(fileURLToPath(import.meta.url));
const webRoot = dirname(scriptDir);
const dashboardEditableConfig = join(webRoot, "config.js");
const publicConfig = join(webRoot, "public", "config.js");

if (!existsSync(dashboardEditableConfig)) {
  throw new Error(`Missing runtime config template: ${dashboardEditableConfig}`);
}

const backendUrl = process.env.BACKEND_URL || DEFAULT_BACKEND_URL;
const source = readFileSync(dashboardEditableConfig, "utf8").replace(
  /BACKEND_URL:\s*""/,
  `BACKEND_URL: ${JSON.stringify(backendUrl)}`,
);

mkdirSync(dirname(publicConfig), { recursive: true });
writeFileSync(publicConfig, source);
console.log(`Synced ${dashboardEditableConfig} -> ${publicConfig}`);
