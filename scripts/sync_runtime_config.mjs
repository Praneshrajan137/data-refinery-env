import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space";
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
