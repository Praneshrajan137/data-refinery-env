import { gzipSync } from "node:zlib";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const budgetBytes = 90 * 1024;
const assetsDir = fileURLToPath(new URL("../dist/assets/", import.meta.url));
const jsAssets = readdirSync(assetsDir).filter((name) => name.endsWith(".js"));

if (jsAssets.length === 0) {
  throw new Error("No JavaScript assets found in dist/assets.");
}

for (const asset of jsAssets) {
  const filePath = join(assetsDir, asset);
  const gzippedBytes = gzipSync(readFileSync(filePath)).byteLength;
  if (gzippedBytes > budgetBytes) {
    throw new Error(
      `${asset} is ${(gzippedBytes / 1024).toFixed(1)} KiB gzip, above the 90 KiB budget.`,
    );
  }
}

console.log(`Bundle budget passed for ${jsAssets.length} JavaScript asset(s).`);
