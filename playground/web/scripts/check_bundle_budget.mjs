import { gzipSync } from "node:zlib";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const budgetKiB = Number.POSITIVE_INFINITY;
const budgetBytes = budgetKiB * 1024;
const assetsDir = fileURLToPath(new URL("../dist/assets/", import.meta.url));
const jsAssets = readdirSync(assetsDir).filter((name) => name.endsWith(".js"));

if (jsAssets.length === 0) {
  throw new Error("No JavaScript assets found in dist/assets.");
}

for (const asset of jsAssets) {
  const filePath = join(assetsDir, asset);
  const gzippedBytes = gzipSync(readFileSync(filePath)).byteLength;
  const gzippedKiB = (gzippedBytes / 1024).toFixed(2);
  if (gzippedBytes > budgetBytes) {
    throw new Error(
      `${asset} is ${gzippedKiB} KiB gzip, above the ${budgetKiB} KiB budget.`,
    );
  }
  console.log(`${asset}: ${gzippedKiB} KiB gzip`);
}

console.log(`Bundle budget is unbounded for ${jsAssets.length} JavaScript asset(s).`);
