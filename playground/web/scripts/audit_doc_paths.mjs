import { existsSync } from "node:fs";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

/**
 * A documentation index that points at deleted files is worse than no index: it tells a
 * reader the code is somewhere it is not. Section 10 of the quantitative grammar listed
 * four modules that the addressability rewrite removed, and nothing caught it.
 *
 * This checks every repo-relative source path mentioned in the design docs.
 */
const ROOT = resolve(import.meta.dirname, "../../..");
const DOCS = [
  "docs/design/quantitative-grammar.md",
  "docs/design/perceptual-language.md",
];

// `tsx` must precede `ts` in the alternation, and the extension must not be followed by
// another word character -- otherwise `.tsx` matches as `.ts` and the audit reports
// dangling references that are really its own parsing bug.
const PATH_PATTERN =
  /(?:playground\/(?:web\/src|web\/scripts|web\/e2e|api)|dataforge|scripts)\/[\w./-]+\.(?:tsx|ts|mjs|py|json|css)(?![\w])/g;

let failures = 0;
let checked = 0;

for (const doc of DOCS) {
  const full = resolve(ROOT, doc);
  if (!existsSync(full)) {
    continue;
  }
  const body = readFileSync(full, "utf8");
  const seen = new Set(body.match(PATH_PATTERN) ?? []);
  for (const candidate of seen) {
    checked += 1;
    if (!existsSync(resolve(ROOT, candidate))) {
      console.error(`${doc} references '${candidate}', which does not exist.`);
      failures += 1;
    }
  }
}

if (failures > 0) {
  console.error(`\nDesign-doc path audit failed: ${failures} dangling reference(s).`);
  process.exit(1);
}
console.log(`Design-doc path audit passed (${checked} source paths, ${DOCS.length} docs).`);
