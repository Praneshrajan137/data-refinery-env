/**
 * Verify the generated domain vocabulary is current -- without needing Python.
 *
 * The generated TypeScript embeds a SHA-256 of `dataforge/domain/vocabulary.py`. This
 * script hashes that file directly and compares, so the frontend build can detect both
 * a stale generation and a hand-edit of the generated file in a Node-only environment.
 *
 * The authoritative regeneration check is still `make lint`
 * (scripts/ci/generate_domain_vocabulary.py --check), which compares the full rendered
 * text. This is the portable tripwire, not a replacement for it.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { auditRawTokens, humanisedFields } from "./raw_tokens.mjs";

const ROOT = resolve(import.meta.dirname, "../../..");
const SOURCE = resolve(ROOT, "dataforge/domain/vocabulary.py");
const GENERATED = resolve(ROOT, "playground/web/src/domain/vocabulary.generated.ts");

function fail(message) {
  console.error(message);
  console.error("Run: python scripts/ci/generate_domain_vocabulary.py --write");
  process.exit(1);
}

let generated;
try {
  generated = readFileSync(GENERATED, "utf8");
} catch {
  fail(`Missing ${GENERATED}: the domain vocabulary has never been generated.`);
}

const declared = /Source hash: sha256:([0-9a-f]{64})/.exec(generated);
if (declared === null) {
  fail(
    "The generated vocabulary carries no source hash. Either it was hand-edited or it " +
      "predates the fingerprint; regenerate it.",
  );
}

// Normalise line endings before hashing: this checkout stores CRLF, and a line-ending
// difference is not a vocabulary change.
const source = readFileSync(SOURCE).toString("utf8").replace(/\r\n/g, "\n");
const actual = createHash("sha256").update(source, "utf8").digest("hex");

if (actual !== declared[1]) {
  fail(
    "STALE: dataforge/domain/vocabulary.py has changed since the TypeScript was " +
      `generated.\n  source:    sha256:${actual}\n  generated: sha256:${declared[1]}\n` +
      "The browser and the engine would disagree about the trust vocabulary.",
  );
}

console.log("Domain vocabulary fingerprint verified (generated file matches Python source).");

// Second law, same file: a vocabulary that exists is not the same as a vocabulary that is USED.
// The fingerprint above proves the humaniser tables are current; this proves the components
// actually call them instead of printing the machine token.
const rawTokens = auditRawTokens(resolve(ROOT, "playground/web/src"), generated);
if (rawTokens.length > 0) {
  console.error("Raw domain tokens reach the browser:");
  for (const problem of rawTokens) {
    console.error(`  ${problem}`);
  }
  console.error(
    "Wrap each in the matching humanizer from src/observatory.ts, or add a *_HUMAN table to " +
      "dataforge/domain/vocabulary.py and regenerate.",
  );
  process.exit(1);
}

console.log(
  `Raw-token audit passed (${humanisedFields(generated).join(", ")} are humanised at every render site).`,
);
