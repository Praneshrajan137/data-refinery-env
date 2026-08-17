/**
 * Detects domain enum values rendered raw into JSX.
 *
 * WHY THIS EXISTS
 *
 * The browser used to print machine tokens as prose: `Metric label="Verifier"` rendered
 * "not_run", the safety metric rendered "escalate", and one loop-rail line concatenated both
 * into "allow safety, accept verifier". Humanisers were added and every known site fixed -- but
 * the gate that was supposed to stop it recurring was promised and never written, so the fix
 * had no floor under it. This is that floor.
 *
 * THE FIELD LIST IS DERIVED, NOT LISTED. Every `*_HUMAN` table in the generated vocabulary
 * implies a field that must not be rendered raw: VERIFIER_VERDICT_HUMAN implies
 * `verifier_verdict`, PROVENANCE_HUMAN implies `provenance`, and so on. Adding a humaniser to
 * dataforge/domain/vocabulary.py therefore extends this gate by itself, with no second list to
 * drift out of sync -- which matters, because a duplicated vocabulary is the specific mistake
 * this repo has made repeatedly.
 */

import { readFileSync, readdirSync, statSync } from "node:fs";

/** `VERIFIER_VERDICT_HUMAN` -> `verifier_verdict`. */
export function humanisedFields(generatedVocabulary) {
  return [...generatedVocabulary.matchAll(/export const ([A-Z0-9_]+)_HUMAN\b/g)]
    .map((match) => match[1].toLowerCase())
    .sort();
}

/** Strips comments so that discussing a token in prose is not a violation. */
function withoutComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^[ \t]*\/\/.*$/gm, "");
}

/**
 * Removes `key={...}` expressions.
 *
 * A React key is never displayed, so a token inside one is not a render. Done with a
 * balanced-brace scan rather than a regex because keys are commonly template literals
 * containing their own `${}` -- `key={`${item.row}:${item.review_reason}`}` has nested braces
 * and defeated the regex, producing a false positive on the real code.
 */
function withoutKeyProps(source) {
  let result = "";
  let index = 0;
  while (index < source.length) {
    const start = source.indexOf("key={", index);
    if (start === -1) {
      result += source.slice(index);
      break;
    }
    result += source.slice(index, start);
    let depth = 0;
    let cursor = start + "key=".length;
    for (; cursor < source.length; cursor += 1) {
      if (source[cursor] === "{") {
        depth += 1;
      } else if (source[cursor] === "}") {
        depth -= 1;
        if (depth === 0) {
          cursor += 1;
          break;
        }
      }
    }
    result += "key={KEY}";
    index = cursor;
  }
  return result;
}

/**
 * Returns `{ line, text, field }` for each raw render found.
 *
 * SCOPED TO TEXT POSITIONS, and narrowly. The first draft of this rule flagged any field
 * access outside a humaniser and produced three hits, all of which were false: a React `key`,
 * and two props passed to CertificatePanel which only ever COMPARES them (`=== "agreed"`) to
 * pick a chip. Reading a token is legitimate; printing it is not. A gate that fires on
 * non-defects gets weakened, so the rule matches only the shapes that put a value in front of
 * a person:
 *
 *   value={...}            the prop Metric renders verbatim
 *   `...${...}...`         template-literal interpolation
 *   >{...}  and  {...}<    a JSX expression standing as text between tags
 *
 * Every one of the four defects this gate was written for is one of those three shapes, and
 * raw_tokens.test.mjs asserts that against the original code.
 */
export function rawTokenRenders(source, fields) {
  const cleaned = withoutKeyProps(withoutComments(source)).replace(
    /humanize[A-Za-z]*\([^()]*\)/g,
    "HUMANISED",
  );
  const lineOf = (index) => cleaned.slice(0, index).split(/\r?\n/).length;
  const violations = [];

  for (const field of fields) {
    const escaped = `\\.${field}\\b`;
    const patterns = [
      new RegExp(`value=\\{[^{}]*${escaped}[^{}]*\\}`, "g"),
      new RegExp("`[^`]*\\$\\{[^{}]*" + escaped + "[^{}]*\\}[^`]*`", "g"),
      new RegExp(`>\\s*\\{[^{}]*${escaped}[^{}]*\\}`, "g"),
      new RegExp(`\\{[^{}]*${escaped}[^{}]*\\}\\s*<`, "g"),
    ];
    for (const pattern of patterns) {
      for (const match of cleaned.matchAll(pattern)) {
        violations.push({
          line: lineOf(match.index),
          text: match[0].replace(/\s+/g, " ").trim().slice(0, 100),
          field,
        });
      }
    }
  }

  // One offence per site, however many patterns matched it.
  const seen = new Set();
  return violations.filter((violation) => {
    const key = `${violation.line}:${violation.field}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

/** Every .tsx file under a root, excluding tests. */
export function componentFiles(root) {
  const found = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir)) {
      const path = `${dir}/${entry}`;
      if (statSync(path).isDirectory()) {
        walk(path);
        continue;
      }
      if (entry.endsWith(".tsx") && !entry.includes(".test.")) {
        found.push(path);
      }
    }
  };
  walk(root);
  return found;
}

/** Audits a tree, returning printable violation strings. */
export function auditRawTokens(srcRoot, generatedVocabulary) {
  const fields = humanisedFields(generatedVocabulary);
  if (fields.length === 0) {
    // Fail closed. An empty field list would make this gate vacuous while still passing, which
    // is the failure mode the whole exercise exists to remove.
    return ["No *_HUMAN tables found in the generated vocabulary; the raw-token gate is vacuous."];
  }
  const problems = [];
  for (const file of componentFiles(srcRoot)) {
    for (const found of rawTokenRenders(readFileSync(file, "utf8"), fields)) {
      problems.push(
        `${file}:${found.line} renders ${found.field} raw; wrap it in a humaniser: ${found.text}`,
      );
    }
  }
  return problems;
}
