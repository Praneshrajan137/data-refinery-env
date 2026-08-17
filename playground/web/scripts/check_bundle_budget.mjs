/**
 * Bundle budget gate.
 *
 * WHY THIS FILE IS SHAPED LIKE THIS
 *
 * The previous version of this gate declared an infinite ceiling and then compared against
 * it. The comparison could not fail, in any build, ever; the script's own final line
 * announced that the budget was unbounded. It was a budget in name only -- the same
 * failure class the perceptual gates were rewritten to remove, where a law was stated and
 * then verified symbolically instead of actually.
 *
 * Three properties are therefore structural here rather than conventional:
 *
 *   1. `assertBudgetsAreEnforceable` REJECTS a non-finite ceiling. Restoring Infinity is
 *      now a configuration error, not a silent no-op.
 *   2. Every emitted JS asset must match a budgeted chunk role. A new unbudgeted chunk
 *      FAILS rather than escaping measurement -- an unmeasured asset is how an unbounded
 *      total hides.
 *   3. The comparison lives in the pure `evaluateBundle`, unit-tested in
 *      check_bundle_budget.test.mjs. A gate that needs a 1.5s production build before it
 *      can be exercised does not get exercised; the mutation harness cannot reach this
 *      one at all, because mutants run before Playwright builds `dist`.
 *
 * CEILINGS ARE DERIVED, NOT CHOSEN. Each is the measurement taken at the commit that
 * introduced this gate, plus a 5% growth allowance, rounded up. They are gzip bytes,
 * computed with the same `gzipSync` defaults used below, because a ceiling measured by a
 * different compressor than the gate uses is not a ceiling.
 *
 *   index            44,422 B  ->  47,000
 *   vendor          108,235 B  -> 114,000
 *   rolldown-runtime    471 B  ->   2,000  (floor; too small for a 5% band to mean anything)
 *   total JS        153,128 B  -> 161,000
 *   total CSS        12,724 B  ->  14,000
 *
 * The index ceiling has moved once, from 44,000, and the reason belongs here rather than only
 * in a commit message: this gate FAILED on the work that followed it. The app chunk grew
 * 41,556 -> 44,422 B (+6.9%) when the offline state, the shareable-link plumbing, the verdict
 * humanisers, the strength legend and the stale-result notice were added. That is real
 * user-facing behaviour rather than accidental weight, so the ceiling was re-derived by the
 * same rule instead of the growth being hidden. A budget raised silently is the unbounded
 * budget again, in slower motion.
 *
 * Vendor is 71% of the shipped JS. That is the honest headline of this measurement and
 * the reason the chunk split exists: the application is 43 KiB gzip, its dependencies
 * are 106 KiB, and a single monolithic chunk stated neither.
 *
 * SECOND MEASUREMENT (round 2). "Dependencies are 106 KiB" was itself an aggregate that hid
 * its own largest term. Splitting per package and measuring gzip directly:
 *
 *     motion       43.95 KiB gzip   (134.27 KiB raw)
 *     vendor       65.91 KiB gzip   (react, react-dom, papaparse, lucide, everything else)
 *     index        45.68 KiB gzip   (application code)
 *     runtime       0.47 KiB gzip
 *     ---------------------------------
 *     total JS    156.01 KiB gzip
 *
 * `motion` alone is 28% of shipped JS. An earlier note in this repo estimated it at ~48 KiB
 * gzip by apportioning sourcemap bytes; measured, it is 43.95 KiB. The estimate was in the
 * right region and still wrong, which is the argument for measuring rather than apportioning.
 * It now carries its own ceiling so that number cannot drift unattributed.
 *
 * Usage:
 *   node scripts/check_bundle_budget.mjs
 */

import { gzipSync } from "node:zlib";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

/** Chunk roles are matched by filename prefix, because Vite appends a content hash. */
export const BUDGETS = {
  perChunkBytes: {
    index: 47_000,
    // Tightened from 114,000. `motion` moved into its own chunk, so vendor's contents shrank
    // from 106 KiB to 65.9 KiB gzip. Leaving the old ceiling would have let vendor grow 73%
    // unnoticed -- a budget that no longer binds is the unbounded budget again, just quieter.
    vendor: 70_000,
    // New chunk, new ceiling. Measured 43,950 B gzip + 5%, rounded up, by the same rule as the
    // others.
    motion: 47_000,
    "rolldown-runtime": 2_000,
  },
  totalJsBytes: 161_000,
  totalCssBytes: 14_000,
};

/**
 * Maps `index-CDpSX4T6.js` to `index`. Returns null when no budgeted role matches, which
 * the caller must treat as a failure rather than as "unbudgeted, therefore fine".
 *
 * Longest prefix wins so that a future `vendor-react-*` chunk cannot be silently absorbed
 * by a shorter `vendor` budget.
 */
export function chunkRole(assetName, roles) {
  const matches = Object.keys(roles)
    .filter((role) => assetName.startsWith(`${role}-`) || assetName === `${role}.js`)
    .sort((left, right) => right.length - left.length);
  return matches[0] ?? null;
}

/**
 * Rejects a budget table that cannot fail. Infinity, NaN, negatives and non-numbers are
 * all ways of writing "no budget" while looking like one.
 */
export function assertBudgetsAreEnforceable(budgets) {
  const problems = [];
  const check = (label, value) => {
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      problems.push(`${label} must be a finite positive byte count, got ${String(value)}`);
    }
  };
  check("totalJsBytes", budgets.totalJsBytes);
  check("totalCssBytes", budgets.totalCssBytes);
  const roles = Object.entries(budgets.perChunkBytes ?? {});
  if (roles.length === 0) {
    problems.push("perChunkBytes declares no chunk roles, so no asset would be budgeted");
  }
  for (const [role, value] of roles) {
    check(`perChunkBytes.${role}`, value);
  }
  return problems;
}

/**
 * Pure budget comparison. `assets` is a list of `{ name, kind, gzippedBytes }`.
 * Returns a list of human-readable violations; empty means the build is within budget.
 */
export function evaluateBundle(assets, budgets) {
  const configProblems = assertBudgetsAreEnforceable(budgets);
  if (configProblems.length > 0) {
    return configProblems;
  }

  const violations = [];
  const js = assets.filter((asset) => asset.kind === "js");
  const css = assets.filter((asset) => asset.kind === "css");

  if (js.length === 0) {
    violations.push("No JavaScript assets found in dist/assets.");
  }

  for (const asset of js) {
    const role = chunkRole(asset.name, budgets.perChunkBytes);
    if (role === null) {
      violations.push(
        `${asset.name} matches no budgeted chunk role (${Object.keys(budgets.perChunkBytes).join(", ")}); ` +
          "add a ceiling for it rather than shipping an unmeasured chunk.",
      );
      continue;
    }
    const ceiling = budgets.perChunkBytes[role];
    if (asset.gzippedBytes > ceiling) {
      violations.push(
        `${asset.name} is ${asset.gzippedBytes} B gzip, above the ${ceiling} B ceiling for "${role}".`,
      );
    }
  }

  const totalJs = js.reduce((sum, asset) => sum + asset.gzippedBytes, 0);
  if (totalJs > budgets.totalJsBytes) {
    violations.push(`Total JS is ${totalJs} B gzip, above the ${budgets.totalJsBytes} B budget.`);
  }

  const totalCss = css.reduce((sum, asset) => sum + asset.gzippedBytes, 0);
  if (totalCss > budgets.totalCssBytes) {
    violations.push(
      `Total CSS is ${totalCss} B gzip, above the ${budgets.totalCssBytes} B budget.`,
    );
  }

  return violations;
}

/** Reads dist/assets and measures gzip. Sourcemaps are excluded: they are not served. */
export function measureAssets(assetsDir) {
  const kindOf = (name) => {
    if (name.endsWith(".js")) return "js";
    if (name.endsWith(".css")) return "css";
    return null;
  };
  return readdirSync(assetsDir)
    .map((name) => ({ name, kind: kindOf(name) }))
    .filter((asset) => asset.kind !== null)
    .map((asset) => ({
      ...asset,
      gzippedBytes: gzipSync(readFileSync(join(assetsDir, asset.name))).byteLength,
    }))
    .sort((left, right) => right.gzippedBytes - left.gzippedBytes);
}

function main() {
  const assetsDir = fileURLToPath(new URL("../dist/assets/", import.meta.url));
  const assets = measureAssets(assetsDir);
  const violations = evaluateBundle(assets, BUDGETS);

  for (const asset of assets) {
    const role = asset.kind === "js" ? chunkRole(asset.name, BUDGETS.perChunkBytes) : "css";
    const ceiling =
      asset.kind === "js" && role !== null ? BUDGETS.perChunkBytes[role] : BUDGETS.totalCssBytes;
    const share = ceiling > 0 ? Math.round((asset.gzippedBytes / ceiling) * 100) : 0;
    console.log(
      `${asset.name}: ${asset.gzippedBytes} B gzip (${share}% of the ${ceiling} B ${role ?? "unbudgeted"} ceiling)`,
    );
  }

  if (violations.length > 0) {
    for (const violation of violations) {
      console.error(`bundle budget: ${violation}`);
    }
    return 1;
  }

  const totalJs = assets
    .filter((asset) => asset.kind === "js")
    .reduce((sum, asset) => sum + asset.gzippedBytes, 0);
  console.log(
    `Bundle within budget: ${totalJs} B / ${BUDGETS.totalJsBytes} B gzip JS across ${assets.length} asset(s).`,
  );
  return 0;
}

// Only run the CLI when invoked directly, so the unit test can import the pure helpers.
// Compared as resolved paths rather than URLs: under vitest, process.argv[1] is the test
// runner's path, which is not a file URL and threw "The URL must be of scheme file".
const invokedDirectly =
  process.argv[1] !== undefined &&
  resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
if (invokedDirectly) {
  process.exit(main());
}
