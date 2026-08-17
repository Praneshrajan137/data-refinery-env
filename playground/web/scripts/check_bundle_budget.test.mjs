/**
 * Unit tests for the bundle budget gate.
 *
 * These exist because the gate they cover was, until this commit, incapable of failing:
 * `budgetKiB = Number.POSITIVE_INFINITY` made the one comparison in the file unreachable
 * in every build. The mutation harness could not have caught it -- mutants run before
 * Playwright builds `dist`, so the gate has no artifact to read at that point. So the
 * comparison was extracted into a pure function and is exercised here instead.
 *
 * The first test is the regression itself. The last test binds these assertions to the
 * SHIPPED budget table rather than to synthetic fixtures, because a gate proven correct
 * on invented numbers while the real config says Infinity is exactly the failure being
 * removed.
 */

import { describe, expect, it } from "vitest";

import {
  BUDGETS,
  assertBudgetsAreEnforceable,
  chunkRole,
  evaluateBundle,
} from "./check_bundle_budget.mjs";

const budgets = {
  perChunkBytes: { index: 1_000, vendor: 2_000 },
  totalJsBytes: 2_500,
  totalCssBytes: 500,
};

const js = (name, gzippedBytes) => ({ name, kind: "js", gzippedBytes });
const css = (name, gzippedBytes) => ({ name, kind: "css", gzippedBytes });

describe("assertBudgetsAreEnforceable", () => {
  it("rejects an infinite ceiling, which is how this gate was disabled", () => {
    const problems = assertBudgetsAreEnforceable({
      ...budgets,
      totalJsBytes: Number.POSITIVE_INFINITY,
    });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain("totalJsBytes");
    expect(problems[0]).toContain("finite");
  });

  it("rejects an infinite per-chunk ceiling", () => {
    const problems = assertBudgetsAreEnforceable({
      ...budgets,
      perChunkBytes: { index: Number.POSITIVE_INFINITY },
    });
    expect(problems.some((problem) => problem.includes("perChunkBytes.index"))).toBe(true);
  });

  it("rejects NaN, zero and negative ceilings", () => {
    for (const value of [Number.NaN, 0, -1, "1000", null, undefined]) {
      expect(assertBudgetsAreEnforceable({ ...budgets, totalJsBytes: value })).not.toHaveLength(0);
    }
  });

  it("rejects an empty chunk table, under which no asset would be budgeted at all", () => {
    const problems = assertBudgetsAreEnforceable({ ...budgets, perChunkBytes: {} });
    expect(problems.some((problem) => problem.includes("no chunk roles"))).toBe(true);
  });

  it("accepts a finite positive table", () => {
    expect(assertBudgetsAreEnforceable(budgets)).toEqual([]);
  });
});

describe("chunkRole", () => {
  it("strips the content hash Vite appends", () => {
    expect(chunkRole("index-CDpSX4T6.js", budgets.perChunkBytes)).toBe("index");
  });

  it("returns null for a chunk no ceiling covers", () => {
    expect(chunkRole("lazy-route-Abc123.js", budgets.perChunkBytes)).toBeNull();
  });

  it("prefers the longest matching role so a specific chunk is not absorbed by a general one", () => {
    const roles = { vendor: 1, "vendor-react": 2 };
    expect(chunkRole("vendor-react-Xy.js", roles)).toBe("vendor-react");
  });

  it("does not match a prefix that is not followed by a separator", () => {
    expect(chunkRole("indexes-Ab12.js", budgets.perChunkBytes)).toBeNull();
  });
});

describe("evaluateBundle", () => {
  it("passes a build inside every ceiling", () => {
    expect(
      evaluateBundle([js("index-a.js", 900), js("vendor-b.js", 1_500), css("s.css", 400)], budgets),
    ).toEqual([]);
  });

  it("fails a chunk over its own ceiling", () => {
    const violations = evaluateBundle([js("index-a.js", 1_001)], budgets);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("above the 1000 B ceiling");
  });

  it("fails on the total even when every individual chunk is within its ceiling", () => {
    const violations = evaluateBundle([js("index-a.js", 1_000), js("vendor-b.js", 2_000)], budgets);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("Total JS is 3000 B");
  });

  it("fails an unbudgeted chunk rather than letting it ship unmeasured", () => {
    const violations = evaluateBundle([js("index-a.js", 100), js("mystery-b.js", 100)], budgets);
    expect(violations.some((violation) => violation.includes("matches no budgeted chunk role"))).toBe(
      true,
    );
  });

  it("fails when there is no JS at all, so a broken build cannot read as a pass", () => {
    expect(evaluateBundle([css("s.css", 10)], budgets)).toContain(
      "No JavaScript assets found in dist/assets.",
    );
  });

  it("enforces the CSS total separately from JS", () => {
    const violations = evaluateBundle([js("index-a.js", 10), css("s.css", 501)], budgets);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("Total CSS is 501 B");
  });

  it("reports the configuration problem instead of the asset comparison when both are wrong", () => {
    const violations = evaluateBundle([js("index-a.js", 9_999)], {
      ...budgets,
      totalJsBytes: Number.POSITIVE_INFINITY,
    });
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("finite");
  });

  it("ignores sourcemaps, which are not served to users", () => {
    expect(
      evaluateBundle(
        [js("index-a.js", 900), { name: "index-a.js.map", kind: null, gzippedBytes: 999_999 }],
        budgets,
      ),
    ).toEqual([]);
  });
});

describe("the shipped budget table", () => {
  it("is enforceable, so the Infinity regression cannot return unnoticed", () => {
    expect(assertBudgetsAreEnforceable(BUDGETS)).toEqual([]);
  });

  it("budgets a role for every chunk the build actually emits", () => {
    for (const name of ["index-CDpSX4T6.js", "vendor-DlkJlkDf.js", "rolldown-runtime-Cyuzqnbw.js"]) {
      expect(chunkRole(name, BUDGETS.perChunkBytes)).not.toBeNull();
    }
  });

  it("keeps the total at or below the sum of its parts, so the total cannot be vacuous", () => {
    const sumOfChunks = Object.values(BUDGETS.perChunkBytes).reduce((sum, n) => sum + n, 0);
    expect(BUDGETS.totalJsBytes).toBeLessThanOrEqual(sumOfChunks);
  });
});
