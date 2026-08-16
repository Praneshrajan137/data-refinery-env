/**
 * Tests for the proportion interval.
 *
 * The goldens are the SAME values asserted in `tests/unit/test_trust_ledger.py`, so the
 * TypeScript and Python implementations are checked against shared numbers rather than
 * against each other. That is the pattern already established for the attestation verifier:
 * two independent implementations agreeing on committed vectors is what makes a definition a
 * definition rather than a program.
 */

import { describe, expect, it } from "vitest";

import { proportionInterval, upperBound } from "./interval";

describe("upperBound", () => {
  it("matches the Python closed form for zero failures", () => {
    // 1 - 0.05 ** (1/10). Same golden as test_trust_ledger.py::test_zero_failures_closed_form.
    expect(upperBound(0, 10)).toBeCloseTo(0.2589, 3);
  });

  it("matches the Python bisection result for one failure in ten", () => {
    // Same golden as test_trust_ledger.py::test_known_value.
    expect(upperBound(1, 10)).toBeCloseTo(0.3941, 3);
  });

  it("claims nothing when there is no evidence", () => {
    // The agent_fix_count: 0 case. An empty denominator must not read as a clean result.
    expect(upperBound(0, 0)).toBe(1);
  });

  it("is certain only when everything failed", () => {
    expect(upperBound(10, 10)).toBe(1);
  });

  it("tightens monotonically as the sample grows", () => {
    const bounds = [10, 100, 1000, 10000].map((n) => upperBound(0, n));
    expect(bounds).toEqual([...bounds].sort((a, b) => b - a));
  });
});

describe("proportionInterval", () => {
  it("brackets the point estimate", () => {
    const interval = proportionInterval(30, 100);
    expect(interval.estimate).toBeCloseTo(0.3, 6);
    expect(interval.lower).toBeLessThan(interval.estimate);
    expect(interval.upper).toBeGreaterThan(interval.estimate);
  });

  it("gives the known Clopper-Pearson interval for 30 of 100", () => {
    // Standard published value: (0.2124, 0.3998).
    const interval = proportionInterval(30, 100);
    expect(interval.lower).toBeCloseTo(0.2124, 3);
    expect(interval.upper).toBeCloseTo(0.3998, 3);
  });

  it("gives the known interval for 0 of 10", () => {
    // Lower is exactly 0; upper is the two-sided 97.5% bound, 0.3085.
    const interval = proportionInterval(0, 10);
    expect(interval.estimate).toBe(0);
    expect(interval.lower).toBe(0);
    expect(interval.upper).toBeCloseTo(0.3085, 3);
  });

  it("gives the known interval for 10 of 10", () => {
    const interval = proportionInterval(10, 10);
    expect(interval.estimate).toBe(1);
    expect(interval.lower).toBeCloseTo(0.6915, 3);
    expect(interval.upper).toBe(1);
  });

  it("does not collapse to a point when the count is zero", () => {
    // The failure mode this whole module exists to prevent: an empty bin is not a
    // measurement of zero with no uncertainty.
    const interval = proportionInterval(0, 40);
    expect(interval.upper).toBeGreaterThan(0);
  });

  it("returns the whole range with no trials", () => {
    expect(proportionInterval(0, 0)).toEqual({ estimate: 0, lower: 0, upper: 1 });
  });

  it("survives the largest measured class without overflow", () => {
    // 10,261 of 10,373 cells share one confidence value on the measured hospital queue.
    // Naive binomial coefficients overflow a double well before this; log space does not.
    const interval = proportionInterval(10261, 10373);
    expect(Number.isFinite(interval.lower)).toBe(true);
    expect(Number.isFinite(interval.upper)).toBe(true);
    expect(interval.lower).toBeGreaterThan(0.98);
    expect(interval.upper).toBeLessThan(1);
    // A large sample buys a tight interval, which is the whole point of reporting one.
    expect(interval.upper - interval.lower).toBeLessThan(0.01);
  });

  it("gives a wide interval for a small class, so smallness is visible", () => {
    const small = proportionInterval(3, 5);
    const large = proportionInterval(6000, 10000);
    expect(small.upper - small.lower).toBeGreaterThan(large.upper - large.lower);
  });

  it("clamps a count above the total rather than returning nonsense", () => {
    const interval = proportionInterval(12, 10);
    expect(interval.estimate).toBe(1);
  });
});
