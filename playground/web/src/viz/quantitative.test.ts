import { describe, expect, it } from "vitest";
import type { AnalyzeResponse, FlaggedCells, VerifiedFix } from "../types";
import { buildEvidenceModel } from "./model";
import { buildDensityField, densityFieldViolations, summariseColumns } from "./density";
import { buildClaimSet, claimSetViolations, CLAIM_DISPLAY_LIMIT } from "./claims";
import { buildConfidenceDistribution } from "./confidence";
import {
  addressableMinHeightPx,
  aggregationRungAllowed,
  depthForAddressable,
  isProvenRung,
  maxDepthPx,
  measureRungSalience,
  rungOrder,
  rungSpecs,
  undrawableRungs,
} from "./grammar";

const COLUMNS = ["a", "b", "c", "d"];

/**
 * Build the payload the way the server does: the columnar index carries every
 * flagged position, and `cells` carries full records for a bounded prefix. Tests
 * must exercise the index, because that is the channel the map reads.
 */
function flaggedFrom(cells: FlaggedCells["cells"], columns: string[] = COLUMNS): FlaggedCells {
  return {
    index: {
      column_indices: cells.map((cell) => columns.indexOf(cell.column)),
      rows: cells.map((cell) => cell.row),
    },
    cells,
    confidence_histogram: [],
    total: cells.length,
    truncated: false,
    note: `All ${cells.length} flagged cells are located.`,
  };
}

function fix(partial: Partial<VerifiedFix>): VerifiedFix {
  return {
    row: 0,
    column: "a",
    old_value: "x",
    new_value: "y",
    detector_id: "decimal_shift",
    reason: "r",
    confidence: 0.9,
    provenance: "deterministic",
    ...partial,
  } as VerifiedFix;
}

function analysis(partial: {
  rows?: number;
  columns?: string[];
  repairs?: VerifiedFix[];
  suggested?: unknown[];
  failures?: unknown[];
  flagged?: unknown;
  issuesCount?: number;
  independent?: "agreed" | "not_run";
}): AnalyzeResponse {
  return {
    source: {
      name: "t.csv",
      size_bytes: 1,
      sha256: "0".repeat(64),
      rows: partial.rows ?? 100,
      columns: (partial.columns ?? COLUMNS).length,
      column_names: partial.columns ?? COLUMNS,
    },
    issues: [],
    flagged_cells: partial.flagged ?? {
      index: { column_indices: [], rows: [] }, confidence_histogram: [], cells: [],
      total: partial.issuesCount ?? 0,
      truncated: false,
      note: "n",
    },
    repairs: partial.repairs ?? [],
    verification: {
      safety_verdict: "allow",
      verifier_verdict: "accept",
      accepted_constraint_ids: [],
      failures: partial.failures ?? [],
      abstentions: [],
      failure_reasons: [],
    },
    receipt: {
      root_causes: [],
      suggested_fixes: partial.suggested ?? [],
      issues_count: partial.issuesCount ?? 0,
      independent_verification: partial.independent ?? "not_run",
    },
  } as unknown as AnalyzeResponse;
}

// --- L2: the addressability law ----------------------------------------------

describe("L2: aggregated marks carry no rung", () => {
  it("declares rungs forbidden on aggregated marks", () => {
    expect(aggregationRungAllowed).toBe(false);
  });

  it("emits density marks with no rung field at all", () => {
    const cells = Array.from({ length: 60 }, (_, i) => ({
      row: i,
      column: COLUMNS[i % COLUMNS.length],
      issue_type: "fd_violation",
      severity: "unsafe" as const,
      confidence: 0.95,
      actual: "x",
      expected: null,
      reason: "r",
    }));
    const model = buildEvidenceModel(
      analysis({
        rows: 60,
        // One proven fix among many detections: under the old min-rung rule this bin
        // would have been dragged to `held`; under max-rung it would have been
        // promoted to `proven`. Now the band reports neither, only a count.
        repairs: [fix({ row: 0, column: "a" })],
        flagged: flaggedFrom(cells),
        issuesCount: 60,
      }),
    );
    const field = buildDensityField(model, { widthPx: 400, heightPx: 12 });

    expect(field.marks.length).toBeGreaterThan(0);
    for (const mark of field.marks) {
      expect(Object.keys(mark)).not.toContain("rung");
      expect(Object.keys(mark)).not.toContain("depthPx");
      expect(Object.keys(mark)).not.toContain("mixed");
    }
    expect(densityFieldViolations(field)).toEqual([]);
  });

  it("reports count as data without letting it become a rung", () => {
    const cells = Array.from({ length: 40 }, (_, i) => ({
      row: i,
      column: "a",
      issue_type: "outlier",
      severity: "review" as const,
      confidence: 0.5,
      actual: "x",
      expected: null,
      reason: "r",
    }));
    const model = buildEvidenceModel(
      analysis({
        rows: 40,
        flagged: flaggedFrom(cells),
        issuesCount: 40,
      }),
    );
    const field = buildDensityField(model, { widthPx: 400, heightPx: 3 });
    const mark = field.marks.find((m) => m.column === "a")!;
    expect(mark.count).toBe(40);
    expect(field.maxCount).toBe(40);
  });

  it("emits no overlapping marks on a dense queue", () => {
    const cells = Array.from({ length: 5000 }, (_, i) => ({
      row: i % 1000,
      column: COLUMNS[i % COLUMNS.length],
      issue_type: "fd_violation",
      severity: "unsafe" as const,
      confidence: 0.95,
      actual: "x",
      expected: null,
      reason: "r",
    }));
    const model = buildEvidenceModel(
      analysis({
        rows: 1000,
        flagged: flaggedFrom(cells),
        issuesCount: 5000,
      }),
    );
    const field = buildDensityField(model, { widthPx: 800, heightPx: 420 });
    expect(densityFieldViolations(field)).toEqual([]);
    expect(field.bandCount).toBe(140);
  });

  it("summarises columns with exact counts for the accessible twin", () => {
    const cells = [0, 1, 2].map((row) => ({
      row,
      column: "b",
      issue_type: "outlier",
      severity: "review" as const,
      confidence: 0.4,
      actual: "x",
      expected: null,
      reason: "r",
    }));
    const model = buildEvidenceModel(
      analysis({
        rows: 100,
        flagged: flaggedFrom(cells),
        issuesCount: 3,
      }),
    );
    const field = buildDensityField(model, { widthPx: 400, heightPx: 200 });
    const summary = summariseColumns(field, model);
    expect(summary).toHaveLength(1);
    expect(summary[0].column).toBe("b");
    expect(summary[0].cellCount).toBe(3);
    expect(summary[0].bandsTotal).toBe(field.bandCount);
  });
});

// --- L5: depth, now conditional on addressability ----------------------------

describe("L5: depth renders only on addressable marks", () => {
  it("returns zero depth below the addressability floor", () => {
    // This is the defect that made the previous version dead code: real bands are
    // 3.0px tall, so nothing could ever show depth.
    expect(depthForAddressable("plausibility_only", 3)).toBe(0);
    expect(depthForAddressable("plausibility_only", addressableMinHeightPx)).toBe(
      rungSpecs.plausibility_only.depthPx,
    );
  });

  it("keeps the addressability floor high enough to perceive", () => {
    expect(addressableMinHeightPx).toBeGreaterThanOrEqual(8);
  });

  it("measures strictly monotonic salience across the ladder", () => {
    const totals = rungOrder.map((rung) => measureRungSalience(rung).total);
    for (let i = 1; i < totals.length; i += 1) {
      expect(totals[i]).toBeGreaterThan(totals[i - 1]);
    }
  });

  it("keeps a floating plausibility mark below a landed proven mark", () => {
    expect(rungSpecs.plausibility_only.depthPx).toBeGreaterThan(rungSpecs.proven.depthPx);
    expect(measureRungSalience("plausibility_only").total).toBeLessThan(
      measureRungSalience("proven").total,
    );
    expect(measureRungSalience("plausibility_only").ink).toBeLessThan(
      measureRungSalience("proven").ink,
    );
  });

  it("reserves ground contact and glow for proof, and bounds every offset", () => {
    for (const rung of rungOrder) {
      const spec = rungSpecs[rung];
      expect(Math.abs(spec.depthPx)).toBeLessThanOrEqual(maxDepthPx);
      if (!isProvenRung(rung)) {
        expect(spec.groundContact).not.toBe("contact");
        expect(spec.glowEligible).toBe(false);
      } else {
        expect(spec.groundContact).toBe("contact");
      }
    }
  });

  it("never draws an idle mark", () => {
    expect(undrawableRungs.has("idle")).toBe(true);
  });
});

// --- Claims: the addressable half --------------------------------------------

describe("claims: one object per claim, no aggregation", () => {
  it("carries the rung and orders by descending strength", () => {
    const model = buildEvidenceModel(
      analysis({
        rows: 50,
        repairs: [
          fix({ row: 10, column: "a", provenance: "deterministic" }),
          fix({ row: 11, column: "a", provenance: "llm_live" }),
        ],
        issuesCount: 2,
      }),
    );
    const set = buildClaimSet(model, { column: "a" });
    expect(set.claims.map((c) => c.rung)).toEqual(["proven", "plausibility_only"]);
    expect(claimSetViolations(set)).toEqual([]);
  });

  it("filters by column and row range", () => {
    const model = buildEvidenceModel(
      analysis({
        rows: 50,
        repairs: [
          fix({ row: 5, column: "a" }),
          fix({ row: 40, column: "a" }),
          fix({ row: 5, column: "b" }),
        ],
        issuesCount: 3,
      }),
    );
    expect(buildClaimSet(model, { column: "a" }).claims).toHaveLength(2);
    expect(
      buildClaimSet(model, { column: "a", rowStart: 0, rowEnd: 10 }).claims,
    ).toHaveLength(1);
  });

  it("bounds the set and reports the remainder rather than hiding it", () => {
    const repairs = Array.from({ length: CLAIM_DISPLAY_LIMIT + 25 }, (_, i) =>
      fix({ row: i, column: "a" }),
    );
    const model = buildEvidenceModel(
      analysis({ rows: 400, repairs, issuesCount: repairs.length }),
    );
    const set = buildClaimSet(model, { column: "a" });
    expect(set.claims).toHaveLength(CLAIM_DISPLAY_LIMIT);
    expect(set.matched).toBe(CLAIM_DISPLAY_LIMIT + 25);
    expect(set.truncated).toBe(true);
  });

  it("catches a claim whose rung contradicts its provenance", () => {
    const model = buildEvidenceModel(
      analysis({
        rows: 10,
        repairs: [fix({ row: 0, column: "a", provenance: "llm_live" })],
        issuesCount: 1,
      }),
    );
    const set = buildClaimSet(model, null);
    expect(set.claims[0].rung).toBe("plausibility_only");
    expect(set.claims[0].provenance).toBe("llm_live");
    expect(claimSetViolations(set)).toEqual([]);

    // Promote it the way a regression would, and assert the guard fires. This is the
    // assertion that makes the guard non-tautological: it re-derives strength from
    // provenance rather than from the rung it is checking.
    const promoted = {
      ...set,
      claims: [{ ...set.claims[0], rung: "proven" as const }],
    };
    const problems = claimSetViolations(promoted);
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain("untrusted provenance 'llm_live'");
    expect(problems[0]).toContain("proven");
  });

  it("catches entity_consensus promoted to proven", () => {
    const model = buildEvidenceModel(
      analysis({
        rows: 10,
        repairs: [fix({ row: 2, column: "a", provenance: "entity_consensus" })],
        issuesCount: 1,
      }),
    );
    const set = buildClaimSet(model, null);
    const promoted = { ...set, claims: [{ ...set.claims[0], rung: "corroborated" as const }] };
    expect(claimSetViolations(promoted)[0]).toContain("entity_consensus");
  });

  it("accepts a deterministic claim labelled proven", () => {
    const model = buildEvidenceModel(
      analysis({
        rows: 10,
        repairs: [fix({ row: 0, column: "a", provenance: "deterministic" })],
        issuesCount: 1,
      }),
    );
    const set = buildClaimSet(model, null);
    expect(set.claims[0].rung).toBe("proven");
    expect(claimSetViolations(set)).toEqual([]);
  });

  it("distinguishes zero from not-measured in a selection", () => {
    const empty = buildEvidenceModel(analysis({ rows: 100, issuesCount: 0 }));
    expect(buildClaimSet(empty, { column: "a" }).absence).toBe("zero");
    const unmeasured = buildEvidenceModel(analysis({ rows: 0, columns: [], issuesCount: 0 }));
    expect(buildClaimSet(unmeasured, null).absence).toBe("not_measured");
  });
});

// --- Confidence: what the signal cannot do -----------------------------------

describe("confidence distribution states its own limits", () => {
  function histogram(
    issueType: string,
    modeShare: number,
    count: number,
    distinct: number,
  ): FlaggedCells {
    return {
      index: { column_indices: [], rows: [] },
      cells: [],
      confidence_histogram: [
        {
          issue_type: issueType,
          bins: Array.from({ length: 10 }, (_, i) => ({
            from_value: i / 10,
            to_value: (i + 1) / 10,
            count: i === 9 ? Math.round(count * modeShare) : 0,
          })),
          count,
          distinct_values: distinct,
          mode_value: 0.95,
          mode_share: modeShare,
        },
      ],
      total: count,
      truncated: false,
      note: "n",
    };
  }

  it("flags a near-degenerate class", () => {
    // Mirrors the measured hospital FD regime: almost everything at one value.
    const dist = buildConfidenceDistribution(histogram("fd_violation", 0.97, 10373, 3));
    const entry = dist.classes[0];
    expect(entry.degenerate).toBe(true);
    expect(entry.modeValue).toBe(0.95);
    expect(entry.modeShare).toBeCloseTo(0.97, 5);
    expect(dist.finding).toContain("near-degenerate");
  });

  it("does not flag a genuinely spread class", () => {
    const dist = buildConfidenceDistribution(histogram("outlier", 0.2, 100, 40));
    expect(dist.classes[0].degenerate).toBe(false);
    expect(dist.finding).not.toContain("near-degenerate");
  });

  it("reports the population size, not the bounded detail prefix", () => {
    // The load-bearing guard against a biased distribution: `cells` is a
    // severity-ordered prefix, so it must never be the basis for these statistics.
    const payload = histogram("fd_violation", 0.97, 10373, 3);
    payload.cells = Array.from({ length: 500 }, (_, i) => ({
      row: i,
      column: "a",
      issue_type: "fd_violation",
      severity: "unsafe" as const,
      confidence: 0.95,
      actual: "x",
      expected: null,
      reason: "r",
    }));
    payload.truncated = true;
    const dist = buildConfidenceDistribution(payload);
    expect(dist.totalCells).toBe(10373);
    expect(dist.totalCells).not.toBe(payload.cells.length);
  });

  it("does not silently report zero when a populated run omits the histogram", () => {
    const dist = buildConfidenceDistribution({
      index: { column_indices: [], rows: [] },
      cells: [],
      confidence_histogram: [],
      total: 4200,
      truncated: true,
      note: "n",
    });
    expect(dist.absence).toBe("not_measured");
  });

  it("separates not-measured from zero", () => {
    expect(buildConfidenceDistribution(null).absence).toBe("not_measured");
    expect(
      buildConfidenceDistribution({ index: { column_indices: [], rows: [] }, confidence_histogram: [], cells: [], total: 0, truncated: false, note: "n" }).absence,
    ).toBe("zero");
  });
});
