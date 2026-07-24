import { describe, expect, it } from "vitest";
import { strengthOf, humanizeReviewReason } from "./observatory";
import type { CandidateRepair } from "./types";

// The engine's ReviewReason literal, mirrored so we can assert full coverage.
const REVIEW_REASONS = [
  "failed_conformal_threshold",
  "safety_escalation",
  "not_inferable_from_data",
  "floor_cannot_verify",
  "ambiguous_fd",
  "out_of_inferred_domain",
  "unverified_transposition",
  "inferred_fd_not_declared",
  "stale_precondition",
  "invalid_target",
  "safety_denied",
  "verifier_rejected",
] as const;

function candidate(partial: Partial<CandidateRepair>): CandidateRepair {
  return {
    row: 0,
    column: "c",
    old_value: "a",
    new_value: "b",
    detector: "d",
    confidence: 0.9,
    provenance: "deterministic",
    ...partial,
  } as CandidateRepair;
}

describe("earned salience: overtrust is unrenderable", () => {
  it("never labels a plausibility-only value as proven", () => {
    expect(strengthOf(candidate({ verification_strength: "plausibility_only", provenance: "external" }))).toBe(
      "plausibility_only",
    );
    // Untrusted provenance with no explicit strength falls back to plausibility.
    expect(strengthOf(candidate({ provenance: "llm_live" }))).toBe("plausibility_only");
    expect(strengthOf(candidate({ provenance: "llm_cache" }))).toBe("plausibility_only");
    expect(strengthOf(candidate({ provenance: "external" }))).toBe("plausibility_only");
  });

  it("only labels deterministic / explicitly-proven values as proven", () => {
    expect(strengthOf(candidate({ provenance: "deterministic" }))).toBe("proven");
    expect(strengthOf(candidate({ verification_strength: "proven", provenance: "external" }))).toBe("proven");
  });
});

describe("review-reason humanizer (text twin)", () => {
  it("renders every review reason without leaking a raw token", () => {
    for (const reason of REVIEW_REASONS) {
      const rendered = humanizeReviewReason(reason);
      expect(rendered).not.toContain("_");
      expect(rendered.length).toBeGreaterThan(0);
    }
  });

  it("still renders an unknown reason readably and an empty reason honestly", () => {
    expect(humanizeReviewReason("brand_new_reason")).not.toContain("_");
    expect(humanizeReviewReason(null).toLowerCase()).toContain("review");
  });
});
