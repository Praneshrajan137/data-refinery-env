/**
 * Unit tests for the raw-token gate.
 *
 * The decisive tests are the four in "the defects this gate was written for": each fixture is
 * the ACTUAL code that shipped a machine token to users, taken from git history. A gate
 * verified only against invented examples proves nothing about the bug it claims to prevent --
 * that is how a check ends up passing while the defect it names is live.
 */

import { describe, expect, it } from "vitest";

import { humanisedFields, rawTokenRenders } from "./raw_tokens.mjs";

const FIELDS = [
  "independent_verification",
  "provenance",
  "review_reason",
  "safety_verdict",
  "verifier_verdict",
];

const found = (source) => rawTokenRenders(source, FIELDS).map((violation) => violation.field);

describe("humanisedFields", () => {
  it("derives the field list from the generated humaniser tables", () => {
    const generated = `
      export const VERIFIER_VERDICT_HUMAN: Record<string, string> = {};
      export const SAFETY_VERDICT_HUMAN: Record<string, string> = {};
      export const PROVENANCE_HUMAN: Record<string, string> = {};
    `;
    expect(humanisedFields(generated)).toEqual([
      "provenance",
      "safety_verdict",
      "verifier_verdict",
    ]);
  });

  it("returns nothing when there are no tables, so the caller can fail closed", () => {
    expect(humanisedFields("export const NOT_A_TABLE = {};")).toEqual([]);
  });
});

describe("the defects this gate was written for", () => {
  it("catches a verdict rendered through Metric's value prop", () => {
    expect(
      found('<Metric label="Verifier" value={analysis.receipt.verifier_verdict} />'),
    ).toEqual(["verifier_verdict"]);
  });

  it("catches two verdicts concatenated into a template literal", () => {
    expect(
      found(
        "detail: analysis ? `${analysis.receipt.safety_verdict} safety, " +
          "${analysis.receipt.verifier_verdict} verifier` : null,",
      ).sort(),
    ).toEqual(["safety_verdict", "verifier_verdict"]);
  });

  it("catches a provenance token standing as JSX text", () => {
    expect(
      found("<span>{candidate.detector_id} - {candidate.operation} - {candidate.provenance}</span>"),
    ).toEqual(["provenance"]);
  });

  it("catches a nullish-coalesced verdict in a value prop", () => {
    expect(found('value={analysis.receipt.independent_verification ?? "not_run"}')).toEqual([
      "independent_verification",
    ]);
  });
});

describe("shapes that read a token without printing it", () => {
  it("permits a humanised render", () => {
    expect(
      found('<Metric label="Verifier" value={humanizeVerifierVerdict(a.receipt.verifier_verdict)} />'),
    ).toEqual([]);
  });

  it("permits a React key, which nobody sees", () => {
    // A real false positive from the first draft of this rule.
    expect(found('key={`${item.row}:${item.review_reason ?? "held"}`}')).toEqual([]);
  });

  it("permits a prop passed to a component that only compares it", () => {
    // The other two false positives: CertificatePanel compares this to pick a chip.
    expect(found('independentVerification={result.receipt.independent_verification ?? "not_run"}')).toEqual(
      [],
    );
  });

  it("permits a comparison", () => {
    expect(found('tone: a.receipt.safety_verdict === "allow" ? "verified" : "danger",')).toEqual([]);
  });

  it("permits a token named in a comment", () => {
    expect(found("// value={analysis.receipt.verifier_verdict} was the old defect")).toEqual([]);
    expect(found("/* value={analysis.receipt.safety_verdict} */")).toEqual([]);
  });

  it("permits a type declaration", () => {
    expect(found("  independent_verification?: string;")).toEqual([]);
  });
});

describe("reporting", () => {
  it("reports each site once even when several patterns match it", () => {
    const violations = rawTokenRenders(
      "<span>{a.provenance}</span>",
      FIELDS,
    );
    expect(violations).toHaveLength(1);
  });

  it("reports the line number so the failure is actionable", () => {
    const violations = rawTokenRenders(
      ["const x = 1;", "", '<Metric value={a.receipt.verifier_verdict} />'].join("\n"),
      FIELDS,
    );
    expect(violations[0].line).toBe(3);
  });
});
