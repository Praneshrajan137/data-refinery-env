import quantitativeTokens from "../design/quantitative-tokens.json";

/**
 * Typed derivation of the quantitative grammar.
 *
 * The single source is src/design/quantitative-tokens.json, which
 * scripts/audit_quantitative.mjs also reads. Nothing here may hand-duplicate a
 * value from that file, for the same reason the motion system stopped
 * hand-syncing its durations: a constant in two places is a constant that will
 * disagree.
 *
 * `Rung` obeys that rule now. It used to be a hand-written seven-member union
 * immediately below this comment -- a duplicate of the `rungs` keys in the JSON, in the
 * very file forbidding duplicates, and invisible to the audit because the audit only
 * compared `rungOrder` against `rungs` INSIDE the JSON. The ladder is now generated
 * from dataforge/domain/vocabulary.py, so the engine's strengths and the presentation
 * ladder cannot drift apart either.
 *
 * See docs/design/quantitative-grammar.md.
 */

import type { Rung } from "../domain/vocabulary.generated";

export type { Rung };

export type AbsenceState = "zero" | "not_measured" | "truncated";
export type GroundContact = "contact" | "weakening" | "none";
export type FillStyle = "filled" | "unfilled";
/**
 * `witnessed` is solid plus a witness rail, and it exists because of a measurement: the
 * identity law found `proven` and `corroborated` colour-collapsed at OKLab distance 0.0000
 * under normal vision in both themes, sharing the token `success-30`, while the grammar
 * declared both as `filled|solid|contact`. The rail was already rendering; the grammar was
 * not describing it. See specs/SPEC_perceptual_verification.md.
 */
export type StrokeStyle = "solid" | "dashed" | "struck" | "none" | "witnessed";

export interface RungSpec {
  strength: number;
  depthPx: number;
  groundContact: GroundContact;
  fill: FillStyle;
  stroke: StrokeStyle;
  glowEligible: boolean;
  witnessAccent: boolean;
  tokenFamily: string;
  text: string;
}

export const rungSpecs = quantitativeTokens.rungs as Record<Rung, RungSpec>;

/** Weakest to strongest by warrant to write. */
export const rungOrder = quantitativeTokens.rungOrder as readonly Rung[];

export const maxDepthPx: number = quantitativeTokens.maxDepthPx;
export const minMarkHeightPx: number = quantitativeTokens.minMarkHeightPx;
/**
 * A mark may carry a rung only if it stands for exactly one claim AND is at least
 * this tall (L2). The previous design used a `depthLegibleMinPx` of 8 to SUPPRESS
 * depth on 3px aggregated marks, which made depth unreachable on every real
 * dataset. Suppression was the wrong fix; not putting rungs on aggregates is the
 * right one.
 */
export const addressableMinHeightPx: number = quantitativeTokens.addressableMinHeightPx;
export const absenceStates = quantitativeTokens.absenceStates as readonly AbsenceState[];

/**
 * Depth for an addressable mark. Returns 0 for anything shorter than the
 * addressability floor, so a caller that renders depth on a density band gets
 * nothing rather than a misleading offset.
 */
export function depthForAddressable(rung: Rung, markHeightPx: number): number {
  return markHeightPx >= addressableMinHeightPx ? rungSpecs[rung].depthPx : 0;
}

/** Rungs that may never be drawn. `idle` is not a mark; it is an absence. */
export const undrawableRungs = new Set<Rung>(
  quantitativeTokens.undrawableRungs as Rung[],
);

export const aggregationRungAllowed: boolean =
  quantitativeTokens.aggregation.rungOnAggregatedMarks;

export function rungStrength(rung: Rung): number {
  return rungSpecs[rung].strength;
}

export function depthFor(rung: Rung): number {
  return rungSpecs[rung].depthPx;
}

export function isProvenRung(rung: Rung): boolean {
  return rung === "proven" || rung === "corroborated";
}

export interface SalienceMeasure {
  fill: number;
  stroke: number;
  contact: number;
  glow: number;
  accent: number;
  /** Fill + stroke: the fraction of the mark covered in ink. */
  ink: number;
  total: number;
}

const weights = quantitativeTokens.salienceWeights;

/**
 * Measure a rung's rendered salience so ladder monotonicity can be ASSERTED
 * rather than claimed (perceptual-language section 7.2, extended to depth).
 *
 * This exists because of one specific objection to earned depth: a floating mark
 * might read as more prominent than a landed one, which would invert the one
 * law. The structural answer is that floating marks are unfilled dashed
 * hairlines with no contact shadow, so they carry strictly less ink. This
 * function makes that answer checkable.
 */
export function measureRungSalience(rung: Rung): SalienceMeasure {
  const spec = rungSpecs[rung];
  const fill = weights.fill[spec.fill];
  const stroke = weights.stroke[spec.stroke];
  const contact = weights.contact[spec.groundContact];
  const glow = spec.glowEligible ? weights.glow.true : weights.glow.false;
  const accent = spec.witnessAccent ? weights.accent.true : weights.accent.false;
  return {
    fill,
    stroke,
    contact,
    glow,
    accent,
    ink: fill + stroke,
    total: fill + stroke + contact + glow + accent,
  };
}
