/**
 * GENERATED FILE -- DO NOT EDIT BY HAND.
 *
 * Source:      dataforge/domain/vocabulary.py
 * Source hash: sha256:98acad9d128a1bac6c919a9953a0794e39c5c61cdeaa74e3f5489702b240c888
 * Generator:   scripts/ci/generate_domain_vocabulary.py
 * Verify:      python scripts/ci/generate_domain_vocabulary.py --check
 *              (or, without Python: npm run audit:vocabulary)
 *
 * These are the closed vocabularies the engine reasons about. They are generated
 * rather than transcribed because transcription failed three times: `entity_consensus`
 * went missing from the untrusted-provenance set (so an untrusted value reported as
 * `proven` in the one function every trust surface routes through), the review-reason
 * humanizer carried 12 of 13 reasons (so a held fix rendered as a raw machine token),
 * and the certificate verifier carried a three-member set against the engine's four.
 *
 * A constant in two places is a constant that will disagree.
 */

/** How strong the claim behind an applied value is. The product's core distinction. */
export type VerificationStrength =
  | "proven"
  | "plausibility_only";

/** Every strength, for runtime membership checks in a verifier. */
export const VERIFICATION_STRENGTHS: readonly VerificationStrength[] = ["proven", "plausibility_only"] as const;

/** Where a proposed value came from. */
export type Provenance =
  | "deterministic"
  | "llm_live"
  | "llm_cache"
  | "external"
  | "entity_consensus";

/** Every provenance, in declaration order. */
export const PROVENANCE_ORDER: readonly Provenance[] = ["deterministic", "llm_live", "llm_cache", "external", "entity_consensus"] as const;

/** Provenances correct by construction. Trust decisions read THIS set, never a denylist. */
export const TRUSTED_PROVENANCE: ReadonlySet<string> = new Set(["deterministic"]);

/** Provenances that cannot be proven without an authoritative schema. */
export const UNTRUSTED_PROVENANCE: ReadonlySet<string> = new Set(["entity_consensus", "external", "llm_cache", "llm_live"]);

/** Strict subset of untrusted that additionally needs a calibrated threshold. */
export const CALIBRATED_PROVENANCE: ReadonlySet<string> = new Set(["llm_cache", "llm_live"]);

/**
 * True only for a provenance known to be correct by construction.
 *
 * Written against the allowlist deliberately. Testing membership of an untrusted
 * denylist fails OPEN: a provenance added by a future corrector, a typo, or a missing
 * value would all read as trustworthy.
 */
export function isTrustedProvenance(provenance: string | null | undefined): boolean {
  if (provenance === null || provenance === undefined) {
    return false;
  }
  return TRUSTED_PROVENANCE.has(provenance);
}

/**
 * Derive how strong a claim is from where it came from.
 *
 * `authoritativeSchemaPresent` must be decided for the fix's OWN column. A table-level
 * boolean once granted authority over columns the schema never mentioned, which let a
 * garbage external value be applied and stamped `proven`.
 */
export function verificationStrengthFor(
  provenance: string | null | undefined,
  { authoritativeSchemaPresent }: { authoritativeSchemaPresent: boolean },
): VerificationStrength {
  if (isTrustedProvenance(provenance) || authoritativeSchemaPresent) {
    return "proven";
  }
  return "plausibility_only";
}

/** Why a proposal was not applied. The machine contract behind every held fix. */
export type ReviewReason =
  | "failed_conformal_threshold"
  | "safety_escalation"
  | "safety_denied"
  | "not_inferable_from_data"
  | "verifier_rejected"
  | "floor_cannot_verify"
  | "ambiguous_fd"
  | "out_of_inferred_domain"
  | "inferred_fd_not_declared"
  | "unverified_transposition"
  | "unverified_entity_consensus"
  | "stale_precondition"
  | "invalid_target";

/** The sentence a human reads for each review reason. Identical to the terminal's. */
export const REVIEW_REASON_HUMAN: Record<string, string> = {
  failed_conformal_threshold: "Confidence did not clear the distribution-free auto-apply threshold.",
  safety_escalation: "The safety constitution escalated this for human confirmation.",
  safety_denied: "The safety constitution denied this change.",
  not_inferable_from_data: "The correct value is not derivable from the data in the table.",
  verifier_rejected: "The independent verifier rejected this proposal.",
  floor_cannot_verify: "The deterministic verifier could not prove this change safe.",
  ambiguous_fd: "The functional dependency was ambiguous, so no single correct value could be derived.",
  out_of_inferred_domain: "The proposed value falls outside the values inferred from the column.",
  inferred_fd_not_declared: "The supporting dependency was inferred, not declared, so it is not auto-applied.",
  unverified_transposition: "A transposition was proposed but could not be proven.",
  unverified_entity_consensus: "Sibling rows for this entity agree on a different value, but agreement is evidence, not proof, so it is suggested rather than applied.",
  stale_precondition: "The row changed after the proposal, so it was not applied.",
  invalid_target: "The proposed value failed the target's constraints.",
};

/** Every review reason, for runtime membership checks in a verifier. */
export const REVIEW_REASONS: readonly ReviewReason[] = ["failed_conformal_threshold", "safety_escalation", "safety_denied", "not_inferable_from_data", "verifier_rejected", "floor_cannot_verify", "ambiguous_fd", "out_of_inferred_domain", "inferred_fd_not_declared", "unverified_transposition", "unverified_entity_consensus", "stale_precondition", "invalid_target"] as const;

/** Issue severity, ascending. */
export type Severity =
  | "safe"
  | "review"
  | "unsafe";

/** Ascending severity order. */
export const SEVERITY_ORDER: readonly Severity[] = ["safe", "review", "unsafe"] as const;

/** What the independent verifier decided. */
export type VerifierVerdict =
  | "accept"
  | "reject"
  | "unknown"
  | "not_run";

/** Every verifier verdict, for runtime membership checks. */
export const VERIFIER_VERDICTS: readonly VerifierVerdict[] = ["accept", "reject", "unknown", "not_run"] as const;

/** What the safety constitution decided. */
export type SafetyVerdict =
  | "allow"
  | "escalate"
  | "deny";

/** Every safety verdict, for runtime membership checks. */
export const SAFETY_VERDICTS: readonly SafetyVerdict[] = ["allow", "escalate", "deny"] as const;

/** The epistemic ladder, weakest to strongest. Perceptual intensity is monotonic in it. */
export type Rung =
  | "idle"
  | "rejected"
  | "plausibility_only"
  | "downgraded"
  | "held"
  | "proven"
  | "corroborated";

/** Weakest to strongest. Index order is meaningful and load-bearing. */
export const RUNG_ORDER: readonly Rung[] = ["idle", "rejected", "plausibility_only", "downgraded", "held", "proven", "corroborated"] as const;
