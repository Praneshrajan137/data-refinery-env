"""The ubiquitous language of DataForge, defined exactly once.

Every closed vocabulary the product reasons about lives here: how strong a claim is,
where a value came from, why something was held, how severe an issue is, and what a
verifier decided. Other modules re-export these names; nothing re-declares them.

Why this module exists
----------------------
The same six vocabularies were previously written out by hand in three to five
places -- the engine, the terminal humanizer, the HTTP contract, the browser, and the
design tokens -- with prose comments asserting they were "kept in sync". They were
not. Three drifts shipped:

1. ``entity_consensus`` was missing from the browser's untrusted-provenance set, so
   ``strengthOf()`` reported an untrusted value as ``proven`` in the one function
   every trust surface routes through.
2. ``REVIEW_REASON_COPY`` carried 12 of 13 reasons, so one held fix rendered as a raw
   machine token.
3. ``dataforge/certificate.py`` carried a three-member untrusted set against the
   engine's four -- in the artifact a third party reads to decide whether to trust a
   write. That one said "proven" about a value nothing had proven.

A constant in two places is a constant that will disagree. The product's claim is
truthfulness, so a vocabulary drift here is not a style problem; it is the failure
mode itself.

Two design rules
----------------
**This module is a leaf.** It imports nothing but the standard library -- no pydantic,
no engine, no rich. That purity is load-bearing: it lets the dependency-free
certificate verifier import the real vocabulary instead of copying it, and it keeps
this file readable as the source text a second implementation is written from.

**Trust decisions read the TRUSTED set, never the untrusted one.** Testing membership
of a known-bad list fails open: any value nobody thought of is treated as safe. Every
predicate here is written against the allowlist, so an unrecognised provenance is
untrusted by construction. That is the difference between a vocabulary and a guess.
"""

from __future__ import annotations

from typing import Final, Literal

__all__ = [
    "ALL_PROVENANCE",
    "CALIBRATED_PROVENANCE",
    "CONSTRAINT_CHECKABLE_DETECTORS",
    "INDEPENDENT_VERIFICATION_HUMAN",
    "PROVENANCE_HUMAN",
    "PROVENANCE_ORDER",
    "REVIEW_REASONS",
    "REVIEW_REASON_HUMAN",
    "RUNG_ORDER",
    "SAFETY_VERDICTS",
    "SAFETY_VERDICT_HUMAN",
    "SEVERITIES",
    "SEVERITY_ORDER",
    "TRUSTED_PROVENANCE",
    "UNTRUSTED_PROVENANCE",
    "VERIFICATION_STRENGTHS",
    "VERIFIER_VERDICTS",
    "VERIFIER_VERDICT_HUMAN",
    "Provenance",
    "ReviewReason",
    "Rung",
    "SafetyVerdict",
    "Severity",
    "VerificationStrength",
    "VerifierVerdict",
    "is_trusted_provenance",
    "rung_for",
    "verification_strength_for",
]


# --- Verification strength ----------------------------------------------------
# How strong the claim behind an applied value is. This is the product's core
# distinction and must never be blurred: `proven` may be written automatically,
# `plausibility_only` may not (absent an explicit, recorded opt-in).

VerificationStrength = Literal["proven", "plausibility_only"]

VERIFICATION_STRENGTHS: Final[tuple[VerificationStrength, ...]] = (
    "proven",
    "plausibility_only",
)


# --- Provenance ---------------------------------------------------------------
# Where a proposed value came from.
#
# **`deterministic` means the PROCEDURE is deterministic, not that the INFERENCE is
# sound.** An earlier version of this comment read "`deterministic` is correct by
# construction -- derived from the table by a rule that cannot invent a value". That
# was false, and the falsehood was load-bearing: it justified letting deterministic
# fixes bypass the calibration gate entirely.
#
# Measured 2026-08-22. The `decimal_shift` rule is deterministic and infers a repair
# from the column's own distribution. It scored precision **0.0000** on hospital
# (39 flags), flights (92) and rayyan (112) -- zero true errors found on any of them --
# and on error-free TPC-H it would rewrite **263,428** monetary values, plus 9.86% of
# `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.total_elapsed_time`. A live CLI run rewrote a
# legitimate `1131.20` as `113120` and recorded it as `proven`.
#
# A rule that cannot invent a value can still deterministically choose the wrong one.
# Trust is earned by measurement, not conferred by implementation category -- which is
# what `PRODUCT.md` already says, and the deterministic path was the one place it was
# not applied. See `CONSTRAINT_CHECKABLE_DETECTORS` below, and
# `docs/trust/deterministic-is-not-sound.md`.
#
# Everything other than `deterministic` is a proposal from a source that can be
# confidently wrong, including `entity_consensus`: sibling-row agreement is evidence,
# not proof, because the dirty-data majority is sometimes itself the error.

Provenance = Literal[
    "deterministic",
    "llm_live",
    "llm_cache",
    "external",
    "entity_consensus",
]

TRUSTED_PROVENANCE: Final[frozenset[str]] = frozenset({"deterministic"})

UNTRUSTED_PROVENANCE: Final[frozenset[str]] = frozenset(
    {
        "llm_live",
        "llm_cache",
        "external",
        "entity_consensus",
    }
)

ALL_PROVENANCE: Final[frozenset[str]] = TRUSTED_PROVENANCE | UNTRUSTED_PROVENANCE

# --- Which deterministic repairs may bypass the calibration gate --------------
# A deterministic fix historically auto-applied unconditionally: `partition_auto_apply`
# read `if deterministic or policy.action_for(...)`, so `enabled_classes == []` gave no
# protection at all. That is the hole `decimal_shift` fell through.
#
# Membership here is a claim that the repair is **checkable against a reference** --
# another row sharing a determinant, a declared type, a functional dependency -- rather
# than **inferred from the shape of the column's own distribution**. A distributional
# inference has an error rate, so it must clear a calibrated threshold like any other
# fallible source.
#
# **This is an allowlist, and that is deliberate.** Per this module's contract, a
# denylist of known-bad detectors fails open: any detector nobody thought of would
# bypass calibration by default. A new detector is calibration-bound until it earns
# an entry here with a committed measurement.
#
# `decimal_shift` is excluded by measurement, not by taste: precision 0.0000 on all
# three benchmark datasets and 263,428 false rewrites on a table with no errors in it.
# A schema does not rescue it either -- `113120` satisfies every plausible numeric
# constraint on a price column, so the premise cannot catch this class of wrongness.
CONSTRAINT_CHECKABLE_DETECTORS: Final[frozenset[str]] = frozenset(
    {
        "type_mismatch",
        "fd_violation",
        "missing_value",
    }
)

# A strict subset of the untrusted set: values that additionally require a calibrated
# per-class threshold before they may be written. `external` and `entity_consensus` are
# untrusted but carry no calibration map -- an external fix proven against an
# authoritative schema auto-applies directly -- so they are deliberately excluded.
#
# Note for readers of the browser code: the frontend constant historically called
# `LLM_PROVENANCE` mirrors UNTRUSTED_PROVENANCE, not this set. The generated
# TypeScript uses the accurate name, because two different sets sharing one name is
# how the `entity_consensus` drift went unnoticed.
CALIBRATED_PROVENANCE: Final[frozenset[str]] = frozenset({"llm_live", "llm_cache"})

# Structural invariants, asserted at import so a future edit cannot quietly break the
# relationships the engine depends on.
assert CALIBRATED_PROVENANCE < UNTRUSTED_PROVENANCE, (
    "calibrated provenances must be a strict subset of the untrusted ones"
)
assert not (TRUSTED_PROVENANCE & UNTRUSTED_PROVENANCE), (
    "a provenance cannot be both trusted and untrusted"
)

# Declaration order, for generated artifacts that need a stable sequence.
PROVENANCE_ORDER: Final[tuple[Provenance, ...]] = (
    "deterministic",
    "llm_live",
    "llm_cache",
    "external",
    "entity_consensus",
)


def is_trusted_provenance(provenance: str | None) -> bool:
    """Return True only for a provenance known to be correct by construction.

    Written against the allowlist on purpose. The equivalent
    ``provenance not in UNTRUSTED_PROVENANCE`` fails open: a provenance added by a
    future corrector, a typo, or ``None`` would all read as trustworthy. Here they
    read as untrusted, which at worst holds a good fix for review and at best refuses
    to write a bad one.
    """
    if provenance is None:
        return False
    return provenance in TRUSTED_PROVENANCE


def verification_strength_for(
    provenance: str | None,
    *,
    authoritative_schema_present: bool,
) -> VerificationStrength:
    """Derive how strong a claim is from where it came from.

    An untrusted value becomes ``proven`` only when an authoritative schema exists to
    verify it against -- the proof comes from the schema, not from the proposer's
    confidence. Strength is always *computed* here rather than read from a field a
    caller could set, because the recorded field is stamped late, is frequently
    absent, and would be spoofable.
    """
    if is_trusted_provenance(provenance) or authoritative_schema_present:
        return "proven"
    return "plausibility_only"


# --- Review reasons -----------------------------------------------------------
# Why a proposal was not applied. The token is the machine contract; the sentence is
# what a human reads. They live together so a new reason cannot ship without its
# phrasing -- the drift that previously let a raw token reach a user.

ReviewReason = Literal[
    "failed_conformal_threshold",
    "safety_escalation",
    "safety_denied",
    "not_inferable_from_data",
    "verifier_rejected",
    "floor_cannot_verify",
    "ambiguous_fd",
    "out_of_inferred_domain",
    "inferred_fd_not_declared",
    "unverified_transposition",
    "unverified_entity_consensus",
    "stale_precondition",
    "invalid_target",
]

REVIEW_REASON_HUMAN: Final[dict[str, str]] = {
    "failed_conformal_threshold": (
        "Confidence did not clear the distribution-free auto-apply threshold."
    ),
    "safety_escalation": "The safety constitution escalated this for human confirmation.",
    "safety_denied": "The safety constitution denied this change.",
    "not_inferable_from_data": "The correct value is not derivable from the data in the table.",
    "verifier_rejected": "The independent verifier rejected this proposal.",
    "floor_cannot_verify": "The deterministic verifier could not prove this change safe.",
    "ambiguous_fd": (
        "The functional dependency was ambiguous, so no single correct value could be derived."
    ),
    "out_of_inferred_domain": (
        "The proposed value falls outside the values inferred from the column."
    ),
    "inferred_fd_not_declared": (
        "The supporting dependency was inferred, not declared, so it is not auto-applied."
    ),
    "unverified_transposition": "A transposition was proposed but could not be proven.",
    "unverified_entity_consensus": (
        "Sibling rows for this entity agree on a different value, but agreement is "
        "evidence, not proof, so it is suggested rather than applied."
    ),
    "stale_precondition": "The row changed after the proposal, so it was not applied.",
    "invalid_target": "The proposed value failed the target's constraints.",
}

REVIEW_REASONS: Final[tuple[ReviewReason, ...]] = (
    "failed_conformal_threshold",
    "safety_escalation",
    "safety_denied",
    "not_inferable_from_data",
    "verifier_rejected",
    "floor_cannot_verify",
    "ambiguous_fd",
    "out_of_inferred_domain",
    "inferred_fd_not_declared",
    "unverified_transposition",
    "unverified_entity_consensus",
    "stale_precondition",
    "invalid_target",
)


# --- Severity -----------------------------------------------------------------

Severity = Literal["safe", "review", "unsafe"]

# Ascending severity, so index order is meaningful.
SEVERITY_ORDER: Final[tuple[Severity, ...]] = ("safe", "review", "unsafe")

SEVERITIES: Final[frozenset[str]] = frozenset(SEVERITY_ORDER)


# --- Verdicts -----------------------------------------------------------------
# Two independent gates, each with its own vocabulary. They are deliberately not
# merged: "the verifier could not decide" and "the constitution refused" are
# different facts and a user is owed the difference.

VerifierVerdict = Literal["accept", "reject", "unknown", "not_run"]

VERIFIER_VERDICTS: Final[tuple[VerifierVerdict, ...]] = (
    "accept",
    "reject",
    "unknown",
    "not_run",
)

SafetyVerdict = Literal["allow", "escalate", "deny"]

SAFETY_VERDICTS: Final[tuple[SafetyVerdict, ...]] = ("allow", "escalate", "deny")

# The sentence a human reads for each verdict.
#
# These live here, beside the enums, for the same reason REVIEW_REASON_HUMAN does:
# the browser was rendering the raw tokens. `Metric label="Verifier"` printed
# "not_run", the safety metric printed "escalate", and one loop-rail line
# concatenated both into "allow safety, accept verifier" -- machine values shown
# to a user as though they were prose. Adding a second table in TypeScript would
# have been the sixth instance of the vocabulary-drift class this repo has already
# been bitten by, so the strings are defined once and generated into the frontend,
# where audit_vocabulary.mjs pins them by hash.
#
# Phrased as short noun phrases rather than sentences because they render inside a
# metric tile, next to a label, not in running text.

VERIFIER_VERDICT_HUMAN: Final[dict[str, str]] = {
    "accept": "proved safe",
    "reject": "proved unsafe",
    "unknown": "could not decide",
    "not_run": "not required",
}

SAFETY_VERDICT_HUMAN: Final[dict[str, str]] = {
    "allow": "allowed",
    "escalate": "needs confirmation",
    "deny": "refused",
}

# What the second, independently written verifier did. "not_run" is not a failure:
# the deterministic gate already proved the applied set, so a cross-check was not
# required. Saying "not run" alone reads as an omission, which is why the UI says
# "single verifier" instead.
INDEPENDENT_VERIFICATION_HUMAN: Final[dict[str, str]] = {
    "agreed": "independently verified",
    "disagreed": "second verifier disagreed",
    "not_run": "single verifier",
}

# Where a proposed value came from. Provenance is the difference between a value
# that was derived and one that was guessed, so it must not surface as an
# implementation token like "llm_cache".
PROVENANCE_HUMAN: Final[dict[str, str]] = {
    "deterministic": "derived by rule",
    "llm_live": "model proposal",
    "llm_cache": "model proposal (cached)",
    "entity_consensus": "sibling rows agree",
    "external": "supplied by caller",
    "external_untrusted": "supplied by caller, untrusted",
    "external_llm": "model proposal via caller",
}


# --- The epistemic rung ladder ------------------------------------------------
# The presentation vocabulary: how strong a claim looks. Ordered weakest to
# strongest, because the perceptual language's one law is that intensity is
# monotonic in epistemic strength. Defined here rather than in the browser so the
# ladder and the engine's strengths cannot drift apart.

Rung = Literal[
    "idle",
    "rejected",
    "plausibility_only",
    "downgraded",
    "held",
    "proven",
    "corroborated",
]

RUNG_ORDER: Final[tuple[Rung, ...]] = (
    "idle",
    "rejected",
    "plausibility_only",
    "downgraded",
    "held",
    "proven",
    "corroborated",
)


def rung_for(
    strength: str | None,
    *,
    independently_verified: bool = False,
) -> Rung:
    """Map an engine verification strength onto a presentation rung.

    ``corroborated`` is reserved for a proven claim that two independent verifiers
    agreed on -- the only rung above ``proven``. An unknown or absent strength maps to
    ``plausibility_only`` rather than to ``held``, so the failure mode of a missing
    field is under-claiming rather than over-claiming.
    """
    if strength == "proven":
        return "corroborated" if independently_verified else "proven"
    if strength == "plausibility_only":
        return "plausibility_only"
    return "plausibility_only"
