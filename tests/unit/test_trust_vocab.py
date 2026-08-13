"""Tests for the shared trust vocabulary (the non-visual twin of the perceptual language)."""

from __future__ import annotations

import pytest

from dataforge.domain import vocabulary
from dataforge.engine.repair import ReviewReason
from dataforge.ui import trust_vocab


def test_review_reason_phrasing_covers_exactly_the_engine_vocabulary() -> None:
    """Phrasing and tokens must match EXACTLY, in both directions.

    This assertion used to be ``<=``, which only catches a missing sentence. A stale
    EXTRA key -- phrasing for a reason the engine no longer emits -- passed silently,
    and a stale entry is worse than a missing one because it makes the vocabulary look
    reviewed when it is not. Both sides now come from the same module, so equality is
    the honest assertion and it also proves the re-export has not been forked.
    """
    literals = set(ReviewReason.__args__)  # type: ignore[attr-defined]
    phrased = set(trust_vocab.REVIEW_REASON_HUMAN)
    assert literals == phrased, (
        f"missing phrasing: {sorted(literals - phrased)}; "
        f"stale phrasing: {sorted(phrased - literals)}"
    )


def test_review_reason_tuple_matches_the_literal() -> None:
    """The ordered tuple used by generators must match the type exactly.

    Generated artifacts iterate REVIEW_REASONS; the type system checks ReviewReason.
    If they diverge, TypeScript would be typed from one list and populated from another.
    """
    assert set(vocabulary.REVIEW_REASONS) == set(ReviewReason.__args__)  # type: ignore[attr-defined]
    assert len(vocabulary.REVIEW_REASONS) == len(set(vocabulary.REVIEW_REASONS))


def test_provenance_literal_and_order_cannot_drift() -> None:
    """The type and the ordered tuple must list exactly the same members.

    Found by mutation: adding a member to the ``Provenance`` Literal alone left every
    other assertion green, because they all compare ``PROVENANCE_ORDER`` against the
    partitions. The type would then permit a value the generator never emits and the
    partitions never classify.
    """
    from dataforge.domain.vocabulary import Provenance

    assert set(Provenance.__args__) == set(vocabulary.PROVENANCE_ORDER)  # type: ignore[attr-defined]
    assert len(vocabulary.PROVENANCE_ORDER) == len(set(vocabulary.PROVENANCE_ORDER))


def test_verification_strength_literal_and_tuple_cannot_drift() -> None:
    from dataforge.domain.vocabulary import VerificationStrength

    assert set(VerificationStrength.__args__) == set(  # type: ignore[attr-defined]
        vocabulary.VERIFICATION_STRENGTHS
    )


def test_rung_literal_and_order_cannot_drift() -> None:
    from dataforge.domain.vocabulary import Rung

    assert set(Rung.__args__) == set(vocabulary.RUNG_ORDER)  # type: ignore[attr-defined]


def test_severity_literal_and_order_cannot_drift() -> None:
    from dataforge.domain.vocabulary import Severity

    assert set(Severity.__args__) == set(vocabulary.SEVERITY_ORDER)  # type: ignore[attr-defined]


def test_provenance_partitions_are_exhaustive_and_disjoint() -> None:
    """Every provenance is either trusted or untrusted, and never both.

    Without this, a provenance could be added to the literal and silently fall into
    neither partition -- which the fail-closed predicate would treat as untrusted, but
    only by accident rather than by declaration.
    """
    assert set(vocabulary.PROVENANCE_ORDER) == (
        vocabulary.TRUSTED_PROVENANCE | vocabulary.UNTRUSTED_PROVENANCE
    )
    assert not (vocabulary.TRUSTED_PROVENANCE & vocabulary.UNTRUSTED_PROVENANCE)
    assert vocabulary.CALIBRATED_PROVENANCE < vocabulary.UNTRUSTED_PROVENANCE


def test_trust_predicate_fails_closed() -> None:
    """An unrecognised provenance is untrusted. This is the fail-closed guarantee."""
    assert vocabulary.is_trusted_provenance("deterministic") is True
    for untrusted in sorted(vocabulary.UNTRUSTED_PROVENANCE):
        assert vocabulary.is_trusted_provenance(untrusted) is False
    # The cases that previously read as trustworthy.
    assert vocabulary.is_trusted_provenance("some_future_corrector") is False
    assert vocabulary.is_trusted_provenance("") is False
    assert vocabulary.is_trusted_provenance(None) is False


def test_strength_is_never_proven_for_an_unknown_provenance_without_authority() -> None:
    assert (
        vocabulary.verification_strength_for("mystery", authoritative_schema_present=False)
        == "plausibility_only"
    )
    # With authority over the column, even an unknown origin is proven -- the proof
    # comes from the schema, not from the proposer.
    assert (
        vocabulary.verification_strength_for("mystery", authoritative_schema_present=True)
        == "proven"
    )


def test_rung_ladder_is_ordered_weakest_to_strongest() -> None:
    """Index order is load-bearing: perceptual intensity is monotonic in it."""
    assert vocabulary.RUNG_ORDER[0] == "idle"
    assert vocabulary.RUNG_ORDER[-1] == "corroborated"
    assert vocabulary.RUNG_ORDER.index("plausibility_only") < vocabulary.RUNG_ORDER.index("proven")
    assert len(vocabulary.RUNG_ORDER) == len(set(vocabulary.RUNG_ORDER))


def test_rung_for_under_claims_on_a_missing_strength() -> None:
    """A missing strength must map DOWN the ladder, never up."""
    assert vocabulary.rung_for("proven") == "proven"
    assert vocabulary.rung_for("proven", independently_verified=True) == "corroborated"
    assert vocabulary.rung_for("plausibility_only") == "plausibility_only"
    assert vocabulary.rung_for(None) == "plausibility_only"
    assert vocabulary.rung_for("nonsense") == "plausibility_only"


def test_humanize_review_reason_known_unknown_and_empty() -> None:
    assert "verifier" in trust_vocab.humanize_review_reason("verifier_rejected").lower()
    # Unknown token is still rendered readably, never as a raw snake_case token.
    rendered = trust_vocab.humanize_review_reason("some_new_reason")
    assert "_" not in rendered
    assert rendered.endswith(".")
    assert "not proven safe" in trust_vocab.humanize_review_reason(None).lower()


def test_verification_strength_label_never_calls_unproven_written() -> None:
    assert trust_vocab.verification_strength_label("proven") == "proven"
    # The overtrust guard in text form: unproven strengths always say "not written".
    assert "not written" in trust_vocab.verification_strength_label("plausibility_only")
    assert "not written" in trust_vocab.verification_strength_label(None)


def test_independent_verification_label() -> None:
    assert "two verifiers" in trust_vocab.independent_verification_label("agreed")
    assert trust_vocab.independent_verification_label("not_run") == "single verifier"


def test_glyphs_are_color_independent() -> None:
    assert trust_vocab.severity_glyph("unsafe") == "[!!]"
    assert trust_vocab.severity_glyph("SAFE") == "[ok]"
    assert trust_vocab.strength_glyph("proven") == "[proven]"
    assert trust_vocab.strength_glyph("plausibility_only") == "[plausible]"


def test_should_use_color_honors_no_color(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    assert trust_vocab.should_use_color() is False
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert trust_vocab.should_use_color() is True
