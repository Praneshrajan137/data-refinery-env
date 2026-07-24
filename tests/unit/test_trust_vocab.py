"""Tests for the shared trust vocabulary (the non-visual twin of the perceptual language)."""

from __future__ import annotations

import pytest

from dataforge.engine.repair import ReviewReason
from dataforge.ui import trust_vocab


def test_every_review_reason_has_human_phrasing() -> None:
    # The humanizer must cover every ReviewReason literal the engine can emit,
    # so no raw machine token ever leaks to a user.
    literals = set(ReviewReason.__args__)  # type: ignore[attr-defined]
    assert literals <= set(trust_vocab.REVIEW_REASON_HUMAN), (
        "trust_vocab is missing human phrasing for: "
        f"{sorted(literals - set(trust_vocab.REVIEW_REASON_HUMAN))}"
    )


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
