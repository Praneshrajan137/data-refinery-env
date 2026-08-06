"""Tests for the schema-constrained (Structured Outputs) corrector mode.

The mode exists to fix three measured defects in the free-text corrector:

1. The candidate pool was a prompt *request* plus a post-filter, so paid samples
   were discarded after the fact and the agreement denominator was polluted.
2. The documented ``min(agreement, model_confidence)`` safety invariant never
   fired, because both system prompts ask for "only the value", never JSON, so
   ``_parse_confidence`` always returned ``None``.
3. ``k=3`` agreement can take only ~3 distinct values, and
   ``conformal.certify_threshold`` searches only observed confidences -- so there
   was almost nowhere to place a certifiable threshold.

Every test here is offline: the provider is replaced by an injected
``completion_fn``, so no key and no network are required.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from dataforge.detectors.base import Issue
from dataforge.repairers.contract import build_correction_contract
from dataforge.repairers.llm_corrector import _ABSTAIN_TOKEN, LLMCorrectorRepairer


def _frame() -> pd.DataFrame:
    """A categorical-heavy frame with a clear frequent-value pool."""
    cities = ["ATLANTA"] * 6 + ["BIRMINGHAM"] * 5 + ["MOBILE"] * 4
    return pd.DataFrame(
        {
            "provider": [f"P{i:03d}" for i in range(len(cities) + 1)],
            "city": [*cities, "ATLNTA"],  # last row is the dirty cell
        }
    )


def _issue(df: pd.DataFrame) -> Issue:
    """The issue pointing at the dirty city cell."""
    row = len(df) - 1
    return Issue(
        row=row,
        column="city",
        issue_type="categorical_normalization",
        severity="review",
        reason="value not in the column's established vocabulary",
        actual=str(df.at[row, "city"]),
        confidence=0.6,
    )


def _corrector(
    *,
    structured: bool,
    responses: list[str],
    samples: int = 3,
    cache_dir: Path | None = None,
) -> tuple[LLMCorrectorRepairer, list[list[dict[str, str]]]]:
    """Build a corrector whose provider calls are replayed from ``responses``."""
    seen: list[list[dict[str, str]]] = []
    calls = {"n": 0}

    def completion_fn(messages: list[dict[str, str]]) -> str:
        seen.append([dict(m) for m in messages])
        index = min(calls["n"], len(responses) - 1)
        calls["n"] += 1
        return responses[index]

    corrector = LLMCorrectorRepairer(
        cache_dir=cache_dir,
        allow_llm=True,
        model="test-model",
        samples=samples,
        completion_fn=completion_fn,  # type: ignore[arg-type]
        structured=structured,
    )
    return corrector, seen


class TestResponseFormatSchema:
    """The strict schema honours every documented Structured Outputs constraint."""

    def test_schema_is_strict_and_closed(self) -> None:
        df = _frame()
        corrector, _ = _corrector(structured=True, responses=["{}"])
        spec = corrector._response_format(df, _issue(df))
        assert spec is not None
        json_schema = spec["json_schema"]
        assert isinstance(json_schema, dict)
        assert json_schema["strict"] is True
        schema = json_schema["schema"]
        assert isinstance(schema, dict)
        assert schema["additionalProperties"] is False
        # Structured Outputs requires every property to be listed as required.
        assert set(schema["required"]) == set(schema["properties"])

    def test_enum_is_the_pool_plus_abstain(self) -> None:
        df = _frame()
        corrector, _ = _corrector(structured=True, responses=["{}"])
        spec = corrector._response_format(df, _issue(df))
        assert spec is not None
        enum = spec["json_schema"]["schema"]["properties"]["value"]["enum"]  # type: ignore[index]
        assert enum[-1] == _ABSTAIN_TOKEN
        # Frequency-ordered pool of values with support >= 2. The dirty value
        # ("ATLNTA", support 1) is correctly absent.
        assert enum[:-1] == ["ATLANTA", "BIRMINGHAM", "MOBILE"]
        assert "ATLNTA" not in enum

    def test_confidence_has_no_unsupported_keywords(self) -> None:
        # `minimum`/`maximum` are unsupported type-specific keywords; the range
        # is enforced locally by _parse_confidence instead.
        df = _frame()
        corrector, _ = _corrector(structured=True, responses=["{}"])
        spec = corrector._response_format(df, _issue(df))
        assert spec is not None
        confidence = spec["json_schema"]["schema"]["properties"]["confidence"]  # type: ignore[index]
        assert confidence == {"type": "number"}

    def test_free_text_mode_requests_no_response_format(self) -> None:
        df = _frame()
        corrector, _ = _corrector(structured=False, responses=["{}"])
        assert corrector._response_format(df, _issue(df)) is None

    def test_empty_pool_abstains_from_the_structured_path(self) -> None:
        # No closed candidate set means no enum worth sending; a degenerate
        # one-value schema would force a guess.
        df = pd.DataFrame({"provider": ["P1", "P2"], "city": ["ONLY", "OTHER"]})
        issue = Issue(
            row=1,
            column="city",
            issue_type="categorical_normalization",
            severity="review",
            reason="unknown value",
            actual="OTHER",
            confidence=0.6,
        )
        corrector, _ = _corrector(structured=True, responses=["{}"])
        assert corrector._response_format(df, issue) is None


class TestStructuredProposals:
    """Parsing, abstention, and the resurrected confidence signal."""

    def test_unanimous_structured_answer_is_proposed(self) -> None:
        df = _frame()
        payload = json.dumps({"value": "ATLANTA", "confidence": 0.9})
        corrector, _ = _corrector(structured=True, responses=[payload])
        fix = corrector.propose(_issue(df), df, None)
        assert fix is not None
        assert fix.fix.new_value == "ATLANTA"
        # 3/3 agreement, model confidence 0.9 -> min(1.0, 0.9) = 0.9.
        assert fix.confidence == pytest.approx(0.9)
        assert fix.provenance == "llm_live"

    def test_model_confidence_can_only_lower_never_raise(self) -> None:
        # The monotonic-safety invariant, now actually reachable.
        df = _frame()
        payload = json.dumps({"value": "ATLANTA", "confidence": 0.4})
        corrector, _ = _corrector(structured=True, responses=[payload])
        fix = corrector.propose(_issue(df), df, None)
        assert fix is not None
        assert fix.confidence == pytest.approx(0.4)

    def test_confidence_is_continuous_not_a_three_point_grid(self) -> None:
        # The whole point: agreement alone at k=3 yields ~{0.33, 0.67, 1.0}.
        # A model-emitted confidence makes the score continuous, which is what
        # conformal.certify_threshold needs to isolate a clean slice.
        df = _frame()
        observed = set()
        for value in (0.11, 0.37, 0.62, 0.88):
            payload = json.dumps({"value": "ATLANTA", "confidence": value})
            corrector, _ = _corrector(structured=True, responses=[payload])
            fix = corrector.propose(_issue(df), df, None)
            assert fix is not None
            observed.add(round(fix.confidence, 4))
        assert observed == {0.11, 0.37, 0.62, 0.88}

    def test_abstain_token_yields_no_proposal(self) -> None:
        df = _frame()
        payload = json.dumps({"value": _ABSTAIN_TOKEN, "confidence": 0.2})
        corrector, _ = _corrector(structured=True, responses=[payload])
        assert corrector.propose(_issue(df), df, None) is None

    def test_out_of_range_confidence_falls_back_to_agreement(self) -> None:
        # Untrusted model output: a nonsense confidence must never be trusted,
        # and must never raise the effective confidence.
        df = _frame()
        payload = json.dumps({"value": "ATLANTA", "confidence": 7.5})
        corrector, _ = _corrector(structured=True, responses=[payload])
        fix = corrector.propose(_issue(df), df, None)
        assert fix is not None
        assert fix.confidence == pytest.approx(1.0)  # pure 3/3 agreement

    def test_split_vote_lowers_confidence(self) -> None:
        df = _frame()
        responses = [
            json.dumps({"value": "ATLANTA", "confidence": 1.0}),
            json.dumps({"value": "ATLANTA", "confidence": 1.0}),
            json.dumps({"value": "BIRMINGHAM", "confidence": 1.0}),
        ]
        seen: list[list[dict[str, str]]] = []
        calls = {"n": 0}

        def completion_fn(messages: list[dict[str, str]]) -> str:
            seen.append([dict(m) for m in messages])
            out = responses[calls["n"]]
            calls["n"] += 1
            return out

        corrector = LLMCorrectorRepairer(
            cache_dir=None,
            allow_llm=True,
            model="test-model",
            samples=3,
            completion_fn=completion_fn,  # type: ignore[arg-type]
            structured=True,
        )
        fix = corrector.propose(_issue(df), df, None)
        assert fix is not None
        assert fix.fix.new_value == "ATLANTA"
        assert fix.confidence == pytest.approx(2 / 3)


class TestPromptAndPoolSemantics:
    """Structured mode implies the pool constraint and says so in the prompt."""

    def test_structured_implies_pool_constrained(self) -> None:
        corrector = LLMCorrectorRepairer(cache_dir=None, allow_llm=True, model="m", structured=True)
        # A decode-time enum IS the pool constraint; keeping them independent
        # would allow two disagreeing definitions of "admissible".
        assert corrector._pool_constrained is True

    def test_structured_prompt_legitimises_abstention(self) -> None:
        df = _frame()
        payload = json.dumps({"value": "ATLANTA", "confidence": 0.9})
        corrector, seen = _corrector(structured=True, responses=[payload])
        corrector.propose(_issue(df), df, None)
        system = seen[0][0]["content"]
        assert _ABSTAIN_TOKEN in system
        assert "abstaining is correct" in system
        # The enum enforces membership, so the prompt should not waste words
        # restating it as a hard rule.
        assert "MUST choose" not in system

    def test_value_outside_the_pool_is_still_rejected(self) -> None:
        # Defense in depth: even if a provider ignored the enum, the post-filter
        # still holds.
        df = _frame()
        payload = json.dumps({"value": "SOMEWHERE_ELSE", "confidence": 1.0})
        corrector, _ = _corrector(structured=True, responses=[payload])
        assert corrector.propose(_issue(df), df, None) is None


class TestDefaultPathIsUnchanged:
    """The new mode must be invisible unless explicitly enabled."""

    def test_free_text_prompt_is_byte_identical_to_before(self) -> None:
        df = _frame()
        corrector, seen = _corrector(structured=False, responses=["ATLANTA"])
        corrector.propose(_issue(df), df, None)
        system = seen[0][0]["content"]
        # The historical free-text system prompt, unchanged.
        assert system == (
            "You correct a single erroneous cell in a tabular dataset. "
            "Use only the evidence provided; do not invent facts. "
            "Respond with only the corrected value and nothing else."
        )

    def test_structured_and_free_text_do_not_share_a_cache_entry(self, tmp_path: Path) -> None:
        # The two modes produce different distributions from the same prompt, so
        # sharing a cache file would silently mix them.
        df = _frame()
        issue = _issue(df)
        free, _ = _corrector(structured=False, responses=["ATLANTA"], cache_dir=tmp_path)
        structured, _ = _corrector(
            structured=True,
            responses=[json.dumps({"value": "ATLANTA", "confidence": 0.9})],
            cache_dir=tmp_path,
        )
        free_contract = build_correction_contract(issue, free._constraints_for(df, None))
        structured_contract = build_correction_contract(
            issue, structured._constraints_for(df, None)
        )
        free_path = free._cache_path(issue, df, free._build_messages(issue, df, free_contract))
        structured_path = structured._cache_path(
            issue, df, structured._build_messages(issue, df, structured_contract)
        )
        assert free_path != structured_path
