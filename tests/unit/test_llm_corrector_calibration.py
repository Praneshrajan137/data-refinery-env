"""Corrector calibration + monotonic-safety tests.

Model-emitted confidence is an *input* to the abstention policy only: it can
lower but never raise the self-consistency confidence, and it can never let a
contract/guard-failing value be proposed. Offline: the provider is patched.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dataforge.detectors.base import Issue, Severity
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer


def _issue(*, row: int, column: str, issue_type: str, actual: str) -> Issue:
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,  # type: ignore[arg-type]
        severity=Severity.REVIEW,
        confidence=0.9,
        expected=None,
        actual=actual,
        reason="detected for test",
    )


def _scripted_complete(values: Sequence[str]) -> object:
    calls = {"n": 0}

    async def _fake(messages: object, *, model: str, temperature: float) -> str:
        index = min(calls["n"], len(values) - 1)
        calls["n"] += 1
        return values[index]

    _fake.calls = calls  # type: ignore[attr-defined]
    return _fake


def _value(value: str, confidence: float | None = None) -> str:
    payload: dict[str, object] = {"value": value}
    if confidence is not None:
        payload["confidence"] = confidence
    return json.dumps(payload)


def test_model_confidence_lowers_effective_confidence(tmp_path: Path) -> None:
    df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
    corrector = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=True, model="m", samples=3)
    issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

    # Perfect agreement (3/3) but the model reports only 0.2 confidence.
    fake = _scripted_complete([_value("11", 0.2), _value("11", 0.2), _value("11", 0.2)])
    with patch("dataforge.repairers.llm_corrector.complete", fake):
        fix = corrector.propose(issue, df, None)

    assert fix is not None
    assert fix.fix.new_value == "11"
    assert abs(fix.confidence - 0.2) < 1e-9  # min(agreement=1.0, model=0.2)


def test_model_confidence_never_raises_above_agreement(tmp_path: Path) -> None:
    df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
    corrector = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=True, model="m", samples=3)
    issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

    # 2/3 agreement, but the model claims 0.99 -> effective stays at 2/3.
    fake = _scripted_complete([_value("11", 0.99), _value("11", 0.99), _value("999999", 0.99)])
    with patch("dataforge.repairers.llm_corrector.complete", fake):
        fix = corrector.propose(issue, df, None)

    assert fix is not None
    assert abs(fix.confidence - (2 / 3)) < 1e-9


def test_absent_confidence_falls_back_to_agreement(tmp_path: Path) -> None:
    df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
    corrector = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=True, model="m", samples=3)
    issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

    fake = _scripted_complete(["11", "11", "999999"])  # plain values, no confidence
    with patch("dataforge.repairers.llm_corrector.complete", fake):
        fix = corrector.propose(issue, df, None)

    assert fix is not None
    assert abs(fix.confidence - (2 / 3)) < 1e-9


def test_confident_garbage_is_still_filtered_by_the_floor(tmp_path: Path) -> None:
    # A high model confidence cannot smuggle an out-of-domain value past the
    # contract/guard: the corrector abstains entirely.
    df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
    corrector = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=True, model="m", samples=3)
    issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

    fake = _scripted_complete(
        [_value("999999", 0.99), _value("999999", 0.99), _value("999999", 0.99)]
    )
    with patch("dataforge.repairers.llm_corrector.complete", fake):
        fix = corrector.propose(issue, df, None)

    assert fix is None


def test_cache_preserves_model_confidence(tmp_path: Path) -> None:
    df = pd.DataFrame({"city": ["Boston", "Denver", "Austin", "Reno"]})
    issue = _issue(row=0, column="city", issue_type="missing_value", actual="")

    fake = _scripted_complete(
        [_value("Seattle", 0.3), _value("Seattle", 0.3), _value("Seattle", 0.3)]
    )
    with patch("dataforge.repairers.llm_corrector.complete", fake):
        corrector = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=True, model="m", samples=3)
        first = corrector.propose(issue, df, None)
    assert first is not None
    assert abs(first.confidence - 0.3) < 1e-9

    fake2 = _scripted_complete(["UNUSED"])
    with patch("dataforge.repairers.llm_corrector.complete", fake2):
        corrector2 = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=True, model="m", samples=3)
        second = corrector2.propose(issue, df, None)

    assert second is not None
    assert second.provenance == "llm_cache"
    assert abs(second.confidence - 0.3) < 1e-9  # confidence survived the cache
    assert fake2.calls["n"] == 0  # type: ignore[attr-defined]
