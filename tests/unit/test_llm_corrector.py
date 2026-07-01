"""Tests for the grounded, contract-bound LLM corrector (C2).

The corrector never trusts the model blindly: each sampled value must pass the
CorrectionContract (built from the detector's finding plus inferred/declared
constraints) before it can even be proposed, and self-consistency across k
samples sets the confidence. Offline by construction -- the provider call is
patched, so these tests need no API key and no network.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dataforge.detectors.base import Issue, Severity
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer


def _issue(
    *,
    row: int,
    column: str,
    issue_type: str,
    actual: str,
    expected: str | None = None,
) -> Issue:
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,  # type: ignore[arg-type]
        severity=Severity.REVIEW,
        confidence=0.9,
        expected=expected,
        actual=actual,
        reason="detected for test",
    )


def _scripted_complete(values: Sequence[str]) -> object:
    """Build an async stand-in for the provider that yields scripted values."""
    calls = {"n": 0}

    async def _fake(messages: object, *, model: str, temperature: float) -> str:
        index = min(calls["n"], len(values) - 1)
        calls["n"] += 1
        return values[index]

    _fake.calls = calls  # type: ignore[attr-defined]
    return _fake


class TestCorrectorDisabled:
    def test_returns_none_when_llm_not_allowed(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"city": ["Boston", "Denver", "Austin"]})
        corrector = LLMCorrectorRepairer(cache_dir=tmp_path, allow_llm=False, model="m")
        issue = _issue(row=0, column="city", issue_type="categorical_normalization", actual="boston")

        assert corrector.propose(issue, df, None) is None


class TestSelfConsistency:
    def test_majority_value_is_proposed_with_agreement_confidence(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        corrector = LLMCorrectorRepairer(
            cache_dir=tmp_path, allow_llm=True, model="m", samples=3
        )
        issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

        fake = _scripted_complete(["11", "11", "999999"])
        with patch("dataforge.repairers.llm_corrector.complete", fake):
            fix = corrector.propose(issue, df, None)

        assert fix is not None
        assert fix.fix.new_value == "11"
        # "999999" is out of inferred domain -> filtered by the contract; the two
        # contract-passing "11" votes out of three samples set confidence to 2/3.
        assert abs(fix.confidence - (2 / 3)) < 1e-9
        assert fix.provenance == "llm_live"

    def test_abstains_when_no_sample_passes_contract(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"amount": ["10", "12", "11", "9", "13"]})
        corrector = LLMCorrectorRepairer(
            cache_dir=tmp_path, allow_llm=True, model="m", samples=3
        )
        issue = _issue(row=0, column="amount", issue_type="outlier", actual="9999")

        fake = _scripted_complete(["banana", "999999", ""])
        with patch("dataforge.repairers.llm_corrector.complete", fake):
            fix = corrector.propose(issue, df, None)

        assert fix is None

    def test_abstains_on_no_op_value(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"city": ["Boston", "Denver", "Austin", "Reno"]})
        corrector = LLMCorrectorRepairer(
            cache_dir=tmp_path, allow_llm=True, model="m", samples=3
        )
        issue = _issue(row=0, column="city", issue_type="categorical_normalization", actual="Boston")

        fake = _scripted_complete(["Boston", "Boston", "Boston"])
        with patch("dataforge.repairers.llm_corrector.complete", fake):
            fix = corrector.propose(issue, df, None)

        assert fix is None


class TestCaching:
    def test_second_call_uses_cache_and_skips_provider(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"city": ["Boston", "Denver", "Austin", "Reno"]})
        issue = _issue(row=0, column="city", issue_type="missing_value", actual="")

        fake = _scripted_complete(["Seattle", "Seattle", "Seattle"])
        with patch("dataforge.repairers.llm_corrector.complete", fake):
            corrector = LLMCorrectorRepairer(
                cache_dir=tmp_path, allow_llm=True, model="m", samples=3
            )
            first = corrector.propose(issue, df, None)
        assert first is not None
        assert first.provenance == "llm_live"
        calls_after_first = fake.calls["n"]  # type: ignore[attr-defined]

        # Fresh corrector, same cache dir -> cache hit, provider not called again.
        fake2 = _scripted_complete(["DIFFERENT"])
        with patch("dataforge.repairers.llm_corrector.complete", fake2):
            corrector2 = LLMCorrectorRepairer(
                cache_dir=tmp_path, allow_llm=True, model="m", samples=3
            )
            second = corrector2.propose(issue, df, None)

        assert second is not None
        assert second.fix.new_value == "Seattle"
        assert second.provenance == "llm_cache"
        assert fake2.calls["n"] == 0  # type: ignore[attr-defined]
        assert calls_after_first == 3


class TestContractBinding:
    def test_fd_violating_sample_is_filtered(self, tmp_path: Path) -> None:
        # 12 rows of zip 02134 (one dirty "Bostan") + 8 rows of zip 10001 gives
        # the zip -> city dependency >= 0.95 confidence, so it is enforced.
        zips = ["02134"] * 12 + ["10001"] * 8
        cities = (["Boston"] * 11 + ["Bostan"]) + ["NYC"] * 8
        df = pd.DataFrame({"zip": zips, "city": cities})
        corrector = LLMCorrectorRepairer(
            cache_dir=tmp_path, allow_llm=True, model="m", samples=3
        )
        # Row 11 has zip 02134; the FD consensus says city must be "Boston".
        issue = _issue(row=11, column="city", issue_type="fd_violation", actual="Bostan")

        # The verifier (not the contract) enforces FD, but the corrector must
        # still abstain rather than propose the consensus-violating "Atlanta".
        fake = _scripted_complete(["Atlanta", "Atlanta", "Atlanta"])
        with patch("dataforge.repairers.llm_corrector.complete", fake):
            fix = corrector.propose(issue, df, None)

        assert fix is None
