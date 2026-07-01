"""End-to-end pipeline tests for the LLM corrector (C6).

Fully offline: the provider call is patched. These assert the corrector's
place in the verified pipeline -- suggestions by default, byte-identical
deterministic parity when disabled, and auto-apply only under an explicit
confirmed + calibrated configuration.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from unittest.mock import patch

from dataforge.calibration import AbstentionPolicy
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline


def _write_missing_value_csv(path: Path) -> None:
    """Write a CSV with one blank cell in an otherwise-populated text column."""
    path.write_text(
        "id,city\n"
        "1,Boston\n"
        "2,Denver\n"
        "3,Austin\n"
        "4,Reno\n"
        "5,Miami\n"
        "6,Chicago\n"
        "7,Dallas\n"
        "8,\n",
        encoding="utf-8",
    )


def _fake_complete(value: str) -> Callable[..., Awaitable[str]]:
    async def _complete(messages: object, *, model: str, temperature: float) -> str:
        return value

    return _complete


def _raising_complete() -> Callable[..., Awaitable[str]]:
    async def _complete(messages: object, *, model: str, temperature: float) -> str:
        raise AssertionError("LLM provider must not be called when allow_llm is False")

    return _complete


def _permissive_policy() -> AbstentionPolicy:
    return AbstentionPolicy(
        target_precision=0.95,
        auto_apply_thresholds={"missing_value": 0.0},
        default_threshold=0.0,
    )


class TestParityWhenDisabled:
    def test_allow_llm_false_never_calls_llm_and_has_no_suggestions(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_missing_value_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _raising_complete()):
            result = run_repair_pipeline(
                RepairPipelineRequest(source_path=csv_path, mode="apply", allow_llm=False)
            )

        # No LLM call happened (the raising stub was never invoked), no
        # suggestions were produced, and the source file is unchanged.
        assert result.receipt.suggested_fixes == []
        assert csv_path.read_bytes() == original


class TestSuggestionByDefault:
    def test_corrector_fill_is_a_suggestion_not_applied(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_missing_value_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("Seattle")):
            result = run_repair_pipeline(
                RepairPipelineRequest(source_path=csv_path, mode="apply", allow_llm=True)
            )

        # Propose-not-apply: the corrector's fill surfaces as a suggestion and the
        # file is not mutated (blocked by the unconfirmed-LLM-write escalation).
        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original
        suggested_values = {(s.row, s.column, s.new_value) for s in result.receipt.suggested_fixes}
        assert (7, "city", "Seattle") in suggested_values


class TestConfirmedCalibratedAutoApply:
    def test_applies_only_with_confirmation_and_permissive_policy(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_missing_value_csv(csv_path)

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("Seattle")):
            result = run_repair_pipeline(
                RepairPipelineRequest(
                    source_path=csv_path,
                    mode="apply",
                    allow_llm=True,
                    confirm_escalations=True,
                    corrector_policy=_permissive_policy(),
                )
            )

        # With the escalation confirmed and a calibrated policy that clears the
        # class, the corrector fill is auto-applied through the verified gate.
        assert result.receipt.applied is True
        assert "Seattle" in csv_path.read_text(encoding="utf-8")
        assert result.receipt.suggested_fixes == []
