"""Adversarial safety tests for the LLM corrector (C6).

A hostile or hallucinating model must never corrupt data. Every sampled value
is bound by the correction contract and the inferred-constraint guard, and any
value that survives is still routed through the safety constitution and the SMT
verifier as a suggestion, never a silent write. These tests drive the model to
misbehave and assert nothing bad is applied.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from unittest.mock import patch

from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline


def _write_numeric_missing_csv(path: Path) -> None:
    """Numeric column (int, range 9-13) with one blank cell at row 7."""
    path.write_text(
        "id,score\n"
        "1,10\n"
        "2,12\n"
        "3,11\n"
        "4,9\n"
        "5,13\n"
        "6,10\n"
        "7,12\n"
        "8,\n",
        encoding="utf-8",
    )


def _fake_complete(value: str) -> Callable[..., Awaitable[str]]:
    async def _complete(messages: object, *, model: str, temperature: float) -> str:
        return value

    return _complete


def _run(csv_path: Path) -> object:
    return run_repair_pipeline(
        RepairPipelineRequest(source_path=csv_path, mode="apply", allow_llm=True)
    )


class TestAdversarialValuesNeverApplied:
    def test_wrong_type_value_is_filtered(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "d.csv"
        _write_numeric_missing_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("banana")):
            result = _run(csv_path)

        # Non-numeric value for a numeric column: rejected by the contract, so the
        # corrector abstains -- no suggestion, no mutation.
        assert result.receipt.suggested_fixes == []
        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original

    def test_out_of_domain_value_is_filtered(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "d.csv"
        _write_numeric_missing_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("999999999")):
            result = _run(csv_path)

        assert result.receipt.suggested_fixes == []
        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original

    def test_empty_value_is_filtered(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "d.csv"
        _write_numeric_missing_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("   ")):
            result = _run(csv_path)

        assert result.receipt.suggested_fixes == []
        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original

    def test_plausible_value_is_a_suggestion_not_applied(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "d.csv"
        _write_numeric_missing_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("11")):
            result = _run(csv_path)

        # Even a contract-valid value is not auto-applied by default: it becomes a
        # reviewable suggestion and the file stays untouched.
        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original
        suggested = {(s.row, s.column, s.new_value) for s in result.receipt.suggested_fixes}
        assert (7, "score", "11") in suggested
