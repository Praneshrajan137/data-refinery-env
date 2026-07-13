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
        "id,city\n1,Boston\n2,Denver\n3,Austin\n4,Reno\n5,Miami\n6,Chicago\n7,Dallas\n8,\n",
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
        # Every held fix carries a structured, honest reason it was not applied.
        reasons = {s.review_reason for s in result.receipt.suggested_fixes}
        assert reasons == {"safety_escalation"}

    def test_plausibility_only_fill_is_held_by_proven_only_gate(self, tmp_path: Path) -> None:
        # No authoritative schema + LLM value = plausibility-only. Even with the
        # escalation confirmed, the proven-only gate holds it (never auto-applied),
        # with the honest structured reason floor_cannot_verify.
        csv_path = tmp_path / "data.csv"
        _write_missing_value_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("Seattle")):
            result = run_repair_pipeline(
                RepairPipelineRequest(
                    source_path=csv_path,
                    mode="apply",
                    allow_llm=True,
                    confirm_escalations=True,
                )
            )

        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original
        reasons = {s.review_reason for s in result.receipt.suggested_fixes}
        assert reasons == {"floor_cannot_verify"}

    def test_held_by_calibration_reports_conformal_reason(self, tmp_path: Path) -> None:
        # With the unproven opt-in, the plausibility gate is passed, so the fix is
        # then held by the default (propose-not-apply) CALIBRATION policy -> a
        # distinct, honest structured reason (gate ordering: proven-gate first,
        # then calibration).
        csv_path = tmp_path / "data.csv"
        _write_missing_value_csv(csv_path)
        original = csv_path.read_bytes()

        with patch("dataforge.repairers.llm_corrector.complete", _fake_complete("Seattle")):
            result = run_repair_pipeline(
                RepairPipelineRequest(
                    source_path=csv_path,
                    mode="apply",
                    allow_llm=True,
                    confirm_escalations=True,
                    allow_unproven_autoapply=True,
                )
            )

        assert result.receipt.applied is False
        assert csv_path.read_bytes() == original
        reasons = {s.review_reason for s in result.receipt.suggested_fixes}
        assert reasons == {"failed_conformal_threshold"}


class TestConfirmedCalibratedAutoApply:
    def test_applies_only_with_confirmation_permissive_policy_and_unproven_optin(
        self, tmp_path: Path
    ) -> None:
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
                    allow_unproven_autoapply=True,
                )
            )

        # Auto-apply requires the FULL chain for a plausibility-only fill:
        # confirmed escalation + permissive policy + explicit unproven opt-in.
        assert result.receipt.applied is True
        assert "Seattle" in csv_path.read_text(encoding="utf-8")
        assert result.receipt.suggested_fixes == []
        # The certificate records it truthfully as unproven -- it never lies.
        strengths = {f.verification_strength for f in result.receipt.applied_fixes}
        assert strengths == {"plausibility_only"}
