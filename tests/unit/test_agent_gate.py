"""Unit tests for the agent benchmark / release gate."""

from __future__ import annotations

from pathlib import Path

from dataforge.bench.agent_gate import (
    agent_promotion_verdict,
    compare_agent_vs_deterministic,
    default_gate_fixtures,
)
from dataforge.release.agent_gate import check_agent_release_gate


class TestNonRegressionGate:
    def test_default_fixtures_exist(self) -> None:
        assert default_gate_fixtures()

    def test_agent_matches_deterministic_floor(self, tmp_path: Path) -> None:
        csv = tmp_path / "amounts.csv"
        csv.write_text("id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n", encoding="utf-8")
        report = compare_agent_vs_deterministic([csv])
        assert report.all_parity
        assert report.fixtures[0].agent_fix_count == report.fixtures[0].floor_fix_count

    def test_release_gate_passes_on_bundled_fixtures(self) -> None:
        result = check_agent_release_gate()
        assert result.passed, result.reason


class TestPromotionVerdict:
    def test_weak_agent_is_blocked(self) -> None:
        verdict = agent_promotion_verdict(
            agent_f1=0.14, baseline_f1=0.79, safety_regressions=0, parity_ok=True
        )
        assert verdict.promote is False

    def test_strong_agent_is_promoted(self) -> None:
        verdict = agent_promotion_verdict(
            agent_f1=0.85, baseline_f1=0.79, safety_regressions=0, parity_ok=True
        )
        assert verdict.promote is True

    def test_safety_regression_blocks_promotion(self) -> None:
        verdict = agent_promotion_verdict(
            agent_f1=0.99, baseline_f1=0.79, safety_regressions=1, parity_ok=True
        )
        assert verdict.promote is False

    def test_parity_failure_blocks_promotion(self) -> None:
        verdict = agent_promotion_verdict(
            agent_f1=0.99, baseline_f1=0.79, safety_regressions=0, parity_ok=False
        )
        assert verdict.promote is False

    def test_margin_is_enforced(self) -> None:
        verdict = agent_promotion_verdict(
            agent_f1=0.80, baseline_f1=0.79, safety_regressions=0, parity_ok=True, min_margin=0.05
        )
        assert verdict.promote is False
