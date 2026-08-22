"""Unit tests for the agent benchmark / release gate."""

from __future__ import annotations

from pathlib import Path

from dataforge.bench.agent_gate import (
    agent_promotion_verdict,
    compare_agent_vs_deterministic,
    default_gate_fixtures,
)
from dataforge.release.agent_gate import check_agent_release_gate
from tests.support.tables import RepairableTable


class TestNonRegressionGate:
    def test_default_fixtures_exist(self) -> None:
        assert default_gate_fixtures()

    def test_agent_matches_deterministic_floor(
        self, premised_repairable_table: RepairableTable
    ) -> None:
        """The agent reproduces the legacy floor on a fixture with a REAL floor.

        Previously this used the schema-free ``1020`` table. When ``decimal_shift`` left
        the auto-apply allowlist that floor went to zero, and the assertion degenerated to
        ``0 == 0 and 0 == 0`` -- satisfied by an agent that dropped every fix. It now uses
        the premised table, whose ``fd_violation`` repair the product stands behind, so
        parity is asserted over an actual write.
        """
        report = compare_agent_vs_deterministic(
            [premised_repairable_table.csv_path], schema=premised_repairable_table.schema
        )
        assert report.all_parity
        assert report.non_vacuous, report.vacuity_reason
        assert report.fixtures[0].floor_fix_count == 1
        assert report.fixtures[0].agent_fix_count == report.fixtures[0].floor_fix_count

    def test_a_zero_floor_fails_the_gate_as_vacuous(self, tmp_path: Path) -> None:
        """A gate that cannot fail is theatre: an empty floor must not certify parity.

        This is the mutation test for :attr:`AgentGateReport.non_vacuous` expressed as a
        test rather than a manual mutant: on a clean table every count is zero, ``parity``
        is trivially ``True``, and the gate must still refuse.
        """
        clean = tmp_path / "clean.csv"
        clean.write_text("id,city\n1,boston\n2,boston\n3,boston\n", encoding="utf-8")

        report = compare_agent_vs_deterministic([clean])

        assert report.fixtures[0].parity, "precondition: the zero counts do agree"
        assert not report.non_vacuous
        assert not report.all_parity, (
            "all_parity must not be satisfied by three zeros -- an agent that dropped "
            "every fix would otherwise be certified as reproducing the floor"
        )
        reason = report.vacuity_reason
        assert reason is not None and "ZERO deterministic floor" in reason

    def test_release_gate_passes_on_bundled_fixtures(self) -> None:
        result = check_agent_release_gate()
        assert result.passed, result.reason
        assert result.report.non_vacuous, (
            "the bundled gate passed vacuously; it proves nothing about the agent"
        )


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
