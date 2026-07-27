"""Tests for the entity-consensus repairer and its opt-in auto-apply wiring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataforge.detectors.base import Issue, Severity
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.repairers.entity_consensus import EntityConsensusRepairer


def _issue(expected: str = "v_A", actual: str = "WRONG") -> Issue:
    return Issue(
        row=0,
        column="val",
        issue_type="entity_consensus",
        severity=Severity.REVIEW,
        confidence=1.0,
        expected=expected,
        actual=actual,
        reason="consensus test",
    )


def _multi_entity_csv(path: Path) -> None:
    # 6 entities x 4 rows; each entity's own distinct consensus value (high
    # diversity). Row 0 (entity A) holds a wrong value the consensus repairs.
    rows = ["key,val"]
    for entity in "ABCDEF":
        for _ in range(4):
            rows.append(f"{entity},v_{entity}")
    lines = rows[:1] + ["A,WRONG"] + rows[2:]  # replace first A row's value
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestEntityConsensusRepairer:
    def test_proposes_consensus_value_from_expected(self) -> None:
        df = pd.DataFrame({"val": ["WRONG"] + ["v_A"] * 4})
        fix = EntityConsensusRepairer().propose(_issue(), df, None)
        assert fix is not None
        assert fix.fix.new_value == "v_A"
        assert fix.provenance == "entity_consensus"
        assert fix.fix.detector_id == "entity_consensus"

    def test_returns_none_for_other_issue_types(self) -> None:
        df = pd.DataFrame({"val": ["x"]})
        other = _issue().model_copy(update={"issue_type": "missing_value"})
        assert EntityConsensusRepairer().propose(other, df, None) is None

    def test_returns_none_when_expected_equals_current(self) -> None:
        df = pd.DataFrame({"val": ["v_A"] * 5})
        noop = _issue(expected="v_A", actual="v_A")
        assert EntityConsensusRepairer().propose(noop, df, None) is None


class TestEntityConsensusEngineWiring:
    def test_off_by_default_no_consensus_fix(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "e.csv"
        _multi_entity_csv(csv_path)
        result = run_repair_pipeline(
            RepairPipelineRequest(source_path=csv_path, mode="dry_run", schema=None)
        )
        # Flag off -> the entity_consensus repairer is not even registered.
        assert all(f.detector_id != "entity_consensus" for f in result.receipt.applied_fixes)
        assert all(s.detector_id != "entity_consensus" for s in result.receipt.suggested_fixes)

    def test_held_as_plausibility_suggestion_by_default(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "e.csv"
        _multi_entity_csv(csv_path)
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=csv_path,
                mode="dry_run",
                schema=None,
                allow_entity_consensus=True,
            )
        )
        # Proposed but HELD (plausibility, not proven) -> a review suggestion,
        # never auto-applied without the opt-in.
        suggested = [
            s for s in result.receipt.suggested_fixes if s.detector_id == "entity_consensus"
        ]
        assert suggested, "expected an entity_consensus review suggestion"
        assert suggested[0].new_value == "v_A"
        assert suggested[0].review_reason == "unverified_entity_consensus"
        assert result.receipt.applied is False

    def test_auto_applies_under_opt_in(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "e.csv"
        _multi_entity_csv(csv_path)
        original = csv_path.read_bytes()
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=csv_path,
                mode="apply",
                schema=None,
                allow_entity_consensus=True,
                allow_unproven_autoapply=True,
                create_dry_run_transaction=False,
            )
        )
        applied = [f for f in result.receipt.applied_fixes if f.detector_id == "entity_consensus"]
        assert applied, "expected entity_consensus fix to auto-apply under opt-in"
        assert applied[0].new_value == "v_A"
        assert result.receipt.applied is True
        # The corrected file now holds the consensus value at the wrong cell.
        assert "A,v_A" in csv_path.read_text(encoding="utf-8")
        assert csv_path.read_bytes() != original
