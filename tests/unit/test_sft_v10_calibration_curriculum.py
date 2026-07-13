"""Tests for the SFT-v10 calibration curriculum."""

from __future__ import annotations

from typing import Any

from dataforge.repair_contract import parse_cell_confidences, parse_repair_action
from scripts.data.build_calibration_curriculum import (
    build_calibration_curriculum,
    calibrate_record,
)


def _submit_record(trajectory_id: str = "beers:easy:0:0") -> dict[str, Any]:
    return {
        "trajectory_id": trajectory_id,
        "dataset": "beers",
        "inferability": "deterministic_normalization",
        "curriculum_version": "expert_v9_action_envelope",
        "fix": [{"row": 0, "column": "ounces", "new_value": "12"}],
        "completion": '{"action":"submit_repairs","repairs":[{"column":"ounces","new_value":"12","row":0}]}',
    }


def _abstention_record(trajectory_id: str = "flights:hard:0:0") -> dict[str, Any]:
    return {
        "trajectory_id": trajectory_id,
        "dataset": "flights",
        "inferability": "not_inferable_from_prompt",
        "curriculum_version": "expert_v9_action_envelope",
        "fix": [{"row": 0, "column": "dep_time", "new_value": "guess"}],
        "completion": '{"action":"finish","repairs":[]}',
    }


def test_submit_record_gets_confidence_target() -> None:
    calibrated = calibrate_record(_submit_record())
    assert calibrated["should_abstain"] is False
    assert calibrated["target_confidence"] == 1.0
    confidences = parse_cell_confidences(calibrated["completion"])
    assert confidences == {(0, "ounces"): 1.0}
    assert parse_repair_action(calibrated["completion"], require_explicit_action=True).ok


def test_non_inferable_record_becomes_abstention() -> None:
    calibrated = calibrate_record(_abstention_record())
    assert calibrated["should_abstain"] is True
    assert calibrated["target_confidence"] == 0.0
    assert calibrated["fix"] == []  # the un-inferable guess is dropped
    parsed = parse_repair_action(calibrated["completion"], require_explicit_action=True)
    assert parsed.ok
    assert parsed.action is not None
    assert parsed.action.action == "finish"


def test_build_reports_pass_with_submit_and_abstention() -> None:
    records = [_submit_record("a"), _submit_record("b"), _abstention_record("c")]
    selected, report = build_calibration_curriculum(records)
    assert len(selected) == 3
    assert report["ok"] is True
    assert report["metrics"]["submit_records"] == 2
    assert report["metrics"]["abstention_records"] == 1
    assert report["metrics"]["confidence_coverage"] == 1.0
    assert report["metrics"]["completion_parse_success_rate"] == 1.0


def test_build_blocks_when_no_abstention_slice_present() -> None:
    _, report = build_calibration_curriculum([_submit_record("a")])
    assert report["ok"] is False
    assert "no_abstention_records" in report["blockers"]
