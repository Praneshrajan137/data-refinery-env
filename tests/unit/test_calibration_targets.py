"""Tests for ground-truth-derived calibration targets and the calibrated envelope."""

from __future__ import annotations

import json

import pytest

from dataforge.calibration_targets import (
    CalibrationTarget,
    calibration_samples,
    derive_cell_target,
    derive_targets_for_fixes,
)
from dataforge.repair_contract import (
    parse_cell_confidences,
    render_calibrated_completion,
)


def test_inferable_correct_proposal_targets_full_confidence() -> None:
    target = derive_cell_target(
        proposed_value="Mercy Hospital",
        clean_value="Mercy Hospital",
        inferability="deterministic_normalization",
    )
    assert target.correct is True
    assert target.should_abstain is False
    assert target.target_confidence == 1.0


def test_inferable_wrong_proposal_targets_zero_confidence() -> None:
    target = derive_cell_target(
        proposed_value="Guess Hospital",
        clean_value="Mercy Hospital",
        inferability="context_derivable",
    )
    assert target.correct is False
    assert target.should_abstain is False
    assert target.target_confidence == 0.0


def test_inferable_missing_proposal_is_incorrect() -> None:
    target = derive_cell_target(
        proposed_value=None,
        clean_value="Mercy Hospital",
        inferability="deterministic_normalization",
    )
    assert target.correct is False
    assert target.should_abstain is False
    assert target.target_confidence == 0.0


def test_non_inferable_abstention_is_correct() -> None:
    for slice_name in ("external_reference_required", "not_inferable_from_prompt"):
        target = derive_cell_target(
            proposed_value=None,
            clean_value="anything",
            inferability=slice_name,  # type: ignore[arg-type]
        )
        assert target.should_abstain is True
        assert target.correct is True
        assert target.target_confidence == 0.0


def test_non_inferable_guess_is_incorrect() -> None:
    target = derive_cell_target(
        proposed_value="a wild guess",
        clean_value="unknowable",
        inferability="not_inferable_from_prompt",
    )
    assert target.should_abstain is True
    assert target.correct is False
    assert target.target_confidence == 0.0


def test_strict_value_normalization_matches_scoring() -> None:
    # Trailing whitespace is stripped exactly like the official scorer.
    target = derive_cell_target(
        proposed_value="4.5   ",
        clean_value="4.5",
        inferability="deterministic_normalization",
    )
    assert target.correct is True
    assert target.target_confidence == 1.0


def test_derive_targets_for_fixes_covers_union_of_cells() -> None:
    proposed = {(0, "Score"): "4.5", (1, "Phone"): "999"}
    clean = {(0, "Score"): "4.5", (2, "City"): "Chicago"}
    inferability = {
        (0, "Score"): "deterministic_normalization",
        (1, "Phone"): "not_inferable_from_prompt",
        (2, "City"): "context_derivable",
    }
    targets = derive_targets_for_fixes(
        proposed_by_cell=proposed,
        clean_by_cell=clean,
        inferability_by_cell=inferability,  # type: ignore[arg-type]
    )
    assert targets[(0, "Score")].correct is True  # inferable + matches GT
    assert targets[(1, "Phone")].should_abstain is True  # guessed a non-inferable cell
    assert targets[(1, "Phone")].correct is False
    assert targets[(2, "City")].correct is False  # inferable GT cell left unrepaired


def test_calibration_samples_projection() -> None:
    targets = [
        CalibrationTarget(
            inferability="deterministic_normalization",
            correct=True,
            should_abstain=False,
            target_confidence=1.0,
            rationale="match",
        ),
        CalibrationTarget(
            inferability="context_derivable",
            correct=False,
            should_abstain=False,
            target_confidence=0.0,
            rationale="miss",
        ),
    ]
    assert calibration_samples(targets) == [(1.0, True), (0.0, False)]


def test_target_confidence_bounds_enforced() -> None:
    with pytest.raises(ValueError):
        CalibrationTarget(
            inferability="context_derivable",
            correct=True,
            should_abstain=False,
            target_confidence=1.5,
            rationale="bad",
        )


# --- Calibrated envelope --------------------------------------------------


def test_render_without_confidence_matches_strict_v3_envelope() -> None:
    rendered = render_calibrated_completion([{"row": 0, "column": "Score", "new_value": "4.5"}])
    expected = json.dumps(
        {
            "action": "submit_repairs",
            "repairs": [{"row": 0, "column": "Score", "new_value": "4.5"}],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    assert rendered == expected


def test_render_with_confidence_includes_key() -> None:
    rendered = render_calibrated_completion(
        [{"row": 0, "column": "Score", "new_value": "4.5", "confidence": 0.9}]
    )
    payload = json.loads(rendered)
    assert payload["repairs"][0]["confidence"] == 0.9


def test_render_empty_repairs_is_finish() -> None:
    payload = json.loads(render_calibrated_completion([]))
    assert payload == {"action": "finish", "repairs": []}


def test_render_rejects_out_of_range_confidence() -> None:
    with pytest.raises(ValueError):
        render_calibrated_completion(
            [{"row": 0, "column": "Score", "new_value": "4.5", "confidence": 1.2}]
        )


def test_parse_cell_confidences_roundtrip() -> None:
    text = render_calibrated_completion(
        [
            {"row": 0, "column": "Score", "new_value": "4.5", "confidence": 0.8},
            {"row": 1, "column": "Phone", "new_value": "999"},
        ]
    )
    confidences = parse_cell_confidences(text)
    assert confidences == {(0, "Score"): 0.8}


def test_parse_cell_confidences_handles_malformed() -> None:
    assert parse_cell_confidences("not json at all") == {}
    assert parse_cell_confidences('{"action":"finish","repairs":[]}') == {}


def test_parse_cell_confidences_drops_invalid_values() -> None:
    text = json.dumps(
        {
            "action": "submit_repairs",
            "repairs": [
                {"row": 0, "column": "A", "new_value": "x", "confidence": 1.5},
                {"row": 1, "column": "B", "new_value": "y", "confidence": True},
                {"row": 2, "column": "C", "new_value": "z", "confidence": 0.5},
            ],
        }
    )
    assert parse_cell_confidences(text) == {(2, "C"): 0.5}
