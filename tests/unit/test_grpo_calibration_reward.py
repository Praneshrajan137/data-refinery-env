"""Offline proof that the flag-gated GRPO reward actually rewards calibration.

These tests need no model and no training run: they assert the reward *shape*
is correct so Phase C trains the right objective. The F1-only path (flag off)
must stay byte-identical.
"""

from __future__ import annotations

from archive.training.grpo_contract import score_grpo_completion
from dataforge.repair_contract import render_calibrated_completion

_ALLOWED = ["A", "B"]
_ROWS = [0, 1]
_TRUTH = [
    {"row": 0, "column": "A", "clean_value": "good"},
    {"row": 1, "column": "B", "clean_value": "right"},
]


def _score(completion: str, *, enable_calibration: bool, inferability: object = None) -> float:
    reward, _ = score_grpo_completion(
        completion,
        raw_truth=_TRUTH,
        raw_allowed_columns=_ALLOWED,
        raw_valid_rows=_ROWS,
        raw_inferability=inferability,
        enable_calibration=enable_calibration,
    )
    return reward


def test_flag_off_is_byte_identical_regardless_of_confidence() -> None:
    with_conf = render_calibrated_completion(
        [
            {"row": 0, "column": "A", "new_value": "good", "confidence": 1.0},
            {"row": 1, "column": "B", "new_value": "bad", "confidence": 1.0},
        ]
    )
    without_conf = render_calibrated_completion(
        [
            {"row": 0, "column": "A", "new_value": "good"},
            {"row": 1, "column": "B", "new_value": "bad"},
        ]
    )
    assert _score(with_conf, enable_calibration=False) == _score(
        without_conf, enable_calibration=False
    )


def test_flag_off_omits_calibration_components() -> None:
    completion = render_calibrated_completion(
        [{"row": 0, "column": "A", "new_value": "good", "confidence": 1.0}]
    )
    _, diagnostics = score_grpo_completion(
        completion,
        raw_truth=_TRUTH,
        raw_allowed_columns=_ALLOWED,
        raw_valid_rows=_ROWS,
        enable_calibration=False,
    )
    assert "calibration_brier" not in diagnostics["reward_components"]


def test_well_calibrated_beats_miscalibrated_at_equal_f1() -> None:
    # Same repairs => identical F1. Only confidence differs.
    well = render_calibrated_completion(
        [
            {"row": 0, "column": "A", "new_value": "good", "confidence": 1.0},  # correct, sure
            {"row": 1, "column": "B", "new_value": "bad", "confidence": 0.0},  # wrong, unsure
        ]
    )
    mis = render_calibrated_completion(
        [
            {"row": 0, "column": "A", "new_value": "good", "confidence": 0.0},  # correct, unsure
            {"row": 1, "column": "B", "new_value": "bad", "confidence": 1.0},  # wrong, sure
        ]
    )
    assert _score(well, enable_calibration=True) > _score(mis, enable_calibration=True)


def test_confident_wrong_is_penalized() -> None:
    sure_wrong = render_calibrated_completion(
        [
            {"row": 0, "column": "A", "new_value": "good", "confidence": 1.0},
            {"row": 1, "column": "B", "new_value": "bad", "confidence": 1.0},
        ]
    )
    unsure_wrong = render_calibrated_completion(
        [
            {"row": 0, "column": "A", "new_value": "good", "confidence": 1.0},
            {"row": 1, "column": "B", "new_value": "bad", "confidence": 0.0},
        ]
    )
    assert _score(sure_wrong, enable_calibration=True) < _score(
        unsure_wrong, enable_calibration=True
    )


def test_correct_abstention_beats_confident_guess_on_non_inferable() -> None:
    truth = [{"row": 0, "column": "A", "clean_value": "unknowable"}]
    abstain = render_calibrated_completion([])  # finish, no repairs
    wrong_guess = render_calibrated_completion(
        [{"row": 0, "column": "A", "new_value": "a guess", "confidence": 1.0}]
    )
    abstain_reward, _ = score_grpo_completion(
        abstain,
        raw_truth=truth,
        raw_allowed_columns=["A"],
        raw_valid_rows=[0],
        raw_inferability="not_inferable_from_prompt",
        enable_calibration=True,
    )
    guess_reward, _ = score_grpo_completion(
        wrong_guess,
        raw_truth=truth,
        raw_allowed_columns=["A"],
        raw_valid_rows=[0],
        raw_inferability="not_inferable_from_prompt",
        enable_calibration=True,
    )
    assert abstain_reward > guess_reward
