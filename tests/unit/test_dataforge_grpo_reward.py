"""Unit tests for the stateless DataForge GRPO reward function."""

from __future__ import annotations

import json

from archive.training.rewards.dataforge_reward import dataforge_reward


def _completion(repairs: list[dict[str, object]], *, action: str = "submit_repairs") -> str:
    return json.dumps({"action": action, "repairs": repairs}, sort_keys=True)


def test_grpo_reward_scores_exact_repair_as_one() -> None:
    rewards = dataforge_reward(
        [_completion([{"row": 0, "column": "Name", "new_value": "Alice", "reason": "fix"}])],
        ground_truth=[[{"row": 0, "column": "Name", "clean_value": "Alice"}]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
    )

    assert rewards == [1.0]
    assert dataforge_reward.last_diagnostics[0]["score"]["tp"] == 1


def test_grpo_reward_scores_finish_on_clean_chunk_as_one() -> None:
    rewards = dataforge_reward(
        [_completion([], action="finish")],
        ground_truth=[[]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
    )

    assert rewards == [1.0]
    assert dataforge_reward.last_diagnostics[0]["failure_taxonomy"] == {}
    assert dataforge_reward.last_diagnostics[0]["empty_repair_on_truth_positive"] is False


def test_grpo_reward_penalizes_wrong_value_and_schema_case_errors() -> None:
    rewards = dataforge_reward(
        [_completion([{"row": 0, "column": "name", "new_value": "Bob", "reason": "fix"}])],
        ground_truth=[[{"row": 0, "column": "Name", "clean_value": "Alice"}]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
    )

    assert rewards == [0.0]
    assert dataforge_reward.last_diagnostics[0]["failure_taxonomy"]["schema_case_error"] == 1


def test_grpo_reward_requires_explicit_v2_action() -> None:
    rewards = dataforge_reward(
        [json.dumps({"repairs": [{"row": 0, "column": "Name", "new_value": "Alice"}]})],
        ground_truth=[[{"row": 0, "column": "Name", "clean_value": "Alice"}]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
    )

    assert rewards == [0.0]
    assert dataforge_reward.last_diagnostics[0]["error_kind"] == "schema_error"


def test_grpo_reward_rejects_wrong_row_and_wrong_column_contract_errors() -> None:
    rewards = dataforge_reward(
        [
            _completion([{"row": 99, "column": "Name", "new_value": "Alice", "reason": "fix"}]),
            _completion([{"row": 0, "column": "Age", "new_value": "42", "reason": "fix"}]),
        ],
        ground_truth=[
            [{"row": 0, "column": "Name", "clean_value": "Alice"}],
            [{"row": 0, "column": "Name", "clean_value": "Alice"}],
        ],
        allowed_columns=[["Name"], ["Name"]],
        valid_rows=[[0], [0]],
    )

    assert rewards == [0.0, 0.0]
    assert dataforge_reward.last_diagnostics[0]["error_kind"] == "invalid_row"
    assert dataforge_reward.last_diagnostics[1]["error_kind"] == "invalid_column"


def test_grpo_reward_gives_shaping_for_canonicalized_value_match() -> None:
    rewards = dataforge_reward(
        [
            _completion(
                [{"row": 0, "column": "Name", "new_value": " alice  smith ", "reason": "fix"}]
            )
        ],
        ground_truth=[[{"row": 0, "column": "Name", "clean_value": "Alice Smith"}]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
    )

    assert 0.0 < rewards[0] < 1.0
    diagnostics = dataforge_reward.last_diagnostics[0]
    assert diagnostics["score"]["f1"] == 0.0
    assert diagnostics["canonicalized_score"]["f1"] == 1.0
    assert diagnostics["reward_components"]["recall_gap"] == 1.0


def test_grpo_reward_diagnoses_empty_repairs_on_truth_positive_tasks() -> None:
    rewards = dataforge_reward(
        [_completion([], action="finish")],
        ground_truth=[[{"row": 0, "column": "Name", "clean_value": "Alice"}]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
    )

    assert rewards == [0.0]
    diagnostics = dataforge_reward.last_diagnostics[0]
    assert diagnostics["truth_cell_count"] == 1
    assert diagnostics["predicted_repair_count"] == 0
    assert diagnostics["empty_repair_on_truth_positive"] is True
    assert diagnostics["failure_taxonomy"]["missed_repair"] == 1
    assert diagnostics["reward_components"]["empty_truth_positive_penalty"] == 0.05


def test_grpo_reward_penalizes_overrepair_but_keeps_correct_signal() -> None:
    rewards = dataforge_reward(
        [
            _completion(
                [
                    {"row": 0, "column": "Name", "new_value": "Alice", "reason": "fix"},
                    {"row": 0, "column": "City", "new_value": "Paris", "reason": "extra"},
                ]
            )
        ],
        ground_truth=[[{"row": 0, "column": "Name", "clean_value": "Alice"}]],
        allowed_columns=[["Name", "City"]],
        valid_rows=[[0]],
    )

    assert 0.0 < rewards[0] < 1.0
    assert dataforge_reward.last_diagnostics[0]["failure_taxonomy"]["overrepair"] == 1


def test_grpo_reward_zeroes_abstention_slice_overrepair() -> None:
    rewards = dataforge_reward(
        [_completion([{"row": 0, "column": "Name", "new_value": "Alice", "reason": "guess"}])],
        ground_truth=[[]],
        allowed_columns=[["Name"]],
        valid_rows=[[0]],
        inferability=["external_reference_required"],
    )

    assert rewards == [0.0]
    diagnostics = dataforge_reward.last_diagnostics[0]
    assert diagnostics["inferability"] == "external_reference_required"
    assert diagnostics["reward_components"]["abstention_overrepair"] == 1.0
    assert diagnostics["failure_taxonomy"]["overrepair"] == 1


def test_grpo_reward_handles_malformed_json_without_raising() -> None:
    rewards = dataforge_reward(
        ["not json"], ground_truth=[[{"row": 0, "column": "A", "clean_value": "x"}]]
    )

    assert rewards == [0.0]
    assert dataforge_reward.last_diagnostics[0]["parse_ok"] is False
    assert dataforge_reward.last_diagnostics[0]["error_kind"] == "parse_failure"


def test_grpo_reward_uses_last_write_wins_for_duplicate_repairs() -> None:
    rewards = dataforge_reward(
        [
            _completion(
                [
                    {"row": 0, "column": "A", "new_value": "wrong", "reason": "first"},
                    {"row": 0, "column": "A", "new_value": "right", "reason": "second"},
                ]
            )
        ],
        ground_truth=[[{"row": 0, "column": "A", "clean_value": "right"}]],
        allowed_columns=[["A"]],
        valid_rows=[[0]],
    )

    assert rewards == [1.0]


def test_grpo_reward_accepts_chat_style_completion_and_preserves_batch_order() -> None:
    rewards = dataforge_reward(
        [
            [
                {
                    "role": "assistant",
                    "content": _completion(
                        [{"row": 0, "column": "A", "new_value": "x", "reason": "fix"}]
                    ),
                }
            ],
            _completion([{"row": 0, "column": "A", "new_value": "wrong", "reason": "fix"}]),
            _completion([], action="finish"),
        ],
        ground_truth=[
            [{"row": 0, "column": "A", "clean_value": "x"}],
            [{"row": 0, "column": "A", "clean_value": "x"}],
            [],
        ],
        allowed_columns=[["A"], ["A"], ["A"]],
        valid_rows=[[0], [0], [0]],
    )

    assert rewards == [1.0, 0.0, 1.0]
