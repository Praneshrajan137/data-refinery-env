"""Parity tests for GRPO reward, parser, and failure taxonomy helpers."""

from __future__ import annotations

import json

from archive.training.grpo_contract import TruthCell, score_grpo_completion
from dataforge.repair_contract import (
    parse_repair_action,
    repair_failure_taxonomy,
    score_repair_fixes,
)


def _completion(column: str, value: str) -> str:
    return json.dumps(
        {
            "action": "submit_repairs",
            "repairs": [{"row": 0, "column": column, "new_value": value, "reason": "fix"}],
        },
        sort_keys=True,
    )


def test_grpo_reward_score_matches_strict_repair_scorer_for_valid_completion() -> None:
    truth = [{"row": 0, "column": "Name", "clean_value": "Alice"}]
    completion = _completion("Name", "Alice")

    reward, diagnostics = score_grpo_completion(
        completion,
        raw_truth=truth,
        raw_allowed_columns=["Name"],
        raw_valid_rows=[0],
    )
    parsed = parse_repair_action(
        completion,
        allowed_columns=["Name"],
        valid_rows=[0],
        require_explicit_action=True,
    )
    direct_score = score_repair_fixes(
        [TruthCell(row=0, column="Name", clean_value="Alice")],
        parsed.action.repairs if parsed.action else [],
    )

    assert reward == 1.0
    assert diagnostics["score"] == direct_score.model_dump(mode="json")


def test_grpo_reward_taxonomy_matches_schema_case_parser_failure() -> None:
    completion = _completion("name", "Alice")

    reward, diagnostics = score_grpo_completion(
        completion,
        raw_truth=[{"row": 0, "column": "Name", "clean_value": "Alice"}],
        raw_allowed_columns=["Name"],
        raw_valid_rows=[0],
    )
    parsed = parse_repair_action(
        completion,
        allowed_columns=["Name"],
        valid_rows=[0],
        require_explicit_action=True,
    )
    relaxed = parse_repair_action(completion, require_explicit_action=False)
    taxonomy = repair_failure_taxonomy(
        ground_truth=[TruthCell(row=0, column="Name", clean_value="Alice")],
        fixes=relaxed.action.repairs if relaxed.action else [],
        allowed_columns=["Name"],
        valid_rows=[0],
    )

    assert reward == 0.0
    assert parsed.error_kind == "invalid_column"
    assert diagnostics["failure_taxonomy"]["schema_case_error"] == taxonomy["schema_case_error"]
