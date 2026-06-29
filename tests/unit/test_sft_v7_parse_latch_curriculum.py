"""Tests for the SFT-v7 parse-latch curriculum builder."""

from __future__ import annotations

import json

from dataforge.repair_contract import CONTRACT_VERSION_V3, SYSTEM_PROMPT_V3, parse_repair_action
from scripts.data.build_parse_latch_curriculum import (
    CURRICULUM_VERSION,
    build_parse_latch_curriculum,
)


def _record(*, repairs: list[dict[str, object]], trajectory_id: str) -> dict[str, object]:
    action = "submit_repairs" if repairs else "finish"
    user_payload = {
        "contract_version": CONTRACT_VERSION_V3,
        "schema_summary": {"dataset": "beers", "columns": ["ounces", "abv"]},
        "allowed_columns": ["ounces", "abv"],
        "valid_rows": [1, 2],
        "target_rows": [{"_row": "1", "ounces": "12 oz.", "abv": "5%"}],
        "context_rows": [],
    }
    return {
        "schema_version": "expert_v4",
        "prompt_contract_version": CONTRACT_VERSION_V3,
        "dataset": "beers",
        "inferability": "deterministic_normalization" if repairs else "not_inferable_from_prompt",
        "trajectory_id": trajectory_id,
        "fix": repairs,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT_V3},
            {"role": "user", "content": json.dumps(user_payload, sort_keys=True)},
            {
                "role": "assistant",
                "content": json.dumps({"action": action, "repairs": repairs}, sort_keys=True),
            },
        ],
    }


def test_parse_latch_curriculum_oversamples_repairs_and_keeps_finish_empty() -> None:
    source = [
        _record(
            trajectory_id="repair",
            repairs=[{"row": 1, "column": "ounces", "new_value": "12"}],
        ),
        _record(trajectory_id="finish", repairs=[]),
    ]

    selected, report = build_parse_latch_curriculum(
        source,
        submit_repair_copies=2,
        finish_copies=1,
    )

    assert len(selected) == 3
    assert report["metrics"]["submit_repair_records"] == 2
    assert report["metrics"]["finish_records"] == 1
    assert report["metrics"]["assistant_reason_fields"] == 0
    assert report["metrics"]["system_wrapper_mentions"] == 0
    assert report["metrics"]["finish_with_repairs"] == 0
    assert report["blockers"] == [
        "finish_records_under_450",
        "submit_repair_records_under_1800",
    ]

    trajectory_ids = {str(record["trajectory_id"]) for record in selected}
    assert len(trajectory_ids) == len(selected)
    for record in selected:
        assert record["curriculum_version"] == CURRICULUM_VERSION
        assert record["prompt_contract_version"] == CONTRACT_VERSION_V3
        user_payload = json.loads(record["messages"][1]["content"])
        assistant = json.loads(record["messages"][2]["content"])
        parsed = parse_repair_action(
            record["messages"][2]["content"],
            allowed_columns=user_payload["allowed_columns"],
            valid_rows=user_payload["valid_rows"],
            require_explicit_action=True,
        )
        assert parsed.ok is True
        if assistant["action"] == "finish":
            assert assistant["repairs"] == []
        else:
            assert assistant["action"] == "submit_repairs"
            assert sorted(assistant["repairs"][0]) == ["column", "new_value", "row"]
            assert "reason" not in assistant["repairs"][0]
