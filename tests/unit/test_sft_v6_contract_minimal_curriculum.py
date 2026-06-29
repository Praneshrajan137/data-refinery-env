"""Tests for the SFT-v6 contract-minimal curriculum builder."""

from __future__ import annotations

import json

from dataforge.repair_contract import (
    CONTRACT_VERSION_V2,
    CONTRACT_VERSION_V3,
    SYSTEM_PROMPT,
    parse_repair_action,
)
from scripts.data.build_contract_minimal_curriculum import (
    CONTRACT_FIRST_SYSTEM_PROMPT,
    CURRICULUM_VERSION,
    build_contract_minimal_curriculum,
)


def _record(*, repairs: list[dict[str, object]]) -> dict[str, object]:
    payload = {
        "contract_version": CONTRACT_VERSION_V2,
        "schema_summary": {"dataset": "beers", "columns": ["ounces", "abv"]},
        "allowed_columns": ["ounces", "abv"],
        "valid_rows": [1, 2, 3],
        "target_rows": [{"_row": "1", "ounces": "12 oz.", "abv": "5%"}],
        "context_rows": [],
    }
    action = "submit_repairs" if repairs else "finish"
    return {
        "schema_version": "expert_v4",
        "prompt_contract_version": CONTRACT_VERSION_V2,
        "dataset": "beers",
        "inferability": "deterministic_normalization" if repairs else "not_inferable_from_prompt",
        "trajectory_id": f"beers:{action}",
        "fix": repairs,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, sort_keys=True)},
            {
                "role": "assistant",
                "content": json.dumps({"action": action, "repairs": repairs}, sort_keys=True),
            },
        ],
    }


def test_contract_minimal_curriculum_strips_reasons_and_caps_repairs() -> None:
    source = [
        _record(
            repairs=[
                {"row": 1, "column": "ounces", "new_value": "12", "reason": "unit"},
                {"row": 2, "column": "abv", "new_value": "0.05", "reason": "percent"},
                {"row": 3, "column": "ounces", "new_value": "12", "reason": "unit"},
            ]
        ),
        _record(repairs=[]),
    ]

    selected, report = build_contract_minimal_curriculum(
        source,
        max_repairs_per_record=2,
    )

    assert selected[0]["curriculum_version"] == CURRICULUM_VERSION
    system_prompt = selected[0]["messages"][0]["content"]
    assert system_prompt == CONTRACT_FIRST_SYSTEM_PROMPT
    assert '"reason"' not in system_prompt
    assert "markdown" not in system_prompt.lower()
    assert "code fence" not in system_prompt.lower()
    assistant = json.loads(selected[0]["messages"][2]["content"])
    assert assistant == {
        "action": "submit_repairs",
        "repairs": [
            {"row": 1, "column": "ounces", "new_value": "12"},
            {"row": 2, "column": "abv", "new_value": "0.05"},
        ],
    }
    assert json.loads(selected[1]["messages"][2]["content"]) == {
        "action": "finish",
        "repairs": [],
    }
    assert report["metrics"]["assistant_reason_fields"] == 0
    assert report["metrics"]["system_reason_field_mentions"] == 0
    assert report["metrics"]["system_wrapper_mentions"] == 0
    assert report["metrics"]["user_contract_version_mismatches"] == 0
    assert report["metrics"]["record_contract_version_mismatches"] == 0
    assert report["metrics"]["repair_cells"] == 2
    assert report["blockers"] == [
        "finish_records_under_256",
        "submit_repair_records_under_512",
    ]

    user_payload = json.loads(selected[0]["messages"][1]["content"])
    assert selected[0]["prompt_contract_version"] == CONTRACT_VERSION_V3
    assert user_payload["contract_version"] == CONTRACT_VERSION_V3
    parsed = parse_repair_action(
        selected[0]["messages"][2]["content"],
        allowed_columns=user_payload["allowed_columns"],
        valid_rows=user_payload["valid_rows"],
        require_explicit_action=True,
    )
    assert parsed.ok is True
