"""Tests for the SFT-v8 schema-distill curriculum builder."""

from __future__ import annotations

import json

from dataforge.repair_contract import CONTRACT_VERSION_V3, SYSTEM_PROMPT_V3, parse_repair_action
from scripts.data.build_schema_distill_curriculum import (
    CURRICULUM_VERSION,
    build_schema_distill_curriculum,
)


def _record(
    *,
    repairs: list[dict[str, object]],
    trajectory_id: str,
    inferability: str = "deterministic_normalization",
) -> dict[str, object]:
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
        "inferability": inferability,
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


def test_schema_distill_curriculum_uses_prompt_completion_and_compact_json() -> None:
    source = [
        _record(
            trajectory_id="repair",
            repairs=[{"row": 1, "column": "ounces", "new_value": "12"}],
        ),
        _record(trajectory_id="finish", repairs=[], inferability="not_inferable_from_prompt"),
    ]

    selected, report = build_schema_distill_curriculum(
        source,
        submit_repair_copies=2,
        finish_copies=2,
        submit_micro_drills=0,
        finish_micro_drills=0,
    )

    assert len(selected) == 4
    assert report["metrics"]["prompt_completion_records"] == 4
    assert report["metrics"]["completion_parse_failure_count"] == 0
    assert report["metrics"]["completion_reason_text_count"] == 0
    assert report["metrics"]["completion_code_fence_count"] == 0
    assert report["metrics"]["finish_with_repairs"] == 0
    assert report["label_mask_audit"]["ok"] is True
    assert "submit_repair_records_under_1000" in report["blockers"]

    for record in selected:
        assert record["curriculum_version"] == CURRICULUM_VERSION
        assert record["training_format"] == "prompt_completion"
        assert "messages" not in record
        prompt = record["prompt"]
        completion = record["completion"]
        assert [message["role"] for message in prompt] == ["system", "user"]
        assert isinstance(completion, str)
        assert completion.startswith("{")
        assert completion.endswith("}")
        assert "reason" not in completion.lower()
        assert "```" not in completion
        user_payload = json.loads(prompt[1]["content"])
        parsed = parse_repair_action(
            completion,
            allowed_columns=user_payload["allowed_columns"],
            valid_rows=user_payload["valid_rows"],
            require_explicit_action=True,
        )
        assert parsed.ok is True
        assert parsed.action is not None
        if parsed.action.action == "finish":
            assert parsed.action.repairs == []
        else:
            assert sorted(parsed.action.repairs[0].model_dump(mode="json")) == [
                "column",
                "new_value",
                "reason",
                "row",
            ]


def test_schema_distill_defaults_external_reference_to_finish() -> None:
    selected, report = build_schema_distill_curriculum(
        [
            _record(
                trajectory_id="external",
                repairs=[{"row": 1, "column": "ounces", "new_value": "12"}],
                inferability="external_reference_required",
            )
        ],
        submit_repair_copies=1,
        finish_copies=1,
        submit_micro_drills=0,
        finish_micro_drills=0,
    )

    assert report["metrics"]["finish_records"] == 1
    assert json.loads(selected[0]["completion"]) == {"action": "finish", "repairs": []}
