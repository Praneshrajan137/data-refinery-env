"""Tests for the SFT-v9 action-envelope curriculum builder."""

from __future__ import annotations

import json

from dataforge.repair_contract import CONTRACT_VERSION_V3, SYSTEM_PROMPT_V3, parse_repair_action
from scripts.data.build_action_envelope_curriculum import (
    CURRICULUM_VERSION,
    build_action_envelope_curriculum,
)


def _record(
    *,
    repairs: list[dict[str, object]],
    trajectory_id: str,
    inferability: str = "deterministic_normalization",
    eval_rows: list[int] | None = None,
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
        "prompt": [
            {"role": "system", "content": SYSTEM_PROMPT_V3},
            {"role": "user", "content": json.dumps(user_payload, sort_keys=True)},
        ],
        "completion": json.dumps({"action": action, "repairs": repairs}, sort_keys=True),
        "training_format": "prompt_completion",
        "provenance": {"eval_rows": eval_rows or [99], "split": "train"},
    }


def test_action_envelope_curriculum_uses_prompt_completion_and_negative_contrasts() -> None:
    selected, report = build_action_envelope_curriculum(
        [
            _record(
                trajectory_id="repair",
                repairs=[{"row": 1, "column": "ounces", "new_value": "12"}],
            ),
            _record(trajectory_id="finish", repairs=[], inferability="not_inferable_from_prompt"),
        ],
        submit_repair_copies=1,
        finish_copies=1,
        submit_envelope_drills=2,
        finish_envelope_drills=2,
    )

    assert len(selected) == 6
    assert report["metrics"]["prompt_completion_records"] == 6
    assert report["metrics"]["completion_parse_failure_count"] == 0
    assert report["metrics"]["completion_parse_success_rate"] == 1.0
    assert report["metrics"]["completion_reason_text_count"] == 0
    assert report["metrics"]["completion_code_fence_count"] == 0
    assert report["metrics"]["finish_with_repairs"] == 0
    assert report["metrics"]["negative_contrast_target_leakage_count"] == 0
    assert report["label_mask_audit"]["ok"] is True
    assert report["product_constrained_preflight"]["parse_structural_success_rate"] == 1.0
    assert "submit_repair_records_under_1000" in report["blockers"]
    assert "finish_records_under_1000" in report["blockers"]
    assert set(report["metrics"]["negative_contrast_shape"]) >= {
        "row_object_output",
        "bare_repair_object",
        "finish_with_repairs",
        "wrong_case_column",
        "invalid_row",
        "extra_key",
    }

    for record in selected:
        assert record["curriculum_version"] == CURRICULUM_VERSION
        assert record["training_format"] == "prompt_completion"
        assert "messages" not in record
        assert record["negative_contrast_targets_supervised"] is False
        prompt = record["prompt"]
        completion = record["completion"]
        assert [message["role"] for message in prompt] == ["system", "user"]
        assert isinstance(completion, str)
        assert completion.startswith("{")
        assert completion.endswith("}")
        assert "reason" not in completion.lower()
        assert "```" not in completion
        prompt_text = json.dumps(prompt, sort_keys=True)
        for contrast in record["negative_contrast_examples"]:
            assert contrast["output"] != completion
            assert contrast["output"] not in prompt_text
        user_payload = json.loads(prompt[1]["content"])
        parsed = parse_repair_action(
            completion,
            allowed_columns=user_payload["allowed_columns"],
            valid_rows=user_payload["valid_rows"],
            require_explicit_action=True,
        )
        assert parsed.ok is True


def test_action_envelope_curriculum_defaults_external_reference_to_finish() -> None:
    selected, _report = build_action_envelope_curriculum(
        [
            _record(
                trajectory_id="external",
                repairs=[{"row": 1, "column": "ounces", "new_value": "12"}],
                inferability="external_reference_required",
            )
        ],
        submit_repair_copies=1,
        finish_copies=1,
        submit_envelope_drills=0,
        finish_envelope_drills=0,
    )

    assert json.loads(selected[0]["completion"]) == {"action": "finish", "repairs": []}


def test_action_envelope_curriculum_blocks_heldout_leakage() -> None:
    _selected, report = build_action_envelope_curriculum(
        [
            _record(
                trajectory_id="leak",
                repairs=[{"row": 1, "column": "ounces", "new_value": "12"}],
                eval_rows=[1],
            )
        ],
        submit_repair_copies=1,
        finish_copies=1,
        submit_envelope_drills=0,
        finish_envelope_drills=0,
    )

    assert report["ok"] is False
    assert "heldout_leakage" in report["blockers"]
    assert report["leakage_samples"][0]["rows"] == [1]
