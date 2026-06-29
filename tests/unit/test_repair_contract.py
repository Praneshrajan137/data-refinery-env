"""Tests for the canonical DataForge repair contract."""

from __future__ import annotations

import json

from pydantic import BaseModel

from dataforge.repair_contract import (
    CONTRACT_VERSION,
    CONTRACT_VERSION_V2,
    CONTRACT_VERSION_V3,
    RepairFix,
    parse_repair_action,
    render_repair_messages,
    repair_action_json_schema,
    repair_failure_taxonomy,
    score_repair_fixes,
    score_repair_fixes_canonicalized,
    validate_repair_action_json_schema_payload,
)


class _Truth(BaseModel):
    row: int
    column: str
    clean_value: str


def test_render_repair_messages_uses_canonical_payload_shape() -> None:
    messages = render_repair_messages(
        schema_summary={"dataset": "synthetic", "columns": ["Score"]},
        target_rows=[{"_row": 0, "Score": "45"}],
        context_rows=[],
        allowed_columns=["Score"],
        label_source="fixture",
        repairs=[RepairFix(row=0, column="Score", new_value="4.5", reason="decimal shift")],
    )

    assert [message["role"] for message in messages] == ["system", "user", "assistant"]
    user_payload = json.loads(messages[1]["content"])
    assert user_payload["contract_version"] == CONTRACT_VERSION
    assert user_payload["contract_version"] == CONTRACT_VERSION_V2
    assert user_payload["allowed_columns"] == ["Score"]
    assert user_payload["valid_rows"] == [0]
    assert user_payload["target_rows"] == [{"_row": "0", "Score": "45"}]
    assistant_payload = json.loads(messages[2]["content"])
    assert assistant_payload == {
        "action": "submit_repairs",
        "repairs": [{"row": 0, "column": "Score", "new_value": "4.5", "reason": "decimal shift"}],
    }


def test_render_repair_messages_supports_contract_minimal_v3_shape() -> None:
    messages = render_repair_messages(
        schema_summary={"dataset": "synthetic", "columns": ["Score"]},
        target_rows=[{"_row": 0, "Score": "45"}],
        context_rows=[],
        allowed_columns=["Score"],
        repairs=[RepairFix(row=0, column="Score", new_value="4.5", reason="decimal shift")],
        contract_version=CONTRACT_VERSION_V3,
    )

    assert '"reason"' not in messages[0]["content"]
    assert "markdown" not in messages[0]["content"].lower()
    assert "code fence" not in messages[0]["content"].lower()
    assert "start with { and end with }" in messages[0]["content"]
    assert "Never put repairs in a finish action" in messages[0]["content"]
    user_payload = json.loads(messages[1]["content"])
    assistant_payload = json.loads(messages[2]["content"])

    assert user_payload["contract_version"] == CONTRACT_VERSION_V3
    assert assistant_payload == {
        "action": "submit_repairs",
        "repairs": [{"row": 0, "column": "Score", "new_value": "4.5"}],
    }


def test_parse_repair_action_accepts_objects_arrays_and_fences() -> None:
    object_result = parse_repair_action(
        '{"action":"submit_repairs","repairs":[{"row":0,"column":"Score","new_value":"4.5"}]}'
    )
    array_result = parse_repair_action('[{"row":1,"column":"Phone","new_value":"217"}]')
    fenced_result = parse_repair_action('```json\n{"action":"finish","repairs":[]}\n```')

    assert object_result.ok
    assert object_result.action is not None
    assert object_result.action.repairs[0].reason == "repair proposal"
    assert array_result.ok
    assert fenced_result.ok
    assert fenced_result.action is not None
    assert fenced_result.action.action == "finish"


def test_parse_repair_action_v2_enforces_columns_rows_and_explicit_action() -> None:
    missing_action = parse_repair_action(
        '{"repairs":[{"row":0,"column":"Score","new_value":"4.5"}]}',
        allowed_columns=["Score"],
        valid_rows=[0],
        require_explicit_action=True,
    )
    bad_column = parse_repair_action(
        '{"action":"submit_repairs","repairs":[{"row":0,"column":"score","new_value":"4.5"}]}',
        allowed_columns=["Score"],
        valid_rows=[0],
        require_explicit_action=True,
    )
    bad_row = parse_repair_action(
        '{"action":"submit_repairs","repairs":[{"row":99,"column":"Score","new_value":"4.5"}]}',
        allowed_columns=["Score"],
        valid_rows=[0],
        require_explicit_action=True,
    )
    nonempty_finish = parse_repair_action(
        '{"action":"finish","repairs":[{"row":0,"column":"Score","new_value":"4.5"}]}',
        allowed_columns=["Score"],
        valid_rows=[0],
        require_explicit_action=True,
    )

    assert missing_action.error_kind == "schema_error"
    assert bad_column.error_kind == "invalid_column"
    assert bad_column.diagnostics["schema_case_error"] is True
    assert bad_row.error_kind == "invalid_row"
    assert nonempty_finish.error_kind == "finish_with_repairs"
    assert nonempty_finish.diagnostics["repair_count"] == 1
    assert nonempty_finish.action is not None
    assert nonempty_finish.action.repairs[0].column == "Score"


def test_repair_action_json_schema_models_v3_action_latch() -> None:
    schema = repair_action_json_schema(allowed_columns=["Score"], valid_rows=[0, 1])

    assert schema["additionalProperties"] is False
    finish_branch, submit_branch = schema["oneOf"]
    assert finish_branch["properties"]["action"] == {"const": "finish"}
    assert finish_branch["properties"]["repairs"]["maxItems"] == 0
    repair_schema = submit_branch["properties"]["repairs"]["items"]
    assert submit_branch["properties"]["action"] == {"const": "submit_repairs"}
    assert repair_schema["additionalProperties"] is False
    assert repair_schema["required"] == ["row", "column", "new_value"]
    assert repair_schema["properties"]["row"]["enum"] == [0, 1]
    assert repair_schema["properties"]["column"]["enum"] == ["Score"]


def test_strict_repair_action_schema_validator_rejects_invalid_envelopes() -> None:
    valid_finish = {"action": "finish", "repairs": []}
    valid_one = {
        "action": "submit_repairs",
        "repairs": [{"row": 0, "column": "Score", "new_value": "4.5"}],
    }
    valid_two = {
        "action": "submit_repairs",
        "repairs": [
            {"row": 0, "column": "Score", "new_value": "4.5"},
            {"row": 1, "column": "Phone", "new_value": "217"},
        ],
    }
    invalid_cases = {
        "row_object": {"_row": "0", "Score": "4.5"},
        "bare_repair": {"row": 0, "column": "Score", "new_value": "4.5"},
        "finish_with_repairs": {
            "action": "finish",
            "repairs": [{"row": 0, "column": "Score", "new_value": "4.5"}],
        },
        "extra_key": {
            "action": "submit_repairs",
            "repairs": [{"row": 0, "column": "Score", "new_value": "4.5", "reason": "no"}],
        },
        "wrong_case_column": {
            "action": "submit_repairs",
            "repairs": [{"row": 0, "column": "score", "new_value": "4.5"}],
        },
        "invalid_row": {
            "action": "submit_repairs",
            "repairs": [{"row": 99, "column": "Score", "new_value": "4.5"}],
        },
    }

    assert validate_repair_action_json_schema_payload(
        valid_finish,
        allowed_columns=["Score", "Phone"],
        valid_rows=[0, 1],
    ).ok
    assert validate_repair_action_json_schema_payload(
        valid_one,
        allowed_columns=["Score", "Phone"],
        valid_rows=[0, 1],
    ).ok
    assert validate_repair_action_json_schema_payload(
        valid_two,
        allowed_columns=["Score", "Phone"],
        valid_rows=[0, 1],
    ).ok
    for payload in invalid_cases.values():
        result = validate_repair_action_json_schema_payload(
            payload,
            allowed_columns=["Score", "Phone"],
            valid_rows=[0, 1],
        )
        assert result.ok is False


def test_parse_repair_action_deduplicates_cells_last_write_wins() -> None:
    result = parse_repair_action(
        '{"action":"submit_repairs","repairs":['
        '{"row":0,"column":"Score","new_value":"wrong"},'
        '{"row":0,"column":"Score","new_value":"4.5"}'
        "]}"
    )

    assert result.ok
    assert result.action is not None
    assert result.diagnostics["duplicate_cell_count"] == 1
    assert result.action.repairs == [
        RepairFix(row=0, column="Score", new_value="4.5", reason="repair proposal")
    ]


def test_score_and_taxonomy_keep_exact_scoring_strict() -> None:
    truth = [
        _Truth(row=0, column="Score", clean_value="4.5"),
        _Truth(row=1, column="Phone", clean_value="217"),
    ]
    fixes = [
        RepairFix(row=0, column="score", new_value="4.5", reason="wrong case"),
        RepairFix(row=1, column="Phone", new_value="999", reason="wrong value"),
        RepairFix(row=2, column="Phone", new_value="217", reason="wrong row"),
    ]

    score = score_repair_fixes(truth, fixes)
    canonicalized = score_repair_fixes_canonicalized(
        [_Truth(row=0, column="Score", clean_value="Mercy Hospital")],
        [RepairFix(row=0, column="Score", new_value="  MERCY   HOSPITAL  ", reason="format")],
    )
    taxonomy = repair_failure_taxonomy(
        ground_truth=truth,
        fixes=fixes,
        allowed_columns=["Score", "Phone"],
        valid_rows=[0, 1],
    )

    assert score.tp == 0
    assert score.fp == 3
    assert score.fn == 2
    assert canonicalized.f1 == 1.0
    assert taxonomy == {
        "missed_repair": 1,
        "schema_case_error": 1,
        "wrong_cell": 1,
        "wrong_value": 1,
    }
