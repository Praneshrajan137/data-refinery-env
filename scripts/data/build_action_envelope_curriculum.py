"""Build the SFT-v9 action-envelope prompt-completion curriculum."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, cast

from dataforge.repair_contract import (
    CONTRACT_VERSION_V3,
    SYSTEM_PROMPT_V3,
    parse_repair_action,
)

CURRICULUM_VERSION = "expert_v9_action_envelope"
REPORT_SCHEMA_VERSION = "dataforge_sft_v9_action_envelope_curriculum_report_v1"
DEFAULT_INPUT = Path("data/sft_traj/expert_v8_schema_distill.jsonl")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v9_action_envelope.jsonl")
DEFAULT_REPORT = Path("eval/results/sft_v9_action_envelope_curriculum_report.json")
DEFAULT_SUBMIT_ENVELOPE_DRILLS = 700
DEFAULT_FINISH_ENVELOPE_DRILLS = 700


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"{path}:{line_number} must contain a JSON object.")
        records.append(cast(dict[str, Any], payload))
    return records


def _compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))


def _compact_action(repairs: list[dict[str, Any]]) -> str:
    return _compact_json(
        {
            "action": "submit_repairs" if repairs else "finish",
            "repairs": repairs,
        }
    )


def _prompt_messages(user_payload: Mapping[str, Any]) -> list[dict[str, str]]:
    payload = dict(user_payload)
    payload["contract_version"] = CONTRACT_VERSION_V3
    return [
        {"role": "system", "content": SYSTEM_PROMPT_V3},
        {"role": "user", "content": _compact_json(payload)},
    ]


def _user_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    prompt = record.get("prompt")
    if not isinstance(prompt, list) or len(prompt) != 2:
        raise ValueError("record.prompt must contain system and user messages.")
    user_message = prompt[1]
    if not isinstance(user_message, Mapping) or user_message.get("role") != "user":
        raise ValueError("record.prompt[1] must be the user message.")
    content = user_message.get("content")
    if not isinstance(content, str):
        raise ValueError("user message content must be a string.")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("user message content must decode to an object.")
    return cast(dict[str, Any], payload)


def _completion_repairs(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    completion = record.get("completion")
    if not isinstance(completion, str):
        raise ValueError("record.completion must be a string.")
    user_payload = _user_payload(record)
    parsed = parse_repair_action(
        completion,
        allowed_columns=user_payload.get("allowed_columns", []),
        valid_rows=user_payload.get("valid_rows", []),
        require_explicit_action=True,
    )
    if not parsed.ok or parsed.action is None:
        raise ValueError(f"source completion is not a valid action: {parsed.error_kind}")
    if str(record.get("inferability")) in {"external_reference_required", "not_inferable_from_prompt"}:
        return []
    return [
        {"row": repair.row, "column": repair.column, "new_value": repair.new_value}
        for repair in parsed.action.repairs[:2]
    ]


def _first_valid_row(user_payload: Mapping[str, Any]) -> int:
    valid_rows = user_payload.get("valid_rows", [0])
    if isinstance(valid_rows, list) and valid_rows:
        return int(valid_rows[0])
    return 0


def _first_allowed_column(user_payload: Mapping[str, Any]) -> str:
    allowed_columns = user_payload.get("allowed_columns", ["value"])
    if isinstance(allowed_columns, list) and allowed_columns:
        return str(allowed_columns[0])
    return "value"


def _negative_contrasts(user_payload: Mapping[str, Any]) -> list[dict[str, str]]:
    row = _first_valid_row(user_payload)
    column = _first_allowed_column(user_payload)
    wrong_case_column = column.upper() if column != column.upper() else column.lower()
    invalid_row = row + 100000
    return [
        {
            "kind": "row_object_output",
            "output": _compact_json({"_row": str(row), column: "clean"}),
            "why_invalid": "row objects are prompt data, not assistant actions",
        },
        {
            "kind": "bare_repair_object",
            "output": _compact_json({"row": row, "column": column, "new_value": "clean"}),
            "why_invalid": "repair objects must be inside action=submit_repairs",
        },
        {
            "kind": "finish_with_repairs",
            "output": _compact_action([{"row": row, "column": column, "new_value": "clean"}]).replace(
                '"submit_repairs"', '"finish"', 1
            ),
            "why_invalid": "finish must have repairs=[]",
        },
        {
            "kind": "wrong_case_column",
            "output": _compact_action(
                [{"row": row, "column": wrong_case_column, "new_value": "clean"}]
            ),
            "why_invalid": "column names are case-sensitive",
        },
        {
            "kind": "invalid_row",
            "output": _compact_action([{"row": invalid_row, "column": column, "new_value": "clean"}]),
            "why_invalid": "row must be one of valid_rows",
        },
        {
            "kind": "extra_key",
            "output": _compact_json(
                {
                    "action": "submit_repairs",
                    "repairs": [
                        {
                            "row": row,
                            "column": column,
                            "new_value": "clean",
                            "reason": "not allowed",
                        }
                    ],
                }
            ),
            "why_invalid": "repair objects must contain exactly row, column, and new_value",
        },
    ]


def _heldout_leakage(record: Mapping[str, Any], user_payload: Mapping[str, Any]) -> list[int]:
    provenance = record.get("provenance", {})
    eval_rows = provenance.get("eval_rows", []) if isinstance(provenance, Mapping) else []
    if not isinstance(eval_rows, list):
        return []
    heldout = {int(row) for row in eval_rows if isinstance(row, int)}
    exposed: set[int] = set()
    for key in ("target_rows", "context_rows"):
        rows = user_payload.get(key, [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, Mapping) and "_row" in row:
                exposed.add(int(str(row["_row"])))
    return sorted(heldout & exposed)


def _distill_record(
    record: Mapping[str, Any],
    *,
    role: str,
    copy_index: int,
) -> dict[str, Any]:
    user_payload = _user_payload(record)
    repairs = _completion_repairs(record)
    completion = _compact_action(repairs)
    source_id = str(record.get("trajectory_id", f"record-{copy_index}"))
    payload = dict(record)
    payload.pop("messages", None)
    payload["prompt"] = _prompt_messages(user_payload)
    payload["completion"] = completion
    payload["fix"] = repairs
    payload["prompt_contract_version"] = CONTRACT_VERSION_V3
    payload["training_format"] = "prompt_completion"
    payload["curriculum_version"] = CURRICULUM_VERSION
    payload["curriculum_source_version"] = str(
        record.get("curriculum_version", "expert_v8_schema_distill")
    )
    payload["curriculum_role"] = role
    payload["curriculum_source_trajectory_id"] = source_id
    payload["negative_contrast_examples"] = _negative_contrasts(user_payload)
    payload["negative_contrast_targets_supervised"] = False
    payload["trajectory_id"] = f"{source_id}:{CURRICULUM_VERSION}:{role}:{copy_index}"
    return payload


def _micro_user_payload(*, row: int, values: dict[str, str]) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION_V3,
        "schema_summary": {
            "dataset": "action_envelope_micro_drill",
            "columns": ["value", "status"],
            "source_revision": "synthetic_action_envelope_v1",
        },
        "allowed_columns": ["value", "status"],
        "valid_rows": [row, row + 1],
        "target_rows": [{"_row": str(row), **values}],
        "context_rows": [],
        "dataset_note": "Synthetic envelope drill; learn only the valid JSON action form.",
        "label_source": "synthetic_action_envelope_v1",
    }


def _micro_record(index: int, *, action_kind: str) -> dict[str, Any]:
    row = index % 11
    if action_kind == "finish":
        repairs: list[dict[str, Any]] = []
        user_payload = _micro_user_payload(row=row, values={"value": "clean", "status": "ok"})
    elif action_kind == "submit_one":
        repairs = [{"row": row, "column": "value", "new_value": "clean"}]
        user_payload = _micro_user_payload(row=row, values={"value": "clen", "status": "ok"})
    elif action_kind == "submit_two":
        repairs = [
            {"row": row, "column": "value", "new_value": "clean"},
            {"row": row + 1, "column": "status", "new_value": "ok"},
        ]
        user_payload = _micro_user_payload(row=row, values={"value": "clen", "status": "o k"})
        user_payload["target_rows"].append({"_row": str(row + 1), "value": "clean", "status": "o k"})
    else:  # pragma: no cover - guarded by callers
        raise ValueError(f"Unknown action envelope drill: {action_kind}")
    return {
        "schema_version": "expert_v4",
        "dataset": "action_envelope_micro_drill",
        "difficulty": "schema",
        "inferability": "not_inferable_from_prompt" if action_kind == "finish" else "deterministic_normalization",
        "prompt_contract_version": CONTRACT_VERSION_V3,
        "training_format": "prompt_completion",
        "curriculum_version": CURRICULUM_VERSION,
        "curriculum_source_version": "synthetic_action_envelope_v1",
        "curriculum_role": f"action_envelope_micro_{action_kind}",
        "trajectory_id": f"action_envelope_micro_drill:{index:04d}:{action_kind}:{CURRICULUM_VERSION}",
        "prompt": _prompt_messages(user_payload),
        "completion": _compact_action(repairs),
        "fix": repairs,
        "negative_contrast_examples": _negative_contrasts(user_payload),
        "negative_contrast_targets_supervised": False,
        "provenance": {
            "collection_method": "synthetic_action_envelope_v1",
            "split": "train",
            "eval_rows": [],
            "heldout_policy": "synthetic_micro_drills_have_no_benchmark_rows",
        },
    }


def _add_micro_drills(
    selected: list[dict[str, Any]],
    *,
    submit_envelope_drills: int,
    finish_envelope_drills: int,
) -> None:
    for index in range(finish_envelope_drills):
        selected.append(_micro_record(index, action_kind="finish"))
    for index in range(submit_envelope_drills):
        action_kind = "submit_two" if index % 4 == 0 else "submit_one"
        selected.append(_micro_record(index, action_kind=action_kind))


def _validate_record(
    record: Mapping[str, Any],
    *,
    index: int,
) -> tuple[str | None, dict[str, Any] | None]:
    prompt = record.get("prompt")
    completion = record.get("completion")
    if not isinstance(prompt, list) or len(prompt) != 2:
        return "prompt_shape_error", {"index": index, "trajectory_id": record.get("trajectory_id")}
    if [message.get("role") for message in prompt if isinstance(message, dict)] != ["system", "user"]:
        return "prompt_role_error", {"index": index, "trajectory_id": record.get("trajectory_id")}
    if not isinstance(completion, str) or not completion:
        return "completion_shape_error", {"index": index, "trajectory_id": record.get("trajectory_id")}
    if "```" in completion:
        return "completion_code_fence", {"index": index, "trajectory_id": record.get("trajectory_id")}
    if "reason" in completion.lower():
        return "completion_reason_text", {"index": index, "trajectory_id": record.get("trajectory_id")}
    user_message = cast(Mapping[str, Any], prompt[1])
    if completion in str(user_message.get("content", "")):
        return "completion_leaked_into_user_prompt", {
            "index": index,
            "trajectory_id": record.get("trajectory_id"),
        }
    user_payload = json.loads(str(user_message["content"]))
    leaked_rows = _heldout_leakage(record, user_payload)
    if leaked_rows:
        return "heldout_leakage", {
            "index": index,
            "trajectory_id": record.get("trajectory_id"),
            "rows": leaked_rows[:10],
        }
    negative_contrasts = record.get("negative_contrast_examples", [])
    if not isinstance(negative_contrasts, list) or not negative_contrasts:
        return "negative_contrast_examples_missing", {
            "index": index,
            "trajectory_id": record.get("trajectory_id"),
        }
    for contrast in negative_contrasts:
        if isinstance(contrast, Mapping) and completion == contrast.get("output"):
            return "negative_contrast_target_leakage", {
                "index": index,
                "trajectory_id": record.get("trajectory_id"),
                "kind": contrast.get("kind"),
            }
    if record.get("negative_contrast_targets_supervised") is not False:
        return "negative_contrast_targets_supervised", {
            "index": index,
            "trajectory_id": record.get("trajectory_id"),
        }
    parsed = parse_repair_action(
        completion,
        allowed_columns=user_payload.get("allowed_columns", []),
        valid_rows=user_payload.get("valid_rows", []),
        require_explicit_action=True,
    )
    if not parsed.ok or parsed.action is None:
        return "completion_parse_failure", {
            "index": index,
            "trajectory_id": record.get("trajectory_id"),
            "error_kind": parsed.error_kind,
            "error_message": parsed.error_message,
        }
    if parsed.action.action == "finish" and parsed.action.repairs:
        return "finish_with_repairs", {"index": index, "trajectory_id": record.get("trajectory_id")}
    return None, None


def build_action_envelope_curriculum(
    records: Iterable[Mapping[str, Any]],
    *,
    submit_repair_copies: int,
    finish_copies: int,
    submit_envelope_drills: int,
    finish_envelope_drills: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return SFT-v9 prompt-completion records and a preflight report."""
    selected: list[dict[str, Any]] = []
    invalid_samples: list[dict[str, Any]] = []
    validation_failures: list[dict[str, Any]] = []
    leakage_samples: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for index, record in enumerate(records):
        try:
            repairs = _completion_repairs(record)
            role = "action_envelope_real_submit" if repairs else "action_envelope_real_finish"
            copies = submit_repair_copies if repairs else finish_copies
            for copy_index in range(copies):
                selected.append(_distill_record(record, role=role, copy_index=copy_index))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            if len(invalid_samples) < 25:
                invalid_samples.append({"index": index, "error": str(exc)})

    _add_micro_drills(
        selected,
        submit_envelope_drills=submit_envelope_drills,
        finish_envelope_drills=finish_envelope_drills,
    )

    for index, record in enumerate(selected):
        failure, sample = _validate_record(record, index=index)
        if failure:
            counts[failure] += 1
            if failure == "heldout_leakage" and sample is not None and len(leakage_samples) < 25:
                leakage_samples.append(sample)
            if sample is not None and len(validation_failures) < 25:
                validation_failures.append({"failure": failure, **sample})
            continue
        prompt = cast(list[dict[str, str]], record["prompt"])
        completion = str(record["completion"])
        user_payload = json.loads(prompt[1]["content"])
        parsed = parse_repair_action(
            completion,
            allowed_columns=user_payload.get("allowed_columns", []),
            valid_rows=user_payload.get("valid_rows", []),
            require_explicit_action=True,
        )
        action = parsed.action.action if parsed.action is not None else "unknown"
        inferability = str(record.get("inferability", "unknown"))
        counts["completion_parse_ok"] += int(parsed.ok)
        counts[f"action:{action}"] += 1
        counts[f"{record.get('dataset', 'unknown')}:{inferability}:{action}"] += 1
        counts["repair_cells"] += len(parsed.action.repairs if parsed.action is not None else [])
        if record.get("training_format") != "prompt_completion":
            counts["training_format_mismatches"] += 1
        if record.get("messages") is not None:
            counts["legacy_messages_present"] += 1
        if user_payload.get("contract_version") != CONTRACT_VERSION_V3:
            counts["user_contract_version_mismatches"] += 1
        if record.get("prompt_contract_version") != CONTRACT_VERSION_V3:
            counts["record_contract_version_mismatches"] += 1
        negative_contrasts = record.get("negative_contrast_examples", [])
        if isinstance(negative_contrasts, list):
            counts["negative_contrast_examples"] += len(negative_contrasts)
            for contrast in negative_contrasts:
                if isinstance(contrast, Mapping) and contrast.get("kind"):
                    counts[f"negative:{contrast['kind']}"] += 1

    output_records = len(selected)
    submit_records = counts["action:submit_repairs"]
    finish_records = counts["action:finish"]
    submit_ratio = round(submit_records / output_records, 4) if output_records else 0.0
    finish_ratio = round(finish_records / output_records, 4) if output_records else 0.0
    parse_success = round(counts["completion_parse_ok"] / output_records, 4) if output_records else 0.0

    blockers: list[str] = []
    if invalid_samples:
        blockers.append("invalid_source_records")
    if validation_failures:
        blockers.append("prompt_completion_validation_failures")
    if leakage_samples:
        blockers.append("heldout_leakage")
    if submit_records < 1000:
        blockers.append("submit_repair_records_under_1000")
    if finish_records < 1000:
        blockers.append("finish_records_under_1000")
    if not (0.40 <= finish_ratio <= 0.55):
        blockers.append("finish_ratio_outside_0.40_0.55")
    if not (0.45 <= submit_ratio <= 0.60):
        blockers.append("submit_ratio_outside_0.45_0.60")
    if parse_success < 1.0:
        blockers.append("completion_parse_success_below_1.0")
    for metric in (
        "completion_code_fence",
        "completion_reason_text",
        "completion_leaked_into_user_prompt",
        "finish_with_repairs",
        "training_format_mismatches",
        "legacy_messages_present",
        "user_contract_version_mismatches",
        "record_contract_version_mismatches",
        "negative_contrast_examples_missing",
        "negative_contrast_target_leakage",
        "negative_contrast_targets_supervised",
    ):
        if counts[metric]:
            blockers.append(f"{metric}_present")

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "curriculum_version": CURRICULUM_VERSION,
        "ok": not blockers,
        "status": "pass" if not blockers else "block",
        "settings": {
            "submit_repair_copies": submit_repair_copies,
            "finish_copies": finish_copies,
            "submit_envelope_drills": submit_envelope_drills,
            "finish_envelope_drills": finish_envelope_drills,
        },
        "metrics": {
            "output_records": output_records,
            "prompt_completion_records": output_records,
            "submit_repair_records": submit_records,
            "finish_records": finish_records,
            "submit_ratio": submit_ratio,
            "finish_ratio": finish_ratio,
            "repair_cells": counts["repair_cells"],
            "completion_parse_failure_count": counts["completion_parse_failure"],
            "completion_parse_success_rate": parse_success,
            "completion_code_fence_count": counts["completion_code_fence"],
            "completion_reason_text_count": counts["completion_reason_text"],
            "finish_with_repairs": counts["finish_with_repairs"],
            "legacy_messages_present": counts["legacy_messages_present"],
            "training_format_mismatches": counts["training_format_mismatches"],
            "user_contract_version_mismatches": counts["user_contract_version_mismatches"],
            "record_contract_version_mismatches": counts["record_contract_version_mismatches"],
            "heldout_leakage_count": len(leakage_samples),
            "invalid_count": len(invalid_samples),
            "validation_failure_count": len(validation_failures),
            "negative_contrast_examples": counts["negative_contrast_examples"],
            "negative_contrast_target_leakage_count": counts["negative_contrast_target_leakage"],
            "shape": {key: value for key, value in sorted(counts.items()) if ":" in key},
            "negative_contrast_shape": {
                key.removeprefix("negative:"): value
                for key, value in sorted(counts.items())
                if key.startswith("negative:")
            },
        },
        "label_mask_audit": {
            "ok": True,
            "method": "structural_prompt_completion_split",
            "prompt_contains_roles": ["system", "user"],
            "completion_column": "single compact assistant JSON action",
            "completion_only_loss_required": True,
            "runner_required": "Kaggle runner must pass completion_only_loss=True to TRL SFTConfig.",
        },
        "product_constrained_preflight": {
            "schema_version": "dataforge_product_constrained_eval_v1",
            "status": "curriculum_action_shape_preflight",
            "parse_structural_success_rate": parse_success,
            "strict_macro_f1": 0.0,
            "deterministic_normalization_f1": 0.0,
            "not_inferable_from_prompt_f1": 0.0,
            "rejected_invalid_repairs": counts["completion_parse_failure"],
            "verifier_accepted_repairs": counts["repair_cells"],
            "claim_policy": "Curriculum parse success is a preflight only; product repair quality requires held-out constrained eval.",
        },
        "blockers": sorted(set(blockers)),
        "invalid_samples": invalid_samples,
        "validation_failures": validation_failures,
        "leakage_samples": leakage_samples,
        "limitations": [
            "Negative contrast examples are audit metadata and are never assistant targets.",
            "Strict held-out eval remains unchanged; this curriculum only changes the SFT action-envelope objective.",
            "Product-constrained parse reliability must be reported separately from repair F1.",
        ],
    }
    return selected, report


def write_action_envelope_curriculum(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
    submit_repair_copies: int,
    finish_copies: int,
    submit_envelope_drills: int,
    finish_envelope_drills: int,
) -> dict[str, Any]:
    selected, report = build_action_envelope_curriculum(
        _load_jsonl(input_path),
        submit_repair_copies=submit_repair_copies,
        finish_copies=finish_copies,
        submit_envelope_drills=submit_envelope_drills,
        finish_envelope_drills=finish_envelope_drills,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            for record in selected
        ),
        encoding="utf-8",
    )
    report["artifacts"] = {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "report_path": str(report_path),
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--submit-repair-copies", type=int, default=1)
    parser.add_argument("--finish-copies", type=int, default=1)
    parser.add_argument("--submit-envelope-drills", type=int, default=DEFAULT_SUBMIT_ENVELOPE_DRILLS)
    parser.add_argument("--finish-envelope-drills", type=int, default=DEFAULT_FINISH_ENVELOPE_DRILLS)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = write_action_envelope_curriculum(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
        submit_repair_copies=args.submit_repair_copies,
        finish_copies=args.finish_copies,
        submit_envelope_drills=args.submit_envelope_drills,
        finish_envelope_drills=args.finish_envelope_drills,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
