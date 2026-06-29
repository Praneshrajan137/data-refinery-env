"""Build the SFT-v8 schema-distill prompt-completion curriculum."""

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

CURRICULUM_VERSION = "expert_v8_schema_distill"
REPORT_SCHEMA_VERSION = "dataforge_sft_v8_schema_distill_curriculum_report_v1"
DEFAULT_INPUT = Path("data/sft_traj/expert_v6_contract_minimal.jsonl")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v8_schema_distill.jsonl")
DEFAULT_REPORT = Path("eval/results/sft_v8_schema_distill_curriculum_report.json")
DEFAULT_SUBMIT_MICRO_DRILLS = 300
DEFAULT_FINISH_MICRO_DRILLS = 200


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


def _message(record: Mapping[str, Any], role: str) -> dict[str, Any]:
    messages = record.get("messages")
    if not isinstance(messages, list):
        raise ValueError("record.messages must be a list.")
    for message in messages:
        if isinstance(message, dict) and message.get("role") == role:
            return cast(dict[str, Any], message)
    raise ValueError(f"record.messages is missing role={role!r}.")


def _user_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    content = _message(record, "user").get("content")
    if not isinstance(content, str):
        raise ValueError("user message content must be a string.")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("user message content must decode to an object.")
    return cast(dict[str, Any], payload)


def _minimal_repairs(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    repairs = record.get("fix")
    if not isinstance(repairs, list):
        return []
    minimal: list[dict[str, Any]] = []
    for repair in repairs[:2]:
        if not isinstance(repair, Mapping):
            continue
        minimal.append(
            {
                "row": int(repair["row"]),
                "column": str(repair["column"]),
                "new_value": str(repair["new_value"]),
            }
        )
    return minimal


def _compact_action(repairs: list[dict[str, Any]]) -> str:
    payload = {
        "action": "submit_repairs" if repairs else "finish",
        "repairs": repairs,
    }
    return json.dumps(payload, separators=(",", ":"))


def _prompt_messages(user_payload: Mapping[str, Any]) -> list[dict[str, str]]:
    payload = dict(user_payload)
    payload["contract_version"] = CONTRACT_VERSION_V3
    return [
        {"role": "system", "content": SYSTEM_PROMPT_V3},
        {
            "role": "user",
            "content": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        },
    ]


def _completion_action(record: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    inferability = str(record.get("inferability", "unknown"))
    repairs = _minimal_repairs(record)
    if inferability in {"external_reference_required", "not_inferable_from_prompt"}:
        repairs = []
    completion = _compact_action(repairs)
    return repairs, completion


def _distill_record(
    record: Mapping[str, Any],
    *,
    role: str,
    copy_index: int,
) -> dict[str, Any]:
    user_payload = _user_payload(record)
    repairs, completion = _completion_action(record)
    source_id = str(record.get("trajectory_id", f"record-{copy_index}"))
    prompt = _prompt_messages(user_payload)
    payload = dict(record)
    payload.pop("messages", None)
    payload["prompt"] = prompt
    payload["completion"] = completion
    payload["fix"] = repairs
    payload["prompt_contract_version"] = CONTRACT_VERSION_V3
    payload["training_format"] = "prompt_completion"
    payload["curriculum_version"] = CURRICULUM_VERSION
    payload["curriculum_source_version"] = str(
        record.get("curriculum_version", "expert_v6_contract_minimal")
    )
    payload["curriculum_role"] = role
    payload["curriculum_source_trajectory_id"] = source_id
    payload["trajectory_id"] = f"{source_id}:{CURRICULUM_VERSION}:{role}:{copy_index}"
    return payload


def _micro_user_payload(*, row: int, columns: list[str], values: dict[str, str]) -> dict[str, Any]:
    target = {"_row": str(row), **values}
    return {
        "contract_version": CONTRACT_VERSION_V3,
        "schema_summary": {
            "dataset": "schema_micro_drill",
            "columns": columns,
            "source_revision": "synthetic_schema_distill_v1",
        },
        "allowed_columns": columns,
        "valid_rows": [row],
        "target_rows": [target],
        "context_rows": [],
        "dataset_note": "Synthetic schema drill; only the JSON action envelope matters.",
        "label_source": "synthetic_schema_distill_v1",
    }


def _micro_record(index: int, *, action_kind: str) -> dict[str, Any]:
    row = index % 7
    if action_kind == "finish":
        repairs: list[dict[str, Any]] = []
        user_payload = _micro_user_payload(
            row=row,
            columns=["value", "status"],
            values={"value": "clean", "status": "ok"},
        )
    elif action_kind == "submit_one":
        repairs = [{"row": row, "column": "value", "new_value": "clean"}]
        user_payload = _micro_user_payload(
            row=row,
            columns=["value", "status"],
            values={"value": "clen", "status": "ok"},
        )
    elif action_kind == "submit_two":
        repairs = [
            {"row": row, "column": "value", "new_value": "clean"},
            {"row": row, "column": "status", "new_value": "ok"},
        ]
        user_payload = _micro_user_payload(
            row=row,
            columns=["value", "status"],
            values={"value": "clen", "status": "o k"},
        )
    else:  # pragma: no cover - guarded by callers
        raise ValueError(f"Unknown micro drill action: {action_kind}")
    return {
        "schema_version": "expert_v4",
        "dataset": "schema_micro_drill",
        "difficulty": "schema",
        "inferability": "not_inferable_from_prompt" if action_kind == "finish" else "deterministic_normalization",
        "prompt_contract_version": CONTRACT_VERSION_V3,
        "training_format": "prompt_completion",
        "curriculum_version": CURRICULUM_VERSION,
        "curriculum_source_version": "synthetic_schema_distill_v1",
        "curriculum_role": f"schema_micro_{action_kind}",
        "trajectory_id": f"schema_micro_drill:{index:04d}:{action_kind}:{CURRICULUM_VERSION}",
        "prompt": _prompt_messages(user_payload),
        "completion": _compact_action(repairs),
        "fix": repairs,
        "provenance": {
            "collection_method": "synthetic_schema_distill_v1",
            "split": "train",
            "eval_rows": [],
            "heldout_policy": "synthetic_micro_drills_have_no_benchmark_rows",
        },
    }


def _add_micro_drills(
    selected: list[dict[str, Any]],
    *,
    submit_micro_drills: int,
    finish_micro_drills: int,
) -> None:
    for index in range(finish_micro_drills):
        selected.append(_micro_record(index, action_kind="finish"))
    for index in range(submit_micro_drills):
        action_kind = "submit_two" if index % 5 == 0 else "submit_one"
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
    user_payload = json.loads(str(cast(Mapping[str, Any], prompt[1])["content"]))
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


def build_schema_distill_curriculum(
    records: Iterable[Mapping[str, Any]],
    *,
    submit_repair_copies: int,
    finish_copies: int,
    submit_micro_drills: int,
    finish_micro_drills: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return prompt-completion records and a validation report."""
    selected: list[dict[str, Any]] = []
    invalid_samples: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for index, record in enumerate(records):
        try:
            repairs, _completion = _completion_action(record)
            role = "schema_distill_submit_repairs" if repairs else "schema_distill_finish_empty"
            copies = submit_repair_copies if repairs else finish_copies
            for copy_index in range(copies):
                selected.append(_distill_record(record, role=role, copy_index=copy_index))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            if len(invalid_samples) < 25:
                invalid_samples.append({"index": index, "error": str(exc)})

    _add_micro_drills(
        selected,
        submit_micro_drills=submit_micro_drills,
        finish_micro_drills=finish_micro_drills,
    )

    validation_failures: list[dict[str, Any]] = []
    for index, record in enumerate(selected):
        failure, sample = _validate_record(record, index=index)
        if failure:
            counts[failure] += 1
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

    output_records = len(selected)
    submit_records = counts["action:submit_repairs"]
    finish_records = counts["action:finish"]
    finish_ratio = round(finish_records / output_records, 4) if output_records else 0.0
    submit_ratio = round(submit_records / output_records, 4) if output_records else 0.0

    blockers: list[str] = []
    if invalid_samples:
        blockers.append("invalid_source_records")
    if validation_failures:
        blockers.append("prompt_completion_validation_failures")
    if submit_records < 1000:
        blockers.append("submit_repair_records_under_1000")
    if finish_records < 900:
        blockers.append("finish_records_under_900")
    if not (0.40 <= finish_ratio <= 0.50):
        blockers.append("finish_ratio_outside_0.40_0.50")
    if not (0.50 <= submit_ratio <= 0.60):
        blockers.append("submit_ratio_outside_0.50_0.60")
    for metric in (
        "completion_code_fence",
        "completion_reason_text",
        "completion_leaked_into_user_prompt",
        "finish_with_repairs",
        "training_format_mismatches",
        "legacy_messages_present",
        "user_contract_version_mismatches",
        "record_contract_version_mismatches",
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
            "submit_micro_drills": submit_micro_drills,
            "finish_micro_drills": finish_micro_drills,
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
            "completion_code_fence_count": counts["completion_code_fence"],
            "completion_reason_text_count": counts["completion_reason_text"],
            "finish_with_repairs": counts["finish_with_repairs"],
            "legacy_messages_present": counts["legacy_messages_present"],
            "training_format_mismatches": counts["training_format_mismatches"],
            "user_contract_version_mismatches": counts["user_contract_version_mismatches"],
            "record_contract_version_mismatches": counts["record_contract_version_mismatches"],
            "invalid_count": len(invalid_samples),
            "validation_failure_count": len(validation_failures),
            "shape": {key: value for key, value in sorted(counts.items()) if ":" in key},
        },
        "label_mask_audit": {
            "ok": True,
            "method": "structural_prompt_completion_split",
            "prompt_contains_roles": ["system", "user"],
            "completion_column": "single compact assistant JSON action",
            "runner_required": "Kaggle runner must pass completion_only_loss=True to TRL SFTConfig.",
        },
        "blockers": sorted(set(blockers)),
        "invalid_samples": invalid_samples,
        "validation_failures": validation_failures,
        "limitations": [
            "This curriculum changes the SFT training interface only; strict held-out eval remains unchanged.",
            "Product-grade JSON reliability still requires a constrained decoding track; raw-model eval remains reported separately.",
        ],
    }
    return selected, report


def write_schema_distill_curriculum(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
    submit_repair_copies: int,
    finish_copies: int,
    submit_micro_drills: int,
    finish_micro_drills: int,
) -> dict[str, Any]:
    selected, report = build_schema_distill_curriculum(
        _load_jsonl(input_path),
        submit_repair_copies=submit_repair_copies,
        finish_copies=finish_copies,
        submit_micro_drills=submit_micro_drills,
        finish_micro_drills=finish_micro_drills,
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
    parser.add_argument("--finish-copies", type=int, default=2)
    parser.add_argument("--submit-micro-drills", type=int, default=DEFAULT_SUBMIT_MICRO_DRILLS)
    parser.add_argument("--finish-micro-drills", type=int, default=DEFAULT_FINISH_MICRO_DRILLS)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = write_schema_distill_curriculum(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
        submit_repair_copies=args.submit_repair_copies,
        finish_copies=args.finish_copies,
        submit_micro_drills=args.submit_micro_drills,
        finish_micro_drills=args.finish_micro_drills,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
