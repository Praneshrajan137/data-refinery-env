"""Build the SFT-v7 parse-latch curriculum from SFT-v6 contract-minimal records."""

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

CURRICULUM_VERSION = "expert_v7_parse_latch"
REPORT_SCHEMA_VERSION = "dataforge_sft_v7_parse_latch_curriculum_report_v1"
DEFAULT_INPUT = Path("data/sft_traj/expert_v6_contract_minimal.jsonl")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v7_parse_latch.jsonl")
DEFAULT_REPORT = Path("eval/results/sft_v7_parse_latch_curriculum_report.json")


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
            return message
    raise ValueError(f"record.messages is missing role={role!r}.")


def _user_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    content = _message(record, "user").get("content")
    payload = json.loads(str(content))
    if not isinstance(payload, dict):
        raise ValueError("user message content must decode to an object.")
    return cast(dict[str, Any], payload)


def _minimal_repairs(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    repairs = record.get("fix")
    if not isinstance(repairs, list):
        return []
    minimal: list[dict[str, Any]] = []
    for repair in repairs:
        if not isinstance(repair, Mapping):
            continue
        minimal.append(
            {
                "column": str(repair["column"]),
                "new_value": str(repair["new_value"]),
                "row": int(repair["row"]),
            }
        )
    return minimal


def _latch_record(record: Mapping[str, Any], *, role: str, copy_index: int) -> dict[str, Any]:
    repairs = _minimal_repairs(record)
    action = "submit_repairs" if repairs else "finish"
    assistant_payload = {"action": action, "repairs": repairs}
    user_payload = _user_payload(record)
    user_payload["contract_version"] = CONTRACT_VERSION_V3

    payload = dict(record)
    messages = [dict(message) for message in cast(list[dict[str, Any]], record["messages"])]
    for message in messages:
        if message.get("role") == "system":
            message["content"] = SYSTEM_PROMPT_V3
        elif message.get("role") == "user":
            message["content"] = json.dumps(
                user_payload,
                sort_keys=True,
                separators=(",", ":"),
            )
        elif message.get("role") == "assistant":
            message["content"] = json.dumps(
                assistant_payload,
                sort_keys=True,
                separators=(",", ":"),
            )
    source_id = str(record.get("trajectory_id", f"record-{copy_index}"))
    payload["messages"] = messages
    payload["fix"] = repairs
    payload["prompt_contract_version"] = CONTRACT_VERSION_V3
    payload["curriculum_version"] = CURRICULUM_VERSION
    payload["curriculum_source_version"] = str(
        record.get("curriculum_version", "expert_v6_contract_minimal")
    )
    payload["curriculum_role"] = role
    payload["curriculum_source_trajectory_id"] = source_id
    payload["trajectory_id"] = f"{source_id}:{CURRICULUM_VERSION}:{role}:{copy_index}"
    return payload


def build_parse_latch_curriculum(
    records: Iterable[Mapping[str, Any]],
    *,
    submit_repair_copies: int,
    finish_copies: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return parse-latch records and a validation report."""
    selected: list[dict[str, Any]] = []
    invalid_samples: list[dict[str, Any]] = []
    parse_failures: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for index, record in enumerate(records):
        repairs = _minimal_repairs(record)
        inferability = str(record.get("inferability", "unknown"))
        copies = submit_repair_copies if repairs else finish_copies
        role = "parse_latch_submit_repairs" if repairs else "parse_latch_finish_empty"
        for copy_index in range(copies):
            try:
                latched = _latch_record(record, role=role, copy_index=copy_index)
                user_payload = _user_payload(latched)
                assistant_content = str(_message(latched, "assistant")["content"])
                parsed = parse_repair_action(
                    assistant_content,
                    allowed_columns=user_payload.get("allowed_columns", []),
                    valid_rows=user_payload.get("valid_rows", []),
                    require_explicit_action=True,
                )
                if not parsed.ok or parsed.action is None:
                    if len(parse_failures) < 25:
                        parse_failures.append(
                            {
                                "index": index,
                                "copy_index": copy_index,
                                "trajectory_id": latched.get("trajectory_id"),
                                "error_kind": parsed.error_kind,
                                "error_message": parsed.error_message,
                            }
                        )
                    continue
                if parsed.action.action == "finish" and parsed.action.repairs:
                    counts["finish_with_repairs"] += 1
                    continue
                if '"reason"' in assistant_content:
                    counts["assistant_reason_fields"] += 1
                system_content = str(_message(latched, "system").get("content", ""))
                if '"reason"' in system_content:
                    counts["system_reason_field_mentions"] += 1
                if "```" in system_content or "<json" in system_content.lower():
                    counts["system_wrapper_mentions"] += 1
                if user_payload.get("contract_version") != CONTRACT_VERSION_V3:
                    counts["user_contract_version_mismatches"] += 1
                if latched.get("prompt_contract_version") != CONTRACT_VERSION_V3:
                    counts["record_contract_version_mismatches"] += 1
                counts[f"action:{parsed.action.action}"] += 1
                counts[
                    f"{latched.get('dataset', 'unknown')}:{inferability}:{parsed.action.action}"
                ] += 1
                counts["repair_cells"] += len(parsed.action.repairs)
                selected.append(latched)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                if len(invalid_samples) < 25:
                    invalid_samples.append(
                        {"index": index, "copy_index": copy_index, "error": str(exc)}
                    )

    blockers: list[str] = []
    if invalid_samples:
        blockers.append("invalid_records")
    if parse_failures:
        blockers.append("parse_latch_assistant_parse_failures")
    if counts["action:submit_repairs"] < 1800:
        blockers.append("submit_repair_records_under_1800")
    if counts["action:finish"] < 450:
        blockers.append("finish_records_under_450")
    for metric in (
        "assistant_reason_fields",
        "system_reason_field_mentions",
        "system_wrapper_mentions",
        "finish_with_repairs",
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
        },
        "metrics": {
            "output_records": len(selected),
            "submit_repair_records": counts["action:submit_repairs"],
            "finish_records": counts["action:finish"],
            "repair_cells": counts["repair_cells"],
            "assistant_reason_fields": counts["assistant_reason_fields"],
            "system_reason_field_mentions": counts["system_reason_field_mentions"],
            "system_wrapper_mentions": counts["system_wrapper_mentions"],
            "finish_with_repairs": counts["finish_with_repairs"],
            "user_contract_version_mismatches": counts["user_contract_version_mismatches"],
            "record_contract_version_mismatches": counts["record_contract_version_mismatches"],
            "invalid_count": len(invalid_samples),
            "parse_failure_count": len(parse_failures),
            "shape": {key: value for key, value in sorted(counts.items()) if ":" in key},
        },
        "blockers": sorted(set(blockers)),
        "invalid_samples": invalid_samples,
        "parse_failures": parse_failures,
        "limitations": [
            "This curriculum changes SFT targets only; strict held-out eval remains unchanged.",
            "Oversampling duplicates train-only records to emphasize action/repair consistency.",
        ],
    }
    return selected, report


def write_parse_latch_curriculum(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
    submit_repair_copies: int,
    finish_copies: int,
) -> dict[str, Any]:
    selected, report = build_parse_latch_curriculum(
        _load_jsonl(input_path),
        submit_repair_copies=submit_repair_copies,
        finish_copies=finish_copies,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in selected
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
    parser.add_argument("--submit-repair-copies", type=int, default=2)
    parser.add_argument("--finish-copies", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = write_parse_latch_curriculum(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
        submit_repair_copies=args.submit_repair_copies,
        finish_copies=args.finish_copies,
    )
    print(
        json.dumps(
            {
                "schema_version": report["schema_version"],
                "ok": report["ok"],
                "blockers": report["blockers"],
                "metrics": report["metrics"],
            },
            sort_keys=True,
        )
    )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
