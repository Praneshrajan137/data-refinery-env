"""Build a contract-minimal SFT curriculum from the failed SFT-v5 evidence path."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, cast

from dataforge.repair_contract import parse_repair_action

CURRICULUM_VERSION = "expert_v6_contract_minimal"
REPORT_SCHEMA_VERSION = "dataforge_sft_v6_contract_minimal_curriculum_report_v1"
DEFAULT_INPUT = Path("data/sft_traj/expert_v5_repair_curriculum.jsonl")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v6_contract_minimal.jsonl")
DEFAULT_REPORT = Path("eval/results/sft_v6_contract_minimal_curriculum_report.json")
CONTRACT_FIRST_SYSTEM_PROMPT = (
    "You repair tabular data by proposing exact cell replacements. "
    "Output exactly one compact JSON object and nothing else. "
    'Use {"action":"finish","repairs":[]} when no cells should be changed. '
    'Use {"action":"submit_repairs","repairs":[{"row":0,"column":"column","new_value":"value"}]} '
    "only when a cell should be changed. Each repair object must have exactly row, "
    "column, and new_value keys. The column value must exactly match one string from "
    "allowed_columns, and row must be an integer from valid_rows. No prose, comments, "
    "wrappers, or extra keys."
)


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
    if not isinstance(content, str):
        raise ValueError("user message content must be a string.")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("user message content must decode to an object.")
    return cast(dict[str, Any], payload)


def _assistant_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    content = _message(record, "assistant").get("content")
    if not isinstance(content, str):
        raise ValueError("assistant message content must be a string.")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("assistant message content must decode to an object.")
    return cast(dict[str, Any], payload)


def _minimal_repairs(payload: Mapping[str, Any], *, max_repairs_per_record: int) -> list[dict[str, Any]]:
    repairs = payload.get("repairs")
    if not isinstance(repairs, list):
        return []
    minimal: list[dict[str, Any]] = []
    for repair in repairs[:max_repairs_per_record]:
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


def _with_minimal_assistant(
    record: Mapping[str, Any],
    *,
    max_repairs_per_record: int,
) -> dict[str, Any]:
    payload = dict(record)
    source_id = str(record.get("trajectory_id", ""))
    assistant_payload = _assistant_payload(record)
    repairs = _minimal_repairs(assistant_payload, max_repairs_per_record=max_repairs_per_record)
    action = "submit_repairs" if repairs else "finish"
    minimal_assistant = {"action": action, "repairs": repairs}
    messages = [dict(message) for message in cast(list[dict[str, Any]], record["messages"])]
    for message in messages:
        if message.get("role") == "system":
            message["content"] = CONTRACT_FIRST_SYSTEM_PROMPT
            continue
        if message.get("role") == "assistant":
            message["content"] = json.dumps(
                minimal_assistant,
                sort_keys=True,
                separators=(",", ":"),
            )
            break
    payload["messages"] = messages
    payload["fix"] = repairs
    payload["curriculum_version"] = CURRICULUM_VERSION
    payload["curriculum_source_version"] = str(record.get("curriculum_version", "expert_v5"))
    payload["curriculum_role"] = f"contract_minimal_{action}"
    payload["contract_minimal_max_repairs"] = max_repairs_per_record
    if source_id:
        payload["curriculum_source_trajectory_id"] = source_id
        payload["trajectory_id"] = f"{source_id}:{CURRICULUM_VERSION}:{action}"
    return payload


def build_contract_minimal_curriculum(
    records: Iterable[Mapping[str, Any]],
    *,
    max_repairs_per_record: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return contract-minimal records and a validation report."""
    selected: list[dict[str, Any]] = []
    invalid_samples: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    parse_failures: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        try:
            minimal = _with_minimal_assistant(
                record,
                max_repairs_per_record=max_repairs_per_record,
            )
            user_payload = _user_payload(minimal)
            assistant = _message(minimal, "assistant")
            content = str(assistant["content"])
            parsed = parse_repair_action(
                content,
                allowed_columns=user_payload.get("allowed_columns", []),
                valid_rows=user_payload.get("valid_rows", []),
                require_explicit_action=True,
            )
            if not parsed.ok:
                if len(parse_failures) < 25:
                    parse_failures.append(
                        {
                            "index": index,
                            "trajectory_id": minimal.get("trajectory_id"),
                            "error_kind": parsed.error_kind,
                            "error_message": parsed.error_message,
                        }
                    )
                continue
            repairs = parsed.action.repairs if parsed.action is not None else []
            action = parsed.action.action if parsed.action is not None else "unknown"
            counts[f"{minimal.get('dataset', 'unknown')}:{minimal.get('inferability', 'unknown')}:{action}"] += 1
            counts[f"action:{action}"] += 1
            counts["repair_cells"] += len(repairs)
            if '"reason"' in content:
                counts["assistant_reason_fields"] += 1
            system = _message(minimal, "system")
            system_content = str(system.get("content", ""))
            if '"reason"' in system_content:
                counts["system_reason_field_mentions"] += 1
            if "markdown" in system_content.lower() or "code fence" in system_content.lower():
                counts["system_wrapper_mentions"] += 1
            selected.append(minimal)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            if len(invalid_samples) < 25:
                invalid_samples.append({"index": index, "error": str(exc)})

    blockers: list[str] = []
    if invalid_samples:
        blockers.append("invalid_records")
    if parse_failures:
        blockers.append("minimal_assistant_parse_failures")
    if counts["action:submit_repairs"] < 512:
        blockers.append("submit_repair_records_under_512")
    if counts["action:finish"] < 256:
        blockers.append("finish_records_under_256")
    if counts["assistant_reason_fields"]:
        blockers.append("assistant_reason_fields_present")
    if counts["system_reason_field_mentions"]:
        blockers.append("system_reason_field_mentions_present")
    if counts["system_wrapper_mentions"]:
        blockers.append("system_wrapper_mentions_present")

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "curriculum_version": CURRICULUM_VERSION,
        "ok": not blockers,
        "status": "pass" if not blockers else "block",
        "settings": {"max_repairs_per_record": max_repairs_per_record},
        "metrics": {
            "output_records": len(selected),
            "submit_repair_records": counts["action:submit_repairs"],
            "finish_records": counts["action:finish"],
            "repair_cells": counts["repair_cells"],
            "assistant_reason_fields": counts["assistant_reason_fields"],
            "system_reason_field_mentions": counts["system_reason_field_mentions"],
            "system_wrapper_mentions": counts["system_wrapper_mentions"],
            "shape": {key: value for key, value in sorted(counts.items()) if ":" in key},
            "invalid_count": len(invalid_samples),
            "parse_failure_count": len(parse_failures),
        },
        "blockers": sorted(set(blockers)),
        "invalid_samples": invalid_samples,
        "parse_failures": parse_failures,
        "limitations": [
            "This curriculum changes training targets only; strict held-out eval remains unchanged.",
            "Repair actions are capped to reduce small-model JSON drift and overlong completions.",
        ],
    }
    return selected, report


def write_contract_minimal_curriculum(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
    max_repairs_per_record: int,
) -> dict[str, Any]:
    selected, report = build_contract_minimal_curriculum(
        _load_jsonl(input_path),
        max_repairs_per_record=max_repairs_per_record,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in selected),
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
    parser.add_argument("--max-repairs-per-record", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = write_contract_minimal_curriculum(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
        max_repairs_per_record=args.max_repairs_per_record,
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
