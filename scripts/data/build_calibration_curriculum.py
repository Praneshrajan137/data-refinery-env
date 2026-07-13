"""Build the SFT-v10 calibration curriculum from the v9 action envelope.

This is the stage that makes calibration *trainable*: it rewrites each verified
v9 completion into the calibrated action envelope, attaching a ground-truth
grounded per-cell confidence target. Verified teacher repairs (which passed the
episode F1 gate) are correct by construction, so their target confidence is 1.0;
records on the non-inferable slices become correct abstentions (finish). The
confidence signal is what Phase C's calibration-aware GRPO reward supervises.

The core ``RepairFix``/``RepairAction`` product path is untouched: the calibrated
envelope is produced only here, via ``render_calibrated_completion``.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, cast

from dataforge.evaluation_contract import ABSTENTION_SLICES
from dataforge.repair_contract import (
    parse_cell_confidences,
    parse_repair_action,
    render_calibrated_completion,
)

CURRICULUM_VERSION = "expert_v10_calibration"
REPORT_SCHEMA_VERSION = "dataforge_sft_v10_calibration_curriculum_report_v1"
DEFAULT_INPUT = Path("data/sft_traj/expert_v9_action_envelope.jsonl")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v10_calibration.jsonl")
DEFAULT_REPORT = Path("eval/results/sft_v10_calibration_curriculum_report.json")
VERIFIED_REPAIR_CONFIDENCE = 1.0


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


def _repairs_from_record(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the verified repair cells from a v9 record's fix or completion."""
    fix = record.get("fix")
    if isinstance(fix, list):
        return [
            {
                "row": int(item["row"]),
                "column": str(item["column"]),
                "new_value": str(item["new_value"]),
            }
            for item in fix
            if isinstance(item, Mapping)
        ]
    completion = record.get("completion")
    if not isinstance(completion, str):
        return []
    parsed = parse_repair_action(completion, require_explicit_action=True)
    if not parsed.ok or parsed.action is None:
        return []
    return [
        {"row": repair.row, "column": repair.column, "new_value": repair.new_value}
        for repair in parsed.action.repairs
    ]


def calibrate_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Rewrite one v9 record into the calibrated (v10) envelope."""
    inferability = str(record.get("inferability", "context_derivable"))
    should_abstain = inferability in ABSTENTION_SLICES
    repairs = [] if should_abstain else _repairs_from_record(record)

    calibrated = [{**repair, "confidence": VERIFIED_REPAIR_CONFIDENCE} for repair in repairs]
    completion = render_calibrated_completion(calibrated)

    payload = dict(record)
    payload["completion"] = completion
    payload["fix"] = repairs
    payload["curriculum_version"] = CURRICULUM_VERSION
    payload["curriculum_source_version"] = str(record.get("curriculum_version", "expert_v9"))
    payload["target_confidence"] = VERIFIED_REPAIR_CONFIDENCE if repairs else 0.0
    payload["should_abstain"] = should_abstain
    source_id = str(record.get("trajectory_id", "record"))
    payload["curriculum_source_trajectory_id"] = source_id
    payload["trajectory_id"] = f"{source_id}:{CURRICULUM_VERSION}"
    return payload


def build_calibration_curriculum(
    records: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return calibrated SFT records and a readiness report."""
    selected: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    failures: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        try:
            calibrated = calibrate_record(record)
        except (KeyError, TypeError, ValueError) as exc:
            if len(failures) < 25:
                failures.append({"index": index, "error": str(exc)})
            continue
        selected.append(calibrated)

        completion = str(calibrated["completion"])
        parsed = parse_repair_action(completion, require_explicit_action=True)
        counts["completion_parse_ok"] += int(parsed.ok)
        confidences = parse_cell_confidences(completion)
        if calibrated["should_abstain"]:
            counts["abstention_records"] += 1
        elif calibrated["fix"]:
            counts["submit_records"] += 1
            counts["confidence_present"] += int(bool(confidences))
        else:
            counts["finish_records"] += 1

    output_records = len(selected)
    submit_records = counts["submit_records"]
    parse_success = (
        round(counts["completion_parse_ok"] / output_records, 4) if output_records else 0.0
    )
    confidence_coverage = (
        round(counts["confidence_present"] / submit_records, 4) if submit_records else 0.0
    )

    blockers: list[str] = []
    if failures:
        blockers.append("source_record_failures")
    if not output_records:
        blockers.append("no_output_records")
    if parse_success < 1.0:
        blockers.append("completion_parse_success_below_1.0")
    if submit_records and confidence_coverage < 1.0:
        blockers.append("submit_records_missing_confidence")
    if counts["abstention_records"] == 0:
        blockers.append("no_abstention_records")

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "curriculum_version": CURRICULUM_VERSION,
        "ok": not blockers,
        "status": "pass" if not blockers else "block",
        "metrics": {
            "output_records": output_records,
            "submit_records": submit_records,
            "finish_records": counts["finish_records"],
            "abstention_records": counts["abstention_records"],
            "completion_parse_success_rate": parse_success,
            "confidence_coverage": confidence_coverage,
            "source_record_failures": len(failures),
        },
        "blockers": sorted(set(blockers)),
        "failures": failures,
        "limitations": [
            "Verified teacher repairs are treated as correct (target confidence 1.0); "
            "the confident-wrong signal is supervised only by the GRPO calibration reward.",
            "The product constrained-decoding path stays strict v3; the calibrated "
            "envelope is a training objective, not a product output shape change.",
        ],
    }
    return selected, report


def write_calibration_curriculum(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    selected, report = build_calibration_curriculum(_load_jsonl(input_path))
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
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = write_calibration_curriculum(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
