"""Build a repair-heavy SFT v5 curriculum from split-safe expert-v4 records."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, cast

from dataforge.repair_contract import CONTRACT_VERSION_V2

CURRICULUM_VERSION = "expert_v5_repair_curriculum"
REPORT_SCHEMA_VERSION = "dataforge_sft_v5_repair_curriculum_report_v1"
PROMOTION_INFERABILITY = "deterministic_normalization"
ABSTENTION_INFERABILITIES = {"external_reference_required", "not_inferable_from_prompt"}
# These two read from ``data/sft_traj/`` like every other build_*_curriculum script.
# Until 2026-08-27 they pointed into ``training/kaggle_grpo_candidate_handoff/``, a
# gitignored Kaggle upload bundle -- so the one script that STARTS the v5->v10 chain
# depended on a build artifact while every script downstream of it read the source of
# truth. The bundle's copies were byte-identical (sha256-verified) to the files named
# here, so this is a re-point, not a change of input.
DEFAULT_INPUT = Path("data/sft_traj/expert_v4_candidate.jsonl")
DEFAULT_SPLIT_MANIFEST = Path("data/sft_traj/split_manifest_v4_candidate.json")
DEFAULT_OUTPUT = Path("data/sft_traj/expert_v5_repair_curriculum.jsonl")
DEFAULT_REPORT = Path("eval/results/sft_v5_repair_curriculum_report.json")


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


def _load_eval_rows(path: Path) -> dict[str, set[int]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    rows: dict[str, set[int]] = defaultdict(set)
    for raw_dataset in payload.get("datasets", []):
        if not isinstance(raw_dataset, dict):
            continue
        dataset = str(raw_dataset.get("dataset", ""))
        if not dataset:
            continue
        for raw_row in raw_dataset.get("eval", []):
            if isinstance(raw_row, dict) and "row" in raw_row:
                rows[dataset].add(int(raw_row["row"]))
    return dict(rows)


def _message_with_role(record: Mapping[str, Any], role: str) -> Mapping[str, Any]:
    messages = record.get("messages")
    if not isinstance(messages, list):
        raise ValueError("record.messages must be a list.")
    for message in messages:
        if isinstance(message, Mapping) and message.get("role") == role:
            return message
    raise ValueError(f"record.messages is missing role={role!r}.")


def _user_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    message = _message_with_role(record, "user")
    content = message.get("content")
    if not isinstance(content, str):
        raise ValueError("user message content must be a string.")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("user message content must decode to an object.")
    return cast(dict[str, Any], payload)


def _rows_from_payload(payload: Mapping[str, Any], field: str) -> set[int]:
    rows: set[int] = set()
    raw_rows = payload.get(field)
    if not isinstance(raw_rows, list):
        return rows
    for raw_row in raw_rows:
        if isinstance(raw_row, Mapping) and "_row" in raw_row:
            rows.add(int(str(raw_row["_row"])))
    return rows


def _base_dataset(record: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    schema_summary = payload.get("schema_summary")
    if isinstance(schema_summary, Mapping) and schema_summary.get("base_dataset"):
        return str(schema_summary["base_dataset"])
    provenance = record.get("provenance")
    if isinstance(provenance, Mapping) and provenance.get("base_dataset"):
        return str(provenance["base_dataset"])
    return str(record.get("dataset", ""))


def _fix_count(record: Mapping[str, Any]) -> int:
    fixes = record.get("fix")
    return len(fixes) if isinstance(fixes, list) else 0


def _with_curriculum_metadata(
    record: Mapping[str, Any],
    *,
    role: str,
    repeat_index: int,
) -> dict[str, Any]:
    payload = dict(record)
    source_id = str(record.get("trajectory_id", ""))
    payload["schema_version"] = "expert_v4"
    payload["curriculum_version"] = CURRICULUM_VERSION
    payload["curriculum_role"] = role
    payload["curriculum_repeat_index"] = repeat_index
    payload["curriculum_source_trajectory_id"] = source_id
    if source_id:
        payload["trajectory_id"] = f"{source_id}:{CURRICULUM_VERSION}:{role}:{repeat_index:04d}"
    return payload


def _round_robin(
    records: list[dict[str, Any]], target_count: int, max_repeats: int
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    if not records or target_count <= 0:
        return selected
    repeats = Counter(str(record.get("trajectory_id", "")) for record in records)
    for key in list(repeats):
        repeats[key] = 0
    index = 0
    while len(selected) < target_count:
        record = records[index % len(records)]
        source_id = str(record.get("trajectory_id", ""))
        if repeats[source_id] < max_repeats:
            selected.append(record)
            repeats[source_id] += 1
        if all(count >= max_repeats for count in repeats.values()):
            break
        index += 1
    return selected


def build_curriculum(
    records: Iterable[Mapping[str, Any]],
    *,
    eval_rows: Mapping[str, set[int]],
    deterministic_min_per_base_dataset: int,
    deterministic_min_total: int,
    noop_ratio: float,
    hard_negative_min_total: int,
    max_repeats_per_record: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return curriculum records plus a JSON-ready report."""
    deterministic_by_base: dict[str, list[dict[str, Any]]] = defaultdict(list)
    hard_negative_by_base: dict[str, list[dict[str, Any]]] = defaultdict(list)
    leakage_samples: list[dict[str, Any]] = []
    invalid_samples: list[dict[str, Any]] = []
    input_counts: Counter[str] = Counter()

    for index, record in enumerate(records):
        try:
            if record.get("schema_version") != "expert_v4":
                raise ValueError("curriculum input must use schema_version=expert_v4.")
            if record.get("prompt_contract_version") != CONTRACT_VERSION_V2:
                raise ValueError("curriculum input must use repair_contract_v2.")
            payload = _user_payload(record)
            dataset = str(record.get("dataset", ""))
            base_dataset = _base_dataset(record, payload)
            inferability = str(record.get("inferability", ""))
            fix_count = _fix_count(record)
            input_counts[f"{base_dataset}:{inferability}:{'repair' if fix_count else 'noop'}"] += 1
            heldout_rows = set(eval_rows.get(dataset, set())) | set(
                eval_rows.get(base_dataset, set())
            )
            touched_rows = _rows_from_payload(payload, "target_rows") | _rows_from_payload(
                payload, "context_rows"
            )
            leaked = sorted(touched_rows & heldout_rows)
            if leaked:
                if len(leakage_samples) < 25:
                    leakage_samples.append(
                        {
                            "index": index,
                            "dataset": dataset,
                            "base_dataset": base_dataset,
                            "trajectory_id": record.get("trajectory_id"),
                            "rows": leaked,
                        }
                    )
                continue
            normalized = dict(record)
            if inferability == PROMOTION_INFERABILITY and fix_count > 0:
                deterministic_by_base[base_dataset].append(normalized)
            elif inferability in ABSTENTION_INFERABILITIES and fix_count == 0:
                hard_negative_by_base[base_dataset].append(normalized)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            if len(invalid_samples) < 25:
                invalid_samples.append({"index": index, "error": str(exc)})

    selected: list[dict[str, Any]] = []
    deterministic_selected_by_base: Counter[str] = Counter()
    for base_dataset in sorted(deterministic_by_base):
        group = deterministic_by_base[base_dataset]
        base_target = max(len(group), deterministic_min_per_base_dataset)
        for repeat_index, record in enumerate(
            _round_robin(group, base_target, max_repeats_per_record)
        ):
            selected.append(
                _with_curriculum_metadata(
                    record,
                    role="deterministic_repair",
                    repeat_index=repeat_index,
                )
            )
            deterministic_selected_by_base[base_dataset] += 1

    current_deterministic = sum(deterministic_selected_by_base.values())
    if current_deterministic < deterministic_min_total:
        all_deterministic = [
            record
            for base_dataset in sorted(deterministic_by_base)
            for record in deterministic_by_base[base_dataset]
        ]
        top_up = _round_robin(
            all_deterministic,
            deterministic_min_total - current_deterministic,
            max_repeats_per_record,
        )
        for repeat_index, record in enumerate(top_up, start=current_deterministic):
            payload = _user_payload(record)
            base_dataset = _base_dataset(record, payload)
            selected.append(
                _with_curriculum_metadata(
                    record,
                    role="deterministic_repair_topup",
                    repeat_index=repeat_index,
                )
            )
            deterministic_selected_by_base[base_dataset] += 1

    deterministic_total = sum(deterministic_selected_by_base.values())
    hard_negative_target = max(
        hard_negative_min_total, int(round(deterministic_total * noop_ratio))
    )
    hard_negative_selected_by_base: Counter[str] = Counter()
    hard_negative_datasets = sorted(hard_negative_by_base)
    if hard_negative_datasets:
        base_target = hard_negative_target // len(hard_negative_datasets)
        remainder = hard_negative_target % len(hard_negative_datasets)
        repeat_index = 0
        for dataset_index, base_dataset in enumerate(hard_negative_datasets):
            group_target = base_target + (1 if dataset_index < remainder else 0)
            for record in _round_robin(
                hard_negative_by_base[base_dataset],
                group_target,
                max_repeats_per_record,
            ):
                selected.append(
                    _with_curriculum_metadata(
                        record,
                        role="abstention_hard_negative",
                        repeat_index=repeat_index,
                    )
                )
                hard_negative_selected_by_base[base_dataset] += 1
                repeat_index += 1
        selected_hard_negatives = sum(hard_negative_selected_by_base.values())
        if selected_hard_negatives < hard_negative_target:
            all_hard_negatives = [
                record
                for base_dataset in hard_negative_datasets
                for record in hard_negative_by_base[base_dataset]
            ]
            for record in _round_robin(
                all_hard_negatives,
                hard_negative_target - selected_hard_negatives,
                max_repeats_per_record,
            ):
                payload = _user_payload(record)
                base_dataset = _base_dataset(record, payload)
                selected.append(
                    _with_curriculum_metadata(
                        record,
                        role="abstention_hard_negative_topup",
                        repeat_index=repeat_index,
                    )
                )
                hard_negative_selected_by_base[base_dataset] += 1
                repeat_index += 1

    blockers: list[str] = []
    if leakage_samples:
        blockers.append("heldout_leakage")
    if invalid_samples:
        blockers.append("invalid_records")
    if deterministic_total < deterministic_min_total:
        blockers.append("deterministic_repair_total_under_min")
    for dataset in ("hospital", "flights", "beers"):
        if deterministic_selected_by_base[dataset] < deterministic_min_per_base_dataset:
            blockers.append(f"{dataset}_deterministic_repair_under_min")
    if sum(hard_negative_selected_by_base.values()) < hard_negative_min_total:
        blockers.append("hard_negative_noop_under_min")

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "curriculum_version": CURRICULUM_VERSION,
        "ok": not blockers,
        "status": "pass" if not blockers else "block",
        "settings": {
            "deterministic_min_per_base_dataset": deterministic_min_per_base_dataset,
            "deterministic_min_total": deterministic_min_total,
            "noop_ratio": noop_ratio,
            "hard_negative_min_total": hard_negative_min_total,
            "max_repeats_per_record": max_repeats_per_record,
        },
        "metrics": {
            "output_records": len(selected),
            "deterministic_repair_records": deterministic_total,
            "hard_negative_noop_records": sum(hard_negative_selected_by_base.values()),
            "deterministic_by_base_dataset": dict(sorted(deterministic_selected_by_base.items())),
            "hard_negative_by_base_dataset": dict(sorted(hard_negative_selected_by_base.items())),
            "source_shape": dict(sorted(input_counts.items())),
            "leakage_count": len(leakage_samples),
            "invalid_count": len(invalid_samples),
        },
        "blockers": sorted(set(blockers)),
        "leakage_samples": leakage_samples,
        "invalid_samples": invalid_samples,
        "limitations": [
            "Curriculum duplicates records to change training emphasis; it does not alter held-out eval.",
            "External-reference and not-inferable rows remain no-op abstention examples.",
        ],
    }
    return selected, report


def write_curriculum(
    *,
    input_path: Path,
    split_manifest_path: Path,
    output_path: Path,
    report_path: Path,
    deterministic_min_per_base_dataset: int,
    deterministic_min_total: int,
    noop_ratio: float,
    hard_negative_min_total: int,
    max_repeats_per_record: int,
) -> dict[str, Any]:
    records, report = build_curriculum(
        _load_jsonl(input_path),
        eval_rows=_load_eval_rows(split_manifest_path),
        deterministic_min_per_base_dataset=deterministic_min_per_base_dataset,
        deterministic_min_total=deterministic_min_total,
        noop_ratio=noop_ratio,
        hard_negative_min_total=hard_negative_min_total,
        max_repeats_per_record=max_repeats_per_record,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records
        ),
        encoding="utf-8",
    )
    report["artifacts"] = {
        "input_path": str(input_path),
        "split_manifest_path": str(split_manifest_path),
        "output_path": str(output_path),
        "report_path": str(report_path),
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--deterministic-min-per-base-dataset", type=int, default=128)
    parser.add_argument("--deterministic-min-total", type=int, default=512)
    parser.add_argument("--noop-ratio", type=float, default=0.5)
    parser.add_argument("--hard-negative-min-total", type=int, default=128)
    parser.add_argument("--max-repeats-per-record", type=int, default=32)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = write_curriculum(
        input_path=args.input,
        split_manifest_path=args.split_manifest,
        output_path=args.output,
        report_path=args.report,
        deterministic_min_per_base_dataset=args.deterministic_min_per_base_dataset,
        deterministic_min_total=args.deterministic_min_total,
        noop_ratio=args.noop_ratio,
        hard_negative_min_total=args.hard_negative_min_total,
        max_repeats_per_record=args.max_repeats_per_record,
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
