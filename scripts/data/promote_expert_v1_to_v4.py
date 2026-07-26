"""Promote gpt-5.6-sol teacher trajectories (expert_v1) to expert_v4.

The ReAct teacher collector emits ``expert_v1`` records with no ``inferability``
label and no ``prompt_contract_version``. The SFT curriculum chain requires
``expert_v4`` (repair_contract_v2 + a non-null inferability label + the
inferability<->repair coupling). ``build_oracle_sft_trajectories.py`` mints v4
independently from clean/dirty diffs, so nothing bridges a real teacher
trajectory forward to v4.

This adapter bridges it by REUSING the oracle builder's classifier
(``inferability_for_record``) and coupling (``_v4_output_repairs``) - it never
reimplements label semantics, so a promoted record is classified identically to
an oracle record for the same dataset rows and repairs.

HONEST OUTCOME (measured, see scripts/data/measure_teacher_grounding.py and
DECISIONS 2026-07-25): on the banked gpt-5.6-sol teacher data, essentially every
repair is classified ``external_reference_required`` / ``not_inferable_from_prompt``
and its fix is stripped, because the teacher's marginal repairs are exactly those
the guarded FD machinery cannot robustly derive (only ~7-13% are grounded by a
support/near-key/confidence-guarded full-table FD; the rest are coincidental
local-window matches). So this adapter's product is a set of *verified-abstention
hard-negative* v4 records - correct "finish / no-repair" examples on cells a small
model should NOT guess - not repair supervision. An earlier draft tried to
distil the teacher's rationale on these cells; that was retracted because the
grounding was spurious.

The verified product path is untouched: this only produces training data.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.bench.core import BenchmarkRepair  # noqa: E402
from dataforge.datasets.real_world import (  # noqa: E402
    RealWorldDataset,
    load_real_world_dataset,
)
from dataforge.evaluation_contract import InferabilityLabel  # noqa: E402
from dataforge.repair_contract import CONTRACT_VERSION_V2  # noqa: E402
from scripts.data.build_oracle_sft_trajectories import (  # noqa: E402
    EXPERT_V4_SCHEMA,
    _diagnosis_for_record,
    _v4_output_repairs,
    inferability_for_record,
)
from scripts.data.collect_sft_trajectories import (  # noqa: E402
    validate_trajectory_record,
)

TEACHER_LABEL_SOURCE = "teacher_react_verified"
PROMOTED_COLLECTION_METHOD = "promoted_expert_v1_to_v4"
# An empty teacher fix is a finish/no-repair example: the teacher chose not to
# change any cell, so it belongs to an abstention slice under the v4 coupling
# (deterministic_normalization must carry >= 1 repair).
FINISH_INFERABILITY: InferabilityLabel = "not_inferable_from_prompt"


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


def _row_indices(rows: Any) -> tuple[int, ...]:
    """Return absolute dataset row ids from a state rows list carrying ``_row``."""
    indices: list[int] = []
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and "_row" in row:
                indices.append(int(str(row["_row"])))
    return tuple(indices)


def _repairs_from_fix(fix: Any) -> list[BenchmarkRepair]:
    """Convert a teacher ``fix`` list into BenchmarkRepair objects."""
    repairs: list[BenchmarkRepair] = []
    if not isinstance(fix, list):
        return repairs
    for item in fix:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason") or "repair proposal").strip() or "repair proposal"
        repairs.append(
            BenchmarkRepair(
                row=int(item["row"]),
                column=str(item["column"]),
                new_value=str(item["new_value"]),
                reason=reason,
            )
        )
    return repairs


class _DatasetCache:
    """Load RealWorldDataset objects once per dataset name."""

    def __init__(self, *, verify_hashes: bool) -> None:
        self._verify_hashes = verify_hashes
        self._cache: dict[str, RealWorldDataset] = {}

    def get(self, name: str) -> RealWorldDataset:
        if name not in self._cache:
            self._cache[name] = load_real_world_dataset(name, verify_hashes=self._verify_hashes)
        return self._cache[name]


def promote_record(record: dict[str, Any], *, datasets: _DatasetCache) -> dict[str, Any]:
    """Promote one expert_v1 teacher record to an expert_v4 record.

    Reuses ``inferability_for_record`` (label) and ``_v4_output_repairs``
    (coupling). Preserves the teacher's diagnosis on kept repairs; replaces it
    with an honest abstention note when repairs are dropped.
    """
    dataset_name = str(record.get("dataset", ""))
    state = record.get("state") or {}
    target_rows = state.get("target_rows") if isinstance(state, dict) else None
    context_rows = state.get("context_rows") if isinstance(state, dict) else None
    row_indices = _row_indices(target_rows)
    context_indices = _row_indices(context_rows)
    repairs = _repairs_from_fix(record.get("fix"))

    if not repairs:
        inferability = FINISH_INFERABILITY
        output_repairs: list[BenchmarkRepair] = []
    else:
        dataset = datasets.get(dataset_name)
        inferability = inferability_for_record(
            dataset=dataset,
            row_indices=row_indices,
            context_indices=context_indices,
            repairs=repairs,
            configured="auto",
        )
        output_repairs = _v4_output_repairs(
            repairs, inferability=inferability, abstain_noninferable=True
        )

    teacher_diagnosis = record.get("diagnosis")
    if output_repairs and isinstance(teacher_diagnosis, list) and teacher_diagnosis:
        # Preserve the frontier model's own rationale on repairs we keep.
        diagnosis = [str(item) for item in teacher_diagnosis]
    else:
        diagnosis = _diagnosis_for_record(
            output_repairs=output_repairs,
            original_repairs=repairs,
            inferability=inferability,
        )

    promoted = dict(record)
    promoted["schema_version"] = EXPERT_V4_SCHEMA
    promoted["prompt_contract_version"] = CONTRACT_VERSION_V2
    promoted["inferability"] = inferability
    promoted["fix"] = [repair.model_dump(mode="json") for repair in output_repairs]
    promoted["diagnosis"] = diagnosis

    provenance = dict(record.get("provenance") or {})
    provenance.setdefault("base_collection_method", provenance.get("collection_method"))
    provenance["collection_method"] = PROMOTED_COLLECTION_METHOD
    provenance["label_source"] = TEACHER_LABEL_SOURCE
    provenance["prompt_contract_version"] = CONTRACT_VERSION_V2
    provenance["inferability"] = inferability
    provenance["base_dataset"] = dataset_name
    provenance["teacher_verified"] = True
    promoted["provenance"] = provenance

    return validate_trajectory_record(promoted)


def promote_records(
    records: list[dict[str, Any]], *, verify_hashes: bool = True
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Promote a batch of v1 records to v4 and return the records plus a report."""
    datasets = _DatasetCache(verify_hashes=verify_hashes)
    promoted: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    label_counts: dict[str, int] = {}
    repair_records = 0

    for index, record in enumerate(records):
        try:
            new_record = promote_record(record, datasets=datasets)
        except (KeyError, TypeError, ValueError) as exc:
            if len(failures) < 25:
                failures.append({"index": index, "error": str(exc)})
            continue
        promoted.append(new_record)
        label = str(new_record.get("inferability", ""))
        label_counts[label] = label_counts.get(label, 0) + 1
        if new_record.get("fix"):
            repair_records += 1

    report = {
        "schema_version": "dataforge_promote_v1_to_v4_report_v1",
        "input_records": len(records),
        "promoted_records": len(promoted),
        "repair_records": repair_records,
        "abstention_records": len(promoted) - repair_records,
        "inferability_distribution": dict(sorted(label_counts.items())),
        "failures": failures,
        "limitations": [
            "Teacher data is only as broad as what was collected; promotion adds no rows.",
            "Inferability is derived by the shared oracle classifier - promoted labels "
            "match an oracle record for the same dataset rows and repairs.",
            "Only repairs that survive the deterministic coupling keep the teacher's "
            "rationale; dropped repairs become honest abstentions.",
        ],
    }
    return promoted, report


def write_promoted(
    *, input_path: Path, output_path: Path, report_path: Path, verify_hashes: bool = True
) -> dict[str, Any]:
    """Promote an expert_v1 JSONL file to expert_v4 and write outputs."""
    promoted, report = promote_records(_load_jsonl(input_path), verify_hashes=verify_hashes)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in promoted
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
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/sft_traj/expert_v1_gpt56sol.jsonl"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sft_traj/expert_v4_gpt56sol_promoted.jsonl"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("eval/results/promote_v1_to_v4_report.json"),
    )
    parser.add_argument(
        "--no-verify-dataset-hashes",
        action="store_true",
        help="Skip pinned dataset hash verification (tests / offline only).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = write_promoted(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
        verify_hashes=not args.no_verify_dataset_hashes,
    )
    print(
        json.dumps(
            {
                "schema_version": report["schema_version"],
                "promoted_records": report["promoted_records"],
                "inferability_distribution": report["inferability_distribution"],
                "failures": len(report["failures"]),
            },
            sort_keys=True,
        )
    )
    return 0 if report["promoted_records"] and not report["failures"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
