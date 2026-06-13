"""Tests for the SFT-v5 repair-heavy curriculum builder."""

from __future__ import annotations

import json

from dataforge.repair_contract import CONTRACT_VERSION_V2, SYSTEM_PROMPT
from scripts.data.build_repair_curriculum import (
    CURRICULUM_VERSION,
    build_curriculum,
)


def _record(
    dataset: str,
    row: int,
    *,
    inferability: str,
    repair: bool,
    base_dataset: str | None = None,
) -> dict[str, object]:
    schema_summary = {"dataset": dataset, "columns": ["Name"]}
    if base_dataset is not None:
        schema_summary["base_dataset"] = base_dataset
    payload = {
        "contract_version": CONTRACT_VERSION_V2,
        "schema_summary": schema_summary,
        "allowed_columns": ["Name"],
        "valid_rows": [row],
        "target_rows": [{"_row": str(row), "Name": "Alic"}],
        "context_rows": [],
    }
    fixes = (
        [{"row": row, "column": "Name", "new_value": "Alice", "reason": "oracle"}] if repair else []
    )
    return {
        "schema_version": "expert_v4",
        "prompt_contract_version": CONTRACT_VERSION_V2,
        "dataset": dataset,
        "inferability": inferability,
        "trajectory_id": f"{dataset}:{row}:{inferability}:{repair}",
        "fix": fixes,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, sort_keys=True)},
            {
                "role": "assistant",
                "content": json.dumps(
                    {"action": "submit_repairs" if fixes else "finish", "repairs": fixes},
                    sort_keys=True,
                ),
            },
        ],
    }


def _source_records() -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for offset, dataset in enumerate(("hospital", "flights", "beers")):
        records.append(
            _record(
                "hospital_synthetic_deterministic_v1" if dataset == "hospital" else dataset,
                offset * 10,
                inferability="deterministic_normalization",
                repair=True,
                base_dataset="hospital" if dataset == "hospital" else None,
            )
        )
        records.append(
            _record(
                dataset,
                offset * 10 + 1,
                inferability="not_inferable_from_prompt",
                repair=False,
            )
        )
    return records


def test_repair_curriculum_oversamples_deterministic_repairs_and_hard_negatives() -> None:
    selected, report = build_curriculum(
        _source_records(),
        eval_rows={"hospital": set(), "flights": set(), "beers": set()},
        deterministic_min_per_base_dataset=2,
        deterministic_min_total=6,
        noop_ratio=0.5,
        hard_negative_min_total=3,
        max_repeats_per_record=4,
    )

    assert report["ok"] is True
    assert report["metrics"]["deterministic_repair_records"] == 6
    assert report["metrics"]["hard_negative_noop_records"] == 3
    assert report["metrics"]["deterministic_by_base_dataset"] == {
        "beers": 2,
        "flights": 2,
        "hospital": 2,
    }
    assert selected
    assert all(record["schema_version"] == "expert_v4" for record in selected)
    assert all(record["curriculum_version"] == CURRICULUM_VERSION for record in selected)


def test_repair_curriculum_blocks_heldout_leakage() -> None:
    selected, report = build_curriculum(
        _source_records(),
        eval_rows={"hospital": {0}},
        deterministic_min_per_base_dataset=2,
        deterministic_min_total=6,
        noop_ratio=0.5,
        hard_negative_min_total=3,
        max_repeats_per_record=4,
    )

    assert selected
    assert report["ok"] is False
    assert "heldout_leakage" in report["blockers"]
    assert report["leakage_samples"][0]["base_dataset"] == "hospital"
