"""Tests for GRPO readiness diagnostics and prompt construction."""

from __future__ import annotations

import json

from archive.training.grpo_readiness import (
    GRPO_BALANCED_RECALL_SYSTEM_PROMPT,
    GrpoReadinessSettings,
    analyze_grpo_readiness,
    build_prompt_example,
)
from dataforge.datasets.registry import DATASET_REGISTRY
from dataforge.repair_contract import CONTRACT_VERSION_V2, SYSTEM_PROMPT


def _record(
    dataset: str,
    row: int,
    *,
    dirty: bool,
    contract_version: str = CONTRACT_VERSION_V2,
    base_dataset: str | None = None,
    inferability: str | None = None,
) -> dict[str, object]:
    schema_summary = {"dataset": dataset, "columns": ["Name", "City"]}
    if base_dataset is not None:
        schema_summary["base_dataset"] = base_dataset
    payload = {
        "contract_version": contract_version,
        "schema_summary": schema_summary,
        "allowed_columns": ["Name", "City"],
        "valid_rows": [row],
        "target_rows": [{"_row": str(row), "Name": "Alic", "City": "Paris"}],
        "context_rows": [],
    }
    fixes = (
        [{"row": row, "column": "Name", "new_value": "Alice", "reason": "oracle"}] if dirty else []
    )
    return {
        "schema_version": "expert_v4",
        "prompt_contract_version": contract_version,
        "dataset": dataset,
        "inferability": inferability
        or ("deterministic_normalization" if dirty else "not_inferable_from_prompt"),
        "trajectory_id": f"{dataset}:{row}:{dirty}",
        "fix": fixes,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, sort_keys=True)},
            {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "action": "submit_repairs" if fixes else "finish",
                        "repairs": fixes,
                    },
                    sort_keys=True,
                ),
            },
        ],
    }


def _settings() -> GrpoReadinessSettings:
    return GrpoReadinessSettings(
        min_records=6,
        min_records_per_dataset=2,
        min_repair_records=3,
        min_repair_signal_domains=3,
        min_dirty_records=3,
        min_clean_records=3,
        min_reward_std=0.05,
    )


def _balanced_records() -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for index, dataset in enumerate(("hospital", "flights", "beers")):
        records.append(_record(dataset, index * 10, dirty=True))
        records.append(_record(dataset, index * 10 + 1, dirty=False))
    return records


def test_build_prompt_example_uses_v2_contract_and_required_fields() -> None:
    example = build_prompt_example(_record("hospital", 7, dirty=True))

    assert example["prompt_contract_version"] == CONTRACT_VERSION_V2
    assert example["ground_truth"] == [
        {"row": 7, "column": "Name", "new_value": "Alice", "reason": "oracle"}
    ]
    assert example["allowed_columns"] == ["Name", "City"]
    assert example["valid_rows"] == [7]
    assert example["dirty"] is True
    assert example["inferability"] == "deterministic_normalization"
    assert example["prompt"][0]["content"] == GRPO_BALANCED_RECALL_SYSTEM_PROMPT
    assert "do not return empty repairs" in example["prompt"][0]["content"]


def test_grpo_readiness_passes_balanced_leak_free_records() -> None:
    report = analyze_grpo_readiness(
        _balanced_records(),
        split_eval_rows={"hospital": {99}, "flights": {98}, "beers": {97}},
        settings=_settings(),
    )

    assert report["ok"] is True
    assert report["status"] == "pass"
    assert report["metrics"]["valid_prompt_records"] == 6
    assert report["metrics"]["balanced_prompt_records"] == 6
    assert report["metrics"]["reward_probe"]["reward_std"] >= 0.05
    assert report["blockers"] == []


def test_grpo_readiness_blocks_heldout_leakage() -> None:
    report = analyze_grpo_readiness(
        _balanced_records(),
        split_eval_rows={"hospital": {0}, "flights": set(), "beers": set()},
        settings=_settings(),
    )

    assert report["ok"] is False
    assert "heldout_leakage" in report["blockers"]
    assert report["failure_samples"][0]["error"] == "held-out row leakage"


def test_grpo_readiness_blocks_stale_contract_v1_records() -> None:
    records = _balanced_records()
    records[0] = _record("hospital", 0, dirty=True, contract_version="repair_contract_v1")

    report = analyze_grpo_readiness(records, settings=_settings())

    assert report["ok"] is False
    assert "invalid_records" in report["blockers"]
    assert "repair_contract_v2" in report["failure_samples"][0]["error"]


def test_grpo_readiness_blocks_low_reward_variance() -> None:
    clean_only = [_record("hospital", row, dirty=False) for row in range(6)]
    settings = GrpoReadinessSettings(
        required_datasets=("hospital",),
        min_records=6,
        min_records_per_dataset=6,
        min_repair_records=0,
        min_repair_signal_domains=0,
        min_dirty_records=0,
        min_clean_records=6,
        min_reward_std=0.99,
    )

    report = analyze_grpo_readiness(clean_only, settings=settings)

    assert report["ok"] is False
    assert "reward_variance_too_low" in report["blockers"]


def test_grpo_readiness_requires_canonical_source_provenance_when_configured() -> None:
    settings = GrpoReadinessSettings(
        required_datasets=("hospital",),
        min_records=2,
        min_records_per_dataset=2,
        min_repair_records=1,
        min_repair_signal_domains=1,
        min_dirty_records=1,
        min_clean_records=1,
        min_reward_std=0.05,
        require_source_provenance=True,
    )
    expected = DATASET_REGISTRY["hospital"]

    missing = analyze_grpo_readiness(
        [_record("hospital", 0, dirty=True), _record("hospital", 1, dirty=False)],
        settings=settings,
    )
    assert "dataset_hospital_source_provenance_missing" in missing["blockers"]

    present = analyze_grpo_readiness(
        [_record("hospital", 0, dirty=True), _record("hospital", 1, dirty=False)],
        split_source_provenance={
            "hospital": {
                "source_revision": expected.source_revision,
                "dirty_sha256": expected.dirty_sha256,
                "clean_sha256": expected.clean_sha256,
                "n_rows": expected.n_rows,
                "n_columns": expected.n_columns,
            }
        },
        settings=settings,
    )

    assert present["ok"] is True
    assert (
        present["metrics"]["per_dataset"]["hospital"]["source_provenance"]["source_revision"]
        == expected.source_revision
    )


def test_grpo_readiness_counts_synthetic_hospital_as_repair_signal_not_public_coverage() -> None:
    records = [
        _record("hospital", 0, dirty=False),
        _record("hospital", 1, dirty=False),
        _record(
            "hospital_synthetic_deterministic_v1",
            2,
            dirty=True,
            base_dataset="hospital",
        ),
        _record("flights", 10, dirty=True),
        _record("flights", 11, dirty=False),
        _record("beers", 20, dirty=False),
        _record("beers", 21, dirty=True),
    ]
    settings = GrpoReadinessSettings(
        min_records=7,
        min_records_per_dataset=2,
        min_repair_records=3,
        min_repair_signal_domains=3,
        min_clean_records=3,
        min_reward_std=0.05,
    )

    report = analyze_grpo_readiness(records, settings=settings)

    assert report["ok"] is True
    assert report["metrics"]["counts_by_dataset"]["hospital"] == 2
    assert report["metrics"]["dirty_by_base_dataset"]["hospital"] == 1
    assert report["metrics"]["repair_signal_domains"] == ["beers", "flights", "hospital"]
