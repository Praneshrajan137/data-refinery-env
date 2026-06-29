"""Tests for canonical GRPO held-out evaluation helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from dataforge.datasets.registry import DATASET_REGISTRY
from dataforge.repair_contract import CONTRACT_VERSION_V3
from training.grpo_contract import TruthCell
from training.grpo_eval import (
    GrpoEvalTask,
    build_heldout_tasks,
    evaluate_completions,
    evaluate_product_constrained_actions,
    evaluate_product_constrained_finish_baseline,
    grpo_gate_failures,
)


def _fake_dataset(name: str = "hospital") -> RealWorldDataset:
    metadata = DATASET_REGISTRY[name].model_copy(
        update={
            "n_rows": 8,
            "n_columns": 2,
            "dirty_sha256": "a" * 64,
            "clean_sha256": "b" * 64,
        }
    )
    dirty_df = pd.DataFrame(
        [
            ["Alice", "Boston"],
            ["Bob", "NY"],
            ["Alicee", "Boston"],
            ["Cara", "LA"],
            ["Dan", "SF"],
            ["Eve", "SEA"],
            ["Frank", "CHI"],
            ["Gina", "DAL"],
        ],
        columns=["Name", "City"],
    )
    clean_df = dirty_df.copy()
    clean_df.loc[2, "Name"] = "Alice"
    return RealWorldDataset(
        metadata=metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=("Name", "City"),
        ground_truth=(
            GroundTruthCell(row=2, column="Name", dirty_value="Alicee", clean_value="Alice"),
        ),
        dirty_sha256="a" * 64,
        clean_sha256="b" * 64,
    )


def test_heldout_task_manifest_is_deterministic_and_label_safe(monkeypatch, tmp_path: Path) -> None:
    def fake_loader(name: str, **kwargs: object) -> RealWorldDataset:
        assert kwargs["verify_hashes"] is True
        assert kwargs["allow_embedded_fallback"] is False
        return _fake_dataset(name)

    monkeypatch.setattr("training.grpo_eval.load_real_world_dataset", fake_loader)

    tasks_a, manifest_a = build_heldout_tasks(
        datasets=("hospital",),
        heldout_tasks=3,
        benchmark_seeds=(0, 1, 2),
        cache_root=tmp_path,
    )
    tasks_b, manifest_b = build_heldout_tasks(
        datasets=("hospital",),
        heldout_tasks=3,
        benchmark_seeds=(0, 1, 2),
        cache_root=tmp_path,
    )

    assert [task.task_id for task in tasks_a] == [task.task_id for task in tasks_b]
    assert manifest_a == manifest_b
    assert manifest_a["source_audit"]["ok"] is True
    assert manifest_a["tasks"][0]["prompt_hash"]
    assert "ground_truth" not in manifest_a["tasks"][0]
    assert "hidden_ground_truth" not in manifest_a["tasks"][0]


def test_heldout_tasks_can_use_contract_minimal_v3(monkeypatch, tmp_path: Path) -> None:
    def fake_loader(name: str, **kwargs: object) -> RealWorldDataset:
        return _fake_dataset(name)

    monkeypatch.setattr("training.grpo_eval.load_real_world_dataset", fake_loader)

    tasks, _ = build_heldout_tasks(
        datasets=("hospital",),
        heldout_tasks=1,
        cache_root=tmp_path,
        contract_version=CONTRACT_VERSION_V3,
    )

    assert tasks[0].prompt["contract_version"] == CONTRACT_VERSION_V3
    assert '"reason"' not in tasks[0].messages[0]["content"]


def _task(*, truth: list[TruthCell] | None = None) -> GrpoEvalTask:
    return GrpoEvalTask(
        task_id="task-1",
        dataset="hospital",
        seed=1,
        prompt={"contract_version": "repair_contract_v2"},
        messages=[],
        allowed_columns=["Name"],
        valid_rows=[0],
        target_rows=[{"_row": "0", "Name": "Alicee"}],
        context_rows=[],
        ground_truth=truth if truth is not None else [TruthCell(0, "Name", "Alice")],
        inferability="deterministic_normalization",
        source={"source_revision": "abcdef0", "dirty_sha256": "a" * 64, "clean_sha256": "b" * 64},
    )


def test_eval_scores_exact_finish_and_schema_case_errors() -> None:
    exact = '{"action":"submit_repairs","repairs":[{"row":0,"column":"Name","new_value":"Alice"}]}'
    wrong_case = (
        '{"action":"submit_repairs","repairs":[{"row":0,"column":"name","new_value":"Alice"}]}'
    )
    finish = '{"action":"finish","repairs":[]}'

    exact_summary, _ = evaluate_completions([_task()], lambda task: exact, model_label="grpo")
    case_summary, case_diag = evaluate_completions(
        [_task()], lambda task: wrong_case, model_label="grpo"
    )
    clean_summary, _ = evaluate_completions(
        [_task(truth=[])], lambda task: finish, model_label="grpo"
    )

    assert exact_summary["macro_f1"] == 1.0
    assert exact_summary["parse_success_rate"] == 1.0
    assert case_summary["macro_f1"] == 0.0
    assert case_summary["parse_success_rate"] == 0.0
    assert case_summary["schema_case_error_count"] == 1
    assert case_diag["failure_samples"][0]["ground_truth_count"] == 1
    assert clean_summary["macro_f1"] == 1.0


def test_eval_splits_finish_with_repairs_and_completion_artifacts() -> None:
    bad_finish = '```json\n{"action":"finish","repairs":[{"row":0,"column":"Name","new_value":"Alice"}]}\n```'

    summary, diagnostics = evaluate_completions(
        [_task()], lambda task: bad_finish, model_label="sft"
    )

    assert summary["parse_success_rate"] == 0.0
    assert summary["parse_error_counts"] == {"finish_with_repairs": 1}
    assert summary["failure_taxonomy"]["finish_with_repairs"] == 1
    assert summary["completion_artifacts"] == {
        "code_fence_count": 1,
        "code_fence_rate": 1.0,
        "reason_text_count": 0,
        "reason_text_rate": 0.0,
    }
    assert diagnostics["task_scores"][0]["parse_error_kind"] == "finish_with_repairs"
    assert diagnostics["task_scores"][0]["has_code_fence"] is True


def test_product_constrained_track_reports_parse_separately_from_repair_f1() -> None:
    track, diagnostics = evaluate_product_constrained_finish_baseline(
        [_task()],
        raw_research_summary={"macro_f1": 0.0},
    )

    assert track["schema_version"] == "dataforge_product_constrained_eval_v1"
    assert track["parse_structural_success_rate"] == 1.0
    assert track["strict_macro_f1"] == 0.0
    assert track["deterministic_normalization_f1"] == 0.0
    assert track["repair_quality_claim_allowed"] is False
    assert track["rejected_invalid_repairs"] == 0
    assert diagnostics["task_scores"][0]["parse_ok"] is True


def test_product_constrained_track_rejects_invalid_repairs_without_public_claims() -> None:
    invalid_row_object = {"_row": "0", "Name": "Alice"}

    track, diagnostics = evaluate_product_constrained_actions(
        [_task()],
        lambda _task: invalid_row_object,
        raw_research_summary={"macro_f1": 0.0},
    )

    assert track["parse_structural_success_rate"] == 0.0
    assert track["strict_macro_f1"] == 0.0
    assert track["rejected_invalid_repairs"] == 1
    assert track["repair_quality_claim_allowed"] is False
    assert diagnostics["task_scores"][0]["parse_error_kind"] == "schema_error"


def test_grpo_gate_failures_require_quality_parse_and_schema() -> None:
    sft_eval = {"macro_f1": 0.01}
    passing = {"macro_f1": 0.05, "parse_success_rate": 1.0, "schema_case_error_count": 0}
    failing = {"macro_f1": 0.02, "parse_success_rate": 0.98, "schema_case_error_count": 1}

    assert grpo_gate_failures(sft_eval=sft_eval, grpo_eval=passing) == []
    assert grpo_gate_failures(sft_eval=sft_eval, grpo_eval=failing) == [
        "grpo_f1-sft_f1>=0.03",
        "parse_success_rate>=0.99",
        "schema_case_error_count==0",
    ]


def test_grpo_gate_failures_support_v2_absolute_and_noop_slice_targets() -> None:
    sft_eval = {"macro_f1": 0.0053}
    grpo_eval = {
        "macro_f1": 0.1393,
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "slice_scores": {"not_inferable_from_prompt": {"macro_f1": 1.0}},
    }

    assert grpo_gate_failures(
        sft_eval=sft_eval,
        grpo_eval=grpo_eval,
        target_strict_macro_f1=0.25,
        min_not_inferable_slice_f1=0.95,
    ) == ["grpo_f1>=0.25"]

    grpo_eval["macro_f1"] = 0.251
    grpo_eval["slice_scores"]["not_inferable_from_prompt"]["macro_f1"] = 0.94
    assert grpo_gate_failures(
        sft_eval=sft_eval,
        grpo_eval=grpo_eval,
        target_strict_macro_f1=0.25,
        min_not_inferable_slice_f1=0.95,
    ) == ["not_inferable_from_prompt_f1>=0.95"]


def test_grpo_gate_failures_support_v3_deterministic_slice_target() -> None:
    sft_eval = {"macro_f1": 0.0053}
    grpo_eval = {
        "macro_f1": 0.251,
        "parse_success_rate": 0.99,
        "schema_case_error_count": 0,
        "slice_scores": {
            "deterministic_normalization": {"macro_f1": 0.49},
            "not_inferable_from_prompt": {"macro_f1": 0.96},
        },
    }

    assert grpo_gate_failures(
        sft_eval=sft_eval,
        grpo_eval=grpo_eval,
        target_strict_macro_f1=0.25,
        min_not_inferable_slice_f1=0.95,
        min_deterministic_normalization_slice_f1=0.50,
    ) == ["deterministic_normalization_f1>=0.5"]
