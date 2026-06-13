"""Tests for SFT-v5 predecessor promotion reports."""

from __future__ import annotations

from training.sft_promotion import (
    active_repair_metrics,
    build_sft_v5_promotion_report,
    build_sft_v6_promotion_report,
    sft_v5_promotion_gate_failures,
    sft_v6_promotion_gate_failures,
)


def _passing_eval() -> dict[str, object]:
    return {
        "macro_f1": 0.12,
        "parse_success_rate": 1.0,
        "schema_case_error_count": 0,
        "dataset_f1": {"hospital": 0.2, "flights": 0.1, "beers": 0.06},
        "slice_scores": {
            "deterministic_normalization": {"macro_f1": 0.31},
            "not_inferable_from_prompt": {"macro_f1": 0.95},
        },
        "failure_taxonomy": {"missed_repair": 10},
    }


def test_sft_v5_promotion_gate_requires_repair_and_noop_slices() -> None:
    passing = _passing_eval()
    assert sft_v5_promotion_gate_failures(passing) == []

    failing = dict(passing)
    failing["macro_f1"] = 0.09
    failing["parse_success_rate"] = 0.98
    failing["schema_case_error_count"] = 1
    failing["slice_scores"] = {
        "deterministic_normalization": {"macro_f1": 0.29},
        "not_inferable_from_prompt": {"macro_f1": 0.89},
    }

    assert sft_v5_promotion_gate_failures(failing) == [
        "sft_f1>=0.1",
        "parse_success_rate>=0.99",
        "schema_case_error_count<=0",
        "deterministic_normalization_f1>=0.3",
        "not_inferable_from_prompt_f1>=0.9",
    ]


def test_active_repair_metrics_measure_truth_positive_recall() -> None:
    metrics = active_repair_metrics(
        [
            {"tp": 1, "fp": 0, "fn": 0},
            {"tp": 0, "fp": 1, "fn": 1},
            {"tp": 0, "fp": 3, "fn": 0},
        ]
    )

    assert metrics == {
        "truth_positive_tasks": 2,
        "predicted_positive_tasks": 2,
        "tp": 1,
        "fp": 1,
        "fn": 1,
        "precision": 0.5,
        "recall": 0.5,
        "f1": 0.5,
    }


def test_promotion_report_only_unlocks_grpo_when_model_was_uploaded() -> None:
    diagnostics = {"task_scores": [{"tp": 1, "fp": 0, "fn": 0}]}
    blocked = build_sft_v5_promotion_report(
        status="pass_upload_blocked_missing_hf_token",
        model_repo="user/DataForge-0.5B-SFT-v5-candidate",
        checkpoint="user/DataForge-0.5B-SFT-v5-candidate",
        base_eval={"macro_f1": 0.01},
        sft_eval=_passing_eval(),
        sft_diagnostics=diagnostics,
        model_uploaded=False,
    )
    promoted = build_sft_v5_promotion_report(
        status="pass_uploaded_private_candidate",
        model_repo="user/DataForge-0.5B-SFT-v5-candidate",
        checkpoint="user/DataForge-0.5B-SFT-v5-candidate",
        base_eval={"macro_f1": 0.01},
        sft_eval=_passing_eval(),
        sft_diagnostics=diagnostics,
        model_uploaded=True,
    )

    assert blocked["promotion_gate_passed"] is True
    assert blocked["promote_to_grpo"] is False
    assert blocked["ok"] is False
    assert blocked["upload_gate_failures"] == ["private_candidate_upload_required"]
    assert promoted["promote_to_grpo"] is True
    assert promoted["ok"] is True
    assert promoted["metrics"]["strict_macro_f1"] == 0.12


def test_sft_v6_report_uses_contract_first_schema_and_same_gates() -> None:
    diagnostics = {"task_scores": [{"tp": 1, "fp": 0, "fn": 0}]}

    assert sft_v6_promotion_gate_failures(_passing_eval()) == []
    report = build_sft_v6_promotion_report(
        status="pass_uploaded_private_candidate",
        model_repo="user/DataForge-0.5B-SFT-v6-candidate",
        checkpoint="user/DataForge-0.5B-SFT-v6-candidate",
        base_eval={"macro_f1": 0.01},
        sft_eval=_passing_eval(),
        sft_diagnostics=diagnostics,
        model_uploaded=True,
    )

    assert report["schema_version"] == "dataforge_sft_v6_candidate_eval_report_v1"
    assert report["promote_to_grpo"] is True
    assert "SFT-v6 is a private contract-first predecessor candidate" in report["limitations"][0]
