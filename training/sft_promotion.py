"""Promotion gates for private SFT predecessor candidates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SFT_V5_PROMOTION_REPORT_SCHEMA = "dataforge_sft_v5_candidate_eval_report_v1"
SFT_V6_PROMOTION_REPORT_SCHEMA = "dataforge_sft_v6_candidate_eval_report_v1"
SFT_V7_PROMOTION_REPORT_SCHEMA = "dataforge_sft_v7_candidate_eval_report_v1"
SFT_V8_PROMOTION_REPORT_SCHEMA = "dataforge_sft_v8_candidate_eval_report_v1"
SFT_V9_PROMOTION_REPORT_SCHEMA = "dataforge_sft_v9_candidate_eval_report_v1"
PROMOTION_REPORT_SCHEMA = SFT_V5_PROMOTION_REPORT_SCHEMA
DEFAULT_THRESHOLDS = {
    "strict_macro_f1_min": 0.10,
    "parse_success_min": 0.99,
    "schema_case_error_max": 0,
    "deterministic_normalization_slice_f1_min": 0.30,
    "not_inferable_slice_f1_min": 0.90,
}


def _thresholds(config: Mapping[str, Any] | None = None) -> dict[str, float | int]:
    merged: dict[str, float | int] = dict(DEFAULT_THRESHOLDS)
    if config:
        for key in DEFAULT_THRESHOLDS:
            if key in config:
                merged[key] = config[key]
    return merged


def _slice_f1(summary: Mapping[str, Any], label: str) -> float:
    slice_scores = summary.get("slice_scores", {})
    if not isinstance(slice_scores, Mapping):
        return 0.0
    row = slice_scores.get(label, {})
    if not isinstance(row, Mapping):
        return 0.0
    return float(row.get("macro_f1", 0.0))


def active_repair_metrics(task_scores: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Return active-repair precision/recall over truth-positive held-out tasks."""
    truth_positive = [
        row
        for row in task_scores
        if int(row.get("tp", 0)) + int(row.get("fn", 0)) > 0
    ]
    tp = sum(int(row.get("tp", 0)) for row in truth_positive)
    fp = sum(int(row.get("fp", 0)) for row in truth_positive)
    fn = sum(int(row.get("fn", 0)) for row in truth_positive)
    predicted_positive_tasks = sum(
        1
        for row in truth_positive
        if int(row.get("tp", 0)) + int(row.get("fp", 0)) > 0
    )
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "truth_positive_tasks": len(truth_positive),
        "predicted_positive_tasks": predicted_positive_tasks,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def sft_promotion_gate_failures(
    sft_eval: Mapping[str, Any],
    *,
    threshold_config: Mapping[str, Any] | None = None,
) -> list[str]:
    """Return gate failures that block an SFT candidate from seeding GRPO."""
    gates = _thresholds(threshold_config)
    failures: list[str] = []
    strict_macro_f1_min = float(gates["strict_macro_f1_min"])
    parse_success_min = float(gates["parse_success_min"])
    schema_case_error_max = int(gates["schema_case_error_max"])
    deterministic_min = float(gates["deterministic_normalization_slice_f1_min"])
    not_inferable_min = float(gates["not_inferable_slice_f1_min"])

    if float(sft_eval.get("macro_f1", 0.0)) < strict_macro_f1_min:
        failures.append(f"sft_f1>={strict_macro_f1_min:g}")
    if float(sft_eval.get("parse_success_rate", 0.0)) < parse_success_min:
        failures.append(f"parse_success_rate>={parse_success_min:g}")
    if int(sft_eval.get("schema_case_error_count", -1)) > schema_case_error_max:
        failures.append(f"schema_case_error_count<={schema_case_error_max:g}")
    if _slice_f1(sft_eval, "deterministic_normalization") < deterministic_min:
        failures.append(f"deterministic_normalization_f1>={deterministic_min:g}")
    if _slice_f1(sft_eval, "not_inferable_from_prompt") < not_inferable_min:
        failures.append(f"not_inferable_from_prompt_f1>={not_inferable_min:g}")
    return failures


def build_sft_promotion_report(
    *,
    status: str,
    model_repo: str,
    checkpoint: str,
    base_eval: Mapping[str, Any],
    sft_eval: Mapping[str, Any],
    sft_diagnostics: Mapping[str, Any],
    threshold_config: Mapping[str, Any] | None = None,
    model_uploaded: bool,
    report_schema_version: str = SFT_V5_PROMOTION_REPORT_SCHEMA,
    candidate_label: str = "SFT-v5",
    candidate_kind: str = "repair-curriculum",
    training_metrics: Mapping[str, Any] | None = None,
    artifacts: Mapping[str, Any] | None = None,
    upload_blocker: str | None = None,
    grpo_consumer_label: str = "GRPO-v3",
) -> dict[str, Any]:
    """Build the local predecessor report consumed by gated GRPO handoff scripts."""
    task_scores = sft_diagnostics.get("task_scores", [])
    if not isinstance(task_scores, Sequence):
        task_scores = []
    gate_failures = sft_promotion_gate_failures(
        sft_eval,
        threshold_config=threshold_config,
    )
    upload_gate_failures = [] if model_uploaded else ["private_candidate_upload_required"]
    promotion_gate_passed = not gate_failures
    promote_to_grpo = promotion_gate_passed and model_uploaded
    base_f1 = float(base_eval.get("macro_f1", 0.0))
    sft_f1 = float(sft_eval.get("macro_f1", 0.0))
    metrics = {
        "base_f1": round(base_f1, 4),
        "strict_macro_f1": round(sft_f1, 4),
        "sft_f1": round(sft_f1, 4),
        "f1_delta": round(sft_f1 - base_f1, 4),
        "parse_success_rate": float(sft_eval.get("parse_success_rate", 0.0)),
        "schema_case_error_count": int(sft_eval.get("schema_case_error_count", -1)),
        "dataset_f1": sft_eval.get("dataset_f1", {}),
        "slice_scores": sft_eval.get("slice_scores", {}),
        "failure_taxonomy": sft_eval.get("failure_taxonomy", {}),
        "parse_error_counts": sft_eval.get("parse_error_counts", {}),
        "completion_artifacts": sft_eval.get("completion_artifacts", {}),
        "active_repair": active_repair_metrics(task_scores),
    }
    return {
        "schema_version": report_schema_version,
        "status": status,
        "ok": promote_to_grpo,
        "promotion_gate_passed": promotion_gate_passed,
        "promote_to_grpo": promote_to_grpo,
        "model_repo": model_repo,
        "checkpoint": checkpoint,
        "model_uploaded": model_uploaded,
        "quality_gate_failures": gate_failures,
        "upload_gate_failures": upload_gate_failures,
        "blockers": gate_failures + upload_gate_failures,
        "upload_blocker": upload_blocker or "",
        "metrics": metrics,
        "thresholds": _thresholds(threshold_config),
        "training_metrics": dict(training_metrics or {}),
        "artifacts": dict(artifacts or {}),
        "limitations": [
            f"{candidate_label} is a private {candidate_kind} predecessor candidate, not a public release.",
            f"{grpo_consumer_label} may consume this report only when promote_to_grpo is true.",
        ],
    }


def sft_v5_promotion_gate_failures(
    sft_eval: Mapping[str, Any],
    *,
    threshold_config: Mapping[str, Any] | None = None,
) -> list[str]:
    """Return gate failures that block SFT-v5 from becoming a GRPO predecessor."""
    return sft_promotion_gate_failures(sft_eval, threshold_config=threshold_config)


def sft_v6_promotion_gate_failures(
    sft_eval: Mapping[str, Any],
    *,
    threshold_config: Mapping[str, Any] | None = None,
) -> list[str]:
    """Return gate failures that block SFT-v6 from becoming a GRPO predecessor."""
    return sft_promotion_gate_failures(sft_eval, threshold_config=threshold_config)


def build_sft_v5_promotion_report(
    **kwargs: Any,
) -> dict[str, Any]:
    """Build the SFT-v5 predecessor report consumed by GRPO-v3 handoff scripts."""
    return build_sft_promotion_report(
        **kwargs,
        report_schema_version=SFT_V5_PROMOTION_REPORT_SCHEMA,
        candidate_label="SFT-v5",
        candidate_kind="repair-curriculum",
    )


def build_sft_v6_promotion_report(
    **kwargs: Any,
) -> dict[str, Any]:
    """Build the SFT-v6 predecessor report consumed by GRPO-v3 handoff scripts."""
    return build_sft_promotion_report(
        **kwargs,
        report_schema_version=SFT_V6_PROMOTION_REPORT_SCHEMA,
        candidate_label="SFT-v6",
        candidate_kind="contract-first",
    )


def build_sft_v7_promotion_report(
    **kwargs: Any,
) -> dict[str, Any]:
    """Build the SFT-v7 predecessor report consumed by GRPO-v3 handoff scripts."""
    return build_sft_promotion_report(
        **kwargs,
        report_schema_version=SFT_V7_PROMOTION_REPORT_SCHEMA,
        candidate_label="SFT-v7",
        candidate_kind="parse-latch",
    )


def build_sft_v8_promotion_report(
    **kwargs: Any,
) -> dict[str, Any]:
    """Build the SFT-v8 predecessor report consumed by GRPO-v4 handoff scripts."""
    return build_sft_promotion_report(
        **kwargs,
        report_schema_version=SFT_V8_PROMOTION_REPORT_SCHEMA,
        candidate_label="SFT-v8",
        candidate_kind="schema-distill",
        grpo_consumer_label="GRPO-v4",
    )


def build_sft_v9_promotion_report(
    **kwargs: Any,
) -> dict[str, Any]:
    """Build the SFT-v9 predecessor report consumed by GRPO-v4 handoff scripts."""
    return build_sft_promotion_report(
        **kwargs,
        report_schema_version=SFT_V9_PROMOTION_REPORT_SCHEMA,
        candidate_label="SFT-v9",
        candidate_kind="action-envelope",
        grpo_consumer_label="GRPO-v4",
    )
