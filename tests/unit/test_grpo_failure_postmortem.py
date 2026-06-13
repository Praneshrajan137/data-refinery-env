"""Tests for GRPO v1 diagnostic postmortem summaries."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.evidence.grpo_failure_postmortem import (
    SCHEMA_VERSION,
    summarize_postmortem,
    write_postmortem,
)


def _row(
    task_id: str,
    dataset: str,
    *,
    f1: float,
    tp: int,
    fp: int,
    fn: int,
    predicted_repairs: list[dict[str, object]] | None = None,
    failure_taxonomy: dict[str, int] | None = None,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "dataset": dataset,
        "inferability": "external_reference_required",
        "f1": f1,
        "canonicalized_f1": f1,
        "precision": 0.0,
        "recall": 0.0,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "parse_ok": True,
        "parse_error_kind": None,
        "schema_case_errors": 0,
        "failure_taxonomy": failure_taxonomy or {},
        "predicted_repairs": predicted_repairs or [],
        "decoded_preview": '{"action":"finish","repairs":[]}',
    }


def _diagnostics() -> dict[str, object]:
    sft_rows = [
        _row(
            "task-1",
            "hospital",
            f1=0.0,
            tp=0,
            fp=2,
            fn=2,
            predicted_repairs=[{"row": 1}, {"row": 2}],
            failure_taxonomy={"overrepair": 2, "missed_repair": 2},
        ),
        _row("task-2", "beers", f1=0.0, tp=0, fp=1, fn=0, failure_taxonomy={"overrepair": 1}),
    ]
    grpo_rows = [
        _row("task-1", "hospital", f1=0.0, tp=0, fp=0, fn=2, failure_taxonomy={"missed_repair": 2}),
        _row("task-2", "beers", f1=1.0, tp=0, fp=0, fn=0),
    ]
    return {
        "schema_version": "dataforge_grpo_eval_diagnostics_v1",
        "benchmark_name": "DataForge-Bench-light-verified",
        "benchmark_seeds": [0, 1, 2],
        "sft_eval": {
            "dataset_f1": {"beers": 0.0, "hospital": 0.0},
            "slice_scores": {},
            "parse_success_rate": 1.0,
            "schema_case_error_count": 0,
        },
        "grpo_eval": {
            "dataset_f1": {"beers": 1.0, "hospital": 0.0},
            "slice_scores": {"not_inferable_from_prompt": {"macro_f1": 1.0, "tasks": 1}},
            "parse_success_rate": 1.0,
            "schema_case_error_count": 0,
        },
        "sft": {"task_scores": sft_rows, "failure_samples": sft_rows},
        "grpo": {"task_scores": grpo_rows, "failure_samples": grpo_rows},
    }


def test_grpo_postmortem_captures_active_repair_and_paired_deltas() -> None:
    summary = summarize_postmortem(_diagnostics())

    assert summary["schema_version"] == SCHEMA_VERSION
    assert summary["grpo"]["per_dataset_f1"] == {"beers": 1.0, "hospital": 0.0}
    assert summary["grpo"]["failure_taxonomy"] == {"missed_repair": 2}
    assert summary["grpo"]["active_repair"]["overall"]["truth_positive_tasks"] == 1
    assert summary["grpo"]["active_repair"]["overall"]["empty_on_truth_positive"] == 1
    assert summary["grpo"]["active_repair"]["overall"]["clean_no_op_rate"] == 1.0
    assert summary["paired_task_comparison"]["improved_tasks"] == 1
    assert summary["paired_task_comparison"]["regressed_tasks"] == 0
    assert summary["v2_target"]["posture"] == "balanced_recall"


def test_grpo_postmortem_writes_json_and_markdown(tmp_path: Path) -> None:
    diagnostics = tmp_path / "eval_diagnostics.json"
    json_output = tmp_path / "postmortem.json"
    md_output = tmp_path / "postmortem.md"
    diagnostics.write_text(json.dumps(_diagnostics()), encoding="utf-8")

    write_postmortem(
        diagnostics_path=diagnostics,
        json_output=json_output,
        md_output=md_output,
    )

    assert json.loads(json_output.read_text(encoding="utf-8"))["schema_version"] == SCHEMA_VERSION
    markdown = md_output.read_text(encoding="utf-8")
    assert "Active Repair" in markdown
    assert "balanced_recall" in markdown
