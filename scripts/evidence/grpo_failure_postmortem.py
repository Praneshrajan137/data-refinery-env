"""Summarize 0.5B GRPO diagnostics into a v2 improvement postmortem."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

DEFAULT_DIAGNOSTICS = Path(
    "eval/results/kaggle_grpo_candidate_heartbeat_output_20260606_090921/"
    "DataForge-0.5B-GRPO-merged/eval_diagnostics.json"
)
DEFAULT_JSON_OUTPUT = Path("eval/results/grpo_05b_v1_failure_postmortem.json")
DEFAULT_MD_OUTPUT = Path("eval/results/grpo_05b_v1_failure_postmortem.md")
SCHEMA_VERSION = "dataforge_grpo_failure_postmortem_v1"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def _task_scores(diagnostics: dict[str, Any], section: str) -> list[dict[str, Any]]:
    payload = diagnostics.get(section)
    if not isinstance(payload, dict):
        raise ValueError(f"Diagnostics missing {section!r} section.")
    task_scores = payload.get("task_scores")
    if not isinstance(task_scores, list):
        raise ValueError(f"Diagnostics {section!r} section missing task_scores.")
    return [row for row in task_scores if isinstance(row, dict)]


def _summary_section(diagnostics: dict[str, Any], section: str) -> dict[str, Any]:
    payload = diagnostics.get(section)
    if not isinstance(payload, dict):
        raise ValueError(f"Diagnostics missing {section!r} summary.")
    return payload


def _active_repair_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_dataset: dict[str, Counter[str]] = {}
    totals: Counter[str] = Counter()
    for row in rows:
        dataset = str(row.get("dataset", "unknown"))
        counts = by_dataset.setdefault(dataset, Counter())
        tp = int(row.get("tp", 0) or 0)
        fp = int(row.get("fp", 0) or 0)
        fn = int(row.get("fn", 0) or 0)
        truth_positive = tp + fn > 0
        predicted_repairs = row.get("predicted_repairs")
        prediction_count = len(predicted_repairs) if isinstance(predicted_repairs, list) else fp + tp
        if truth_positive:
            counts["truth_positive_tasks"] += 1
            totals["truth_positive_tasks"] += 1
            if prediction_count == 0:
                counts["empty_on_truth_positive"] += 1
                totals["empty_on_truth_positive"] += 1
        else:
            counts["no_op_tasks"] += 1
            totals["no_op_tasks"] += 1
            if fp == 0 and prediction_count == 0:
                counts["clean_no_op_tasks"] += 1
                totals["clean_no_op_tasks"] += 1
        if prediction_count > 0:
            counts["non_empty_prediction_tasks"] += 1
            totals["non_empty_prediction_tasks"] += 1
        for key, value in {"tp": tp, "fp": fp, "fn": fn}.items():
            counts[key] += value
            totals[key] += value

    def finalize(counter: Counter[str]) -> dict[str, Any]:
        tp = counter["tp"]
        fp = counter["fp"]
        fn = counter["fn"]
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
        no_op_tasks = counter["no_op_tasks"]
        return {
            "truth_positive_tasks": counter["truth_positive_tasks"],
            "empty_on_truth_positive": counter["empty_on_truth_positive"],
            "non_empty_prediction_tasks": counter["non_empty_prediction_tasks"],
            "no_op_tasks": no_op_tasks,
            "clean_no_op_tasks": counter["clean_no_op_tasks"],
            "clean_no_op_rate": round(counter["clean_no_op_tasks"] / no_op_tasks, 4)
            if no_op_tasks
            else None,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
        }

    return {
        "overall": finalize(totals),
        "by_dataset": {dataset: finalize(counter) for dataset, counter in sorted(by_dataset.items())},
    }


def _failure_taxonomy(rows: list[dict[str, Any]]) -> dict[str, int]:
    taxonomy: Counter[str] = Counter()
    for row in rows:
        raw = row.get("failure_taxonomy", {})
        if isinstance(raw, dict):
            taxonomy.update({str(key): int(value) for key, value in raw.items()})
        if row.get("parse_ok") is False:
            taxonomy[str(row.get("parse_error_kind") or "parse_failure")] += 1
    return {key: taxonomy[key] for key in sorted(taxonomy)}


def _model_summary(diagnostics: dict[str, Any], *, model_section: str, summary_section: str) -> dict[str, Any]:
    rows = _task_scores(diagnostics, model_section)
    summary = _summary_section(diagnostics, summary_section)
    return {
        "model_label": model_section,
        "task_count": len(rows),
        "per_dataset_f1": dict(summary.get("dataset_f1", {})),
        "slice_scores": dict(summary.get("slice_scores", {})),
        "parse_success_rate": summary.get("parse_success_rate"),
        "schema_case_error_count": summary.get("schema_case_error_count"),
        "failure_taxonomy": _failure_taxonomy(rows),
        "active_repair": _active_repair_stats(rows),
    }


def _candidate_report_summary(candidate_report: dict[str, Any] | None) -> dict[str, Any] | None:
    if candidate_report is None:
        return None
    metrics = candidate_report.get("training_metrics", {})
    if not isinstance(metrics, dict):
        metrics = {}
    raw_status = str(candidate_report.get("status", "unknown"))
    normalized_status = (
        "quality_gate_failed_no_upload"
        if raw_status == "gate_failed_no_upload"
        else raw_status
    )
    return {
        "raw_status": raw_status,
        "normalized_status": normalized_status,
        "training_stage": candidate_report.get("training_stage"),
        "attempted_steps": candidate_report.get("attempted_steps"),
        "gpu_name": candidate_report.get("gpu_name"),
        "gpu_hours": candidate_report.get("gpu_hours", metrics.get("gpu_hours")),
        "model_upload_attempted": candidate_report.get("model_upload_attempted"),
        "model_repo_created": candidate_report.get("model_repo_created"),
        "public_claim_updated": candidate_report.get("public_claim_updated"),
        "gate_failures": candidate_report.get(
            "gate_failures", metrics.get("quality_gate_failures", [])
        ),
        "metrics": {
            "sft_f1": metrics.get("sft_f1"),
            "grpo_f1": metrics.get("grpo_f1"),
            "f1_delta": metrics.get("f1_delta"),
            "parse_success_rate": metrics.get("parse_success_rate"),
            "schema_case_error_count": metrics.get("schema_case_error_count"),
        },
    }


def _brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": row.get("task_id"),
        "dataset": row.get("dataset"),
        "f1": row.get("f1"),
        "tp": row.get("tp"),
        "fp": row.get("fp"),
        "fn": row.get("fn"),
        "failure_taxonomy": row.get("failure_taxonomy", {}),
        "decoded_preview": str(row.get("decoded_preview", ""))[:320],
    }


def _paired_comparison(sft_rows: list[dict[str, Any]], grpo_rows: list[dict[str, Any]]) -> dict[str, Any]:
    sft_by_task = {str(row.get("task_id")): row for row in sft_rows if row.get("task_id")}
    grpo_by_task = {str(row.get("task_id")): row for row in grpo_rows if row.get("task_id")}
    common_task_ids = sorted(set(sft_by_task) & set(grpo_by_task))
    deltas: list[dict[str, Any]] = []
    by_dataset: dict[str, Counter[str]] = {}
    for task_id in common_task_ids:
        sft = sft_by_task[task_id]
        grpo = grpo_by_task[task_id]
        dataset = str(grpo.get("dataset") or sft.get("dataset") or "unknown")
        delta = round(float(grpo.get("f1", 0.0) or 0.0) - float(sft.get("f1", 0.0) or 0.0), 4)
        counts = by_dataset.setdefault(dataset, Counter())
        if delta > 0:
            counts["improved"] += 1
        elif delta < 0:
            counts["regressed"] += 1
        else:
            counts["unchanged"] += 1
        deltas.append(
            {
                "task_id": task_id,
                "dataset": dataset,
                "f1_delta": delta,
                "sft": _brief(sft),
                "grpo": _brief(grpo),
            }
        )
    improved = [row for row in deltas if row["f1_delta"] > 0]
    regressed = [row for row in deltas if row["f1_delta"] < 0]
    return {
        "common_tasks": len(common_task_ids),
        "by_dataset": {dataset: dict(counter) for dataset, counter in sorted(by_dataset.items())},
        "improved_tasks": len(improved),
        "regressed_tasks": len(regressed),
        "unchanged_tasks": len(deltas) - len(improved) - len(regressed),
        "largest_improvements": sorted(improved, key=lambda row: row["f1_delta"], reverse=True)[:8],
        "largest_regressions": sorted(regressed, key=lambda row: row["f1_delta"])[:8],
    }


def summarize_postmortem(
    diagnostics: dict[str, Any],
    *,
    run_label: str = "v1",
    candidate_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a JSON-ready GRPO diagnostic summary."""
    sft_rows = _task_scores(diagnostics, "sft")
    grpo_rows = _task_scores(diagnostics, "grpo")
    grpo_summary = _model_summary(diagnostics, model_section="grpo", summary_section="grpo_eval")
    sft_summary = _model_summary(diagnostics, model_section="sft", summary_section="sft_eval")
    grpo_failures = grpo_summary["failure_taxonomy"]
    grpo_dataset_f1 = grpo_summary["per_dataset_f1"]
    return {
        "schema_version": SCHEMA_VERSION,
        "run_label": run_label,
        "benchmark_name": diagnostics.get("benchmark_name"),
        "benchmark_seeds": diagnostics.get("benchmark_seeds", []),
        "candidate_report": _candidate_report_summary(candidate_report),
        "sft": sft_summary,
        "grpo": grpo_summary,
        "paired_task_comparison": _paired_comparison(sft_rows, grpo_rows),
        "v2_target": {
            "posture": "balanced_recall",
            "strict_macro_f1_min": 0.25,
            "parse_success_min": 0.99,
            "schema_case_error_count": 0,
            "not_inferable_slice_min": 0.95,
        },
        "headline_findings": [
            f"GRPO {run_label} fixed parse/schema discipline and removed SFT overrepair.",
            f"GRPO {run_label} mostly learned safe abstention; active repair recall remains the next bottleneck.",
            (
                "GRPO per-dataset strict F1: "
                f"beers={grpo_dataset_f1.get('beers')}, "
                f"flights={grpo_dataset_f1.get('flights')}, "
                f"hospital={grpo_dataset_f1.get('hospital')}."
            ),
            "GRPO failure taxonomy is dominated by "
            + ", ".join(f"{key}={value}" for key, value in grpo_failures.items())
            + ".",
        ],
    }


def render_markdown(summary: dict[str, Any]) -> str:
    """Render a compact Markdown postmortem."""
    grpo = summary["grpo"]
    sft = summary["sft"]
    paired = summary["paired_task_comparison"]
    lines = [
        f"# DataForge 0.5B-GRPO {summary.get('run_label', 'v1')} Failure Postmortem",
        "",
        "## Baseline",
        "",
        f"- Benchmark: `{summary.get('benchmark_name')}` seeds `{summary.get('benchmark_seeds')}`",
        "- SFT per-dataset F1: "
        + ", ".join(f"`{key}`={value}" for key, value in sft["per_dataset_f1"].items()),
        "- GRPO per-dataset F1: "
        + ", ".join(f"`{key}`={value}" for key, value in grpo["per_dataset_f1"].items()),
        f"- GRPO parse success: `{grpo['parse_success_rate']}`",
        f"- GRPO schema-case errors: `{grpo['schema_case_error_count']}`",
    ]
    candidate_report = summary.get("candidate_report")
    if isinstance(candidate_report, dict):
        lines.extend(
            [
                f"- Candidate status: `{candidate_report['normalized_status']}`",
                f"- Gate failures: `{candidate_report.get('gate_failures')}`",
                f"- GPU hours: `{candidate_report.get('gpu_hours')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Active Repair",
            "",
        ]
    )
    lines.extend(
        [
        "- GRPO active-repair precision/recall/F1: "
        f"`{grpo['active_repair']['overall']['precision']}` / "
        f"`{grpo['active_repair']['overall']['recall']}` / "
        f"`{grpo['active_repair']['overall']['f1']}`",
        f"- Empty predictions on truth-positive tasks: `{grpo['active_repair']['overall']['empty_on_truth_positive']}`",
        "- GRPO failure taxonomy: "
        + ", ".join(f"`{key}`={value}" for key, value in grpo["failure_taxonomy"].items()),
        "",
        "## Paired Comparison",
        "",
        f"- Common tasks: `{paired['common_tasks']}`",
        f"- Improved/regressed/unchanged: `{paired['improved_tasks']}` / `{paired['regressed_tasks']}` / `{paired['unchanged_tasks']}`",
        "",
        "## Findings",
        "",
        ]
    )
    lines.extend(f"- {finding}" for finding in summary["headline_findings"])
    lines.extend(["", "## V2 Target", ""])
    target = summary["v2_target"]
    lines.extend(
        [
            f"- Posture: `{target['posture']}`",
            f"- Strict macro F1: `>={target['strict_macro_f1_min']}`",
            f"- Parse success: `>={target['parse_success_min']}`",
            f"- Schema-case errors: `{target['schema_case_error_count']}`",
            f"- Not-inferable slice: `>={target['not_inferable_slice_min']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def write_postmortem(
    *,
    diagnostics_path: Path = DEFAULT_DIAGNOSTICS,
    json_output: Path = DEFAULT_JSON_OUTPUT,
    md_output: Path = DEFAULT_MD_OUTPUT,
    run_label: str = "v1",
    candidate_report_path: Path | None = None,
) -> dict[str, Any]:
    """Read diagnostics and write JSON/Markdown postmortem artifacts."""
    candidate_report = _load_json(candidate_report_path) if candidate_report_path else None
    summary = summarize_postmortem(
        _load_json(diagnostics_path),
        run_label=run_label,
        candidate_report=candidate_report,
    )
    json_output.parent.mkdir(parents=True, exist_ok=True)
    md_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_output.write_text(render_markdown(summary), encoding="utf-8")
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diagnostics", type=Path, default=DEFAULT_DIAGNOSTICS)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--md-output", type=Path, default=DEFAULT_MD_OUTPUT)
    parser.add_argument("--run-label", default="v1")
    parser.add_argument("--candidate-report", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = write_postmortem(
        diagnostics_path=args.diagnostics,
        json_output=args.json_output,
        md_output=args.md_output,
        run_label=args.run_label,
        candidate_report_path=args.candidate_report,
    )
    print(
        json.dumps(
            {
                "schema_version": summary["schema_version"],
                "grpo_dataset_f1": summary["grpo"]["per_dataset_f1"],
                "grpo_failure_taxonomy": summary["grpo"]["failure_taxonomy"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
