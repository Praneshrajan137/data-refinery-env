"""Benchmark report rendering and README marker updates."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import cast

from dataforge.bench.core import AggregateBenchmarkResult, BenchmarkRunOutput


def _format_metric(mean_value: float | None, std_value: float | None) -> str:
    """Format a mean/std metric cell for markdown tables."""
    if mean_value is None:
        return "Skipped"
    if std_value is None:
        return f"{mean_value:.4f}"
    return f"{mean_value:.4f} +/- {std_value:.4f}"


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render a simple markdown table."""
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def load_agent_output(path: Path) -> BenchmarkRunOutput:
    """Load agent comparison JSON output."""
    return BenchmarkRunOutput.model_validate(json.loads(path.read_text(encoding="utf-8")))


def load_sota_output(path: Path) -> dict[str, object]:
    """Load citation-only SOTA comparison JSON output."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("SOTA comparison JSON must be a top-level object.")
    return cast(dict[str, object], raw)


def replace_benchmark_block(readme_text: str, block_text: str) -> str:
    """Replace the README benchmark marker block idempotently."""
    start_marker = "<!-- BENCH:START -->"
    end_marker = "<!-- BENCH:END -->"
    if start_marker not in readme_text or end_marker not in readme_text:
        raise ValueError("README benchmark markers are missing.")
    start = readme_text.index(start_marker) + len(start_marker)
    end = readme_text.index(end_marker)
    return readme_text[:start] + "\n" + block_text.strip() + "\n" + readme_text[end:]


def _aggregate_across_datasets(aggregates: list[AggregateBenchmarkResult]) -> list[list[str]]:
    """Build a simple cross-dataset local summary table."""
    grouped: dict[str, list[AggregateBenchmarkResult]] = defaultdict(list)
    skipped: dict[str, str | None] = {}
    for aggregate in aggregates:
        if aggregate.status == "ok":
            grouped[aggregate.method].append(aggregate)
        else:
            skipped.setdefault(aggregate.method, aggregate.skip_reason)

    rows: list[list[str]] = []
    methods = sorted(set(grouped) | set(skipped))
    for method in methods:
        ok_rows = grouped.get(method, [])
        if not ok_rows:
            rows.append([method, "Skipped", "Skipped", "Skipped", "Skipped", "Skipped"])
            continue
        p_mean = sum(row.precision_mean or 0.0 for row in ok_rows) / len(ok_rows)
        r_mean = sum(row.recall_mean or 0.0 for row in ok_rows) / len(ok_rows)
        f_mean = sum(row.f1_mean or 0.0 for row in ok_rows) / len(ok_rows)
        step_mean = sum(row.avg_steps_mean or 0.0 for row in ok_rows) / len(ok_rows)
        quota_mean = sum(row.quota_units_mean or 0.0 for row in ok_rows) / len(ok_rows)
        rows.append(
            [
                method,
                f"{p_mean:.4f}",
                f"{r_mean:.4f}",
                f"{f_mean:.4f}",
                f"{step_mean:.2f}",
                f"{quota_mean:.4f}",
            ]
        )
    return rows


def _collect_skip_reasons(aggregates: list[AggregateBenchmarkResult]) -> list[str]:
    """Collect distinct aggregate skip reasons in stable order."""
    reasons: list[str] = []
    for aggregate in aggregates:
        reason = aggregate.skip_reason
        if aggregate.status == "ok" or reason is None or reason in reasons:
            continue
        reasons.append(reason)
    return reasons


def build_readme_benchmark_block(agent_output: BenchmarkRunOutput, report_path: Path) -> str:
    """Build the generated README benchmark summary block."""
    rows = _aggregate_across_datasets(agent_output.aggregates)
    table = _render_table(
        ["Method", "Precision", "Recall", "F1", "Avg Steps", "Quota Units"],
        rows,
    )
    skip_reasons = _collect_skip_reasons(agent_output.aggregates)
    skip_note = ""
    if skip_reasons:
        skip_note = "\n\nSkipped methods in this run: " + "; ".join(skip_reasons)
    return (
        "Generated from `eval/results/agent_comparison.json`.\n\n"
        f"{table}\n\n"
        f"See `{report_path.name}` for per-dataset tables, error bars, and citation-only SOTA rows."
        f"{skip_note}"
    )


def render_benchmark_report(
    agent_output: BenchmarkRunOutput,
    sota_output: dict[str, object],
) -> str:
    """Render the full markdown benchmark report."""
    per_dataset_sections: list[str] = []
    by_dataset: dict[str, list[AggregateBenchmarkResult]] = defaultdict(list)
    for aggregate in agent_output.aggregates:
        by_dataset[aggregate.dataset].append(aggregate)

    for dataset, rows in by_dataset.items():
        table_rows = [
            [
                row.method,
                _format_metric(row.precision_mean, row.precision_std),
                _format_metric(row.recall_mean, row.recall_std),
                _format_metric(row.f1_mean, row.f1_std),
                _format_metric(row.avg_steps_mean, row.avg_steps_std),
                _format_metric(row.quota_units_mean, row.quota_units_std),
            ]
            for row in rows
        ]
        per_dataset_sections.append(
            f"### {dataset.title()}\n\n"
            + _render_table(
                ["Method", "Precision", "Recall", "F1", "Avg Steps", "Quota Units"],
                table_rows,
            )
        )

    local_summary = _render_table(
        ["Method", "Precision", "Recall", "F1", "Avg Steps", "Quota Units"],
        _aggregate_across_datasets(agent_output.aggregates),
    )

    raw_rows = sota_output.get("rows", [])
    if not isinstance(raw_rows, list):
        raw_rows = []
    sota_rows = [
        [
            str(row["method"]),
            str(row["dataset"]),
            f"{float(row['precision']):.3f}",
            f"{float(row['recall']):.3f}",
            f"{float(row['f1']):.3f}",
            str(row["note"]),
        ]
        for row in raw_rows
        if isinstance(row, dict)
    ]
    source = sota_output.get("source", {})
    source_title = (
        source.get("title", "Unknown source") if isinstance(source, dict) else "Unknown source"
    )
    source_url = source.get("url", "") if isinstance(source, dict) else ""
    skip_reasons = _collect_skip_reasons(agent_output.aggregates)
    skip_note = ""
    if skip_reasons:
        skip_note = "\nSkipped methods in this reproduced run: " + "; ".join(skip_reasons) + "\n"

    method_values = agent_output.metadata.get("methods", [])
    dataset_values = agent_output.metadata.get("datasets", [])
    methods = [str(method) for method in method_values] if isinstance(method_values, list) else []
    datasets = (
        [str(dataset) for dataset in dataset_values] if isinstance(dataset_values, list) else []
    )
    seeds = str(agent_output.metadata.get("seeds", ""))
    reproduction_command = str(agent_output.metadata.get("reproduction_command", ""))

    return (
        "# Benchmark Report\n\n"
        "## Reproduction\n\n"
        f"`{reproduction_command}`\n\n"
        "## Configuration\n\n"
        f"- Methods: {', '.join(methods)}\n"
        f"- Datasets: {', '.join(datasets)}\n"
        f"- Seeds: {seeds}\n"
        "- Free-tier quota units: `max(llm_calls / 1000, (prompt_tokens + completion_tokens) / 100000)`\n"
        f"{skip_note}\n"
        "## Cross-Dataset Local Results\n\n"
        f"{local_summary}\n\n"
        "## Per-Dataset Local Results\n\n"
        + "\n\n".join(per_dataset_sections)
        + "\n\n## Citation-Only SOTA Reference\n\n"
        + f"Source: [{source_title}]({source_url})\n\n"
        + "HoloClean rows are transcribed from BClean Table 4; see [HoloClean 2017](https://www.vldb.org/pvldb/vol10/p1190-rekatsinas.pdf) for the original system description.\n\n"
        + _render_table(
            ["Method", "Dataset", "Precision", "Recall", "F1", "Note"],
            sota_rows,
        )
        + "\n\n## Methodology\n\n"
        + "Local rows are reproduced from generated JSON. Citation-only SOTA rows are copied from literature and are not rerun in this repository. Quota units are reported in free-tier fractions rather than dollars.\n"
    )


def write_benchmark_outputs(
    *,
    agent_json_path: Path,
    sota_json_path: Path,
    report_path: Path,
    readme_path: Path,
) -> None:
    """Generate the benchmark report and patch the README block."""
    agent_output = load_agent_output(agent_json_path)
    sota_output = load_sota_output(sota_json_path)
    report_text = render_benchmark_report(agent_output, sota_output)
    report_path.write_text(report_text, encoding="utf-8")

    readme_text = readme_path.read_text(encoding="utf-8")
    benchmark_block = build_readme_benchmark_block(agent_output, report_path)
    updated_readme = replace_benchmark_block(readme_text, benchmark_block)
    readme_path.write_text(updated_readme, encoding="utf-8")
