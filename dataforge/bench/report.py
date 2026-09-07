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


def _row_source_label(row: dict[str, object]) -> str:
    """Return a short per-row provenance label, e.g. ``BClean T4``.

    Falls back to the full title when ``source_short`` is absent, so fixtures and
    older artifacts still render a real citation rather than a blank cell.
    """
    short = str(row.get("source_short") or row.get("source_title") or "unknown")
    table = str(row.get("source_table") or "")
    compact = table.replace("Table ", "T")
    return f"{short} {compact}".strip()


def _render_source_provenance(
    raw_rows: list[object],
    primary: object,
) -> str:
    """Render one provenance bullet per DISTINCT source cited by the rows.

    Derived from the rows, never restated. The previous version printed a single
    hardcoded source line taken from the top-level ``source`` key; once the artifact
    cited a second paper, that line named only the first one while the table below it
    displayed rows transcribed from the second. A single source line above rows drawn
    from two different tables is itself a mis-citation, which is the defect class this
    section exists to avoid.
    """
    seen: dict[str, dict[str, str]] = {}
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("source_sha256") or row.get("source_title") or "")
        if not key or key in seen:
            continue
        seen[key] = {
            "title": str(row.get("source_title") or "Unknown source"),
            "url": str(row.get("source_url") or ""),
            "table": str(row.get("source_table") or ""),
            "sha256": str(row.get("source_sha256") or ""),
            "retrieved": str(row.get("retrieved_at_utc") or ""),
        }

    if not seen and isinstance(primary, dict):
        seen["primary"] = {
            "title": str(primary.get("title") or "Unknown source"),
            "url": str(primary.get("url") or ""),
            "table": str(primary.get("table") or ""),
            "sha256": str(primary.get("source_sha256") or ""),
            "retrieved": str(primary.get("retrieved_at_utc") or ""),
        }

    bullets = [
        f"- [{entry['title']}]({entry['url']}); {entry['table']}; "
        f"source SHA-256 `{entry['sha256']}`; retrieved `{entry['retrieved']}`."
        for entry in seen.values()
    ]
    heading = "Sources" if len(bullets) > 1 else "Source"
    return f"{heading}:\n\n" + "\n".join(bullets) + "\n\n"


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
    """Replace a benchmark marker block idempotently."""
    start_marker = "<!-- BENCH:START -->"
    end_marker = "<!-- BENCH:END -->"
    if start_marker not in readme_text or end_marker not in readme_text:
        raise ValueError("Benchmark markers are missing.")
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
            rows.append([method, "Skipped", "Skipped", "Skipped", "Skipped", "Skipped", "Skipped"])
            continue
        p_mean = sum(row.precision_mean or 0.0 for row in ok_rows) / len(ok_rows)
        r_mean = sum(row.recall_mean or 0.0 for row in ok_rows) / len(ok_rows)
        f_mean = sum(row.f1_mean or 0.0 for row in ok_rows) / len(ok_rows)
        step_mean = sum(row.avg_steps_mean or 0.0 for row in ok_rows) / len(ok_rows)
        quota_mean = sum(row.quota_units_mean or 0.0 for row in ok_rows) / len(ok_rows)
        gpu_hours_mean = sum(row.gpu_hours_mean or 0.0 for row in ok_rows) / len(ok_rows)
        rows.append(
            [
                method,
                f"{p_mean:.4f}",
                f"{r_mean:.4f}",
                f"{f_mean:.4f}",
                f"{step_mean:.2f}",
                f"{quota_mean:.4f}",
                f"{gpu_hours_mean:.4f}",
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


def _metadata_list(metadata: dict[str, object], key: str) -> list[str]:
    value = metadata.get(key, [])
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _dataset_revision_summary(agent_output: BenchmarkRunOutput) -> str:
    raw_evidence = agent_output.metadata.get("dataset_evidence", [])
    if not isinstance(raw_evidence, list):
        return ""
    revisions: list[str] = []
    dataset_names: list[str] = []
    for item in raw_evidence:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        revision = str(item.get("source_revision", "")).strip()
        if name:
            dataset_names.append(name)
        if revision and revision not in revisions:
            revisions.append(revision)
    if not revisions:
        return ""
    return (
        "\n\nDataset bytes are pinned to BigDaMa/raha revision "
        f"`{', '.join(revisions)}` for {', '.join(dataset_names)}; "
        "dirty/clean SHA-256s are recorded in the JSON metadata."
    )


def build_readme_benchmark_block(agent_output: BenchmarkRunOutput, report_path: Path) -> str:
    """Build the generated README benchmark summary block."""
    rows = _aggregate_across_datasets(agent_output.aggregates)
    table = _render_table(
        ["Method", "Precision", "Recall", "F1", "Avg Steps", "Quota Units", "GPU Hours"],
        rows,
    )
    skip_reasons = _collect_skip_reasons(agent_output.aggregates)
    skip_note = ""
    if skip_reasons:
        skip_note = "\n\nSkipped methods in this run: " + "; ".join(skip_reasons)
    schema_version = str(agent_output.metadata.get("schema_version", "legacy"))
    seed_values = _metadata_list(agent_output.metadata, "seed_list")
    if not seed_values:
        seed_values = [str(agent_output.metadata.get("seeds", ""))]
    git_commit = str(agent_output.metadata.get("git_commit", "unknown"))
    git_dirty = str(agent_output.metadata.get("git_dirty", "unknown")).lower()
    dataset_summary = _dataset_revision_summary(agent_output)
    return (
        "Generated from `eval/results/agent_comparison.json` "
        f"(schema `{schema_version}`, seeds `{', '.join(seed_values)}`, "
        f"git `{git_commit[:12]}`, dirty `{git_dirty}`).\n\n"
        f"{table}\n\n"
        f"See `{report_path.name}` for per-dataset tables, error bars, and citation-only SOTA rows."
        f"{dataset_summary}"
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
                _format_metric(row.gpu_hours_mean, row.gpu_hours_std),
            ]
            for row in rows
        ]
        per_dataset_sections.append(
            f"### {dataset.title()}\n\n"
            + _render_table(
                [
                    "Method",
                    "Precision",
                    "Recall",
                    "F1",
                    "Avg Steps",
                    "Quota Units",
                    "GPU Hours",
                ],
                table_rows,
            )
        )

    local_summary = _render_table(
        ["Method", "Precision", "Recall", "F1", "Avg Steps", "Quota Units", "GPU Hours"],
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
            _row_source_label(row),
            str(row.get("note", "Citation-only literature result.")),
        ]
        for row in raw_rows
        if isinstance(row, dict)
    ]
    source = sota_output.get("source", {})
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
    seed_list = _metadata_list(agent_output.metadata, "seed_list")
    seeds = str(agent_output.metadata.get("seeds", ""))
    reproduction_command = str(agent_output.metadata.get("reproduction_command", ""))
    schema_version = str(agent_output.metadata.get("schema_version", "legacy"))
    git_commit = str(agent_output.metadata.get("git_commit", "unknown"))
    git_dirty = str(agent_output.metadata.get("git_dirty", "unknown")).lower()
    dataset_summary = _dataset_revision_summary(agent_output).strip()

    return (
        "# Benchmark Report\n\n"
        "## Reproduction\n\n"
        f"`{reproduction_command}`\n\n"
        "## Configuration\n\n"
        f"- Methods: {', '.join(methods)}\n"
        f"- Datasets: {', '.join(datasets)}\n"
        f"- Seeds: {seeds}\n"
        f"- Exact seed list: {', '.join(seed_list) if seed_list else seeds}\n"
        f"- Evidence schema: `{schema_version}`\n"
        f"- Git commit: `{git_commit}`; dirty worktree: `{git_dirty}`\n"
        "- Free-tier quota units: `max(llm_calls / 1000, (prompt_tokens + completion_tokens) / 100000)`\n"
        "- GRPO compute cost is reported as free-tier GPU-hours, not dollars.\n"
        + (f"- {dataset_summary}\n" if dataset_summary else "")
        + f"{skip_note}\n"
        + "## Cross-Dataset Local Results\n\n"
        + f"{local_summary}\n\n"
        + "## Per-Dataset Local Results\n\n"
        + "\n\n".join(per_dataset_sections)
        + "\n\n## Citation-Only SOTA Reference\n\n"
        + _render_source_provenance(raw_rows, source)
        + "HoloClean rows are transcribed from BClean Table 4; see "
        + "[HoloClean 2017](https://www.vldb.org/pvldb/vol10/p1190-rekatsinas.pdf) "
        + "for the original system description.\n\n"
        + _render_table(
            ["Method", "Dataset", "Precision", "Recall", "F1", "Source", "Note"],
            sota_rows,
        )
        + "\n\n## Methodology\n\n"
        + "Local rows are reproduced from generated JSON. Citation-only SOTA rows are copied "
        + "from literature and are not rerun in this repository. LLM quota units are free-tier "
        + "fractions; GRPO compute cost is GPU-hours, not dollars.\n"
    )


def write_benchmark_outputs(
    *,
    agent_json_path: Path,
    sota_json_path: Path,
    report_path: Path,
    readme_path: Path,
    homepage_path: Path | None = None,
) -> None:
    """Generate the benchmark report and patch generated public evidence blocks."""
    agent_output = load_agent_output(agent_json_path)
    sota_output = load_sota_output(sota_json_path)
    report_text = render_benchmark_report(agent_output, sota_output)
    report_path.write_text(report_text, encoding="utf-8")

    readme_text = readme_path.read_text(encoding="utf-8")
    benchmark_block = build_readme_benchmark_block(agent_output, report_path)
    updated_readme = replace_benchmark_block(readme_text, benchmark_block)
    readme_path.write_text(updated_readme, encoding="utf-8")

    if homepage_path is not None:
        homepage_text = homepage_path.read_text(encoding="utf-8")
        updated_homepage = replace_benchmark_block(homepage_text, benchmark_block)
        homepage_path.write_text(updated_homepage, encoding="utf-8")
