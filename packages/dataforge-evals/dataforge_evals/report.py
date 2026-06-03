"""Markdown and JSON report generation for dataforge-evals harness runs.

Produces a self-contained report with per-agent/dataset results tables,
reproducibility metadata, and methodology documentation.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from dataforge_evals.harness import AggregateResult, HarnessRun

logger = logging.getLogger(__name__)


def _metric(mean_value: float | None, std_value: float | None) -> str:
    """Format a mean +/- std metric for markdown tables.

    Args:
        mean_value: Mean metric value, or None if all trials failed.
        std_value: Standard deviation, or None.

    Returns:
        Formatted string for markdown table cell.
    """
    if mean_value is None:
        return "Failed"
    return f"{mean_value:.4f} \u00b1 {(std_value or 0.0):.4f}"


def _table(headers: list[str], rows: list[list[str]]) -> str:
    """Render a markdown table from headers and row data.

    Args:
        headers: Column header strings.
        rows: List of row data (each row is a list of cell strings).

    Returns:
        Formatted markdown table string.
    """
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _failure_taxonomy(aggregate: AggregateResult) -> str:
    """Render an aggregate failure taxonomy compactly.

    Args:
        aggregate: Aggregated result containing failure counts.

    Returns:
        Compact string like ``"timeout=2, exception=1"`` or ``"none"``.
    """
    if not aggregate.failure_taxonomy:
        return "none"
    return ", ".join(
        f"{kind}={count}" for kind, count in sorted(aggregate.failure_taxonomy.items())
    )


def _result_type(aggregate: AggregateResult) -> str:
    """Separate deterministic baselines from model-backed results."""
    if aggregate.agent in {"mock", "random", "heuristic"}:
        return "baseline"
    if aggregate.model and aggregate.model in {"mock-oracle", "random", "heuristic"}:
        return "baseline"
    return "model"


def render_markdown(run: HarnessRun) -> str:
    """Render a complete dataforge-evals markdown report.

    Includes:
    - Per-agent/dataset results table with P/R/F1, steps, quota, runtime
    - Reproducibility block with commits, seeds, models, dependency versions
    - Methodology description
    - Not-a-leaderboard warning

    Args:
        run: Complete harness run with records, aggregates, and reproducibility.

    Returns:
        Formatted markdown string.
    """
    rows = [
        [
            aggregate.agent,
            _result_type(aggregate),
            aggregate.dataset,
            aggregate.inferability or "unknown",
            f"{aggregate.trials_completed}/{aggregate.trials_requested}",
            _metric(aggregate.precision_mean, aggregate.precision_std),
            _metric(aggregate.recall_mean, aggregate.recall_std),
            _metric(aggregate.f1_mean, aggregate.f1_std),
            _metric(aggregate.avg_steps_mean, aggregate.avg_steps_std),
            _metric(aggregate.quota_units_mean, aggregate.quota_units_std),
            f"{aggregate.quota_units_total:.4f}",
            _metric(aggregate.runtime_s_mean, aggregate.runtime_s_std),
            _failure_taxonomy(aggregate),
        ]
        for aggregate in run.aggregates
    ]
    report_table = _table(
        [
            "Agent",
            "Result Type",
            "Dataset",
            "Inferability",
            "Trials",
            "Precision",
            "Recall",
            "F1",
            "Avg Steps",
            "Quota Units / Trial",
            "Quota Units Total",
            "Runtime (s)",
            "Failures",
        ],
        rows,
    )

    reproducibility = run.reproducibility
    provider_models = ", ".join(
        f"{agent}: {model}" for agent, model in sorted(reproducibility.provider_models.items())
    )
    if not provider_models:
        provider_models = "none recorded"

    dep_versions = ", ".join(
        f"{pkg}={ver}" for pkg, ver in sorted(reproducibility.dependency_versions.items())
    )
    if not dep_versions:
        dep_versions = "none captured"

    return (
        "# dataforge-evals Report\n\n"
        "## Results\n\n"
        f"{report_table}\n\n"
        "## Reproducibility\n\n"
        f"- dataforge-evals commit: `{reproducibility.dataforge_evals_commit}`\n"
        f"- dataforge commit: `{reproducibility.dataforge_commit or 'not installed'}`\n"
        f"- Seeds: `{', '.join(str(seed) for seed in reproducibility.seeds)}`\n"
        f"- Run date UTC: `{reproducibility.run_date_utc}`\n"
        f"- Provider model versions: {provider_models}\n"
        f"- Dependency versions: {dep_versions}\n"
        f"- Nondeterminism note: {reproducibility.nondeterminism_note}\n\n"
        "## Not a Leaderboard\n\n"
        "Only compare reports when dataset versions, seeds, provider model "
        "identifiers, run date, and prompt/adapter code are identical. "
        "Otherwise the report is an evaluation artifact, not a leaderboard row.\n\n"
        "## Methodology\n\n"
        "The grader is the only source of truth. Agents return proposed cell "
        "fixes; precision, recall, and F1 are computed by exact match against "
        "ground-truth dirty-to-clean cell diffs after last-write-wins "
        "normalization per cell. Free-tier quota units are provider-normalized "
        "fractions of each provider's daily/minute allocation, with raw call "
        "and token accounting retained in JSON output.\n"
    )


def write_report(run: HarnessRun, markdown_path: Path, *, json_path: Path | None = None) -> None:
    """Write markdown and optional JSON reports for a harness run.

    Args:
        run: Complete harness run data.
        markdown_path: Output path for the markdown report.
        json_path: Optional output path for the JSON report.
    """
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(run), encoding="utf-8")
    logger.info("Markdown report written to %s", markdown_path)
    if json_path is not None:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(run.model_dump(), indent=2), encoding="utf-8")
        logger.info("JSON report written to %s", json_path)
