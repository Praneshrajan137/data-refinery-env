"""CLI subcommand: ``dataforge bench``."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

_console = Console(stderr=True)
run_agent_comparison: Callable[..., Any] | None = None


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated CLI option into a list of strings."""
    values = [item.strip() for item in raw_value.split(",")]
    return [value for value in values if value]


def _parse_seed_list(raw_value: str | None) -> list[int] | None:
    """Parse an optional comma-separated seed list."""
    if raw_value is None:
        return None
    seeds = [item.strip() for item in raw_value.split(",") if item.strip()]
    return [int(seed) for seed in seeds]


def _runner() -> Callable[..., Any]:
    """Load the benchmark runner lazily so core CLI imports stay lightweight."""
    global run_agent_comparison
    if run_agent_comparison is None:
        from dataforge.bench.runner import run_agent_comparison as loaded_runner

        run_agent_comparison = loaded_runner
    return run_agent_comparison


def _render_summary(output: Any) -> None:
    """Render the aggregate F1/quota summary table."""
    table = Table(title="DataForge Benchmark Summary")
    table.add_column("Method")
    table.add_column("Dataset")
    table.add_column("Status")
    table.add_column("F1")
    table.add_column("Avg Steps")
    table.add_column("Quota")
    for aggregate in output.aggregates:
        table.add_row(
            aggregate.method,
            aggregate.dataset,
            aggregate.status,
            "Skipped" if aggregate.f1_mean is None else f"{aggregate.f1_mean:.4f}",
            "Skipped" if aggregate.avg_steps_mean is None else f"{aggregate.avg_steps_mean:.2f}",
            "Skipped"
            if aggregate.quota_units_mean is None
            else f"{aggregate.quota_units_mean:.4f}",
        )
    Console().print(table)
    if any(aggregate.status == "skipped" for aggregate in output.aggregates):
        Console().print(
            Panel(
                "Some LLM baselines were skipped. Set DATAFORGE_LLM_PROVIDER=groq and "
                "GROQ_API_KEY to enable them.",
                title="Benchmark Warning",
                style="yellow",
            )
        )


def _render_coverage_matrix(output: Any) -> None:
    """Render the per-error-class recall matrix (honest coverage view)."""
    from dataforge.bench.error_classes import BENCH_ERROR_CLASSES, class_coverage_matrix

    matrix = class_coverage_matrix(list(output.records))
    if not matrix:
        return
    table = Table(title="Per-Error-Class Detection Recall (coverage)")
    table.add_column("Method/Dataset")
    for error_class in BENCH_ERROR_CLASSES:
        table.add_column(error_class)
    for (method, dataset), scores in sorted(matrix.items()):
        row = [f"{method}/{dataset}"]
        for error_class in BENCH_ERROR_CLASSES:
            score = scores.get(error_class)
            if score is None or score.support == 0:
                row.append("-")
            else:
                row.append(f"{score.detection_recall:.2f}/{score.recall:.2f} (n={score.support})")
        table.add_row(*row)
    Console().print(table)
    Console().print(
        Panel(
            "Each cell shows detection_recall/correction_recall per error class on "
            "the full RAHA datasets ('-' = no ground-truth cells of that class). "
            "Detection credits flagging the error; correction credits producing the "
            "exact right value. The honest split: a class can be well-detected yet "
            "not auto-correctable (no derivable value), which is reported, not hidden.",
            title="Coverage",
            style="cyan",
        )
    )


def bench(
    methods: Annotated[
        str,
        typer.Option(
            "--methods",
            help="Comma-separated benchmark methods.",
        ),
    ] = "heuristic,llm_zeroshot",
    datasets: Annotated[
        str,
        typer.Option(
            "--datasets",
            help="Comma-separated benchmark datasets.",
        ),
    ] = "hospital",
    seeds: Annotated[
        int,
        typer.Option("--seeds", help="Number of seeds per method/dataset pair."),
    ] = 3,
    seed_list: Annotated[
        str | None,
        typer.Option(
            "--seed-list",
            help="Explicit comma-separated seed list. Overrides --seeds for reproducibility.",
        ),
    ] = None,
    really_run_big_bench: Annotated[
        bool,
        typer.Option(
            "--really-run-big-bench",
            help="Override the free-tier benchmark quota guard when estimated calls exceed 500.",
        ),
    ] = False,
    output_json: Annotated[
        Path,
        typer.Option(
            "--output-json",
            help="Where to write eval/results/agent_comparison.json.",
        ),
    ] = Path("eval/results/agent_comparison.json"),
    cache_root: Annotated[
        Path | None,
        typer.Option(
            "--cache-root",
            help="Benchmark dataset cache root. Defaults to the user DataForge cache.",
        ),
    ] = None,
    verify_dataset_hashes: Annotated[
        bool,
        typer.Option(
            "--verify-dataset-hashes/--no-verify-dataset-hashes",
            help="Verify cached benchmark bytes against pinned upstream hashes.",
        ),
    ] = True,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print benchmark results as JSON."),
    ] = False,
    quick: Annotated[
        bool,
        typer.Option(
            "--quick",
            help="Offline coverage check: run random,heuristic on all datasets (1 seed) "
            "and print the per-error-class coverage matrix. No API keys required.",
        ),
    ] = False,
) -> None:
    """Run real-world benchmark methods across cached benchmark datasets."""
    if quick:
        methods = "random,heuristic"
        datasets = datasets if datasets != "hospital" else "hospital,flights,beers"
        seeds = 1
        seed_list = None
    try:
        # Load .env at the CLI boundary (not in the library runner) so real runs
        # pick up provider credentials while the library stays hermetic for tests.
        from dotenv import load_dotenv

        load_dotenv()
        output = _runner()(
            methods=_parse_csv_list(methods),
            datasets=_parse_csv_list(datasets),
            seeds=seeds,
            seed_list=_parse_seed_list(seed_list),
            output_json=output_json,
            really_run_big_bench=really_run_big_bench,
            cache_root=cache_root,
            verify_dataset_hashes=verify_dataset_hashes,
        )
    except Exception as exc:
        _console.print(
            Panel(
                f"[bold red]{exc}[/bold red]",
                title="Benchmark Error",
                style="red",
            )
        )
        raise typer.Exit(code=2) from exc

    if json_output:
        typer.echo(json.dumps(output.model_dump(mode="json"), indent=2, sort_keys=True))
        return

    _render_summary(output)
    if quick:
        _render_coverage_matrix(output)
