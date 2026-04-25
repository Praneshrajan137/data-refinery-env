"""CLI subcommand: ``dataforge bench``."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dataforge.bench.runner import run_agent_comparison

_console = Console(stderr=True)


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated CLI option into a list of strings."""
    values = [item.strip() for item in raw_value.split(",")]
    return [value for value in values if value]


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
) -> None:
    """Run real-world benchmark methods across cached benchmark datasets."""
    try:
        output = run_agent_comparison(
            methods=_parse_csv_list(methods),
            datasets=_parse_csv_list(datasets),
            seeds=seeds,
            output_json=output_json,
            really_run_big_bench=really_run_big_bench,
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
                "Some LLM baselines were skipped. Set DATAFORGE_LLM_PROVIDER=groq and GROQ_API_KEY to enable them.",
                title="Benchmark Warning",
                style="yellow",
            )
        )
