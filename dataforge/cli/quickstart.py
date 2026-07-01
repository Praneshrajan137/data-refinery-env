"""CLI subcommand: ``dataforge quickstart`` - zero-config guided demo.

Runs the full detect -> verified-repair flow on a bundled dataset so a new user
(or a skeptical reviewer) can see DataForge work in seconds, from any install
including a fresh ``pip install`` - the fixture is loaded from packaged data via
``importlib.resources``, not a path relative to the current directory.
"""

from __future__ import annotations

import time
from importlib import resources
from pathlib import Path
from tempfile import TemporaryDirectory

import typer
from rich.console import Console
from rich.panel import Panel


def quickstart() -> None:
    """Run a zero-config demo: profile and verify repairs on bundled data."""
    console = Console()
    from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

    started = time.perf_counter()
    fixtures = resources.files("dataforge").joinpath("fixtures")
    with (
        resources.as_file(fixtures.joinpath("hospital_10rows.csv")) as packaged_csv,
        resources.as_file(fixtures.joinpath("hospital_schema.yaml")) as packaged_schema,
        TemporaryDirectory() as work_dir,
    ):
        # Copy packaged fixture into a writable temp dir (dry-run mutates nothing,
        # but this keeps the demo identical to a real working-file flow).
        working = Path(work_dir) / "hospital.csv"
        working.write_bytes(Path(packaged_csv).read_bytes())

        from dataforge.cli.common import load_schema

        schema = load_schema(Path(packaged_schema))
        result = run_repair_pipeline(
            RepairPipelineRequest(source_path=working, mode="dry_run", schema=schema)
        )

    elapsed = time.perf_counter() - started
    issues = len(result.issues)
    fixes = len(result.fixes)
    console.print(
        Panel(
            f"Profiled a bundled dataset in [bold]{elapsed:.2f}s[/bold].\n"
            f"Detected [bold]{issues}[/bold] data-quality issue(s); "
            f"[green]{fixes}[/green] have a verified, reversible repair.\n\n"
            "Every proposed fix passed an SMT proof and the safety constitution, "
            "and would be applied inside a byte-for-byte reversible transaction.",
            title="DataForge Quickstart",
            style="green",
        )
    )
    console.print(
        Panel(
            "Try it on your own data:\n"
            "  [bold]dataforge profile your.csv[/bold]            # detect issues\n"
            "  [bold]dataforge repair your.csv --dry-run[/bold]   # preview verified repairs\n"
            "  [bold]dataforge repair your.csv --apply[/bold]     # apply (reversible)\n"
            "  [bold]dataforge revert <txn-id>[/bold]             # undo, byte-for-byte\n\n"
            "See honest per-error-class coverage on real benchmarks:\n"
            "  [bold]dataforge bench --quick[/bold]\n\n"
            "DataForge auto-applies only what it can prove correct; everything else "
            "is flagged for review, never silently changed.",
            title="Next steps",
            style="cyan",
        )
    )
    raise typer.Exit(code=0)
