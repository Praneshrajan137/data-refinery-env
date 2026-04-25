"""CLI subcommand: ``dataforge profile <path> [--schema <yaml>]``.

Reads a CSV file, runs all detectors, and renders detected issues as a
rich-formatted terminal table.  Exit code 0 if no UNSAFE issues; 1 otherwise.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from dataforge.cli.common import load_schema, read_csv
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Schema, Severity
from dataforge.ui.profile_view import render_profile_table

_console = Console(stderr=True)


def profile(
    path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            readable=True,
            help="Path to the CSV file to profile.",
        ),
    ],
    schema: Annotated[
        Path | None,
        typer.Option(
            "--schema",
            exists=True,
            readable=True,
            help="Path to a YAML schema file with column types and FDs.",
        ),
    ] = None,
) -> None:
    """Profile a CSV file for data-quality issues.

    Reads the CSV, runs all detectors (type_mismatch, decimal_shift,
    fd_violation), and renders a rich-formatted table of detected issues.

    Exit code 0 if no UNSAFE issues are found; 1 if any UNSAFE issues exist.
    """
    # Load the CSV with dtype=str to avoid pandas type-coercion artifacts.
    try:
        df = read_csv(path)
    except Exception as exc:
        _console.print(f"[bold red]Error reading CSV:[/bold red] {exc}")
        raise typer.Exit(code=2) from exc

    # Optionally load schema.
    parsed_schema: Schema | None = None
    if schema is not None:
        parsed_schema = load_schema(schema)

    # Run all detectors.
    issues = run_all_detectors(df, parsed_schema)

    # Render the results.
    output_console = Console()
    render_profile_table(issues, output_console, file_path=str(path))

    # Exit code based on UNSAFE issues.
    has_unsafe = any(i.severity == Severity.UNSAFE for i in issues)
    if has_unsafe:
        raise typer.Exit(code=1)
