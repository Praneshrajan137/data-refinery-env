"""CLI subcommand: ``dataforge profile <path> [--schema <yaml>]``.

Reads a CSV file, runs all detectors, and renders detected issues as a
rich-formatted terminal table. Diagnostics exit 0 by default; use
``--fail-on`` for CI gating.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal

import typer
from rich.console import Console

from dataforge.cli.common import load_schema, read_csv, resolve_cli_path
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.schema_inference import (
    SchemaInferenceResult,
    build_constraint_review_artifact,
    dump_constraint_review_artifact,
    infer_schema,
)
from dataforge.table import TableLike
from dataforge.ui.profile_view import render_profile_table

_console = Console(stderr=True)

FailOn = Literal["never", "unsafe", "review", "any"]


def _should_fail(issues: Sequence[Issue], fail_on: FailOn) -> bool:
    """Return whether profile findings should trip the requested CI gate."""
    if fail_on == "never":
        return False
    if fail_on == "any":
        return bool(issues)
    severities = [issue.severity for issue in issues]
    if fail_on == "unsafe":
        return any(severity == Severity.UNSAFE for severity in severities)
    return any(severity >= Severity.REVIEW for severity in severities)


def _warn_fd_queue_cost(
    df: TableLike, inference: SchemaInferenceResult, current_issues: int
) -> None:
    """Report what accepting the mined functional dependencies would cost the queue.

    The exchange rate has to be visible at the moment of choice. Accepting an FD candidate
    is one keystroke in ``constraints review``, and the consequence is measured: on hospital
    it turns a 549-cell queue that is 56% real errors into 10,373 cells at 4.4% -- +147 true
    errors bought with +9,824 false positives, and review effort going from 1.78 to 22.80
    cells per real error. Recall genuinely improves (0.61 -> 0.89), which is why this warns
    rather than refuses: it is a dial, not a defect.

    Silent by design when there are no FD candidates, so the common case stays quiet.
    """
    from dataforge.engine.repair import fd_flag_cost

    schema = inference.to_schema(include_inferred_constraints=True)
    if not schema.functional_dependencies:
        return
    projected = fd_flag_cost(df, schema)
    if projected <= current_issues:
        return
    multiple = projected / current_issues if current_issues else 0.0
    _console.print(
        f"[yellow]{len(schema.functional_dependencies)} inferred functional "
        f"dependencies were written to the artifact. Accepting them would flag about "
        f"{projected:,} cells against {current_issues:,} today"
        + (f" (~{multiple:.0f}x)" if multiple >= 2 else "")
        + ". Inferred FDs raise recall but can flood review; accept selectively, and see "
        "docs/trust/constraint-circularity.md.[/yellow]"
    )


def profile(
    path: Annotated[
        Path,
        typer.Argument(
            help="Path to the CSV file to profile.",
        ),
    ],
    schema: Annotated[
        Path | None,
        typer.Option(
            "--schema",
            help="Path to a YAML schema file with column types and FDs.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print profile results as JSON."),
    ] = False,
    constraints_out: Annotated[
        Path | None,
        typer.Option(
            "--constraints-out",
            help="Write inferred constraints as a pending review artifact.",
        ),
    ] = None,
    fail_on: Annotated[
        FailOn,
        typer.Option(
            "--fail-on",
            help="Exit 1 when findings meet this threshold: never, unsafe, review, any.",
        ),
    ] = "never",
) -> None:
    """Profile a CSV file for data-quality issues.

    Reads the CSV, runs all detectors (type_mismatch, decimal_shift,
    fd_violation), and renders a rich-formatted table of detected issues.

    Exit code 0 unless ``--fail-on`` is set and matching findings are present.
    """
    resolved_path = resolve_cli_path(path)
    if not resolved_path.exists():
        _console.print(f"[bold red]CSV file not found:[/bold red] {path}")
        raise typer.Exit(code=2)

    # Load the CSV with dtype=str to avoid pandas type-coercion artifacts.
    try:
        df = read_csv(resolved_path)
    except Exception as exc:
        _console.print(f"[bold red]Error reading CSV:[/bold red] {exc}")
        raise typer.Exit(code=2) from exc

    # Optionally load schema.
    parsed_schema: Schema | None = None
    if schema is not None:
        resolved_schema = resolve_cli_path(schema)
        if not resolved_schema.exists():
            _console.print(f"[bold red]Schema file not found:[/bold red] {schema}")
            raise typer.Exit(code=2)
        parsed_schema = load_schema(resolved_schema)

    # Run all detectors.
    issues = run_all_detectors(df, parsed_schema)
    schema_inference = infer_schema(df)
    source_sha256 = sha256(resolved_path.read_bytes()).hexdigest()
    if constraints_out is not None:
        try:
            artifact = build_constraint_review_artifact(
                schema_inference,
                source_path=resolved_path,
                source_sha256=source_sha256,
            )
            constraints_out.parent.mkdir(parents=True, exist_ok=True)
            constraints_out.write_text(
                dump_constraint_review_artifact(artifact),
                encoding="utf-8",
            )
            _warn_fd_queue_cost(df, schema_inference, len(issues))
        except Exception as exc:
            _console.print(f"[bold red]Error writing constraints artifact:[/bold red] {exc}")
            raise typer.Exit(code=2) from exc

    # Render the results.
    if json_output:
        typer.echo(
            json.dumps(
                {
                    "path": str(resolved_path),
                    "issues_count": len(issues),
                    "fail_on": fail_on,
                    "issues": [issue.model_dump(mode="json") for issue in issues],
                    "schema_inference": schema_inference.model_dump(mode="json"),
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        output_console = Console()
        render_profile_table(issues, output_console, file_path=str(resolved_path))

    if _should_fail(issues, fail_on):
        raise typer.Exit(code=1)
