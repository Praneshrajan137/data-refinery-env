"""`dataforge calibrate` - measure this tool's precision on the user's own table.

Modelled on `release doctor` rather than `profile`: a frozen report with `to_dict`, one
concern per check, and the exit code carrying the verdict. `profile` was the wrong host --
it is a pure offline read that already uses the word "Confidence" for a per-cell detector
heuristic, and putting a dataset-level precision estimate beside it would collide with a
number that means something else.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from dataforge.calibration import table_fingerprint
from dataforge.calibration_session import (
    CalibrationSessionArtifact,
    build_calibration_session,
    calibration_dir_for,
    dump_calibration_session,
    label_calibration_sample,
    load_calibration_session,
    summarize_calibration,
)
from dataforge.cli.common import load_schema, read_csv, resolve_cli_path
from dataforge.detectors import run_all_detectors

_console = Console(stderr=True)


def _session_path(source_path: Path) -> Path:
    """Return the canonical session location for a source table."""
    return calibration_dir_for(source_path) / "session.json"


def _parse_labels(labels: list[str]) -> list[tuple[int, str, str]]:
    """Parse ``row:column=error|correct`` triples, failing loudly on a malformed one.

    Silently skipping a malformed label would drop a user's judgement without telling them,
    and the estimate would then be computed from fewer labels than they believe.
    """
    parsed: list[tuple[int, str, str]] = []
    for raw in labels:
        if "=" not in raw or ":" not in raw.split("=", 1)[0]:
            raise ValueError(f"--label must look like ROW:COLUMN=error|correct (got {raw!r})")
        locator, decision = raw.split("=", 1)
        row_text, column = locator.split(":", 1)
        decision = decision.strip().lower()
        if decision not in ("error", "correct"):
            raise ValueError(f"--label decision must be 'error' or 'correct' (got {decision!r})")
        if not column.strip():
            raise ValueError(f"--label needs a column name (got {raw!r})")
        parsed.append((int(row_text), column.strip(), decision))
    return parsed


def _render(artifact: CalibrationSessionArtifact, queue_counts: dict[str, int]) -> None:
    """Render the measured per-class precision, or explain why there is nothing to show."""
    rows = summarize_calibration(artifact, queue_counts=queue_counts)
    labelled_total = len(artifact.labelled())
    if not labelled_total:
        _console.print(
            f"[yellow]{len(artifact.samples)} cells sampled from "
            f"{artifact.flagged_cells_total:,} flagged, awaiting your judgement. "
            "Label them with --label ROW:COLUMN=error|correct, then rerun to see measured "
            "precision for your table.[/yellow]"
        )
        return

    table = Table(title=f"Measured precision on {Path(artifact.source_path).name}")
    table.add_column("Issue type")
    table.add_column("Labelled", justify="right")
    table.add_column("Real errors", justify="right")
    table.add_column("Precision", justify="right")
    table.add_column("95% interval")
    table.add_column("In queue", justify="right")
    for entry in rows:
        table.add_row(
            entry.issue_type,
            str(entry.labelled),
            str(entry.real_errors),
            "-" if entry.precision is None else f"{entry.precision:.3f}",
            "-"
            if entry.precision_ci95 is None
            else f"[{entry.precision_ci95[0]:.3f}, {entry.precision_ci95[1]:.3f}]",
            str(entry.flagged_cells_in_queue),
        )
    Console().print(table)

    wasted = sum(e.flagged_cells_in_queue for e in rows if e.precision == 0.0 and e.labelled)
    if wasted:
        zero = [e.issue_type for e in rows if e.precision == 0.0 and e.labelled]
        _console.print(
            f"[yellow]{wasted:,} of {artifact.flagged_cells_total:,} flagged cells come from "
            f"classes with no measured true positives on your data ({', '.join(zero)}). "
            "Intervals are wide at this sample size, so treat this as a steer for where to "
            "look first, not proof those classes are always wrong.[/yellow]"
        )
    _console.print(
        f"[dim]{labelled_total} of {len(artifact.samples)} sampled cells labelled. "
        "Certification of auto-apply needs 59 all-correct accepted samples in a single "
        "class; see docs/trust/certification-promises.md.[/dim]"
    )


def calibrate(
    path: Annotated[Path, typer.Argument(help="Path to the CSV file to calibrate against.")],
    schema: Annotated[
        Path | None,
        typer.Option("--schema", help="Path to a YAML schema file with column types and FDs."),
    ] = None,
    per_class: Annotated[
        int,
        typer.Option(
            "--per-class",
            min=1,
            help="Cells to sample per issue type. Sampling is RANDOM within each class, "
            "never highest-confidence-first, because a ranked sample would inflate the "
            "measured precision.",
        ),
    ] = 12,
    seed: Annotated[
        int, typer.Option("--seed", help="Recorded so the draw is reproducible.")
    ] = 20260806,
    label: Annotated[
        list[str] | None,
        typer.Option(
            "--label",
            help="Record a verdict as ROW:COLUMN=error|correct. Repeatable.",
        ),
    ] = None,
    reset: Annotated[
        bool,
        typer.Option("--reset", help="Discard any existing session and draw a fresh sample."),
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print the session and measured precision as JSON.")
    ] = False,
) -> None:
    """Measure DataForge's precision on your table by labelling a small random sample.

    Benchmarks cannot tell you this. Detector precision is 0.561 on hospital, 0.947 on
    flights and 0.342 on rayyan, and nothing observable at runtime predicts which case your
    table resembles. Labelling a few dozen randomly-sampled flagged cells measures it
    directly -- and because the calibration data *is* your table, the exchangeability
    assumption that defeats benchmark-derived guarantees is satisfied by construction.
    """
    resolved_path = resolve_cli_path(path)
    parsed_schema = load_schema(schema) if schema is not None else None
    df = read_csv(resolved_path)
    issues = run_all_detectors(df, parsed_schema)
    queue_counts: dict[str, int] = {}
    for issue in issues:
        queue_counts[issue.issue_type] = queue_counts.get(issue.issue_type, 0) + 1

    source_sha256 = sha256(resolved_path.read_bytes()).hexdigest()
    session_path = _session_path(resolved_path)

    artifact: CalibrationSessionArtifact | None = None
    if session_path.exists() and not reset:
        try:
            existing = load_calibration_session(session_path)
        except Exception as exc:
            _console.print(f"[bold red]Existing session is unreadable:[/bold red] {exc}")
            raise typer.Exit(code=2) from exc
        # Refuse to credit labels gathered on different bytes. Silently reusing them would
        # report a measurement of one table as a measurement of another.
        if existing.source_sha256 != source_sha256:
            _console.print(
                "[yellow]The existing calibration session was recorded against different "
                "bytes of this file, so its labels cannot be applied here. Rerun with "
                "--reset to start a fresh session.[/yellow]"
            )
            raise typer.Exit(code=2)
        artifact = existing

    if artifact is None:
        if not issues:
            _console.print("[green]No issues detected; nothing to calibrate.[/green]")
            raise typer.Exit(code=0)
        # Deliberately does NOT record a corrector model here. This command samples detector
        # findings; no repair has been proposed yet, so naming the currently-configured model
        # would assert it produced proposals it never made. The model is recorded when a
        # repair verdict is attached, by whoever supplies the proposal.
        artifact = build_calibration_session(
            issues,
            source_path=resolved_path,
            source_sha256=source_sha256,
            row_count=len(df),
            columns=list(df.columns),
            table_fingerprint=table_fingerprint(df),
            fd_detection_source="declared" if parsed_schema is not None else "none",
            per_class=per_class,
            seed=seed,
        )

    if label:
        try:
            for row, column, decision in _parse_labels(label):
                artifact = label_calibration_sample(
                    artifact,
                    row=row,
                    column=column,
                    decision=decision,  # type: ignore[arg-type]
                )
        except (ValueError, KeyError) as exc:
            _console.print(f"[bold red]Could not record label:[/bold red] {exc}")
            raise typer.Exit(code=2) from exc

    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text(dump_calibration_session(artifact), encoding="utf-8")

    if json_output:
        typer.echo(
            json.dumps(
                {
                    "session_path": str(session_path),
                    "session": artifact.model_dump(mode="json"),
                    "measured_precision": [
                        entry.model_dump(mode="json")
                        for entry in summarize_calibration(artifact, queue_counts=queue_counts)
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        raise typer.Exit(code=0)

    _render(artifact, queue_counts)
    _console.print(f"[dim]Session: {session_path}[/dim]")
    raise typer.Exit(code=0)
