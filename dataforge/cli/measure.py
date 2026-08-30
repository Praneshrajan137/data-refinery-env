"""``dataforge measure-on-my-table`` -- what a repair would do to a table with no clean copy.

The instrument specified in ``docs/trust/design-partner-instrumentation.md`` and unbuilt until
2026-08-29. It is the only thing that can move ``design_partner_evidence``, the single check in
``dataforge release full-vision`` that cannot be manufactured.

Reads. Never writes. Requires no write permission and produces no transaction.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table as RichTable

from dataforge.cli.common import load_schema, read_csv, resolve_cli_path

_console = Console()


def measure_on_my_table_command(
    path: Annotated[
        Path,
        typer.Argument(exists=True, dir_okay=False, readable=True, help="The CSV to measure."),
    ],
    plants: Annotated[
        int,
        typer.Option("--plants", help="How many known-wrong cells to plant.", min=1),
    ] = 200,
    schema_path: Annotated[
        Path | None,
        typer.Option(
            "--schema",
            help="A declared premise. Without one, constraints are mined from the table.",
        ),
    ] = None,
    constraints: Annotated[
        Path | None,
        typer.Option(
            "--constraints",
            help="A reviewed constraints artifact, as accepted via 'constraints review'.",
        ),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option("--report", help="Write the counts-only report here."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print the report as JSON."),
    ] = False,
    seed: Annotated[
        int,
        typer.Option("--seed", help="Planting seed, so a run is reproducible."),
    ] = 20260829,
) -> None:
    """Measure the write path against planted controls on a real table.

    The report is counts, digests and configuration. No cell value, column name, or row can
    appear in it -- by construction, and checked again over the emitted bytes before anything
    is written.
    """
    from dataforge.detectors import run_all_detectors
    from dataforge.measure_on_my_table import (
        assert_no_plaintext_values,
        measure_on_my_table,
        report_payload,
    )
    from dataforge.schema_inference import (
        load_constraint_review_artifact,
        merge_schema_with_reviewed_constraints,
    )

    resolved = resolve_cli_path(path)
    table_bytes = resolved.read_bytes()
    table = read_csv(resolved)

    declared = load_schema(resolve_cli_path(schema_path)) if schema_path is not None else None
    effective = declared
    if constraints is not None:
        from hashlib import sha256

        artifact, _sha = load_constraint_review_artifact(resolve_cli_path(constraints))
        effective, _ids = merge_schema_with_reviewed_constraints(
            declared, artifact, source_sha256=sha256(table_bytes).hexdigest()
        )

    if effective is None or not effective.functional_dependencies:
        _console.print(
            "[yellow]No premise. Nothing would be written, so there is nothing to measure. "
            "This is not a safety result -- it means no dependency has been accepted. Run "
            "'dataforge profile --constraints-out' then 'dataforge constraints review', and "
            "pass the artifact with --constraints.[/yellow]"
        )
        raise typer.Exit(code=1)

    # Flagged cells are excluded from planting: a plant must sit where no detector noticed
    # anything, so the pre-corruption value is the best available truth.
    issues = run_all_detectors(table, effective)
    flagged = frozenset((issue.row, issue.column) for issue in issues)

    result = measure_on_my_table(
        table,
        table_bytes=table_bytes,
        schema=effective,
        flagged_cells=flagged,
        plants=plants,
        seed=seed,
    )

    payload = report_payload(result)
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    # Privacy is the deliverable, so the check runs before anything leaves memory.
    assert_no_plaintext_values(rendered.encode("utf-8"), table)

    if report is not None:
        target = resolve_cli_path(report)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered + "\n", encoding="utf-8")

    if json_output:
        typer.echo(rendered)
        return

    view = RichTable(title="What a repair would do to THIS table")
    view.add_column("measure")
    view.add_column("value", justify="right")
    for label, key in (
        ("rows", "rows"),
        ("dependencies in the premise", "mined_dependencies"),
        ("known-wrong cells planted", "plants_placed"),
        ("cells it would write", "cells_written_total"),
        ("planted errors repaired", "repaired_a_planted_error"),
        ("planted errors written wrong", "wrong_value_on_a_planted_error"),
        ("planted errors missed", "missed_a_planted_error"),
        ("writes to cells we did NOT plant (damage CEILING)", "wrote_to_a_cell_we_did_not_plant"),
    ):
        view.add_row(label, str(payload[key]))
    _console.print(view)

    _console.print(
        "[yellow]'writes to cells we did NOT plant' is a CEILING on damage, not damage. On a "
        "table with no clean copy those writes cannot be split into repairs of real "
        "pre-existing errors and corruptions of genuinely clean cells. Measured on the "
        "hospital corpus, where truth is retained, 567 such writes were 451 real repairs and "
        "116 real corruptions -- so reading the figure as damage overstated it 4.9x. "
        "'planted errors repaired' is scored against each cell's pre-corruption value rather "
        "than against truth, which UNDERSTATES precision. Recall is not measurable on a table "
        "with no clean copy, and is reported as unmeasurable rather than omitted.[/yellow]"
    )
    if report is not None:
        _console.print(
            f"[green]Counts-only report written[/green] to {report}. It contains no cell "
            "value, column name or row -- checked over the emitted bytes, not just promised."
        )
