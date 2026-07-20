"""CLI subcommand: ``dataforge verify-apply <path> --fixes <json> [--dry-run | --apply]``.

The verification-layer entry: an external actor (agent, tool, or human) supplies
proposed cell fixes as JSON, and DataForge proves each one through the same
safety + verifier gate as an internal repair, applying only the proven ones inside
a reversible, certified transaction.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel

from dataforge.cli.common import load_schema, resolve_cli_path
from dataforge.detectors.base import Schema
from dataforge.schema_inference import ConstraintReviewArtifact, load_constraint_review_artifact
from dataforge.ui.repair_diff import render_repair_diff

if TYPE_CHECKING:
    from dataforge.engine.repair import ExternalFix

_console = Console(stderr=True)


def _print_error(message: str, *, hint: str | None = None) -> None:
    """Render a rich-formatted CLI error."""
    body = f"[bold red]{message}[/bold red]"
    if hint:
        body = f"{body}\n\n[dim]{hint}[/dim]"
    _console.print(Panel(body, title="Verify-Apply Error", style="red"))


def _resolve_schema(schema_path: Path | None) -> Schema | None:
    """Resolve an optional schema path into a parsed Schema."""
    if schema_path is None:
        return None
    resolved = resolve_cli_path(schema_path)
    if not resolved.exists():
        raise typer.BadParameter(f"Schema file '{schema_path}' does not exist.")
    return load_schema(resolved)


def _resolve_constraints(
    constraints_path: Path | None,
) -> tuple[ConstraintReviewArtifact | None, str | None]:
    """Resolve an optional reviewed constraints artifact."""
    if constraints_path is None:
        return None, None
    resolved = resolve_cli_path(constraints_path)
    if not resolved.exists():
        raise typer.BadParameter(f"Constraints file '{constraints_path}' does not exist.")
    return load_constraint_review_artifact(resolved)


def _load_fixes(fixes_path: str) -> list[ExternalFix]:
    """Load externally-proposed fixes from a JSON file or stdin ('-')."""
    from dataforge.engine.repair import ExternalFix

    if fixes_path == "-":
        raw = sys.stdin.read()
    else:
        resolved = resolve_cli_path(Path(fixes_path))
        if not resolved.exists():
            raise typer.BadParameter(f"Fixes file '{fixes_path}' does not exist.")
        raw = resolved.read_text(encoding="utf-8")
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--fixes is not valid JSON: {exc}") from exc
    if isinstance(payload, dict) and "fixes" in payload:
        payload = payload["fixes"]
    if not isinstance(payload, list):
        raise typer.BadParameter(
            "--fixes must be a JSON array of {row, column, new_value, expected_old_value?} objects."
        )
    fixes: list[ExternalFix] = []
    for index, spec in enumerate(payload):
        if not isinstance(spec, dict):
            raise typer.BadParameter(f"Fix #{index} is not a JSON object.")
        try:
            fixes.append(
                ExternalFix(
                    row=int(spec["row"]),
                    column=str(spec["column"]),
                    new_value=str(spec["new_value"]),
                    expected_old_value=(
                        None
                        if spec.get("expected_old_value") is None
                        else str(spec["expected_old_value"])
                    ),
                )
            )
        except KeyError as exc:
            raise typer.BadParameter(f"Fix #{index} is missing required key {exc}.") from exc
    if not fixes:
        raise typer.BadParameter("--fixes contained no fixes.")
    return fixes


def verify_apply(
    path: Annotated[
        str,
        typer.Argument(help="Path to the CSV file to verify externally-proposed fixes against."),
    ],
    fixes: Annotated[
        str,
        typer.Option(
            "--fixes",
            help="Path to a JSON file (or '-' for stdin) with an array of "
            "{row, column, new_value, expected_old_value?} fixes.",
        ),
    ],
    schema: Annotated[
        Path | None,
        typer.Option("--schema", help="Path to a YAML/JSON schema file with column types and FDs."),
    ] = None,
    constraints: Annotated[
        Path | None,
        typer.Option(
            "--constraints",
            help="Path to a reviewed constraints artifact from profile --constraints-out.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Verify and report without changing the file."),
    ] = False,
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Apply proven fixes and record a reversible transaction."),
    ] = False,
    proposer: Annotated[
        str,
        typer.Option("--proposer", help="Attribution recorded on the certificate."),
    ] = "external",
    confirm_escalations: Annotated[
        bool,
        typer.Option(
            "--confirm-escalations",
            help="Confirm the unconfirmed-write escalation that every external write raises. "
            "Required for any external fix to apply.",
        ),
    ] = False,
    allow_unproven: Annotated[
        bool,
        typer.Option(
            "--allow-unproven",
            help="Opt in to applying fixes that are only plausibility-verified (no authoritative "
            "schema). Recorded honestly as not proven. Off by default.",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print the repair result as JSON."),
    ] = False,
) -> None:
    """Verify externally-proposed fixes through the shared gate and apply the proven ones."""
    if dry_run == apply:
        _print_error(
            "Choose exactly one of --dry-run or --apply.",
            hint="Example: dataforge verify-apply data.csv --fixes fixes.json --dry-run",
        )
        raise typer.Exit(code=2)

    try:
        parsed_schema = _resolve_schema(schema)
        constraints_artifact, _constraints_sha256 = _resolve_constraints(constraints)
        external_fixes = _load_fixes(fixes)
        resolved_path = resolve_cli_path(Path(path))
        if not resolved_path.exists():
            raise typer.BadParameter(f"CSV file '{path}' does not exist.")
    except Exception as exc:
        _print_error(str(exc))
        raise typer.Exit(code=2) from exc

    try:
        from dataforge.engine.repair import VerifyAndApplyRequest, verify_and_apply

        result = verify_and_apply(
            VerifyAndApplyRequest(
                source_path=resolved_path,
                fixes=external_fixes,
                mode="apply" if apply else "dry_run",
                schema=parsed_schema,
                constraints=constraints_artifact,
                proposer=proposer,
                confirm_escalations=confirm_escalations,
                allow_unproven_autoapply=allow_unproven,
            )
        )
    except Exception as exc:
        _print_error(
            f"Failed to apply external fixes: {exc}" if apply else f"Failed to verify: {exc}",
            hint="The source file was restored to its pre-apply bytes." if apply else None,
        )
        raise typer.Exit(code=1 if apply else 2) from exc

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True))
        raise typer.Exit(code=0 if result.receipt.applied else 1)

    output_console = Console()
    render_repair_diff(result.fixes, output_console, file_path=str(resolved_path))

    held = result.receipt.suggested_fixes
    if held:
        reason_counts: dict[str, int] = {}
        for candidate in held:
            reason_counts[str(candidate.review_reason)] = (
                reason_counts.get(str(candidate.review_reason), 0) + 1
            )
        summary = "  ".join(f"{reason}: {count}" for reason, count in sorted(reason_counts.items()))
        output_console.print(
            Panel(
                f"[yellow]{len(held)} fix(es) held or rejected[/yellow]\n{summary}",
                title="Held / Rejected",
                style="yellow",
            )
        )

    if result.receipt.applied:
        output_console.print(
            Panel(
                f"[green]Applied {len(result.fixes)} proven fix(es) from "
                f"'{proposer}'.[/green]\nTransaction ID: [bold]{result.receipt.txn_id}[/bold]",
                title="Verify-Apply Applied",
                style="green",
            )
        )
        if result.receipt.revert_command:
            output_console.print(f"[dim]Revert with: {result.receipt.revert_command}[/dim]")
        raise typer.Exit(code=0)

    output_console.print(
        Panel(
            f"[yellow]{result.receipt.reason}[/yellow]",
            title="Verify-Apply Summary",
            style="yellow",
        )
    )
    raise typer.Exit(code=1)
