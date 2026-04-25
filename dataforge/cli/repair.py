"""CLI subcommand: ``dataforge repair <path> [--dry-run | --apply]``."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.panel import Panel

from dataforge.cli.common import load_schema, read_csv
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.repairers import build_repairers
from dataforge.repairers.base import ProposedFix, RepairAttempt, RetryContext
from dataforge.safety import SafetyContext, SafetyFilter, SafetyResult, SafetyVerdict
from dataforge.transactions.log import (
    append_applied_event,
    append_created_transaction,
    cache_dir_for,
    sha256_bytes,
    snapshot_path_for,
)
from dataforge.transactions.txn import CellFix, RepairTransaction, generate_txn_id
from dataforge.ui.repair_diff import render_repair_diff
from dataforge.verifier import SMTVerifier, VerificationVerdict

_console = Console(stderr=True)


def apply_fixes_to_csv(path: Path, fixes: list[CellFix]) -> str:
    """Apply ordered cell fixes to a CSV and return the post-state SHA-256.

    Args:
        path: Source CSV path.
        fixes: Ordered list of cell fixes to apply.

    Returns:
        SHA-256 of the written file bytes.

    Raises:
        ValueError: If a fix references a missing row/column or stale old value.
    """
    df = read_csv(path)
    for fix in fixes:
        if fix.operation != "update":
            raise ValueError(f"Unsupported repair operation '{fix.operation}' for row {fix.row}.")
        if fix.column not in df.columns:
            raise ValueError(f"Column '{fix.column}' not found in '{path}'.")
        if fix.row < 0 or fix.row >= len(df.index):
            raise ValueError(f"Row {fix.row} is out of bounds for '{path}'.")

        current_value = str(df.at[fix.row, fix.column])
        if current_value != fix.old_value:
            raise ValueError(
                f"Refusing to apply stale fix for row {fix.row}, column '{fix.column}': "
                f"expected '{fix.old_value}', found '{current_value}'."
            )
        df.at[fix.row, fix.column] = fix.new_value

    df.to_csv(path, index=False, lineterminator="\n")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve_schema(schema_path: Path | None) -> Schema | None:
    """Resolve an optional schema path into a parsed Schema."""
    if schema_path is None:
        return None
    return load_schema(schema_path)


def _print_error(message: str, *, hint: str | None = None) -> None:
    """Render a rich-formatted CLI error."""
    body = f"[bold red]{message}[/bold red]"
    if hint:
        body = f"{body}\n\n[dim]{hint}[/dim]"
    _console.print(Panel(body, title="Repair Error", style="red"))


def _propose_repairs(
    issues: list[Issue],
    path: Path,
    working_df: pd.DataFrame,
    schema: Schema | None,
    *,
    allow_llm: bool,
    model: str,
    allow_pii: bool,
    confirm_pii: bool,
    confirm_escalations: bool,
    interactive: bool,
) -> tuple[list[ProposedFix], list[list[RepairAttempt]]]:
    """Run repairers and gates issue-by-issue against the working dataframe."""
    repairers = build_repairers(
        cache_dir=cache_dir_for(path),
        allow_llm=allow_llm,
        model=model,
    )
    safety_filter = SafetyFilter()
    verifier = SMTVerifier()
    safety_context = SafetyContext(
        allow_pii=allow_pii,
        confirm_pii=confirm_pii,
        confirm_escalations=confirm_escalations,
    )

    accepted_fixes: list[ProposedFix] = []
    attempt_groups: list[list[RepairAttempt]] = []

    for issue in issues:
        attempts: list[RepairAttempt] = []
        repairer = repairers.get(issue.issue_type)
        if repairer is None:
            attempts.append(
                RepairAttempt(
                    issue=issue,
                    attempt_number=1,
                    status="attempted_not_fixed",
                    reason="No repairer is registered for this issue type.",
                )
            )
            attempt_groups.append(attempts)
            continue

        accepted = False
        retry_context = RetryContext(issue=issue)
        for attempt_number in range(1, 4):
            candidate = repairer.propose(issue, working_df, schema, retry_context=retry_context)
            if candidate is None:
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        status="attempted_not_fixed",
                        reason="No repair proposal was available for this issue.",
                    )
                )
                break

            preferred = safety_filter.choose_preferred([candidate], schema, safety_context)
            safety_result = safety_filter.evaluate(preferred, schema, safety_context)
            if safety_result.verdict == SafetyVerdict.ESCALATE and interactive:
                safety_context, safety_result = _resolve_escalation(
                    preferred,
                    schema,
                    safety_context,
                    safety_filter,
                    safety_result,
                )

            if safety_result.verdict == SafetyVerdict.DENY:
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        fix=preferred,
                        status="denied",
                        reason=safety_result.reason,
                    )
                )
                retry_context = _build_retry_context(issue, attempts)
                continue

            if safety_result.verdict == SafetyVerdict.ESCALATE:
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        fix=preferred,
                        status="escalated",
                        reason=safety_result.reason,
                    )
                )
                break

            verifier_result = verifier.verify(working_df, [preferred], schema)
            if verifier_result.verdict == VerificationVerdict.ACCEPT:
                accepted_fixes.append(preferred)
                working_df.at[preferred.fix.row, preferred.fix.column] = preferred.fix.new_value
                attempts.append(
                    RepairAttempt(
                        issue=issue,
                        attempt_number=attempt_number,
                        fix=preferred,
                        status="accepted",
                        reason=verifier_result.reason,
                    )
                )
                accepted = True
                break

            attempts.append(
                RepairAttempt(
                    issue=issue,
                    attempt_number=attempt_number,
                    fix=preferred,
                    status=(
                        "rejected"
                        if verifier_result.verdict == VerificationVerdict.REJECT
                        else "unknown"
                    ),
                    reason=verifier_result.reason,
                    unsat_core=verifier_result.unsat_core,
                )
            )
            retry_context = _build_retry_context(issue, attempts)

        if (
            not accepted
            and attempts
            and attempts[-1].status not in {"attempted_not_fixed", "escalated"}
        ):
            last_reason = attempts[-1].reason
            attempts[-1] = attempts[-1].model_copy(
                update={
                    "status": "attempted_not_fixed",
                    "reason": (
                        f"Issue was attempted but not fixed after {len(attempts)} attempt(s). "
                        f"Last failure: {last_reason}"
                    ),
                }
            )
        attempt_groups.append(attempts)

    return accepted_fixes, attempt_groups


def _build_retry_context(issue: Issue, attempts: list[RepairAttempt]) -> RetryContext:
    """Build retry hints from previous failed attempts."""
    rejected_values = frozenset(
        attempt.fix.fix.new_value
        for attempt in attempts
        if attempt.fix is not None and attempt.status in {"denied", "rejected", "unknown"}
    )
    hints: list[str] = []
    for attempt in attempts:
        hints.append(attempt.reason)
        hints.extend(attempt.unsat_core)
    return RetryContext(
        issue=issue,
        previous_attempts=tuple(attempts),
        rejected_values=rejected_values,
        hints=tuple(hints),
    )


def _resolve_escalation(
    candidate: ProposedFix,
    schema: Schema | None,
    context: SafetyContext,
    safety_filter: SafetyFilter,
    safety_result: SafetyResult,
) -> tuple[SafetyContext, SafetyResult]:
    """Prompt for safety escalations and re-evaluate if the user confirms."""
    if "NO_PII_OVERWRITE" in safety_result.rule_ids:
        confirmed = typer.confirm(
            f"Candidate fix for row {candidate.fix.row}, column '{candidate.fix.column}' "
            "touches PII. Confirm this edit?",
            default=False,
        )
        if confirmed:
            updated = context.model_copy(update={"confirm_pii": True})
            return updated, safety_filter.evaluate(candidate, schema, updated)
        return context, safety_result

    confirmed = typer.confirm(
        f"Candidate fix for row {candidate.fix.row}, column '{candidate.fix.column}' "
        "touches an aggregate-sensitive column. Confirm this edit?",
        default=False,
    )
    if confirmed:
        updated = context.model_copy(update={"confirm_escalations": True})
        return updated, safety_filter.evaluate(candidate, schema, updated)
    return context, safety_result


def _render_attempt_summary(
    attempt_groups: list[list[RepairAttempt]],
    console: Console,
) -> int:
    """Render a summary for issues that were not accepted."""
    failed_groups = [
        attempts for attempts in attempt_groups if attempts and attempts[-1].status != "accepted"
    ]
    if not failed_groups:
        return 0

    lines: list[str] = []
    for attempts in failed_groups:
        final_attempt = attempts[-1]
        issue = final_attempt.issue
        prefix = ""
        if any(label.startswith("fd::") for label in final_attempt.unsat_core):
            prefix = "functional dependency rejection - "
        elif any(label.startswith("domain::") for label in final_attempt.unsat_core):
            prefix = "domain bound rejection - "
        lines.append(
            f"{issue.issue_type} at {issue.row}:{issue.column} "
            f"after {len(attempts)} attempt(s): {prefix}{final_attempt.reason}"
        )

    console.print("[bold yellow]Attempted But Not Fixed[/bold yellow]")
    for line in lines:
        console.print(line, overflow="fold")
    return len(failed_groups)


def _apply_transaction(
    path: Path,
    fixes: list[ProposedFix],
    source_bytes: bytes,
) -> str:
    """Write a transaction record, apply fixes, and append the applied event."""
    resolved_path = path.resolve()
    txn_id = generate_txn_id()
    snapshot_path = snapshot_path_for(resolved_path, txn_id)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(source_bytes)

    transaction = RepairTransaction(
        txn_id=txn_id,
        created_at=datetime.now(UTC),
        source_path=str(resolved_path),
        source_sha256=sha256_bytes(source_bytes),
        source_snapshot_path=str(snapshot_path.resolve()),
        fixes=[proposal.fix for proposal in fixes],
        applied=False,
    )
    log_path = append_created_transaction(transaction)

    try:
        post_sha256 = apply_fixes_to_csv(path, [proposal.fix for proposal in fixes])
        append_applied_event(log_path, txn_id, post_sha256=post_sha256)
    except Exception:
        path.write_bytes(source_bytes)
        raise

    return txn_id


def repair(
    path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            readable=True,
            help="Path to the CSV file to repair.",
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
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show proposed fixes without changing the file."),
    ] = False,
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Apply fixes and record a reversible transaction."),
    ] = False,
    allow_llm: Annotated[
        bool,
        typer.Option(
            "--allow-llm",
            help="Allow fd_violation repair to call the configured LLM provider if needed.",
        ),
    ] = False,
    allow_pii: Annotated[
        bool,
        typer.Option(
            "--allow-pii",
            help="Allow PII-targeting fixes to be considered by the safety layer.",
        ),
    ] = False,
    confirm_pii: Annotated[
        bool,
        typer.Option(
            "--confirm-pii",
            help="Non-interactively confirm any PII-targeting fixes allowed via --allow-pii.",
        ),
    ] = False,
    confirm_escalations: Annotated[
        bool,
        typer.Option(
            "--confirm-escalations",
            help="Non-interactively confirm soft safety escalations such as aggregate-sensitive edits.",
        ),
    ] = False,
    llm_model: Annotated[
        str,
        typer.Option("--llm-model", help="Model name for fd_violation LLM fallback."),
    ] = "gemini-2.0-flash",
) -> None:
    """Detect, propose, and optionally apply reversible repairs to a CSV."""
    if dry_run == apply:
        _print_error(
            "Choose exactly one of --dry-run or --apply.",
            hint="Example: dataforge repair data.csv --dry-run",
        )
        raise typer.Exit(code=2)

    try:
        parsed_schema = _resolve_schema(schema)
        df = read_csv(path)
    except Exception as exc:
        _print_error(str(exc))
        raise typer.Exit(code=2) from exc

    issues = run_all_detectors(df, parsed_schema)
    accepted_fixes, attempt_groups = _propose_repairs(
        issues,
        path,
        df.copy(deep=True),
        parsed_schema,
        allow_llm=allow_llm,
        model=llm_model,
        allow_pii=allow_pii,
        confirm_pii=confirm_pii,
        confirm_escalations=confirm_escalations,
        interactive=apply,
    )

    output_console = Console()
    render_repair_diff(accepted_fixes, output_console, file_path=str(path))
    failed_issue_count = _render_attempt_summary(attempt_groups, output_console)

    if not accepted_fixes and failed_issue_count == 0:
        raise typer.Exit(code=1)

    if dry_run:
        raise typer.Exit(code=0 if accepted_fixes else 1)

    if not accepted_fixes:
        raise typer.Exit(code=1)

    batch_safety = SafetyFilter().evaluate_batch(accepted_fixes)
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        _print_error(batch_safety.reason)
        raise typer.Exit(code=1)

    source_bytes = path.read_bytes()
    try:
        txn_id = _apply_transaction(path, accepted_fixes, source_bytes)
    except Exception as exc:
        _print_error(
            f"Failed to apply repairs: {exc}",
            hint="The source file was restored to its pre-apply bytes.",
        )
        raise typer.Exit(code=1) from exc

    output_console.print(
        Panel(
            f"[green]Applied {len(accepted_fixes)} fix(es).[/green]\n"
            f"Transaction ID: [bold]{txn_id}[/bold]",
            title="Repair Applied",
            style="green",
        )
    )
    if failed_issue_count:
        output_console.print(
            Panel(
                f"[yellow]{failed_issue_count} issue(s) were attempted but not fixed.[/yellow]",
                title="Week 3 Summary",
                style="yellow",
            )
        )
