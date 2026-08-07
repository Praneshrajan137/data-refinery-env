"""CLI subcommand: ``dataforge repair <path> [--dry-run | --apply]``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, cast

import typer
from rich.console import Console
from rich.panel import Panel

from dataforge.cli.common import load_schema, resolve_cli_path
from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, RepairAttempt
from dataforge.safety import SafetyContext, SafetyFilter, SafetyResult
from dataforge.schema_inference import ConstraintReviewArtifact, load_constraint_review_artifact
from dataforge.stores import (
    TableStoreError,
    is_table_store_uri,
    run_table_store_repair,
    store_from_uri,
)
from dataforge.transactions.txn import CellFix
from dataforge.ui.repair_diff import render_repair_diff

if TYPE_CHECKING:
    import pandas as pd

    from dataforge.engine.repair import FdDetectionSource, RepairPipelineResult

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
    from dataforge.engine.repair import apply_fixes_to_csv as engine_apply_fixes_to_csv

    return engine_apply_fixes_to_csv(path, fixes)


def _resolve_schema(schema_path: Path | None) -> Schema | None:
    """Resolve an optional schema path into a parsed Schema."""
    if schema_path is None:
        return None
    resolved_schema = resolve_cli_path(schema_path)
    if not resolved_schema.exists():
        raise typer.BadParameter(f"Schema file '{schema_path}' does not exist.")
    return load_schema(resolved_schema)


def _resolve_constraints(
    constraints_path: Path | None,
) -> tuple[ConstraintReviewArtifact | None, str | None]:
    """Resolve an optional reviewed constraints artifact."""
    if constraints_path is None:
        return None, None
    resolved_constraints = resolve_cli_path(constraints_path)
    if not resolved_constraints.exists():
        raise typer.BadParameter(f"Constraints file '{constraints_path}' does not exist.")
    return load_constraint_review_artifact(resolved_constraints)


def _parse_fd_detection(value: str) -> FdDetectionSource:
    """Validate --fd-detection, failing fast on a typo rather than silently defaulting.

    A silent fallback here would be dangerous: mistyping the strict value would leave the
    permissive default in place and quietly reinstate a 19x review queue.
    """
    normalized = value.strip().lower()
    if normalized not in ("declared", "accepted", "none"):
        _print_error(
            f"--fd-detection must be 'declared', 'accepted', or 'none' (got {value!r}).",
            hint="'declared' keeps only hand-declared FDs able to raise issues.",
        )
        raise typer.Exit(code=2)
    # String form on purpose: FdDetectionSource is imported only under TYPE_CHECKING, so a
    # bare name here raises NameError at runtime.
    return cast("FdDetectionSource", normalized)


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
    model: str | None,
    allow_pii: bool,
    confirm_pii: bool,
    confirm_escalations: bool,
    interactive: bool,
) -> tuple[list[ProposedFix], list[list[RepairAttempt]]]:
    """Compatibility wrapper around the shared repair engine proposal stage."""
    from dataforge.engine.repair import propose_repairs as engine_propose_repairs

    return engine_propose_repairs(
        issues,
        path,
        working_df,
        schema,
        allow_llm=allow_llm,
        model=model,
        allow_pii=allow_pii,
        confirm_pii=confirm_pii,
        confirm_escalations=confirm_escalations,
        interactive=interactive,
        escalation_resolver=_resolve_escalation,
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
        f"requires confirmation ({', '.join(safety_result.rule_ids)}). Confirm this edit?",
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


def _render_failure_summary(result: RepairPipelineResult, console: Console) -> int:
    """Render a summary for issues that the shared engine could not repair."""
    if not result.failures:
        return 0

    console.print("[bold yellow]Attempted But Not Fixed[/bold yellow]")
    for failure in result.failures:
        prefix = ""
        if any(label.startswith("fd::") for label in failure.unsat_core):
            prefix = "functional dependency rejection - "
        elif any(label.startswith("domain::") for label in failure.unsat_core):
            prefix = "domain bound rejection - "
        console.print(
            f"{failure.issue_type} at {failure.row}:{failure.column} "
            f"after {failure.attempt_count} attempt(s): {prefix}{failure.reason}",
            overflow="fold",
        )
    return len(result.failures)


def _json_result(result: RepairPipelineResult) -> str:
    """Serialize a repair result for CLI/MCP/CI consumers."""
    return json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True)


def _apply_transaction(
    path: Path,
    fixes: list[ProposedFix],
    source_bytes: bytes,
) -> str:
    """Compatibility wrapper around the shared repair engine transaction path."""
    from dataforge.engine.repair import apply_transaction as engine_apply_transaction

    return engine_apply_transaction(path, fixes, source_bytes)


def _run_agent_repair(
    resolved_path: Path,
    schema: Schema | None,
    *,
    apply: bool,
    policy: str,
    provider: str | None,
    max_steps: int,
    model: str | None,
    allow_pii: bool,
    confirm_pii: bool,
    confirm_escalations: bool,
    json_output: bool,
) -> None:
    """Run the verified autonomous agent and render its result."""
    from dataforge.agent import AgentRepairRequest, run_agent_repair

    result = run_agent_repair(
        AgentRepairRequest(
            source_path=resolved_path,
            mode="apply" if apply else "dry_run",
            schema=schema,
            policy=policy,
            provider=provider,
            max_steps=max_steps,
            model=model,
            allow_pii=allow_pii,
            confirm_pii=confirm_pii,
            confirm_escalations=confirm_escalations,
        )
    )

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True))
        raise typer.Exit(code=0 if result.fixes else 1)

    output_console = Console()
    render_repair_diff(result.fixes, output_console, file_path=str(resolved_path))
    output_console.print(
        Panel(
            f"Policy: [bold]{result.policy_name}[/bold]  "
            f"Steps used: {result.steps_used}/{result.max_steps}\n"
            f"Verified fixes: {result.fixes_count} "
            f"([green]{result.floor_fix_count}[/green] deterministic, "
            f"[cyan]{result.agent_fix_count}[/cyan] agent)  "
            f"Residual: {result.residual_count}\n"
            f"Safety: {result.safety_verdict}  "
            f"{'Applied txn ' + str(result.txn_id) if result.applied else 'Dry run (no mutation)'}",
            title="Verified Agent Repair",
            style="green" if result.fixes else "yellow",
        )
    )
    if result.applied and result.revert_command:
        output_console.print(f"[dim]Revert with: {result.revert_command}[/dim]")
    raise typer.Exit(code=0 if result.fixes else 1)


def repair(
    path: Annotated[
        str,
        typer.Argument(
            help="Path to the CSV file, or warehouse:// backend URI, to repair.",
        ),
    ],
    schema: Annotated[
        Path | None,
        typer.Option(
            "--schema",
            help="Path to a YAML schema file with column types and FDs.",
        ),
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
    allow_entity_consensus: Annotated[
        bool,
        typer.Option(
            "--allow-entity-consensus",
            help="Enable cross-row entity-consensus repair (multi-source data, e.g. flights): "
            "propose the value shared by an entity's sibling rows. Held for review by default; "
            "combine with --allow-unproven-autoapply to auto-apply it.",
        ),
    ] = False,
    corrector_pool_constrained: Annotated[
        bool,
        typer.Option(
            "--corrector-pool-constrained",
            help="Constrain the LLM corrector to SELECT a value from the column's frequent-value "
            "pool (vs free-text). Measured ~5-10x higher proposal precision. Requires --allow-llm; "
            "proposals stay review-only (never auto-applied).",
        ),
    ] = False,
    corrector_structured: Annotated[
        bool,
        typer.Option(
            "--corrector-structured",
            help="Enforce the candidate pool as a hard decode-time enum via Structured Outputs "
            "(instead of a prompt request plus post-filter), and require the model to emit a "
            "confidence. Implies --corrector-pool-constrained. Requires --allow-llm and provider "
            "support; proposals stay review-only unless a certified calibration artifact is loaded.",
        ),
    ] = False,
    review_rank: Annotated[
        bool,
        typer.Option(
            "--review-rank",
            help="Order the review queue by an LLM triage score (highest-likelihood real "
            "errors first). Presentation-only: a score can never be applied. Most valuable "
            "when the queue is flooded with detector false positives; adds nothing when the "
            "queue is already high-precision. Requires a provider key.",
        ),
    ] = False,
    review_rank_max_cells: Annotated[
        int,
        typer.Option(
            "--review-rank-max-cells",
            min=1,
            help="Maximum flagged cells to triage with --review-rank (one LLM call each). "
            "Bounds spend. When the queue is larger, the excess is left unranked and a "
            "warning reports how much was covered.",
        ),
    ] = 200,
    fd_detection: Annotated[
        str,
        typer.Option(
            "--fd-detection",
            help="Which functional dependencies may RAISE ISSUES: 'accepted' (default: any "
            "FD in the effective schema), 'declared' (only hand-declared FDs), or 'none'. "
            "Inferred FDs are cheap to accept and expensive to live with: on hospital they "
            "turn a 549-cell queue that is 56% real errors into 10,373 cells at 4.4% -- "
            "+147 true errors for +9,824 false positives. Note that "
            "--require-declared-fds-for-autoapply only blocks writes, not flags.",
        ),
    ] = "accepted",
    allow_unproven_autoapply: Annotated[
        bool,
        typer.Option(
            "--allow-unproven-autoapply",
            help="Auto-apply evidence-strong-but-unproven fixes (entity consensus, inferred-guard "
            "values) without an authoritative schema. Honestly recorded as not-proven in the "
            "certificate; still reversible. Off by default (proven-only auto-apply).",
        ),
    ] = False,
    llm_model: Annotated[
        str | None,
        typer.Option(
            "--llm-model",
            help=(
                "LLM model id override. When omitted, uses "
                "DATAFORGE_<PROVIDER>_MODEL (groq/gemini/bedrock) then the provider default."
            ),
        ),
    ] = None,
    agent: Annotated[
        bool,
        typer.Option(
            "--agent",
            help="Use the verified autonomous agent: deterministic floor first, then "
            "an LLM policy resolves residual issues, every write SMT+constitution gated.",
        ),
    ] = False,
    policy: Annotated[
        str,
        typer.Option(
            "--policy",
            help="Agent policy backend: hosted (provider, default), local (trained model), "
            "deterministic (floor only), or custom:<name>. Only used with --agent.",
        ),
    ] = "hosted",
    provider: Annotated[
        str | None,
        typer.Option(
            "--provider",
            help="Hosted provider: groq or gemini. Falls back to DATAFORGE_LLM_PROVIDER "
            "/ key autodetect. Only used with --agent --policy hosted.",
        ),
    ] = None,
    max_steps: Annotated[
        int,
        typer.Option(
            "--max-steps",
            help="Maximum agent reasoning steps per run. Only used with --agent.",
            min=1,
            max=200,
        ),
    ] = 30,
    row_id: Annotated[
        list[str] | None,
        typer.Option(
            "--row-id",
            help="Stable row identity column for warehouse apply. Repeat for composite keys.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print repair result as JSON."),
    ] = False,
    corrector_calibration: Annotated[
        Path | None,
        typer.Option(
            "--corrector-calibration",
            help="Path to a certified corrector-calibration artifact (per-issue-type "
            "conformal thresholds + calibration maps). Only used with --allow-llm and an "
            "authoritative --schema; lets high-agreement, schema-verified LLM corrections "
            "auto-apply. Without it (default), every LLM correction stays propose-not-apply.",
        ),
    ] = None,
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
        constraints_artifact, constraints_sha256 = _resolve_constraints(constraints)
        if is_table_store_uri(path):
            if constraints_artifact is not None:
                raise typer.BadParameter(
                    "Reviewed constraints are not yet accepted for warehouse URI repair."
                )
            store = store_from_uri(path, row_ids=tuple(row_id or ()))
            store_result = run_table_store_repair(
                store,
                mode="apply" if apply else "dry_run",
                schema=parsed_schema,
                allow_llm=allow_llm,
                model=llm_model,
                allow_pii=allow_pii,
                confirm_pii=confirm_pii,
                confirm_escalations=confirm_escalations,
            )
            if json_output:
                typer.echo(
                    json.dumps(store_result.model_dump(mode="json"), indent=2, sort_keys=True)
                )
                return
            output_console = Console()
            output_console.print(
                Panel(
                    (
                        f"[bold]{store_result.backend}[/bold] patch plan "
                        f"{store_result.patch_plan.plan_id}\n"
                        f"Operations: {len(store_result.patch_plan.operations)}\n"
                        f"Apply supported: {store_result.patch_plan.apply_supported}\n"
                        f"Reason: {store_result.patch_plan.reason}"
                    ),
                    title="Warehouse Repair Plan",
                    style="green" if store_result.patch_plan.apply_supported else "yellow",
                )
            )
            return

        resolved_path = resolve_cli_path(Path(path))
        if not resolved_path.exists():
            raise typer.BadParameter(f"CSV file '{path}' does not exist.")
    except TableStoreError as exc:
        _print_error(str(exc))
        raise typer.Exit(code=1 if apply else 2) from exc
    except Exception as exc:
        _print_error(str(exc))
        raise typer.Exit(code=2) from exc

    if agent and is_table_store_uri(path):
        _print_error(
            "The verified agent currently supports CSV files only, not warehouse URIs.",
            hint="Run without --agent for warehouse repair, or pass a CSV path.",
        )
        raise typer.Exit(code=2)

    if agent:
        try:
            _run_agent_repair(
                resolved_path,
                parsed_schema,
                apply=apply,
                policy=policy,
                provider=provider,
                max_steps=max_steps,
                model=llm_model,
                allow_pii=allow_pii,
                confirm_pii=confirm_pii,
                confirm_escalations=confirm_escalations,
                json_output=json_output,
            )
        except typer.Exit:
            raise
        except Exception as exc:
            _print_error(
                f"Agent repair failed: {exc}",
                hint="The source file was restored to its pre-apply bytes." if apply else None,
            )
            raise typer.Exit(code=1 if apply else 2) from exc
        return

    try:
        from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

        corrector_policy = None
        calibration_maps = None
        corrector_reference = None
        if corrector_structured and not allow_llm:
            _print_error(
                "--corrector-structured requires --allow-llm.",
                hint="Add --allow-llm, or drop --corrector-structured.",
            )
            raise typer.Exit(code=2)
        if corrector_calibration is not None:
            if not allow_llm:
                _print_error(
                    "--corrector-calibration requires --allow-llm.",
                    hint="Add --allow-llm, or drop --corrector-calibration.",
                )
                raise typer.Exit(code=2)
            from dataforge.calibration import load_corrector_calibration

            corrector_policy, calibration_maps, corrector_reference = load_corrector_calibration(
                corrector_calibration
            )

        ranker = None
        if review_rank:
            from dataforge.review import ReviewRanker
            from dataforge.transactions.log import cache_dir_for

            # Presentation-only by construction: the ranker is passed as a
            # separate argument to run_repair_pipeline, not as part of the
            # request, so there is no path from a triage score to a mutation.
            ranker = ReviewRanker(cache_dir=cache_dir_for(resolved_path), model=llm_model)
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=resolved_path,
                mode="apply" if apply else "dry_run",
                schema=parsed_schema,
                constraints=constraints_artifact,
                constraints_artifact_sha256=constraints_sha256,
                allow_llm=allow_llm,
                model=llm_model,
                allow_pii=allow_pii,
                confirm_pii=confirm_pii,
                confirm_escalations=confirm_escalations,
                interactive=apply,
                allow_entity_consensus=allow_entity_consensus,
                allow_unproven_autoapply=allow_unproven_autoapply,
                corrector_pool_constrained=corrector_pool_constrained,
                corrector_structured=corrector_structured,
                corrector_policy=corrector_policy,
                calibration_map_by_class=calibration_maps,
                corrector_reference_confidences=corrector_reference,
                fd_detection_source=_parse_fd_detection(fd_detection),
            ),
            review_ranker=ranker,
            review_ranker_max_cells=review_rank_max_cells,
        )
    except Exception as exc:
        _print_error(
            f"Failed to apply repairs: {exc}" if apply else f"Failed to repair: {exc}",
            hint="The source file was restored to its pre-apply bytes." if apply else None,
        )
        raise typer.Exit(code=1 if apply else 2) from exc

    if json_output:
        typer.echo(_json_result(result))
        raise typer.Exit(code=0 if result.fixes else 1)

    output_console = Console()
    if review_rank:
        # Silence here was the real defect: a capped triage pass on a flooded
        # queue would rank a small slice and say nothing, so the user would
        # believe the whole queue had been ordered. Report coverage explicitly.
        ranked = len(result.receipt.review_ranking)
        flagged = result.receipt.issues_count
        if ranked < flagged:
            output_console.print(
                f"[yellow]Triaged {ranked} of {flagged} flagged cells "
                f"(--review-rank-max-cells={review_rank_max_cells}). The remaining "
                f"{flagged - ranked} are unranked; raise the cap to cover more, at "
                f"one LLM call per cell.[/yellow]"
            )
        else:
            output_console.print(f"[green]Triaged all {ranked} flagged cells.[/green]")
    render_repair_diff(result.fixes, output_console, file_path=str(resolved_path))
    failed_issue_count = _render_failure_summary(result, output_console)

    if not result.fixes and failed_issue_count == 0:
        if result.receipt.reason != "No accepted fixes were produced.":
            output_console.print(
                Panel(
                    f"[yellow]{result.receipt.reason}[/yellow]",
                    title="Repair Summary",
                    style="yellow",
                )
            )
        raise typer.Exit(code=1)

    if dry_run:
        raise typer.Exit(code=0 if result.fixes else 1)

    if not result.fixes or not result.receipt.applied:
        raise typer.Exit(code=1)

    output_console.print(
        Panel(
            f"[green]Applied {len(result.fixes)} fix(es).[/green]\n"
            f"Transaction ID: [bold]{result.receipt.txn_id}[/bold]",
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
