"""CLI subcommand: ``dataforge repair <path> [--dry-run | --apply]``."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, cast

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
from dataforge.ui.repair_diff import render_repair_diff

if TYPE_CHECKING:
    import pandas as pd

    from dataforge.engine.repair import FdDetectionSource, RepairPipelineResult

_console = Console(stderr=True)


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
        # Confirm only the guards that actually fired. Until 2026-08-29 this set
        # `confirm_escalations=True`, which cleared all four soft rules at once -- and
        # because `engine/repair.py` reassigns the returned context into its loop
        # variable, one `y` here disabled the blast-radius, aggregate-dependency and
        # prompt-injection guards for every remaining issue in the run. The operator was
        # asked about one fix and answered for the whole table.
        flags = safety_filter.confirm_flags_for(safety_result.rule_ids)
        if not flags:
            return context, safety_result
        updated = context.model_copy(update=dict.fromkeys(flags, True))
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


def _json_result(result: RepairPipelineResult, *, attestation: dict[str, Any] | None = None) -> str:
    """Serialize a repair result for CLI/MCP/CI consumers.

    ``attestation`` is merged as a SIBLING of the receipt rather than a receipt field:
    ``tests/integration/test_surface_uniformity.py`` asserts the pipeline, agent and
    external-guardrail certificates share one schema and one field set, so adding to the
    receipt would break the parity that makes those three surfaces comparable.
    """
    payload = result.model_dump(mode="json")
    if attestation is not None:
        payload.update(attestation)
    return json.dumps(payload, indent=2, sort_keys=True)


def _attest_result(
    result: RepairPipelineResult,
    *,
    source_path: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    """Attest a completed repair, or say why not.

    Called on every ``--json`` run so the portable proof reaches a consumer without a second
    command. ``dataforge attest build`` remains for attesting a receipt saved earlier.

    Only an APPLIED repair is attested. A dry run has written nothing, so its subject digest
    would be the source rather than a result, and an attestation over an unchanged file
    invites being read as a certificate of a repair that did not happen.

    The declared premise is embedded from the schema FILE rather than from the parsed
    ``Schema`` object, because the dataclass has no JSON projection and inventing one would
    put an unreviewed wire format on the critical path of a normative artifact. Embedding it
    is not optional in general: ``_check_strength`` requires the constraints for any fix whose
    provenance is untrusted and which is proven by schema rather than by construction, which
    is precisely the external-proposer case. Without them the emission is withheld and the
    reason names ``constraints_present`` -- correct behaviour, but useless to a caller who did
    supply a schema.
    """
    from dataforge import __version__
    from dataforge.attestation import attest_repair
    from dataforge.cli.common import load_schema, load_schema_mapping
    from dataforge.witness import witnesses_for_applied_fixes

    if not result.receipt.applied:
        return {
            "attestation_unavailable": (
                "nothing was applied, so there is no post-state to attest. Re-run with "
                "--apply to obtain a portable attestation."
            )
        }

    applied = [fix.model_dump(mode="json") for fix in result.receipt.applied_fixes]
    emission = attest_repair(
        result.receipt.model_dump(mode="json"),
        subject_name=source_path.name,
        tool_version=__version__,
        produced_at=datetime.now(UTC).isoformat(),
        constraints=load_schema_mapping(schema_path) if schema_path is not None else None,
        journal_head_sha256=None,
        data_bytes=source_path.read_bytes() if source_path.is_file() else None,
        witnesses=witnesses_for_applied_fixes(
            source_path,
            applied,
            load_schema(schema_path) if schema_path is not None else None,
        ),
    )
    return emission.as_dict()


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
    allow_unproven_autoapply: bool,
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
            allow_unproven_autoapply=allow_unproven_autoapply,
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
            f"Held unproven: {len(result.held_fixes)}  "
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
    trust_mined_constraints: Annotated[
        bool,
        typer.Option(
            "--trust-mined-constraints",
            help="Let constraints MINED from your table and accepted in `constraints review` "
            "authorise writes, as they did before 2026-09-07. Off by default: accepting a "
            "mined candidate is not the same evidence as declaring one. On the reference "
            "corpus this authorised 451 real repairs and 116 clean-cell corruptions at "
            "proposal stage. Declaring a schema is the better premise: with --schema and "
            "--confirm-escalations it corrects 152 of that corpus's 509 errors with ZERO "
            "corruptions. Without --confirm-escalations it writes nothing, because a batch over "
            "100 cells is held. See docs/trust/fd-repair-yield-mechanism.md.",
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
                allow_unproven_autoapply=allow_unproven_autoapply,
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
                allow_unproven_autoapply=allow_unproven_autoapply,
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
        corrector_scope = None
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
            from dataforge.calibration import (
                load_calibration_scope,
                load_corrector_calibration,
            )

            corrector_policy, calibration_maps, corrector_reference = load_corrector_calibration(
                corrector_calibration
            )
            # Load the artifact's recorded scope so the pipeline can refuse a certificate
            # fitted on a different table. Without this the scope guard is unreachable and
            # any artifact is accepted against any table.
            corrector_scope = load_calibration_scope(corrector_calibration)

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
                corrector_calibration_scope=corrector_scope,
                fd_detection_source=_parse_fd_detection(fd_detection),
                mined_constraints_grant_write_authority=trust_mined_constraints,
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
        typer.echo(
            _json_result(
                result,
                attestation=_attest_result(result, source_path=resolved_path, schema_path=schema),
            )
        )
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
