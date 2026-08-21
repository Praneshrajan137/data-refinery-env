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
from dataforge.table import TableLike

_console = Console(stderr=True)


def _session_path(source_path: Path) -> Path:
    """Return the canonical session location for a source table."""
    return calibration_dir_for(source_path) / "session.json"


def _parse_labels(labels: list[str], *, flag: str = "--label") -> list[tuple[int, str, str]]:
    """Parse ``row:column=error|correct`` triples, failing loudly on a malformed one.

    Silently skipping a malformed label would drop a user's judgement without telling them,
    and the estimate would then be computed from fewer labels than they believe.
    """
    parsed: list[tuple[int, str, str]] = []
    for raw in labels:
        if "=" not in raw or ":" not in raw.split("=", 1)[0]:
            raise ValueError(f"{flag} must look like ROW:COLUMN=error|correct (got {raw!r})")
        locator, decision = raw.split("=", 1)
        row_text, column = locator.split(":", 1)
        decision = decision.strip().lower()
        if decision not in ("error", "correct"):
            raise ValueError(f"{flag} decision must be 'error' or 'correct' (got {decision!r})")
        if not column.strip():
            raise ValueError(f"{flag} needs a column name (got {raw!r})")
        parsed.append((int(row_text), column.strip(), decision))
    return parsed


def _propose_repairs(
    artifact: CalibrationSessionArtifact,
    df: TableLike,
    *,
    max_usd: float,
    session_path: Path | None = None,
    checkpoint_every: int = 10,
) -> tuple[CalibrationSessionArtifact, int, float, str, str]:
    """Attach LLM repair proposals to every unproposed sample in the session.

    This is what makes :func:`~dataforge.calibration_session.certify_from_session` reachable.
    Before it existed, nothing outside the test suite populated ``proposed_repair``, so the
    local certification path -- the ONLY place where exchangeability holds by construction,
    and therefore the only place a conformal guarantee is genuinely valid -- could not be
    exercised at all.

    Uses the structured-enum corrector deliberately. Free-text confidence is measurably at
    chance (ROC-AUC 0.5536 in ``eval/results/corrector_arm_sweep.json``) for a mechanical
    reason: the free-text prompts never request JSON, so the model's own confidence is never
    parsed and the signal collapses to vote agreement over k. Certifying on that would be
    certifying noise.

    Returns:
        ``(artifact, proposed_count, spend_usd, provider, model)``.
    """
    from dataforge.agent.providers import Message, get_provider_name, resolve_model
    from dataforge.bench.runner import _build_azure_client
    from dataforge.calibration_session import label_repair_sample
    from dataforge.repairers.contract import build_correction_contract
    from dataforge.repairers.llm_corrector import LLMCorrectorRepairer
    from dataforge.spend import CostCapExceededError, require_price_for

    provider = get_provider_name()
    model = resolve_model(provider)
    if provider != "azure":
        raise ValueError(
            f"--propose currently supports the azure provider only (active: {provider!r}); "
            "set DATAFORGE_LLM_PROVIDER=azure"
        )
    # Fail closed on price before spending: metering a frontier deployment at a cheaper
    # sibling's rate is how a capped run overspends.
    require_price_for(provider, model)

    client = _build_azure_client()

    def structured_call(messages: list[Message], response_format: dict[str, object] | None) -> str:
        payload = [{str(k): str(v) for k, v in message.items()} for message in messages]
        return client.complete(payload, response_format).text

    corrector = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model=client.model,
        samples=3,
        structured_completion_fn=structured_call,
        structured=True,
    )
    constraints = corrector._constraints_for(df, None)
    issues_by_cell = {(issue.row, issue.column): issue for issue in run_all_detectors(df, None)}

    proposed = 0
    for sample in list(artifact.samples):
        if sample.proposed_repair is not None:
            continue
        issue = issues_by_cell.get((sample.row, sample.column))
        if issue is None:
            continue
        contract = build_correction_contract(issue, constraints)
        if not contract.is_cell_correction:
            continue
        if client.cumulative_usd >= max_usd:
            _console.print(f"[yellow]Proposal budget ${max_usd} reached; stopping.[/yellow]")
            break
        try:
            fix = corrector.propose(issue, df, None)
        except CostCapExceededError:
            _console.print("[yellow]Spend cap reached; stopping proposals.[/yellow]")
            break
        except Exception as exc:  # noqa: BLE001 - one bad cell must not kill a paid run
            _console.print(f"[dim]  row {sample.row} {sample.column}: {type(exc).__name__}[/dim]")
            continue
        if fix is None:
            continue
        artifact = label_repair_sample(
            artifact,
            row=sample.row,
            column=sample.column,
            decision="pending",
            proposed_repair=str(fix.fix.new_value),
            repair_confidence=float(fix.confidence),
            corrector_provider=provider,
            corrector_model=model,
        )
        proposed += 1
        # Checkpoint the session AND the receipt as we go. Without this, a run that dies --
        # a detached shell crossing a session boundary, a stall, a Ctrl-C -- loses both the
        # proposals it already paid for and the audit trail for that spend. That happened
        # once here: a ~1,600-call run died with no session file and no receipt, leaving
        # money spent and unaccounted. Losing data is annoying; losing the audit trail
        # contradicts the project's own spend-accountability doctrine.
        if session_path is not None and proposed % checkpoint_every == 0:
            session_path.parent.mkdir(parents=True, exist_ok=True)
            session_path.write_text(dump_calibration_session(artifact), encoding="utf-8")
            _write_propose_receipt(client, artifact, proposed)
            _console.print(
                f"[dim]  {proposed} proposals, ${client.cumulative_usd:.3f} (checkpointed)[/dim]"
            )

    # Write a ledger receipt. `--propose` is the only paid path in this command, and an
    # unaudited paid path contradicts the project's own spend-accountability doctrine: the
    # ledger is what separates measured spend from reconstructed guesswork.
    if proposed or client.meter.calls:
        _write_propose_receipt(client, artifact, proposed)

    return artifact, proposed, client.cumulative_usd, provider, model


def _write_propose_receipt(
    client: object, artifact: CalibrationSessionArtifact, proposed: int
) -> None:
    """Upsert the ledger receipt for a --propose run.

    **Upsert, not append.** ``SpendMeter.receipt()`` reports CUMULATIVE spend, so appending one
    receipt per checkpoint makes the ledger sum wildly wrong: a 229-proposal run wrote 23
    receipts whose naive total came to $74.49 against a true $14.35, because summing cumulative
    snapshots double-counts and ``ledger_summary`` sums. One receipt per run id, replaced in
    place, keeps mid-flight crash-safety without inflating the total.
    """
    from dataforge.spend import load_ledger

    ledger = Path("eval") / "results" / "spend_ledger.json"
    run_id = f"calibrate-propose-{artifact.source_sha256[:12]}"
    try:
        receipt = client.meter.receipt(  # type: ignore[attr-defined]
            run_id=run_id,
            method="calibrate_propose",
            dataset=Path(artifact.source_path).name,
            notes=(
                f"proposals={proposed}",
                f"samples={len(artifact.samples)}",
                "local calibration session; structured enum corrector",
            ),
        )
        kept: list[dict[str, object]] = []
        if ledger.exists():
            kept = [r for r in load_ledger(ledger) if r.get("run_id") != run_id]
        ledger.parent.mkdir(parents=True, exist_ok=True)
        ledger.write_text(
            json.dumps(
                {
                    "schema": "dataforge_spend_ledger_v1",
                    "receipts": [*kept, receipt.to_payload()],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
        _console.print(f"[yellow]WARNING: spend receipt not written: {exc}[/yellow]")


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
    propose: Annotated[
        bool,
        typer.Option(
            "--propose",
            help="SPENDS MONEY. Ask the LLM corrector for a replacement value for each "
            "sampled cell, so repair verdicts (and therefore local certification) become "
            "possible. Uses the structured-enum corrector, because free-text confidence is "
            "measurably at chance.",
        ),
    ] = False,
    propose_max_usd: Annotated[
        float,
        typer.Option("--propose-max-usd", help="Hard spend ceiling for --propose."),
    ] = 5.0,
    label_repair: Annotated[
        list[str] | None,
        typer.Option(
            "--label-repair",
            help="Judge a PROPOSED VALUE as ROW:COLUMN=correct|error. Distinct from "
            "--label, which judges whether the cell was an error at all. Only repair "
            "verdicts can certify auto-apply. Repeatable.",
        ),
    ] = None,
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

    if propose:
        try:
            artifact, proposed, spend, provider, model = _propose_repairs(
                artifact, df, max_usd=propose_max_usd, session_path=session_path
            )
        except ValueError as exc:
            _console.print(f"[bold red]Could not propose repairs:[/bold red] {exc}")
            raise typer.Exit(code=2) from exc
        _console.print(
            f"[green]Attached {proposed} proposals from {provider}/{model} "
            f"(spent ${spend:.4f}).[/green] Judge them with "
            "--label-repair ROW:COLUMN=correct|error."
        )

    if label_repair:
        from dataforge.calibration_session import label_repair_sample

        try:
            for row, column, decision in _parse_labels(label_repair, flag="--label-repair"):
                # Polarity differs from --label on purpose: here "correct" means the PROPOSED
                # VALUE is right, not that the cell was fine.
                artifact = label_repair_sample(
                    artifact,
                    row=row,
                    column=column,
                    decision="correct" if decision == "correct" else "error",
                )
        except (ValueError, KeyError) as exc:
            _console.print(f"[bold red]Could not record repair verdict:[/bold red] {exc}")
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
