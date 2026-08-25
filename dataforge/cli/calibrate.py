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
    SessionCertification,
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
    # Keyed on the SESSION, not the table. Keying on the source hash alone meant a second
    # `--reset` run on the same file replaced the first run's receipt, so the ledger understated
    # real spend by an entire run -- the mirror image of the historical 5x overstatement, and the
    # same class of defect: a receipt key that does not identify what it is a receipt for.
    session_key = artifact.session_id or artifact.source_sha256[:12]
    run_id = f"calibrate-propose-{session_key}"
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


def _render_certification(certification: SessionCertification) -> None:
    """Print the certification outcome, including the label-noise term when it applies."""
    table = Table(title="Local certification")
    table.add_column("Issue type")
    table.add_column("Threshold", justify="right")
    table.add_column("Outcome")
    for issue_type in sorted(certification.thresholds):
        threshold = certification.thresholds[issue_type]
        certified = issue_type in certification.certified_classes
        table.add_row(
            issue_type,
            "abstain" if not certified else f"{threshold:.2f}",
            "CERTIFIED" if certified else certification.reasons.get(issue_type, "not certified"),
        )
    Console().print(table)
    if certification.label_noise_adjusted:
        binding = certification.binding_control_class
        tallies = certification.label_noise_controls_by_class
        # Attribute the bound to the class that actually produced it. Printing the pooled
        # totals beside a stratified beta reads as though those totals produced it: on measured
        # data the pooled 6-of-38 implies beta <= 0.3125, while the bound shown is 0.8712 from
        # 4-of-8 in one class. Same two numbers, and the wrong one looks like the cause.
        if binding is not None and binding in tallies:
            tally = tallies[binding]
            _console.print(
                f"[dim]Label-noise adjusted: the binding control class is {binding!r}, where "
                f"{tally.false_accepts} of {tally.controls} planted controls were wrongly "
                f"accepted, so your false-accept rate is bounded at beta <= "
                f"{tally.beta_upper:.4f} and the error bound is inflated by "
                f"1/(1-beta) = {1.0 / (1.0 - tally.beta_upper):.2f}x.[/dim]"
            )
        if len(tallies) > 1:
            others = ", ".join(
                f"{name}: {t.false_accepts}/{t.controls} -> {t.beta_upper:.4f}"
                for name, t in sorted(tallies.items())
                if name != binding
            )
            pooled = certification.pooled_beta_upper
            pooled_text = f" Pooled, for reference only: {pooled:.4f}." if pooled else ""
            _console.print(
                f"[dim]Other control classes ({others}) do NOT relax the bound; the worst class "
                f"binds. Adding an easier class cannot lower beta, and because each class pays a "
                f"share of the union correction it slightly raises it.{pooled_text}[/dim]"
            )
        _console.print(f"[dim]{certification.beta_scope_note}[/dim]")
    else:
        _console.print(
            "[yellow]label_source=oracle: no label-noise adjustment was applied. This "
            "certificate is conditional on those verdicts being ground truth, and it does NOT "
            "transfer to human labelling.[/yellow]"
        )
    # Stated because its absence was mistaken for a product route. `SessionCertification` is
    # printed and discarded: there is no serializer, no loader, and `dataforge repair` reads a
    # different artifact through `--corrector-calibration`, whose schema this object cannot
    # satisfy. Nothing here has ever influenced a write, and the pre-registered kill criterion in
    # docs/trust/stratified-label-noise-result.md fired on the measurement that would justify
    # wiring it.
    _console.print(
        "[yellow]This certificate is advisory and is NOT consumed by `dataforge repair`. There is "
        "no path from these thresholds to an applied fix; `--corrector-calibration` reads a "
        "different artifact. Treat the numbers as a measurement of your labelling, not as a "
        "licence to auto-apply.[/yellow]"
    )


def _render(
    artifact: CalibrationSessionArtifact,
    queue_counts: dict[str, int],
    *,
    blind: bool = False,
) -> None:
    """Render the measured per-class precision, or explain why there is nothing to show."""
    rows = summarize_calibration(artifact, queue_counts=queue_counts)
    labelled_total = len(artifact.labelled())
    pending_controls = len(artifact.planted_controls) - len(artifact.labelled_controls())
    if not labelled_total:
        _console.print(
            f"[yellow]{len(artifact.samples)} cells sampled from "
            f"{artifact.flagged_cells_total:,} flagged, awaiting your judgement. "
            "Label them with --label ROW:COLUMN=error|correct, then rerun to see measured "
            "precision for your table.[/yellow]"
        )
        if pending_controls:
            _console.print(
                f"[yellow]{pending_controls} planted controls are also awaiting judgement. "
                "They look like ordinary items and that is deliberate.[/yellow]"
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
    if artifact.label_source == "human" and not artifact.planted_controls:
        _console.print(
            "[yellow]No planted controls. Human verdicts carry a false-accept rate, and "
            "certification will refuse without a measured bound on it -- a certified 0.05 is "
            "really 0.10 at beta=0.5. Add --plant-controls 30. "
            "See docs/trust/human-label-noise.md.[/yellow]"
        )
    elif pending_controls:
        _console.print(f"[dim]{pending_controls} planted controls still awaiting judgement.[/dim]")
    if blind:
        _console.print(
            "[dim]--blind: proposed values are hidden. Write down the value you believe is "
            "correct before revealing them, so your verdict is not anchored on the machine's.[/dim]"
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
    label_source: Annotated[
        str,
        typer.Option(
            "--label-source",
            help="Who is adjudicating: 'human' or 'oracle'. REQUIRED to be accurate, not "
            "convenient. Human verdicts carry a false-accept rate, so certification refuses "
            "them without planted controls (see --plant-controls). Declare 'oracle' only when "
            "the verdicts come from retained ground truth.",
        ),
    ] = "human",
    plant_controls_count: Annotated[
        int,
        typer.Option(
            "--plant-controls",
            min=0,
            help="Mix N known-wrong items into the labelling stream to bound YOUR false-accept "
            "rate. Certification on human labels requires these: if you ratify a wrong repair "
            "with probability beta, the true error rate is measured/(1-beta), so a certified "
            "0.05 is really 0.10 at beta=0.5. 30 is the budget optimum.",
        ),
    ] = 0,
    label_control: Annotated[
        list[str] | None,
        typer.Option(
            "--label-control",
            help="Judge a planted control as ROW:COLUMN=correct|error. 'correct' means you "
            "accepted a known-wrong value -- a false accept. Repeatable.",
        ),
    ] = None,
    blind: Annotated[
        bool,
        typer.Option(
            "--blind",
            help="Do not display proposed replacement values. Use this to label by writing down "
            "the correct value yourself before seeing the machine's answer, which removes the "
            "anchoring that drives automation bias.",
        ),
    ] = False,
    certify: Annotated[
        bool,
        typer.Option(
            "--certify",
            help="Attempt local certification from the repair verdicts gathered so far. Refuses "
            "when --label-source=human and no planted control has been labelled.",
        ),
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
    if label_source not in {"human", "oracle"}:
        _console.print(
            f"[bold red]--label-source must be 'human' or 'oracle'; got {label_source!r}."
            "[/bold red]"
        )
        raise typer.Exit(code=2)
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
            label_source=label_source,  # type: ignore[arg-type]
        )

    if plant_controls_count:
        from dataforge.calibration_session import plant_controls as _plant

        # Pass the FULL detector output, not just the sampled cells, so a plant never lands on a
        # cell some detector independently considers broken -- otherwise its "withheld truth"
        # would be a value the system itself disputes.
        artifact = _plant(
            artifact,
            df,
            count=plant_controls_count,
            seed=seed,
            flagged_cells=[(issue.row, issue.column) for issue in issues],
        )
        _console.print(
            f"[green]Planted {len(artifact.planted_controls)} known-wrong controls.[/green] "
            "They are indistinguishable from real items on purpose. Judge them with "
            "--label-control ROW:COLUMN=correct|error."
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

    if label_control:
        from dataforge.calibration_session import label_planted_control

        try:
            for row, column, decision in _parse_labels(label_control, flag="--label-control"):
                artifact = label_planted_control(
                    artifact,
                    row=row,
                    column=column,
                    decision="correct" if decision == "correct" else "error",
                )
        except (ValueError, KeyError) as exc:
            _console.print(f"[bold red]Could not record control verdict:[/bold red] {exc}")
            raise typer.Exit(code=2) from exc

    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text(dump_calibration_session(artifact), encoding="utf-8")

    certification = None
    certification_error: str | None = None
    if certify:
        from dataforge.calibration_session import certify_from_session

        try:
            certification = certify_from_session(artifact)
        except ValueError as exc:
            certification_error = str(exc)

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
                    "certification": (
                        certification.model_dump(mode="json") if certification else None
                    ),
                    "certification_error": certification_error,
                },
                indent=2,
                sort_keys=True,
            )
        )
        raise typer.Exit(code=0)

    _render(artifact, queue_counts, blind=blind)
    if certification_error:
        _console.print(f"[bold red]Cannot certify:[/bold red] {certification_error}")
    elif certification is not None:
        _render_certification(certification)
    _console.print(f"[dim]Session: {session_path}[/dim]")
    raise typer.Exit(code=0)
