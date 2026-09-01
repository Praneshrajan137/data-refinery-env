"""CLI subcommands for reviewing inferred constraint artifacts."""

from __future__ import annotations

import json
from collections import Counter
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static

from dataforge.cli.common import read_csv, resolve_cli_path
from dataforge.detectors.base import FunctionalDependency
from dataforge.schema_inference import (
    REPAIR_SUPPORTED_CONSTRAINT_KINDS,
    ConstraintCandidate,
    ConstraintDecision,
    ConstraintReviewArtifact,
    ConstraintReviewError,
    load_constraint_review_artifact,
    update_constraint_review_artifact,
    write_constraint_review_artifact_atomic,
)
from dataforge.witness import fd_label

constraints_app = typer.Typer(
    help="Review inferred profile constraints before repair can use them.",
    no_args_is_help=True,
)
_console = Console(stderr=True)


def _candidate_target(candidate: Any) -> str:
    """Return a compact target description for one candidate."""
    columns = ", ".join(candidate.columns)
    if candidate.dependent:
        return f"{columns} -> {candidate.dependent}"
    return columns


def _candidate_summary(reviewed: Any) -> dict[str, Any]:
    """Return a machine-readable review summary for one candidate.

    ``tested_confidence`` is included, and that is the point of it. Added 2026-08-26 because the
    field was shipped as "reported to the human who accepts the constraint" instead of as a gate --
    the right decision -- but it reached that human only inside the English ``evidence`` blob. This
    dict emitted ``confidence`` alone, so a programmatic consumer saw the number that is INFLATED by
    singleton determinant groups and had to parse prose to find the honest one. A named consumer that
    cannot read the field is not a consumer.

    It is ``None`` for candidate kinds that are not functional dependencies, and for FDs mined before
    the field existed. ``None`` means unknown, not high.
    """
    candidate = reviewed.candidate
    return {
        "candidate_id": reviewed.candidate_id,
        "decision": reviewed.decision,
        "kind": candidate.kind,
        "target": _candidate_target(candidate),
        "confidence": candidate.confidence,
        "tested_confidence": getattr(candidate, "tested_confidence", None),
        # Added 2026-09-01 for the same reason tested_confidence was: a number a
        # programmatic consumer cannot read is not reported to it. mu_plus differs from
        # tested_confidence in the way that matters -- its decision point is 0, from a
        # permutation null, rather than a constant fitted to one corpus.
        "mu_plus": getattr(candidate, "mu_plus", None),
        "g3_prime": getattr(candidate, "g3_prime", None),
        "repair_supported": candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS,
        "evidence": candidate.evidence,
        "review_note": reviewed.review_note,
    }


def _artifact_summary(
    artifact: ConstraintReviewArtifact,
    *,
    path: Path,
    sha256: str | None = None,
) -> dict[str, Any]:
    """Return a stable summary payload for CLI and CI consumers."""
    decision_counts = Counter(reviewed.decision for reviewed in artifact.candidates)
    repair_supported_count = sum(
        1
        for reviewed in artifact.candidates
        if reviewed.candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS
    )
    return {
        "path": str(path),
        "schema_version": artifact.schema_version,
        "source_path": artifact.source_path,
        "source_sha256": artifact.source_sha256,
        "row_count": artifact.row_count,
        "candidate_count": len(artifact.candidates),
        "repair_supported_count": repair_supported_count,
        "decision_counts": {
            "accepted": decision_counts.get("accepted", 0),
            "pending": decision_counts.get("pending", 0),
            "rejected": decision_counts.get("rejected", 0),
        },
        "sha256": sha256,
        "candidates": [_candidate_summary(reviewed) for reviewed in artifact.candidates],
    }


def _parse_notes(raw_notes: list[str] | None) -> dict[str, str | None]:
    """Parse repeated ``--note cnd-id=text`` options."""
    parsed: dict[str, str | None] = {}
    for raw_note in raw_notes or []:
        candidate_id, separator, note = raw_note.partition("=")
        if not separator or not candidate_id:
            raise typer.BadParameter("--note must use the form cnd-...=text")
        parsed[candidate_id] = note or None
    return parsed


def _print_review_table(artifact: ConstraintReviewArtifact) -> None:
    """Render a compact non-interactive review table.

    ``Tested`` is its own column rather than only a phrase inside ``Evidence``. It is the confidence
    measured on the rows that can actually falsify the dependency, excluding singleton determinant
    groups, which are consistent with any value and therefore inflate the shipped ``Confidence``. On
    hospital it separates true from false mined dependencies where ``Confidence`` overlaps. It is
    reported, never gated: the threshold that separates is fitted to one corpus and no other corpus
    can validate it, so the number goes to the person accepting the constraint and does not decide.
    """
    table = Table(title="Constraint Review")
    table.add_column("Candidate ID", overflow="fold")
    table.add_column("Decision")
    table.add_column("Kind")
    table.add_column("Target", overflow="fold")
    table.add_column("Confidence", justify="right")
    table.add_column("Tested", justify="right")
    # mu+ earns a column beside Tested because it answers the same question with a decision
    # point of 0 instead of a fitted cut. 0 means "no evidence beyond the dependent's own
    # distribution", which is the sentence a reviewer needs and cannot infer from Confidence.
    table.add_column("mu+", justify="right")
    table.add_column("Repair")
    table.add_column("Evidence", overflow="fold")
    for reviewed in artifact.candidates:
        candidate = reviewed.candidate
        tested = getattr(candidate, "tested_confidence", None)
        corrected = getattr(candidate, "mu_plus", None)
        table.add_row(
            reviewed.candidate_id,
            reviewed.decision,
            candidate.kind,
            _candidate_target(candidate),
            f"{candidate.confidence:.4f}",
            # "n/a" rather than a blank or a zero: absent means this candidate kind has no tested
            # confidence, and a reader must not read that as low.
            f"{tested:.4f}" if tested is not None else "n/a",
            # Same reasoning, and it matters more here: mu+ of 0.0 is a real and meaningful
            # verdict, so an absent value must never render as 0.
            f"{corrected:.4f}" if corrected is not None else "n/a",
            "yes" if candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS else "review-only",
            candidate.evidence,
        )
    Console().print(table)


class ConstraintReviewApp(App[ConstraintReviewArtifact]):
    """Textual review UI for a constraint artifact."""

    CSS = """
    DataTable {
        height: 1fr;
    }

    #detail {
        width: 45%;
        height: 1fr;
        overflow-y: auto;
        border: solid $accent;
        padding: 1;
    }

    #note {
        height: 3;
    }
    """

    BINDINGS = [
        Binding("a", "accept", "Accept"),
        Binding("r", "reject", "Reject"),
        Binding("p", "pending", "Pending"),
        Binding("n", "focus_note", "Note"),
        Binding("s", "save", "Save"),
        Binding("q", "quit_without_save", "Quit"),
    ]

    def __init__(self, artifact: ConstraintReviewArtifact) -> None:
        """Create a review application for an already validated artifact."""
        super().__init__()
        self.artifact = artifact
        self.saved = False
        self.selected_candidate_id = (
            artifact.candidates[0].candidate_id if artifact.candidates else None
        )

    def compose(self) -> ComposeResult:
        """Compose the review screen."""
        yield Header()
        with Vertical():
            with Horizontal():
                yield DataTable(id="candidates")
                yield Static(id="detail")
            yield Input(
                placeholder="Review note for selected candidate; press Enter to save note",
                id="note",
            )
        yield Footer()

    def on_mount(self) -> None:
        """Populate the table when the TUI starts."""
        table = self.query_one("#candidates", DataTable)
        table.cursor_type = "row"
        table.add_columns("ID", "Decision", "Kind", "Target", "Conf", "Repair")
        for reviewed in self.artifact.candidates:
            candidate = reviewed.candidate
            table.add_row(
                reviewed.candidate_id,
                reviewed.decision,
                candidate.kind,
                _candidate_target(candidate),
                f"{candidate.confidence:.4f}",
                "yes" if candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS else "review-only",
                key=reviewed.candidate_id,
            )
        table.focus()
        self._refresh_detail()

    @on(DataTable.RowHighlighted)
    def _on_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Track the currently highlighted candidate."""
        self.selected_candidate_id = str(event.row_key.value)
        self._refresh_detail()

    @on(Input.Submitted, "#note")
    def _on_note_submitted(self, event: Input.Submitted) -> None:
        """Save a note for the selected candidate."""
        if self.selected_candidate_id is None:
            return
        self.artifact = update_constraint_review_artifact(
            self.artifact,
            notes={self.selected_candidate_id: event.value},
        )
        event.input.value = ""
        self._refresh_table()
        self._refresh_detail()

    def action_accept(self) -> None:
        """Accept the selected candidate."""
        self._set_selected_decision("accepted")

    def action_reject(self) -> None:
        """Reject the selected candidate."""
        self._set_selected_decision("rejected")

    def action_pending(self) -> None:
        """Reset the selected candidate to pending."""
        self._set_selected_decision("pending")

    def action_focus_note(self) -> None:
        """Focus the note editor."""
        self.query_one("#note", Input).focus()

    def action_save(self) -> None:
        """Exit the TUI with the reviewed artifact."""
        self.saved = True
        self.exit(self.artifact)

    def action_quit_without_save(self) -> None:
        """Exit the TUI without saving."""
        self.saved = False
        self.exit(self.artifact)

    def _set_selected_decision(self, decision: ConstraintDecision) -> None:
        """Apply a decision to the selected candidate."""
        if self.selected_candidate_id is None:
            return
        if decision == "accepted":
            self.artifact = update_constraint_review_artifact(
                self.artifact,
                accept_ids=(self.selected_candidate_id,),
            )
        elif decision == "rejected":
            self.artifact = update_constraint_review_artifact(
                self.artifact,
                reject_ids=(self.selected_candidate_id,),
            )
        else:
            self.artifact = update_constraint_review_artifact(
                self.artifact,
                pending_ids=(self.selected_candidate_id,),
            )
        self._refresh_table()
        self._refresh_detail()

    def _selected_candidate(self) -> Any | None:
        """Return the selected reviewed candidate."""
        for reviewed in self.artifact.candidates:
            if reviewed.candidate_id == self.selected_candidate_id:
                return reviewed
        return None

    def _refresh_table(self) -> None:
        """Refresh table rows after a decision change."""
        table = self.query_one("#candidates", DataTable)
        table.clear(columns=True)
        table.add_columns("ID", "Decision", "Kind", "Target", "Conf", "Repair")
        for reviewed in self.artifact.candidates:
            candidate = reviewed.candidate
            table.add_row(
                reviewed.candidate_id,
                reviewed.decision,
                candidate.kind,
                _candidate_target(candidate),
                f"{candidate.confidence:.4f}",
                "yes" if candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS else "review-only",
                key=reviewed.candidate_id,
            )

    def _refresh_detail(self) -> None:
        """Refresh the detail pane for the selected candidate."""
        reviewed = self._selected_candidate()
        detail = self.query_one("#detail", Static)
        if reviewed is None:
            detail.update("No constraint candidates.")
            return
        candidate = reviewed.candidate
        repair_note = (
            "Repair-supported in v1."
            if candidate.kind in REPAIR_SUPPORTED_CONSTRAINT_KINDS
            else "Review-only in v1; repair ignores this kind."
        )
        payload = json.dumps(reviewed.model_dump(mode="json"), indent=2, sort_keys=True)
        detail.update(
            "\n".join(
                [
                    f"Candidate: {reviewed.candidate_id}",
                    f"Decision: {reviewed.decision}",
                    f"Source: {self.artifact.source_path}",
                    f"Source SHA-256: {self.artifact.source_sha256}",
                    f"Repair: {repair_note}",
                    "",
                    "Candidate JSON:",
                    payload,
                ]
            )
        )


def _newly_accepted_fds(
    before: ConstraintReviewArtifact, after: ConstraintReviewArtifact
) -> list[tuple[str, FunctionalDependency]]:
    """Return the FD candidates this invocation moved to accepted, in canonical order."""
    was_accepted = {
        entry.candidate_id for entry in before.candidates if entry.decision == "accepted"
    }
    newly: list[tuple[str, FunctionalDependency]] = []
    for entry in after.candidates:
        if entry.decision != "accepted" or entry.candidate.kind != "functional_dependency":
            continue
        if entry.candidate_id in was_accepted:
            continue
        fd = _candidate_as_fd(entry.candidate)
        if fd is not None:
            newly.append((entry.candidate_id, fd))
    return sorted(newly, key=lambda item: tuple(item[1].determinant))


def _candidate_as_fd(candidate: ConstraintCandidate) -> FunctionalDependency | None:
    """Project an FD candidate onto the dependency the repairer would act on."""
    columns = tuple(candidate.columns)
    dependent = candidate.dependent
    if not columns or not dependent:
        return None
    return FunctionalDependency(determinant=tuple(columns), dependent=dependent)


def _accepted_fds_before(artifact: ConstraintReviewArtifact) -> tuple[FunctionalDependency, ...]:
    """Return dependencies already accepted, which is the premise a marginal is measured against."""
    accepted: list[FunctionalDependency] = []
    for entry in artifact.candidates:
        if entry.decision != "accepted" or entry.candidate.kind != "functional_dependency":
            continue
        fd = _candidate_as_fd(entry.candidate)
        if fd is not None:
            accepted.append(fd)
    return tuple(accepted)


def _acceptance_consequence(
    before: ConstraintReviewArtifact,
    after: ConstraintReviewArtifact,
    *,
    source: Path | None,
) -> dict[str, Any]:
    """Compute what accepting these dependencies would do to THIS table.

    Returns data rather than printing, because ``--json`` must emit only JSON: an earlier
    version of this rendered prose unconditionally and corrupted the machine-readable output,
    which a test caught. A machine consumer needs the counts, not the sentence.

    Why this exists. Until 2026-08-29 the only figure a reviewer saw at this keystroke was
    hospital's: 116 corrupted cells and a 0.2046 harmful write rate, measured on a public
    research corpus. A published statistic about somebody else's table, shown at the moment a
    human authorises unsupervised writes to their own. ``PRODUCT.md``:193-201 already records
    that a number a user reads is a published claim regardless of the file extension it lives
    in; this is the same argument one step further, about a number that describes the wrong
    table.

    Why the figure is MARGINAL. ``constraint-additivity.md`` measures that isolated
    per-candidate harm does not compose -- summed over hospital's 85 candidates it is 2779
    cells against 567 for the set accepted together, a 4.9x overstatement -- which is why this
    surface carried no per-candidate number at all. A marginal figure conditions on what is
    already accepted, and ``docs/trust/entailment-witness-result.md`` measures that marginal
    deltas along an acceptance path sum to 567 **exactly**. So the quantity here is defensible
    where a context-free one is not.

    Why it can be unavailable, and why that is said out loud. Computing this needs the table,
    and this command is otherwise a pure artifact operation -- the reason
    :func:`_warn_accepted_fd_queue_cost` counts rather than re-detects. The artifact cannot
    guarantee the CSV is still present or unchanged, so the source is verified against
    ``artifact.source_sha256`` and the preview is refused, with the reason, when it is not
    verifiable. Silently skipping would be worse than not offering it: the reviewer would
    infer the acceptance has no consequence.

    This is display only. It reads no verdict and writes nothing, so no repair decision
    depends on it. Pinned by ``tests/unit/test_entailment_witness.py``, which fails if the
    witness reaches a decision-path module.
    """
    newly = _newly_accepted_fds(before, after)
    if not newly:
        return {"applicable": False}

    resolved = source or (Path(before.source_path) if before.source_path else None)
    if resolved is None or not resolved.is_file():
        return {
            "applicable": True,
            "available": False,
            "reason": (
                "the source table was not found"
                f"{f' at {resolved}' if resolved else ''}. Pass --source to point at it. "
                "Without the table, what these dependencies would rewrite cannot be computed "
                "-- this is not a statement that they would rewrite nothing."
            ),
        }

    if sha256(resolved.read_bytes()).hexdigest() != before.source_sha256:
        return {
            "applicable": True,
            "available": False,
            "reason": (
                "the source table no longer matches the hash recorded when these candidates "
                "were mined, so any preview would describe a different table than the one "
                "reviewed. Re-run 'dataforge profile --constraints-out' against the current "
                "file."
            ),
        }

    from dataforge.witness import marginal_blast_radius

    table = read_csv(resolved)
    accepted = _accepted_fds_before(before)
    dependencies: list[dict[str, Any]] = []
    for _candidate_id, fd in newly:
        witnesses = marginal_blast_radius(table, accepted, fd)
        worst = max(witnesses, key=lambda item: item.destroys, default=None)
        dependencies.append(
            {
                "dependency": fd_label(fd),
                "cells_rewritten": len(witnesses),
                "values_replaced": sum(witness.destroys for witness in witnesses),
                "example": (
                    f"{worst.column} row {worst.row}: {worst.old_value!r} -> {worst.new_value!r}"
                    if worst is not None
                    else None
                ),
            }
        )
        accepted = (*accepted, fd)

    return {
        "applicable": True,
        "available": True,
        "covers": ["functional_dependency"],
        "marginal": True,
        "dependencies": dependencies,
        "total_cells_rewritten": sum(entry["cells_rewritten"] for entry in dependencies),
    }


def _render_acceptance_consequence(result: dict[str, Any]) -> None:
    """Render :func:`_acceptance_consequence` for a human."""
    if not result.get("applicable"):
        return
    if not result.get("available"):
        _console.print(f"[yellow]Consequence preview unavailable: {result['reason']}[/yellow]")
        return

    table_view = Table(title="What accepting these dependencies would do to THIS table")
    table_view.add_column("dependency")
    table_view.add_column("cells it would rewrite", justify="right")
    table_view.add_column("values it would replace", justify="right")
    table_view.add_column("example")
    for entry in result["dependencies"]:
        table_view.add_row(
            str(entry["dependency"]),
            str(entry["cells_rewritten"]),
            str(entry["values_replaced"]),
            str(entry["example"] or "-"),
        )
    _console.print(table_view)

    if result["total_cells_rewritten"] == 0:
        _console.print(
            "[green]No cell in this table would be rewritten by these dependencies. A "
            "dependency is inert where its determinant groups already agree -- measured on "
            "hospital, two dependencies that are false on ground truth rewrote nothing for "
            "exactly this reason.[/green]"
        )
    else:
        _console.print(
            "[yellow]This is CONSEQUENCE, not harm: a replaced value may be exactly the "
            "error you want repaired, and without a clean copy of the table nothing here can "
            "tell the two apart. What it does tell you is the size and shape of the change "
            "you are authorising. Marginal, given what was already accepted, and additive "
            "along this acceptance order. These writes carry 'deterministic' provenance, so "
            "no calibration threshold downstream holds them back. Covers functional "
            "dependencies only.[/yellow]"
        )


def _warn_accepted_fd_queue_cost(artifact: ConstraintReviewArtifact) -> None:
    """Warn when accepted functional dependencies will enlarge the review queue.

    This is the moment of choice. Accepting an FD candidate is one keystroke, and the
    consequence is measured: on hospital it takes the queue from 549 cells at 0.561
    precision to 10,373 at 0.044 -- +147 true errors bought with +9,824 false positives, and
    review effort from 1.78 to 22.80 cells per real error. Recall genuinely rises
    (0.605 -> 0.894), so this warns rather than refuses.

    Counts rather than re-detects on purpose: reading the source CSV here would make a pure
    artifact operation do file IO, and the artifact cannot guarantee the CSV is still
    present. ``dataforge profile --constraints-out`` reports the measured cell cost, and
    ``repair --fd-detection declared`` is the control.

    A second cost, measured 2026-08-25 and corrected 2026-08-26, added here because the queue figure
    alone understates it: an accepted dependency does not only enlarge the review queue, it authorises
    **writes**.

    ``docs/trust/shipped-premise-result.md`` measures the FD repairer under **the premise this
    keystroke actually creates** -- the miner's full output at its 0.90 emission floor, because
    ``ConstraintReviewArtifact.to_schema()`` applies no floor of its own. On hospital: 567 writes, 451
    real errors repaired, and **116 cells that were already correct overwritten**, a harmful write
    rate of 0.2046.

    This warning previously cited **86** and 0.1601. Those figures are retained rather than deleted,
    per this project's convention that withdrawn numbers stay legible, and they describe a *different*
    premise: ``infer_verification_schema`` at a 0.95 floor, which is what the earlier harness measured
    and which **no user is given**. The four dependencies the 0.95 premise excludes are all false on
    ground truth, repaired nothing, and account for the whole 86-to-116 difference.

    Under a premise whose dependencies all hold the same repairer corrupts nothing. Corruptions trace
    to mined dependencies that are false on ground truth -- 23 to ``ZipCode -> HospitalName``, 23 to
    ``ZipCode -> ProviderNumber``; a zip code does not determine a hospital name or a provider number.
    These writes carry ``deterministic`` provenance and therefore bypass the calibration threshold
    entirely, so nothing downstream holds them back.

    The per-dependency figure is deliberately absent, and that absence is a finding rather than an
    omission. ``docs/trust/constraint-additivity.md`` measures that per-candidate harm **does not
    compose**: summed over hospital's 85 candidates in isolation it is 330, while accepting all 85
    together yields 116, because overlapping dependencies mask one another and only one acts per cell.
    So a per-candidate number would overstate harm by a factor that depends on what else the reviewer
    accepts, which is worse than giving no per-candidate number at all.

    Every number in this docstring and in the message below is bound to its artifact in
    ``docs/quantitative_claims.yaml``. Until 2026-08-26 it was bound to nothing: ``readme_truth.py``
    polices documents and ``docs_truth.py`` bound document prose, so the one sentence a user reads
    while authorising an unsupervised write to their own data was the least-guarded claim in the
    product, and it had gone stale in the reassuring direction.
    """
    accepted_fds = [
        entry
        for entry in artifact.candidates
        if entry.decision == "accepted" and entry.candidate.kind == "functional_dependency"
    ]
    if not accepted_fds:
        return
    _console.print(
        f"[yellow]{len(accepted_fds)} accepted functional dependencies will be able to "
        "RAISE ISSUES and AUTHORIZE WRITES. Inferred FDs raise recall but can flood "
        "review (measured on hospital: 549 -> 10,373 flagged cells, 1.78 -> 22.80 cells per "
        "real error), and a false "
        "one lets the repairer overwrite cells that were already correct: measured 116 such "
        "cells on hospital, a 0.2046 harmful write rate, against zero under a premise whose "
        "dependencies all hold. That is measured on the premise THIS keystroke creates -- the "
        "miner's full output -- and supersedes the 86 previously reported here, which described "
        "a stricter premise no user is given. Per-dependency harm does not compose, so there is "
        "no per-candidate figure: summed in isolation it is 330 against 116 accepted together. "
        "Those writes bypass the calibration threshold. Use "
        "'dataforge repair --fd-detection declared' to keep the queue reviewable, and see "
        "docs/trust/shipped-premise-result.md and docs/trust/constraint-additivity.md."
        "[/yellow]"
    )


@constraints_app.command(name="review")
def review_constraints(
    path: Annotated[
        Path,
        typer.Argument(help="Path to a constraint_review_v1 JSON artifact."),
    ],
    accept: Annotated[
        list[str] | None,
        typer.Option("--accept", help="Mark a candidate id accepted. Repeatable."),
    ] = None,
    reject: Annotated[
        list[str] | None,
        typer.Option("--reject", help="Mark a candidate id rejected. Repeatable."),
    ] = None,
    pending: Annotated[
        list[str] | None,
        typer.Option("--pending", help="Reset a candidate id to pending. Repeatable."),
    ] = None,
    note: Annotated[
        list[str] | None,
        typer.Option("--note", help="Set a review note with cnd-...=text. Repeatable."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Write the reviewed artifact to a separate path."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview changes without writing an artifact."),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print review state as JSON."),
    ] = False,
    no_tui: Annotated[
        bool,
        typer.Option("--no-tui", help="Run deterministic non-interactive review mode."),
    ] = False,
    source: Annotated[
        Path | None,
        typer.Option(
            "--source",
            help=(
                "The table these candidates were mined from. Used to show what accepting "
                "them would rewrite in YOUR data. Defaults to the artifact's recorded path."
            ),
        ),
    ] = None,
    consequence: Annotated[
        bool,
        typer.Option(
            "--consequence/--no-consequence",
            help="Show the cells each newly-accepted dependency would rewrite.",
        ),
    ] = True,
) -> None:
    """Review profile-inferred constraint candidates before repair uses them."""
    resolved_path = resolve_cli_path(path)
    try:
        artifact, artifact_sha256 = load_constraint_review_artifact(resolved_path)
        parsed_notes = _parse_notes(note)
        updated = update_constraint_review_artifact(
            artifact,
            accept_ids=tuple(accept or ()),
            reject_ids=tuple(reject or ()),
            pending_ids=tuple(pending or ()),
            notes=parsed_notes,
        )
    except (ConstraintReviewError, typer.BadParameter) as exc:
        _console.print(Panel(f"[bold red]{exc}[/bold red]", title="Constraint Review Error"))
        raise typer.Exit(code=2) from exc

    non_interactive = no_tui or bool(
        accept or reject or pending or note or output or dry_run or json_output
    )
    if not non_interactive:
        app = ConstraintReviewApp(updated)
        tui_result = app.run()
        if tui_result is None or not app.saved:
            _console.print("[yellow]Constraint review cancelled; no file was changed.[/yellow]")
            raise typer.Exit(code=1)
        updated = tui_result

    target_path = resolve_cli_path(output) if output is not None else resolved_path
    consequence_result: dict[str, Any] = {"applicable": False}
    if consequence:
        consequence_result = _acceptance_consequence(
            artifact,
            updated,
            source=resolve_cli_path(source) if source is not None else None,
        )
    if consequence and not json_output:
        # Rendered before the write, so a reviewer surprised by the numbers reads them
        # before a "written" confirmation rather than after it.
        _render_acceptance_consequence(consequence_result)
    written_sha256: str | None = artifact_sha256
    if dry_run:
        written_sha256 = None
    elif updated != artifact or target_path != resolved_path:
        try:
            written_sha256 = write_constraint_review_artifact_atomic(target_path, updated)
        except OSError as exc:
            _console.print(Panel(f"[bold red]{exc}[/bold red]", title="Constraint Review Error"))
            raise typer.Exit(code=2) from exc

    if json_output:
        payload = _artifact_summary(updated, path=target_path, sha256=written_sha256)
        if consequence_result.get("applicable"):
            payload["acceptance_consequence"] = consequence_result
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_review_table(updated)
        _warn_accepted_fd_queue_cost(updated)
        if dry_run:
            _console.print("[yellow]Dry run: no constraint artifact was written.[/yellow]")
