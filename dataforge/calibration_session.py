"""Per-table trust calibration: measure this tool's precision on *your* data.

DataForge measures itself honestly, but inward. Pre-registrations, `eval/results/`,
`DECISIONS.md` and `docs_truth.py` all serve maintainers reasoning about benchmarks. A user
running `dataforge profile` gets a table of flagged cells and cannot answer the only question
that matters to them: **how many of these are real?**

The measured answer varies enormously and is not predictable from anything observable at
runtime. Detector precision is 0.561 on hospital, 0.947 on flights and 0.342 on rayyan
(`eval/results/detector_queue_composition.json`), and confidence dispersion was tested as a
runtime proxy for "will AI triage help here" and refuted. So the honest way to tell a user
what to expect on their table is to measure it on their table.

That is what this module does. The user adjudicates a small, **randomly** sampled set of
flagged cells; from those labels we report per-class precision with Clopper-Pearson intervals.
It costs a few minutes of the review work they were going to do anyway.

Two design decisions carry the whole thing:

* **The sample is random within each class, never rank-ordered.** Sampling the
  highest-confidence cells would inflate the estimate, which is exactly the selected-extremum
  error this project already had to retract once. `sampling_strategy` is recorded in the
  artifact so a future reader can check.
* **Exchangeability holds by construction.** The reason global certification failed is that
  benchmark calibration cannot be shown exchangeable with a user's table. Here the calibration
  data *is* the table, so the assumption the conformal guarantee rests on is satisfied rather
  than hoped for. That is what makes a local guarantee reachable where a global one was not.

The artifact is anchored by ``source_sha256`` exactly as ``ConstraintReviewArtifact`` is, so
labels gathered on one file can never be silently credited to different bytes.
"""

from __future__ import annotations

import json
import random
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from dataforge.detectors.base import Issue

CALIBRATION_SESSION_SCHEMA_VERSION: Literal["dataforge_calibration_session_v1"] = (
    "dataforge_calibration_session_v1"
)

#: A user's verdict on one flagged cell. ``pending`` means unlabelled, and pending samples
#: are excluded from every estimate rather than assumed either way.
CalibrationDecision = Literal["pending", "error", "correct"]

#: How the sample was drawn. Only ``random_within_class`` yields an unbiased estimate; the
#: field exists so a biased sample cannot masquerade as an unbiased one.
SamplingStrategy = Literal["random_within_class"]

_DEFAULT_PER_CLASS = 12
_DEFAULT_SEED = 20260806


class CalibrationSample(BaseModel):
    """One flagged cell offered to the user for adjudication.

    Carries **two independent verdicts**, because they answer different questions and
    conflating them would authorize writing wrong values:

    * ``decision`` -- was this cell genuinely an error? This measures *detection* precision.
    * ``repair_decision`` -- is ``proposed_repair`` the right replacement? This measures
      *corrector* accuracy, and it is the only one that can certify auto-apply.

    A cell can be correctly flagged while the proposed fix is wrong. On hospital, row 3
    ``City`` is ``'birminghxm'`` and should be ``'birmingham'``; a corrector proposing
    ``'Boston'`` is wrong on a correctly-flagged cell. So detection precision 1.0 is fully
    compatible with corrector accuracy 0.0, and certifying repair-writing on detection
    labels would be a category error with data loss as the consequence.
    """

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    issue_type: str = Field(min_length=1)
    detector_confidence: float = Field(ge=0.0, le=1.0)
    flagged_value: str
    reason: str = Field(min_length=1)
    decision: CalibrationDecision = "pending"
    note: str | None = None
    #: The corrector's proposed replacement, when one exists. ``None`` means no repair was
    #: proposed, so this cell can never contribute to certification.
    proposed_repair: str | None = None
    #: Confidence attached to ``proposed_repair``. This -- not ``detector_confidence`` -- is
    #: the value a certified threshold is compared against.
    repair_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    #: Whether ``proposed_repair`` is correct. ``correct`` here means "the proposed value is
    #: right", which is the opposite polarity to ``decision``, where ``error`` means "the
    #: flag was right". Kept separate deliberately.
    repair_decision: CalibrationDecision = "pending"

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class CalibrationSessionArtifact(BaseModel):
    """Strict JSON artifact recording a per-table calibration session."""

    schema_version: Literal["dataforge_calibration_session_v1"] = CALIBRATION_SESSION_SCHEMA_VERSION
    source_path: str = Field(min_length=1)
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(ge=0)
    # list, not tuple: strict mode will not coerce a JSON array back into a tuple, so a
    # tuple field here writes an artifact that can never be read again.
    columns: list[str] = Field(default_factory=list)
    table_fingerprint: str = Field(min_length=1)
    flagged_cells_total: int = Field(ge=0)
    fd_detection_source: str = Field(min_length=1)
    sampling_strategy: SamplingStrategy = "random_within_class"
    seed: int
    #: Provider and model that produced the repair proposals being judged. Recorded because
    #: corrector accuracy is model-specific and NOT monotone in model capability: on hospital,
    #: Azure gpt-5-mini scores precision_at_auto_apply 0.077 against a smaller Gemini model's
    #: 0.16. So a certificate earned under one model says nothing about another, and a
    #: certificate that does not name its model cannot be checked at all.
    corrector_provider: str | None = None
    corrector_model: str | None = None
    samples: list[CalibrationSample] = Field(default_factory=list)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    def labelled(self) -> list[CalibrationSample]:
        """Return only the samples the user has actually adjudicated."""
        return [sample for sample in self.samples if sample.decision != "pending"]


class ClassPrecision(BaseModel):
    """Measured precision for one issue type on this table."""

    issue_type: str
    labelled: int = Field(ge=0)
    real_errors: int = Field(ge=0)
    precision: float | None = None
    precision_ci95: list[float] | None = None
    flagged_cells_in_queue: int = Field(ge=0)
    #: Certification needs 59 all-correct accepted samples at alpha = delta = 0.05.
    #: Recorded per class so a partial result reads as partial, not as failure.
    samples_short_of_certification_floor: int = Field(ge=0)

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


def clopper_pearson_interval(
    successes: int, total: int, *, alpha: float = 0.05
) -> tuple[float, float]:
    """Return an exact two-sided Clopper-Pearson interval for a proportion.

    Exact rather than normal-approximate because these proportions are estimated from tens of
    labels, where the normal interval both understates width and can leave [0, 1].
    """
    if total <= 0:
        return (0.0, 1.0)
    from dataforge.conformal import _clopper_pearson_upper

    upper = _clopper_pearson_upper(successes, total, alpha / 2.0)
    # The lower bound is the mirror of the upper bound on the complementary count.
    lower = 1.0 - _clopper_pearson_upper(total - successes, total, alpha / 2.0)
    return (round(max(0.0, lower), 6), round(min(1.0, upper), 6))


def calibration_dir_for(source_path: Path) -> Path:
    """Return the calibration directory for a source path.

    Sits beside the data under ``.dataforge/`` like transactions and snapshots, because a
    calibration session is per-user, per-table state -- not a benchmark artifact. Writing it
    to ``eval/results/`` would conflate the two and let a user's private labels leak into the
    repository's evidence tree.
    """
    from dataforge.transactions.log import dataforge_root_for

    return dataforge_root_for(source_path) / "calibration"


def build_calibration_session(
    issues: Sequence[Issue],
    *,
    source_path: Path,
    source_sha256: str,
    row_count: int,
    columns: Sequence[str],
    table_fingerprint: str,
    fd_detection_source: str,
    per_class: int = _DEFAULT_PER_CLASS,
    seed: int = _DEFAULT_SEED,
    corrector_provider: str | None = None,
    corrector_model: str | None = None,
) -> CalibrationSessionArtifact:
    """Draw a stratified random sample of flagged cells for adjudication.

    Stratified by ``issue_type`` so a rare class is still measurable, and **random within
    each class** so the estimate is unbiased. Ranking the sample by confidence would inflate
    measured precision -- the selected-extremum error this project already retracted once.

    Args:
        issues: The flagged cells to sample from.
        source_path: The table being calibrated.
        source_sha256: Hash of the exact bytes, so labels cannot be credited to other data.
        row_count: Rows in the table.
        columns: Column names, recorded for the scope fingerprint.
        table_fingerprint: Shape fingerprint from ``dataforge.calibration``.
        fd_detection_source: Which FD regime produced this queue. Recorded because the same
            table yields a 19x larger queue under ``accepted`` than ``declared``, so a
            precision estimate is only meaningful alongside it.
        per_class: Cells to offer per issue type.
        seed: Recorded so the draw is reproducible.

    Returns:
        An all-``pending`` artifact ready for labelling.
    """
    by_class: dict[str, list[Issue]] = {}
    # Deduplicate to one issue per cell FIRST, so (row, column) is a primary key by
    # construction. `run_all_detectors` already yields one issue per cell -- verified across
    # hospital, flights and rayyan under both FD regimes -- because tier-0 precedence resolves
    # each cell. But labelling keys on (row, column), so if that invariant ever changed, one
    # user verdict would silently label a second class's sample and double-count it in the
    # precision estimate. Enforcing it here makes the guarantee local instead of borrowed.
    best_per_cell: dict[tuple[int, str], Issue] = {}
    for issue in issues:
        key = (issue.row, issue.column)
        incumbent = best_per_cell.get(key)
        if incumbent is None or (issue.confidence, issue.issue_type) > (
            incumbent.confidence,
            incumbent.issue_type,
        ):
            best_per_cell[key] = issue
    for issue in best_per_cell.values():
        by_class.setdefault(issue.issue_type, []).append(issue)

    rng = random.Random(seed)
    samples: list[CalibrationSample] = []
    for issue_type in sorted(by_class):
        population = sorted(by_class[issue_type], key=lambda i: (i.row, i.column))
        chosen = population if len(population) <= per_class else rng.sample(population, per_class)
        for issue in sorted(chosen, key=lambda i: (i.row, i.column)):
            samples.append(
                CalibrationSample(
                    row=issue.row,
                    column=issue.column,
                    issue_type=issue_type,
                    detector_confidence=issue.confidence,
                    flagged_value=str(issue.actual),
                    reason=issue.reason[:300],
                )
            )

    return CalibrationSessionArtifact(
        source_path=str(source_path),
        source_sha256=source_sha256,
        row_count=row_count,
        columns=[str(column) for column in columns],
        table_fingerprint=table_fingerprint,
        # Distinct cells, not raw issues. The same conflation made an earlier fd_flag_cost
        # overstate the queue by 5x; the field is named for cells, so it counts cells.
        flagged_cells_total=len(best_per_cell),
        fd_detection_source=fd_detection_source,
        seed=seed,
        corrector_provider=corrector_provider,
        corrector_model=corrector_model,
        samples=samples,
    )


def label_calibration_sample(
    artifact: CalibrationSessionArtifact,
    *,
    row: int,
    column: str,
    decision: CalibrationDecision,
    note: str | None = None,
) -> CalibrationSessionArtifact:
    """Return a new artifact with one sample's decision recorded.

    Pure: the artifact is frozen and this returns a copy, mirroring
    ``update_constraint_review_artifact``.

    Raises:
        KeyError: If the cell is not part of this session. Labelling a cell that was never
            sampled would silently break the random-sampling guarantee, so it fails loudly.
    """
    updated: list[CalibrationSample] = []
    found = False
    for sample in artifact.samples:
        if sample.row == row and sample.column == column:
            found = True
            updated.append(sample.model_copy(update={"decision": decision, "note": note}))
        else:
            updated.append(sample)
    if not found:
        raise KeyError(
            f"cell (row={row}, column={column!r}) is not part of this calibration session; "
            "labelling an unsampled cell would break the random-sample guarantee"
        )
    return artifact.model_copy(update={"samples": updated})


def label_repair_sample(
    artifact: CalibrationSessionArtifact,
    *,
    row: int,
    column: str,
    decision: CalibrationDecision,
    proposed_repair: str | None = None,
    repair_confidence: float | None = None,
    corrector_provider: str | None = None,
    corrector_model: str | None = None,
) -> CalibrationSessionArtifact:
    """Record a verdict on a *proposed replacement value*, not on the flag.

    Separate from :func:`label_calibration_sample` so the two cannot be confused at a call
    site. ``decision="correct"`` here means the proposed value is right.

    ``corrector_model`` is recorded on the artifact the first time it is supplied, because a
    certificate is only meaningful for the model that earned it. Mixing two models in one
    session is refused rather than silently averaged: the resulting threshold would describe
    neither model, and corrector accuracy is measurably model-specific.

    Raises:
        KeyError: If the cell was never sampled.
        ValueError: If there is no proposal to judge, since a verdict on nothing would
            enter certification as a real observation; or if this verdict comes from a
            different model than the session already records.
    """
    if corrector_model is not None and artifact.corrector_model not in (None, corrector_model):
        raise ValueError(
            f"this session's repair verdicts were produced by "
            f"{artifact.corrector_model!r}, but {corrector_model!r} was supplied. A "
            "certificate cannot span two models; start a separate session."
        )
    updated: list[CalibrationSample] = []
    found = False
    for sample in artifact.samples:
        if sample.row == row and sample.column == column:
            found = True
            value = proposed_repair if proposed_repair is not None else sample.proposed_repair
            confidence = (
                repair_confidence if repair_confidence is not None else sample.repair_confidence
            )
            if value is None or confidence is None:
                raise ValueError(
                    f"cell (row={row}, column={column!r}) has no proposed repair and "
                    "confidence to judge; pass proposed_repair and repair_confidence"
                )
            updated.append(
                sample.model_copy(
                    update={
                        "repair_decision": decision,
                        "proposed_repair": value,
                        "repair_confidence": confidence,
                    }
                )
            )
        else:
            updated.append(sample)
    if not found:
        raise KeyError(
            f"cell (row={row}, column={column!r}) is not part of this calibration session"
        )
    changes: dict[str, object] = {"samples": updated}
    if corrector_model is not None:
        changes["corrector_model"] = corrector_model
    if corrector_provider is not None:
        changes["corrector_provider"] = corrector_provider
    return artifact.model_copy(update=changes)


def summarize_calibration(
    artifact: CalibrationSessionArtifact,
    *,
    queue_counts: dict[str, int] | None = None,
    alpha: float = 0.05,
    delta: float = 0.05,
) -> list[ClassPrecision]:
    """Return measured precision per issue type, with exact intervals.

    Pending samples are excluded rather than assumed. A class with no labels yields
    ``precision=None`` -- reporting 0.0 there would invent a measurement.
    """
    from dataforge.conformal import min_samples_for_certification

    floor = min_samples_for_certification(alpha, delta)
    counts = queue_counts or {}
    by_class: dict[str, list[CalibrationSample]] = {}
    for sample in artifact.labelled():
        by_class.setdefault(sample.issue_type, []).append(sample)

    out: list[ClassPrecision] = []
    for issue_type in sorted({s.issue_type for s in artifact.samples}):
        labelled = by_class.get(issue_type, [])
        real = sum(1 for sample in labelled if sample.decision == "error")
        total = len(labelled)
        out.append(
            ClassPrecision(
                issue_type=issue_type,
                labelled=total,
                real_errors=real,
                precision=round(real / total, 6) if total else None,
                precision_ci95=list(clopper_pearson_interval(real, total, alpha=alpha))
                if total
                else None,
                flagged_cells_in_queue=counts.get(issue_type, 0),
                samples_short_of_certification_floor=max(0, floor - real),
            )
        )
    return out


#: Pre-specified, label-independent candidate thresholds, tested in descending order.
#: Fixed as a module constant precisely because ``certify_threshold``'s family-wise error
#: claim is only exact when the grid does not depend on the calibration labels. Deriving a
#: grid from the observed confidences -- which is what passing ``grid=None`` does -- is a
#: *validity* weakness, not merely a power one, so this path never does it.
CERTIFICATION_GRID: tuple[float, ...] = (
    0.99,
    0.98,
    0.97,
    0.96,
    0.95,
    0.94,
    0.92,
    0.90,
    0.88,
    0.85,
    0.82,
    0.80,
    0.75,
    0.70,
    0.65,
    0.60,
)


class SessionCertification(BaseModel):
    """The outcome of certifying auto-apply from a user's own repair labels."""

    alpha: float
    delta: float
    grid: list[float]
    min_support: int
    #: ``{issue_type: threshold}``. ``ABSTAIN_THRESHOLD`` means never auto-apply.
    thresholds: dict[str, float] = Field(default_factory=dict)
    #: Why a class was not certified, keyed by issue type.
    reasons: dict[str, str] = Field(default_factory=dict)
    certified_classes: list[str] = Field(default_factory=list)
    repair_labels_used: int = Field(ge=0)
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    table_fingerprint: str
    #: The model whose proposals earned this certificate. A certificate is void under a
    #: different model; see :func:`certificate_model_mismatch`.
    corrector_provider: str | None = None
    corrector_model: str | None = None

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


def repair_labelled_samples(
    artifact: CalibrationSessionArtifact,
) -> list[CalibrationSample]:
    """Return only samples carrying a usable *repair* verdict.

    A sample qualifies only with a proposed repair, a confidence for it, and a verdict on
    it. Detection verdicts are deliberately ignored here.
    """
    return [
        sample
        for sample in artifact.samples
        if sample.repair_decision != "pending"
        and sample.proposed_repair is not None
        and sample.repair_confidence is not None
    ]


def certify_from_session(
    artifact: CalibrationSessionArtifact,
    *,
    alpha: float = 0.05,
    delta: float = 0.05,
    min_support: int = 30,
) -> SessionCertification:
    """Certify per-class auto-apply thresholds from the user's own repair labels.

    This is the one place a local guarantee is reachable where the global one was not.
    Conformal risk control requires the calibration data to be exchangeable with the target,
    which no benchmark can establish against an unseen user table -- the reason
    ``certify_thresholds_by_class`` ships with ``enabled_classes == []``. Here the
    calibration data *is* the table, so exchangeability holds by construction.

    **Uses repair labels only.** ``decision`` measures whether a flag was right; auto-apply
    needs to know whether the proposed *value* is right. Certifying on detection labels would
    authorize overwriting cells with unvalidated replacements, so this function refuses to
    look at them.

    Raises:
        ValueError: If no sample carries a repair verdict. Silently returning empty
            thresholds would read as "nothing could be certified" when the truth is
            "the wrong question was answered".
    """
    from dataforge.conformal import ABSTAIN_THRESHOLD, certification_reason, certify_threshold

    usable = repair_labelled_samples(artifact)
    if not usable:
        raise ValueError(
            "no repair verdicts in this session, so auto-apply cannot be certified. "
            "Detection verdicts answer 'was this flag right?', while auto-apply requires "
            "'is the proposed replacement right?' -- certifying on the former would "
            "authorize writing unvalidated values."
        )

    by_class: dict[str, list[tuple[float, bool]]] = {}
    for sample in usable:
        assert sample.repair_confidence is not None  # narrowed by repair_labelled_samples
        by_class.setdefault(sample.issue_type, []).append(
            (sample.repair_confidence, sample.repair_decision == "correct")
        )

    thresholds: dict[str, float] = {}
    reasons: dict[str, str] = {}
    certified: list[str] = []
    for issue_type, samples in sorted(by_class.items()):
        threshold = certify_threshold(
            samples,
            alpha=alpha,
            delta=delta,
            min_support=min_support,
            grid=CERTIFICATION_GRID,
        )
        if threshold is None:
            thresholds[issue_type] = ABSTAIN_THRESHOLD
            reason = certification_reason(
                samples, alpha=alpha, delta=delta, min_support=min_support
            )
            reasons[issue_type] = reason or "not certified"
        else:
            thresholds[issue_type] = threshold
            certified.append(issue_type)

    return SessionCertification(
        alpha=alpha,
        delta=delta,
        grid=list(CERTIFICATION_GRID),
        min_support=min_support,
        thresholds=thresholds,
        reasons=reasons,
        certified_classes=certified,
        repair_labels_used=len(usable),
        source_sha256=artifact.source_sha256,
        table_fingerprint=artifact.table_fingerprint,
        corrector_provider=artifact.corrector_provider,
        corrector_model=artifact.corrector_model,
    )


def certificate_model_mismatch(
    certification: SessionCertification,
    *,
    provider: str | None,
    model: str | None,
) -> str | None:
    """Return why a certificate does not apply to this model, or ``None`` if it does.

    Fails **closed on unknown**, matching :func:`~dataforge.calibration.guard_policy_for_scope`.
    A certificate that never recorded its model cannot be shown to apply to the model now
    running, so it is refused rather than assumed portable.

    Model identity is not a formality here. Corrector accuracy is model-specific and does not
    track model capability: on hospital, Azure ``gpt-5-mini`` measured
    ``precision_at_auto_apply`` 0.077 while a smaller Gemini model measured 0.16. Silently
    reusing one model's certificate for another would transfer a guarantee across the exact
    boundary that measurement shows it does not cross.
    """
    if certification.corrector_model is None:
        return (
            "certificate records no corrector model, so it cannot be shown to apply to the "
            "model now running; re-run calibration to earn it for this model"
        )
    if model is None:
        return (
            f"certificate was earned on model {certification.corrector_model!r} but the "
            "running model is unknown"
        )
    if certification.corrector_model != model or (
        certification.corrector_provider is not None
        and provider is not None
        and certification.corrector_provider != provider
    ):
        return (
            f"certificate was earned on {certification.corrector_provider or 'unknown'}"
            f"/{certification.corrector_model} but {provider or 'unknown'}/{model} is "
            "running; corrector accuracy is model-specific, so re-run calibration"
        )
    return None


def dump_calibration_session(artifact: CalibrationSessionArtifact) -> str:
    """Serialize a session with the repo's canonical JSON conventions."""
    return json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def load_calibration_session(path: Path) -> CalibrationSessionArtifact:
    """Load and validate a calibration session artifact."""
    return CalibrationSessionArtifact.model_validate(
        json.loads(Path(path).read_text(encoding="utf-8"))
    )
