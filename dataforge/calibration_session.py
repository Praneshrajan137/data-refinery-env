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
    """One flagged cell offered to the user for adjudication."""

    row: int = Field(ge=0)
    column: str = Field(min_length=1)
    issue_type: str = Field(min_length=1)
    detector_confidence: float = Field(ge=0.0, le=1.0)
    flagged_value: str
    reason: str = Field(min_length=1)
    decision: CalibrationDecision = "pending"
    note: str | None = None

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
    for issue in issues:
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
        flagged_cells_total=len(issues),
        fd_detection_source=fd_detection_source,
        seed=seed,
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


def dump_calibration_session(artifact: CalibrationSessionArtifact) -> str:
    """Serialize a session with the repo's canonical JSON conventions."""
    return json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def load_calibration_session(path: Path) -> CalibrationSessionArtifact:
    """Load and validate a calibration session artifact."""
    return CalibrationSessionArtifact.model_validate(
        json.loads(Path(path).read_text(encoding="utf-8"))
    )
