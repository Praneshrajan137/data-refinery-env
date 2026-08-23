"""Cell-level detection measurement on frequency-preserving corpora.

The companion to :mod:`dataforge.bench.detection`, which scores **distinct values** on
``RT-bench``/``ST-bench``. This module scores **cells** on the RAHA dirty/clean pairs, and it
exists because those two numbers turned out not to be convertible.

Measured on ``rayyan``, same detectors, same data, only the unit varying
(``docs/trust/scoring-unit-reconciliation.md``):

* ``FormatViolation``: 0.2388 at cell level, **0.0000** deduplicated.
* ``MissingValue``: 0.0649 at cell level, **0.2000** deduplicated.

Opposite directions, so there is no conversion factor. A distinct-value precision cannot be
read as a review-queue precision, and the review queue is what a user actually sees.

Two things this module can do that the distinct-value harness cannot, both because these
corpora ship multiplicities:

1. **Score all four applicability classes.** ``frequency_dependent`` detectors
   (``outlier``, ``decimal_shift``, ``categorical_normalization``) are evaluable here and
   nowhere else in this repository. This is their only valid home.
2. **Report the unit a product decision is actually made in.** A false positive on a value
   occurring forty times costs forty review items; deduplicated it costs one.

One thing it cannot do, stated because it is the mirror of the previous correction: **RAHA
ships no debatable label class, so cell-level scoring here is two-way.** Ambiguous cells were
resolved by whoever built the corpus and the resolution is not recorded, so these numbers
carry the identification problem documented in ``docs/trust/semantic-domain-result.md`` --
the ``flights`` arrival-time case is the canonical example. Cell-level scoring buys the right
unit at the cost of the neutral zone. Neither harness dominates the other.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from dataforge.bench.detection import DECLARED_APPLICABILITY, ApplicabilityClass, NotEvaluableError
from dataforge.datasets.real_world import RealWorldDataset
from dataforge.detectors import default_detectors
from dataforge.detectors.base import Detector

__all__ = [
    "CellScore",
    "CellDetectorMeasurement",
    "CellDetectionRunResult",
    "measure_cell_detection",
]

_ROUND = 4


@dataclass(frozen=True, slots=True)
class CellScore:
    """Cell-level detection metrics for one detector.

    ``precision`` and ``recall`` are ``None`` where undefined rather than filled with 1.0,
    matching :class:`dataforge.bench.abstention.ThreeWayScore`. A detector that flagged
    nothing has no precision to report, and reporting 1.0 would let silence look perfect.
    """

    tp: int
    fp: int
    fn: int
    precision: float | None
    recall: float | None
    f1: float | None
    cells_flagged: int
    total_cells: int

    @property
    def flag_rate(self) -> float:
        """Fraction of all cells flagged: the review-queue burden."""
        if self.total_cells == 0:
            return 0.0
        return round(self.cells_flagged / self.total_cells, 6)


@dataclass(frozen=True, slots=True)
class CellDetectorMeasurement:
    """One detector's cell-level result, with its applicability recorded."""

    detector: str
    applicability: ApplicabilityClass
    issue_types: tuple[str, ...]
    score: CellScore | None

    @property
    def fired(self) -> bool:
        """Whether the detector flagged any cell."""
        return self.score is not None and self.score.cells_flagged > 0


@dataclass(frozen=True, slots=True)
class CellDetectionRunResult:
    """Cell-level detection measurement for one frequency-preserving corpus."""

    dataset: str
    error_provenance: str
    tier: str
    rows: int
    columns: int
    ground_truth_cells: int
    total_cells: int
    per_detector: tuple[CellDetectorMeasurement, ...]
    scoring_unit: str = "cell"
    # RAHA pairs have no ground_truth_debatable class, so ambiguous cells are resolved by the
    # corpus author and the resolution is unrecorded. Carried on the result so an artifact
    # cannot omit it.
    debatable_class_available: bool = False

    @property
    def best_precision_detector(self) -> str | None:
        """Detector with the highest defined precision, or None if none is defined."""
        scored = [
            (m.score.precision, m.detector)
            for m in self.per_detector
            if m.score is not None and m.score.precision is not None
        ]
        if not scored:
            return None
        return max(scored)[1]


def _score_cells(
    flagged: set[tuple[int, str]],
    ground_truth: set[tuple[int, str]],
    total_cells: int,
) -> CellScore:
    """Score flagged cells against ground-truth error cells."""
    tp = len(flagged & ground_truth)
    fp = len(flagged - ground_truth)
    fn = len(ground_truth - flagged)
    precision = round(tp / (tp + fp), _ROUND) if (tp + fp) else None
    recall = round(tp / (tp + fn), _ROUND) if (tp + fn) else None
    if precision is None or recall is None:
        f1: float | None = None
    elif precision + recall == 0:
        f1 = 0.0
    else:
        f1 = round(2 * precision * recall / (precision + recall), _ROUND)
    return CellScore(
        tp=tp,
        fp=fp,
        fn=fn,
        precision=precision,
        recall=recall,
        f1=f1,
        cells_flagged=len(flagged),
        total_cells=total_cells,
    )


def measure_cell_detection(
    dataset: RealWorldDataset,
    *,
    detectors: Sequence[Detector] | None = None,
) -> CellDetectionRunResult:
    """Measure every detector at cell level on a frequency-preserving corpus.

    Args:
        dataset: A loaded RAHA dirty/clean pair. Must carry frequencies.
        detectors: Override the ensemble. Defaults to
            :func:`dataforge.detectors.default_detectors`.

    Returns:
        The :class:`CellDetectionRunResult`.

    Raises:
        NotEvaluableError: If the corpus does not carry frequencies, or if a detector is not
            classified in :data:`DECLARED_APPLICABILITY`. Both fail closed for the same
            reason the distinct-value harness does: a number that cannot be valid must not be
            obtainable.
    """
    if not getattr(dataset.metadata, "frequencies_available", False):
        raise NotEvaluableError(
            f"{dataset.metadata.name!r} does not carry value frequencies; cell-level scoring "
            "requires them. Use dataforge.bench.detection for distinct-value corpora."
        )

    ensemble = list(detectors) if detectors is not None else default_detectors()
    undeclared = sorted(
        type(d).__name__ for d in ensemble if type(d).__name__ not in DECLARED_APPLICABILITY
    )
    if undeclared:
        raise NotEvaluableError(
            f"undeclared detector(s) {undeclared}: classify them in DECLARED_APPLICABILITY "
            "rather than letting them inherit a default."
        )

    ground_truth = {(cell.row, cell.column) for cell in dataset.ground_truth}
    total_cells = len(dataset.dirty_df.index) * len(dataset.canonical_columns)

    measurements: list[CellDetectorMeasurement] = []
    for detector in ensemble:
        name = type(detector).__name__
        issues = detector.detect(dataset.dirty_df, None)
        flagged = {(issue.row, issue.column) for issue in issues}
        measurements.append(
            CellDetectorMeasurement(
                detector=name,
                applicability=DECLARED_APPLICABILITY[name],
                issue_types=tuple(sorted({issue.issue_type for issue in issues})),
                # None when nothing was flagged: an absence of evidence, not a zero.
                score=_score_cells(flagged, ground_truth, total_cells) if flagged else None,
            )
        )

    return CellDetectionRunResult(
        dataset=dataset.metadata.name,
        error_provenance=dataset.metadata.error_provenance,
        tier=dataset.metadata.tier,
        rows=len(dataset.dirty_df.index),
        columns=len(dataset.canonical_columns),
        ground_truth_cells=len(ground_truth),
        total_cells=total_cells,
        per_detector=tuple(measurements),
    )
