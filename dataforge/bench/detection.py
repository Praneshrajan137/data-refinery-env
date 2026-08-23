"""Detection measurement over column-level benchmarks with a debatable label class.

Runs DataForge's detector ensemble across a :class:`~dataforge.datasets.column_corpus.ColumnBenchmark`
and scores it under ``specs/SPEC_abstention_scoring.md``.

Three deliberate choices:

* **Per detector, not just the ensemble.** ``run_all_detectors`` deduplicates by cell with
  a precedence rule, so an ensemble number cannot tell you which detector earned it or
  which one is spraying false positives. Two detectors in this repository are carrying
  measured precision of 0.0000 from injected corpora (``outlier``, ``decimal_shift``);
  they get a per-detector hearing on real data here, and the result is reported whichever
  way it comes out.
* **Applicability is declared *and* observed.** A single-column table cannot exercise
  ``fd_violation``, ``missing_value``, ``entity_consensus`` or ``duplicate_row``. Those
  are declared ``not_applicable`` -- which is emphatically **not** recall 0 -- but the
  harness still runs them and records whether they fired. A declaration that turns out to
  be false is information, and hiding it would repeat the error this whole protocol
  exists to fix, one level up.
* **Debatable values are excluded from the frontier samples.** Including them as either
  outcome would reintroduce the penalty the three-way rule removes.

A benchmark row is one column of distinct values, so the synthesised frame has one column
and one row per distinct value. The real header is used, because some detectors key off
the column name and substituting a placeholder would measure a different system.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, TypeAlias

import pandas as pd

from dataforge.bench.abstention import (
    ThreeWayScore,
    aggregate_three_way,
    detection_risk_coverage_frontier,
    score_detection_three_way,
)
from dataforge.conformal import RISK_COVERAGE_GRID, LabeledSample
from dataforge.datasets.column_corpus import BenchmarkColumn, ColumnBenchmark
from dataforge.detectors import default_detectors
from dataforge.detectors.base import Detector, Issue

__all__ = [
    "DECLARED_APPLICABILITY",
    "EVALUABLE_ON_DISTINCT_VALUES",
    "ApplicabilityClass",
    "DetectorMeasurement",
    "DetectionRunResult",
    "NotEvaluableError",
    "measure_column_benchmark",
]

ApplicabilityClass: TypeAlias = Literal[
    "per_value",
    "proportion_gated",
    "frequency_dependent",
    "row_context",
]

# Only these two classes can be honestly scored against a corpus that ships distinct values
# without multiplicities. See docs/trust/frequency-dependence-correction.md.
EVALUABLE_ON_DISTINCT_VALUES: frozenset[str] = frozenset({"per_value", "proportion_gated"})

# What each detector *needs*, keyed on class name rather than on the issue types it happens
# to emit.
#
# This was a TWO-class taxonomy until 2026-08-23 and the missing axis cost a published
# result. `column_intrinsic` conflated "reads only this column" with "reads only this
# *value*", and three detectors need the column's *distribution*:
#
#   * OutlierDetector          -- median + MAD. `outlier.py:74` returns [] when mad == 0,
#                                 which is common on a real column with a dominant value and
#                                 never happens after deduplication. Measured: 0 flags with
#                                 frequencies preserved, 1 flag on the identical distinct
#                                 values deduplicated.
#   * DecimalShiftDetector     -- median + log-space IQR, same class.
#   * CategoricalNormalization -- Counter over exact forms. Two guards
#                                 (`categorical_normalization.py:68` distinct-ratio, `:88`
#                                 strict-majority) make it return [] for EVERY deduplicated
#                                 column. Measured: 5 flags with frequencies, 0 without.
#
# Deduplication is not a conservative bias. Outlier *manufactures* false positives; the
# categorical detector *loses* true positives. Opposite directions, so there is no
# correction factor -- only a refusal to score.
#
# Derivation of the other two classes:
#   * per_value        -- the predicate reads one value. TimeFormatCruft is the clearest
#                         case: a pair of regexes, no state. Its silence on 166,387 real
#                         values is therefore a VALID measurement -- those corpora contain
#                         no "clock time plus date/timezone residue" values, which is a
#                         flights-specific family per its own docstring.
#   * proportion_gated -- a per-value predicate behind a column-TYPE fraction gate
#                         (`date_like / len(non_empty) >= _DATE_COLUMN_FRACTION`).
#                         Deduplication shifts the gate without invalidating it, because the
#                         gate asks "is this a date column", which dedup mostly preserves.
#
# `MissingValueDetector` is per_value and this was also wrong once: it was declared
# row_context on the reasoning that `missing_value` needs a declared functional dependency.
# That is true of *repair*, where the FD derives the fill, and false of *detection*, where
# noticing an empty or placeholder cell needs nothing but the value.
DECLARED_APPLICABILITY: dict[str, ApplicabilityClass] = {
    "TypeMismatchDetector": "per_value",
    "FormatViolationDetector": "per_value",
    "MissingValueDetector": "per_value",
    "TimeFormatCruftDetector": "per_value",
    "SemanticDomainDetector": "per_value",
    "DateTranspositionDetector": "proportion_gated",
    "OutlierDetector": "frequency_dependent",
    "DecimalShiftDetector": "frequency_dependent",
    "CategoricalNormalizationDetector": "frequency_dependent",
    # Needs a declared functional dependency, which a one-column frame cannot carry.
    "FDViolationDetector": "row_context",
    # Needs whole rows. `dist_val` is a DISTINCT-value list, so a duplicate row is
    # unrepresentable by construction.
    "DuplicateRowDetector": "row_context",
    # Needs several columns to form an entity.
    "EntityConsensusDetector": "row_context",
}


class NotEvaluableError(RuntimeError):
    """Raised when a score is requested that this corpus cannot honestly support.

    A number that cannot be valid must not be obtainable. This is the same fail-closed
    discipline the write allowlist uses, applied to measurement: the previous design
    returned a plausible-looking precision for a detector evaluated on a distribution that
    did not exist, and nothing in the return type signalled the problem.
    """


@dataclass(frozen=True, slots=True)
class DetectorMeasurement:
    """One detector's result over a whole column benchmark.

    Three distinct outcomes, kept separate because collapsing any two of them is how a
    misleading number gets published:

    * ``evaluable and score is not None`` -- a real measurement.
    * ``evaluable and score is None`` -- the detector fired nowhere. An absence of evidence,
      excluded from aggregate denominators rather than contributing a 0.0.
    * ``not evaluable`` -- this corpus cannot answer the question. **Not** precision 0 and
      **not** recall 0. ``score`` is None and :attr:`scored` raises if asked.

    ``raw_values_flagged`` is retained even when not evaluable, because it is what the
    corrected ensemble bound in
    ``docs/trust/frequency-dependence-correction.md`` subtracts. It measures the adapter's
    behaviour, not the detector's quality.
    """

    detector: str
    issue_types: tuple[str, ...]
    applicability: ApplicabilityClass
    evaluable: bool
    not_evaluable_reason: str | None
    fired: bool
    score: ThreeWayScore | None
    frontier: tuple[dict[str, float], ...]
    columns_flagged: int
    values_flagged: int

    @property
    def raw_values_flagged(self) -> int:
        """Values flagged, including by a not-evaluable detector."""
        return self.values_flagged

    @property
    def scored(self) -> ThreeWayScore:
        """The score, refusing to hand back a number that cannot be valid.

        Raises:
            NotEvaluableError: If this corpus cannot support a score for this detector.
        """
        if not self.evaluable:
            raise NotEvaluableError(
                f"{self.detector} is {self.applicability} and this corpus cannot score it: "
                f"{self.not_evaluable_reason}"
            )
        if self.score is None:
            raise NotEvaluableError(f"{self.detector} flagged nothing; there is no score")
        return self.score


@dataclass(frozen=True, slots=True)
class DetectionRunResult:
    """The full detection measurement for one benchmark.

    ``evaluable_ensemble`` is the union over **evaluable detectors only**. There is
    deliberately no field for the union over all detectors: the first version of this class
    had one, it silently included 609 false positives from a detector measured on a
    distribution that did not exist, and it was published.
    """

    benchmark: str
    source_revision: str
    sha256: str
    frequencies_available: bool
    columns_scored: int
    columns_quarantined: int
    ground_truth_values: int
    debatable_values: int
    distinct_values: int
    per_detector: tuple[DetectorMeasurement, ...]
    evaluable_ensemble: ThreeWayScore

    @property
    def evaluable_detectors(self) -> tuple[str, ...]:
        """Detectors this corpus could honestly score."""
        return tuple(m.detector for m in self.per_detector if m.evaluable)

    @property
    def not_evaluable_detectors(self) -> tuple[str, ...]:
        """Detectors this corpus cannot score, whatever they did."""
        return tuple(m.detector for m in self.per_detector if not m.evaluable)

    @property
    def excluded_false_positive_flags(self) -> int:
        """Flags emitted by not-evaluable detectors, i.e. what the corrected bound removes.

        An upper bound on the reduction, not the reduction itself: the ensemble is a union
        over values, so these overlap with the evaluable detectors' flags.
        """
        return sum(m.values_flagged for m in self.per_detector if not m.evaluable)

    @property
    def label_density(self) -> float:
        """Unambiguous error values per distinct value.

        Reported because it governs how much weight a recall number can carry: at the
        pinned revision this is ~0.0005, so recall rests on double-digit support while
        precision rests on six figures.
        """
        if self.distinct_values == 0:
            return 0.0
        return round(self.ground_truth_values / self.distinct_values, 6)


def _frame_for(column: BenchmarkColumn) -> pd.DataFrame:
    """Build the one-column frame for a benchmark row.

    Values are kept as strings with the real header, matching how
    :func:`dataforge.datasets.real_world.load_real_world_dataset` presents a corpus and
    how a detector would see a freshly-read CSV.
    """
    return pd.DataFrame({column.header or "column": list(column.distinct_values)}, dtype=str)


def _flagged_values(issues: Sequence[Issue], column: BenchmarkColumn) -> dict[str, float]:
    """Map issues back to the values they flagged, keeping the highest confidence.

    Highest-confidence-wins matches ``run_all_detectors``' per-cell precedence, so a
    detector that flags one value twice is not counted twice.
    """
    values = column.distinct_values
    flagged: dict[str, float] = {}
    for issue in issues:
        if not 0 <= issue.row < len(values):
            continue
        value = values[issue.row]
        previous = flagged.get(value)
        if previous is None or issue.confidence > previous:
            flagged[value] = issue.confidence
    return flagged


def _measure_one(
    detector_name: str,
    issue_types: set[str],
    flagged_per_column: Sequence[dict[str, float]],
    benchmark: ColumnBenchmark,
    *,
    frequencies_available: bool,
) -> DetectorMeasurement:
    """Score one detector from its already-computed flags, or refuse to.

    Raises:
        NotEvaluableError: If the detector is undeclared. An undeclared detector would
            otherwise default to a class and be scored on that guess, which is how the
            two-class taxonomy produced an invalid published number.
    """
    if detector_name not in DECLARED_APPLICABILITY:
        raise NotEvaluableError(
            f"{detector_name} has no entry in DECLARED_APPLICABILITY. Classify it as "
            f"one of {sorted(set(DECLARED_APPLICABILITY.values()))} rather than letting it "
            "inherit a default; a wrong default cost a published result on 2026-08-23."
        )
    applicability = DECLARED_APPLICABILITY[detector_name]

    evaluable = True
    not_evaluable_reason: str | None = None
    if applicability == "row_context":
        evaluable = False
        not_evaluable_reason = (
            "needs row or cross-column context that a one-column benchmark cannot supply"
        )
    elif applicability == "frequency_dependent" and not frequencies_available:
        evaluable = False
        not_evaluable_reason = (
            "needs value multiplicities and this corpus ships distinct values only, so any "
            "score would describe a distribution that does not exist "
            "(docs/trust/frequency-dependence-correction.md)"
        )

    scores: list[ThreeWayScore] = []
    samples: list[LabeledSample] = []
    columns_flagged = 0
    values_flagged = 0

    for column, flagged in zip(benchmark.columns, flagged_per_column, strict=True):
        if flagged:
            columns_flagged += 1
            values_flagged += len(flagged)
        if not evaluable:
            # Flags are still counted -- they are what the corrected ensemble bound
            # subtracts -- but they are never scored.
            continue
        scores.append(
            score_detection_three_way(
                distinct_values=column.distinct_values,
                ground_truth=column.ground_truth,
                debatable=column.debatable,
                predicted=flagged,
            )
        )
        for value, confidence in flagged.items():
            # Debatable values contribute to neither outcome; including them would
            # undo the neutral zone.
            if value in column.debatable:
                continue
            samples.append((confidence, value in column.ground_truth))

    fired = values_flagged > 0

    return DetectorMeasurement(
        detector=detector_name,
        issue_types=tuple(sorted(issue_types)),
        applicability=applicability,
        evaluable=evaluable,
        not_evaluable_reason=not_evaluable_reason,
        fired=fired,
        score=aggregate_three_way(scores) if (evaluable and fired) else None,
        frontier=tuple(detection_risk_coverage_frontier(samples, grid=RISK_COVERAGE_GRID)),
        columns_flagged=columns_flagged,
        values_flagged=values_flagged,
    )


def measure_column_benchmark(
    benchmark: ColumnBenchmark,
    *,
    detectors: Sequence[Detector] | None = None,
) -> DetectionRunResult:
    """Measure the detector ensemble and each detector individually.

    Each detector runs exactly once per column; the per-detector scores and the
    ensemble score are both derived from that single pass.

    Args:
        benchmark: A loaded, hash-verified column benchmark.
        detectors: Override the ensemble. Defaults to
            :func:`dataforge.detectors.default_detectors`.

    Returns:
        The :class:`DetectionRunResult`, carrying per-detector scores and frontiers plus the
        ensemble over **evaluable detectors only**.

    Raises:
        AbstentionScoringError: If the benchmark yields no admissible column. Propagated
            rather than caught: an aggregate over nothing must not report zeros.
        NotEvaluableError: If a detector is not classified in
            :data:`DECLARED_APPLICABILITY`, or if no detector is evaluable against this
            corpus. The second case matters: returning an ensemble over zero detectors
            would report zeros indistinguishable from a measured result.
    """
    ensemble = list(detectors) if detectors is not None else default_detectors()
    frequencies_available = getattr(benchmark.metadata, "frequencies_available", False)

    # One pass. flags[detector_index][column_index] -> {value: confidence}
    flags: list[list[dict[str, float]]] = [[] for _ in ensemble]
    issue_types: list[set[str]] = [set() for _ in ensemble]
    for column in benchmark.columns:
        frame = _frame_for(column)
        for index, detector in enumerate(ensemble):
            issues = detector.detect(frame, None)
            issue_types[index].update(issue.issue_type for issue in issues)
            flags[index].append(_flagged_values(issues, column))

    per_detector = tuple(
        _measure_one(
            type(detector).__name__,
            issue_types[index],
            flags[index],
            benchmark,
            frequencies_available=frequencies_available,
        )
        for index, detector in enumerate(ensemble)
    )

    evaluable_indices = [
        index for index, measurement in enumerate(per_detector) if measurement.evaluable
    ]
    if not evaluable_indices:
        raise NotEvaluableError(
            "no detector in this ensemble can be scored against "
            f"{benchmark.metadata.name!r} (frequencies_available="
            f"{frequencies_available}). An ensemble over zero detectors would report zeros."
        )

    # The ensemble score is the union of the EVALUABLE detectors' flags rather than a second
    # pass through run_all_detectors. Its deduplication keeps one issue per cell by
    # precedence, which changes *which* detector is credited but not *whether* the value was
    # flagged -- and the union is what the three-way rule scores.
    #
    # Restricting the union to evaluable detectors is the correction of 2026-08-23. The
    # previous version unioned everything, so `Outlier` contributed 609 false positives from
    # a measurement on a distribution that did not exist, halving the reported precision.
    ensemble_scores: list[ThreeWayScore] = []
    for column_index, column in enumerate(benchmark.columns):
        union: dict[str, float] = {}
        for detector_index in evaluable_indices:
            for value, confidence in flags[detector_index][column_index].items():
                previous = union.get(value)
                if previous is None or confidence > previous:
                    union[value] = confidence
        ensemble_scores.append(
            score_detection_three_way(
                distinct_values=column.distinct_values,
                ground_truth=column.ground_truth,
                debatable=column.debatable,
                predicted=union,
            )
        )

    return DetectionRunResult(
        benchmark=benchmark.metadata.name,
        source_revision=benchmark.metadata.source_revision,
        sha256=benchmark.sha256,
        frequencies_available=frequencies_available,
        columns_scored=benchmark.n_columns,
        columns_quarantined=len(benchmark.quarantined),
        ground_truth_values=benchmark.n_ground_truth_values,
        debatable_values=benchmark.n_debatable_values,
        distinct_values=benchmark.n_distinct_values,
        per_detector=per_detector,
        evaluable_ensemble=aggregate_three_way(ensemble_scores),
    )
