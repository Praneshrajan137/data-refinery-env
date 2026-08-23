"""Tests for detection measurement over column benchmarks.

Offline and synthetic: a hand-built :class:`ColumnBenchmark` with stub detectors, so a pass
is never evidence that a download or a real detector run happened. The real-corpus numbers
live in ``eval/results/detection_*.json`` and in ``docs/trust/``.

The centre of this file is the **refusal**. A number that cannot be valid must not be
obtainable. The previous version of this module scored every detector against every corpus,
which produced a published ensemble precision that included 609 false positives from a
detector measured on a distribution that did not exist. See
``docs/trust/frequency-dependence-correction.md``.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.bench.abstention import AbstentionScoringError
from dataforge.bench.detection import (
    DECLARED_APPLICABILITY,
    EVALUABLE_ON_DISTINCT_VALUES,
    NotEvaluableError,
    measure_column_benchmark,
)
from dataforge.datasets.column_corpus import BenchmarkColumn, ColumnBenchmark
from dataforge.datasets.registry import COLUMN_BENCHMARK_REGISTRY
from dataforge.detectors import default_detectors
from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike


def _column(
    index: int,
    header: str,
    values: tuple[str, ...],
    truth: tuple[str, ...] = (),
    debatable: tuple[str, ...] = (),
) -> BenchmarkColumn:
    """Build one benchmark column."""
    return BenchmarkColumn(
        index=index,
        header=header,
        distinct_values=values,
        ground_truth=frozenset(truth),
        debatable=frozenset(debatable),
        declared_value_count=len(set(values)),
    )


def _benchmark(*columns: BenchmarkColumn) -> ColumnBenchmark:
    """Build a synthetic benchmark around the registered rt_bench metadata."""
    return ColumnBenchmark(
        metadata=COLUMN_BENCHMARK_REGISTRY["rt_bench"],
        columns=columns,
        quarantined=(),
        sha256="0" * 64,
        padded_rows_discarded=0,
        value_count_mismatches=0,
    )


class _FlagsRows:
    """Stub detector flagging fixed row offsets with a fixed confidence."""

    def __init__(self, rows: tuple[int, ...], *, confidence: float = 0.9) -> None:
        self._rows = rows
        self._confidence = confidence

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Flag the configured rows that exist in this frame."""
        column = str(df.columns[0])
        return [
            Issue(
                row=row,
                column=column,
                issue_type="type_mismatch",
                severity=Severity.REVIEW,
                confidence=self._confidence,
                actual=str(df[column].iloc[row]),
                reason="stub",
            )
            for row in self._rows
            if row < len(df.index)
        ]


class _Silent:
    """Stub detector that never flags anything."""

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Flag nothing."""
        return []


class _FlagsRowsFreqDependent(_FlagsRows):
    """A stub declared frequency_dependent, to exercise the refusal."""


@pytest.fixture(autouse=True)
def _declare_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Declare the stubs, because an undeclared detector is refused.

    Declaring them here rather than weakening the refusal is deliberate: the invariant under
    test is that nothing gets scored on a guessed class, and a test suite exempt from it
    would be testing a different module.
    """
    monkeypatch.setitem(DECLARED_APPLICABILITY, "_FlagsRows", "per_value")
    monkeypatch.setitem(DECLARED_APPLICABILITY, "_Silent", "per_value")
    monkeypatch.setitem(DECLARED_APPLICABILITY, "_Twice", "per_value")
    monkeypatch.setitem(DECLARED_APPLICABILITY, "_FlagsRowsFreqDependent", "frequency_dependent")


class TestTaxonomy:
    """Four classes, and the claim that each is a decision rather than a default."""

    def test_every_default_detector_is_declared(self) -> None:
        undeclared = sorted(
            type(detector).__name__
            for detector in default_detectors()
            if type(detector).__name__ not in DECLARED_APPLICABILITY
        )
        assert not undeclared, (
            f"undeclared detector(s) {undeclared}: classify them, do not let them inherit a "
            "default. A wrong default cost a published result."
        )

    def test_declarations_use_the_four_class_vocabulary(self) -> None:
        assert set(DECLARED_APPLICABILITY.values()) <= {
            "per_value",
            "proportion_gated",
            "frequency_dependent",
            "row_context",
        }

    def test_only_per_value_and_proportion_gated_are_evaluable_on_distinct_values(self) -> None:
        assert {"per_value", "proportion_gated"} == EVALUABLE_ON_DISTINCT_VALUES

    def test_the_three_frequency_dependent_detectors_are_classified_as_such(self) -> None:
        """The regression this whole correction exists to prevent."""
        for name in (
            "OutlierDetector",
            "DecimalShiftDetector",
            "CategoricalNormalizationDetector",
        ):
            assert DECLARED_APPLICABILITY[name] == "frequency_dependent", (
                f"{name} uses the column's distribution and must never be scored against a "
                "corpus that ships distinct values only"
            )

    def test_missing_value_is_per_value_for_detection(self) -> None:
        """Regression on a wrong declaration the harness caught once already.

        `missing_value` needs a declared FD to *repair* (to derive a fill) and nothing beyond
        the value to *detect* an empty or placeholder cell.
        """
        assert DECLARED_APPLICABILITY["MissingValueDetector"] == "per_value"

    def test_time_format_cruft_is_per_value_so_its_silence_is_a_real_measurement(self) -> None:
        """A pair of regexes with no state. Its silence is evidence, not an artifact."""
        assert DECLARED_APPLICABILITY["TimeFormatCruftDetector"] == "per_value"

    def test_an_undeclared_detector_is_refused_rather_than_guessed(self) -> None:
        class _Unclassified:
            def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
                return []

        benchmark = _benchmark(_column(0, "c", ("a", "bx"), truth=("bx",)))
        with pytest.raises(NotEvaluableError, match="DECLARED_APPLICABILITY"):
            measure_column_benchmark(benchmark, detectors=[_Unclassified()])


class TestRefusal:
    """The corpus-capability gate. This is the correction, made executable."""

    def test_a_frequency_dependent_detector_is_not_scored_on_a_distinct_value_corpus(
        self,
    ) -> None:
        benchmark = _benchmark(_column(0, "c", ("a", "bx", "c"), truth=("bx",)))
        result = measure_column_benchmark(
            benchmark, detectors=[_FlagsRows((1,)), _FlagsRowsFreqDependent((0, 2))]
        )
        by_name = {m.detector: m for m in result.per_detector}

        valid = by_name["_FlagsRows"]
        refused = by_name["_FlagsRowsFreqDependent"]

        assert valid.evaluable
        assert not refused.evaluable
        assert refused.score is None
        assert refused.not_evaluable_reason is not None
        assert "multiplicities" in refused.not_evaluable_reason

    def test_asking_a_refused_detector_for_its_score_raises(self) -> None:
        """`score is None` is not enough: a caller can read past None."""
        benchmark = _benchmark(_column(0, "c", ("a", "bx", "c"), truth=("bx",)))
        result = measure_column_benchmark(
            benchmark, detectors=[_FlagsRows((1,)), _FlagsRowsFreqDependent((0,))]
        )
        refused = next(m for m in result.per_detector if not m.evaluable)
        with pytest.raises(NotEvaluableError, match="cannot score it"):
            _ = refused.scored

    def test_refused_flags_are_excluded_from_the_ensemble(self) -> None:
        """The 609-false-positive bug, as an executable assertion.

        Without the exclusion the ensemble precision is 1/3; with it, 1/1.
        """
        benchmark = _benchmark(_column(0, "c", ("a", "bx", "c"), truth=("bx",)))
        result = measure_column_benchmark(
            benchmark, detectors=[_FlagsRows((1,)), _FlagsRowsFreqDependent((0, 2))]
        )
        assert result.evaluable_ensemble.tp == 1
        assert result.evaluable_ensemble.fp == 0, (
            "a not-evaluable detector's false positives must not reach the ensemble"
        )
        assert result.evaluable_ensemble.precision == 1.0
        # Precondition: the excluded detector must actually have flagged something, or the
        # assertion above holds vacuously.
        assert result.excluded_false_positive_flags == 2

    def test_refused_flags_are_still_counted_for_the_correction_bound(self) -> None:
        """They measure the adapter, not the detector, and the bound needs them."""
        benchmark = _benchmark(_column(0, "c", ("a", "bx", "c"), truth=("bx",)))
        result = measure_column_benchmark(
            benchmark, detectors=[_FlagsRows((1,)), _FlagsRowsFreqDependent((0, 2))]
        )
        refused = next(m for m in result.per_detector if not m.evaluable)
        assert refused.values_flagged == 2
        assert refused.raw_values_flagged == 2
        assert result.not_evaluable_detectors == ("_FlagsRowsFreqDependent",)

    def test_an_ensemble_with_no_evaluable_detector_raises(self) -> None:
        """Zero evaluable detectors would report zeros indistinguishable from a result."""
        benchmark = _benchmark(_column(0, "c", ("a", "bx"), truth=("bx",)))
        with pytest.raises(NotEvaluableError, match="no detector"):
            measure_column_benchmark(benchmark, detectors=[_FlagsRowsFreqDependent((0,))])

    def test_row_context_detectors_are_refused_too(self) -> None:
        real = [d for d in default_detectors() if type(d).__name__ == "FDViolationDetector"]
        assert real, "precondition: FDViolationDetector must be in the default ensemble"
        benchmark = _benchmark(_column(0, "c", ("a", "bx", "c"), truth=("bx",)))
        result = measure_column_benchmark(benchmark, detectors=[_FlagsRows((1,)), *real])
        fd = next(m for m in result.per_detector if m.detector == "FDViolationDetector")
        assert not fd.evaluable
        assert fd.not_evaluable_reason is not None
        assert "cross-column" in fd.not_evaluable_reason


class TestFrequencyDependenceIsReal:
    """The two mechanisms, demonstrated rather than asserted from the docs.

    These are the load-bearing evidence for the whole correction. If either stops
    reproducing, the taxonomy above is unjustified and must be revisited.
    """

    def test_outlier_abstains_with_frequencies_and_fires_without_them(self) -> None:
        """`outlier.py:74` returns [] when mad == 0. Deduplication destroys mad == 0."""
        from dataforge.detectors.outlier import OutlierDetector

        real = ["100"] * 500 + [str(101 + i) for i in range(14)] + ["9999"]
        deduped = list(dict.fromkeys(real))
        assert len(set(real)) == len(deduped), "precondition: same distinct values"

        with_frequencies = OutlierDetector().detect(pd.DataFrame({"amount": real}, dtype=str))
        without = OutlierDetector().detect(pd.DataFrame({"amount": deduped}, dtype=str))

        assert with_frequencies == [], "the real column's MAD is 0, so it correctly abstains"
        assert len(without) == 1, "deduplication manufactures a flag the real column refuses"

    def test_categorical_normalization_fires_with_frequencies_and_is_silent_without(
        self,
    ) -> None:
        """Two guards (`:68` distinct-ratio, `:88` strict-majority) close on deduped data."""
        from dataforge.detectors.categorical_normalization import (
            CategoricalNormalizationDetector,
        )

        real = ["NY"] * 500 + ["CA"] * 400 + ["ny"] * 3 + ["ca"] * 2
        deduped = list(dict.fromkeys(real))

        with_frequencies = CategoricalNormalizationDetector().detect(
            pd.DataFrame({"state": real}, dtype=str)
        )
        without = CategoricalNormalizationDetector().detect(
            pd.DataFrame({"state": deduped}, dtype=str)
        )

        assert len(with_frequencies) == 5, "the real column has five case variants to find"
        assert without == [], "deduplication loses every one of them"

    def test_the_two_biases_run_in_opposite_directions(self) -> None:
        """So there is no correction factor, only a refusal to score.

        Outlier manufactures false positives; the categorical detector loses true positives.
        A biased view is not a weaker view of the same population.
        """
        from dataforge.detectors.categorical_normalization import (
            CategoricalNormalizationDetector,
        )
        from dataforge.detectors.outlier import OutlierDetector

        numeric_real = ["100"] * 500 + [str(101 + i) for i in range(14)] + ["9999"]
        numeric_dedup = list(dict.fromkeys(numeric_real))
        cat_real = ["NY"] * 500 + ["CA"] * 400 + ["ny"] * 3 + ["ca"] * 2
        cat_dedup = list(dict.fromkeys(cat_real))

        outlier_delta = len(
            OutlierDetector().detect(pd.DataFrame({"a": numeric_dedup}, dtype=str))
        ) - len(OutlierDetector().detect(pd.DataFrame({"a": numeric_real}, dtype=str)))
        cat_delta = len(
            CategoricalNormalizationDetector().detect(pd.DataFrame({"s": cat_dedup}, dtype=str))
        ) - len(CategoricalNormalizationDetector().detect(pd.DataFrame({"s": cat_real}, dtype=str)))

        assert outlier_delta > 0, "dedup adds outlier flags"
        assert cat_delta < 0, "dedup removes categorical flags"


class TestScoring:
    """Per-detector and ensemble scoring over a synthetic corpus."""

    def test_per_detector_scores_are_attributed_not_pooled_into_the_ensemble(self) -> None:
        benchmark = _benchmark(_column(0, "c", ("a", "bx", "c"), truth=("bx",)))
        result = measure_column_benchmark(
            benchmark, detectors=[_FlagsRows((1,)), _FlagsRows((0, 2))]
        )
        first, second = result.per_detector
        assert first.scored.precision == 1.0
        assert second.scored.precision == 0.0
        assert result.evaluable_ensemble.tp == 1
        assert result.evaluable_ensemble.fp == 2

    def test_a_detector_that_never_fires_scores_none_not_zero(self) -> None:
        benchmark = _benchmark(_column(0, "c", ("a", "bx"), truth=("bx",)))
        result = measure_column_benchmark(benchmark, detectors=[_FlagsRows((1,)), _Silent()])
        silent = next(m for m in result.per_detector if m.detector == "_Silent")
        assert silent.evaluable, "it could have been scored; it simply found nothing"
        assert silent.score is None
        with pytest.raises(NotEvaluableError, match="flagged nothing"):
            _ = silent.scored

    def test_flagging_only_a_debatable_value_costs_no_precision(self) -> None:
        benchmark = _benchmark(
            _column(0, "c", ("a", "total", "bx"), truth=("bx",), debatable=("total",))
        )
        result = measure_column_benchmark(benchmark, detectors=[_FlagsRows((1,))])
        score = result.per_detector[0].scored
        assert score.fp == 0
        assert score.debatable_predicted == 1
        assert score.precision is None

    def test_debatable_values_are_excluded_from_frontier_samples(self) -> None:
        benchmark = _benchmark(
            _column(0, "c", ("total", "bx"), truth=("bx",), debatable=("total",))
        )
        result = measure_column_benchmark(benchmark, detectors=[_FlagsRows((0, 1))])
        frontier = result.per_detector[0].frontier
        assert frontier
        assert frontier[0]["n_accepted"] == 1
        assert frontier[0]["n_errors"] == 0

    def test_highest_confidence_wins_when_one_value_is_flagged_twice(self) -> None:
        benchmark = _benchmark(_column(0, "c", ("a", "bx"), truth=("bx",)))

        class _Twice:
            def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
                column = str(df.columns[0])
                return [
                    Issue(
                        row=1,
                        column=column,
                        issue_type="type_mismatch",
                        severity=Severity.REVIEW,
                        confidence=conf,
                        actual="bx",
                        reason="stub",
                    )
                    for conf in (0.3, 0.8)
                ]

        result = measure_column_benchmark(benchmark, detectors=[_Twice()])
        measurement = result.per_detector[0]
        assert measurement.values_flagged == 1
        assert measurement.frontier[0]["threshold"] <= 0.8


class TestRunMetadata:
    """The artifact must record what was measured, and what could not be."""

    def test_frequencies_available_is_recorded_from_the_corpus(self) -> None:
        benchmark = _benchmark(_column(0, "c", ("a", "bx"), truth=("bx",)))
        result = measure_column_benchmark(benchmark, detectors=[_FlagsRows((1,))])
        assert result.frequencies_available is False, (
            "rt_bench ships dist_val, a distinct-value list"
        )

    def test_label_density_is_reported(self) -> None:
        benchmark = _benchmark(
            _column(0, "c", tuple(f"v{i}" for i in range(99)) + ("bx",), truth=("bx",))
        )
        result = measure_column_benchmark(benchmark, detectors=[_FlagsRows((0,))])
        assert result.ground_truth_values == 1
        assert result.distinct_values == 100
        assert result.label_density == 0.01

    def test_an_empty_corpus_raises_rather_than_scoring_zeros(self) -> None:
        with pytest.raises((AbstentionScoringError, NotEvaluableError)):
            measure_column_benchmark(_benchmark(), detectors=[_FlagsRows((0,))])

    def test_there_is_no_field_holding_an_all_detector_ensemble(self) -> None:
        """A reader who greps for a total must not find one."""
        from dataforge.bench.detection import DetectionRunResult

        assert not hasattr(DetectionRunResult, "ensemble")
        assert "evaluable_ensemble" in DetectionRunResult.__dataclass_fields__
        assert "ensemble" not in DetectionRunResult.__dataclass_fields__
