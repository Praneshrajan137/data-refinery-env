"""Tests for cell-level detection measurement.

Offline and synthetic. The point of this module is the *unit*, so the tests assert that the
unit is honoured and that its limits are carried on the result rather than left to prose.

The one result worth encoding as a test is the asymmetry: a value repeated N times contributes
N cells and one distinct value. That is the whole reason two harnesses exist.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.bench.cell_detection import measure_cell_detection
from dataforge.bench.detection import NotEvaluableError
from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from dataforge.datasets.registry import DATASET_REGISTRY
from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike


def _dataset(
    dirty: dict[str, list[str]],
    clean: dict[str, list[str]],
    *,
    name: str = "rayyan",
) -> RealWorldDataset:
    """Build a synthetic frequency-preserving dataset with a diffed ground truth."""
    dirty_df = pd.DataFrame(dirty, dtype=str)
    clean_df = pd.DataFrame(clean, dtype=str)
    truth = tuple(
        GroundTruthCell(
            row=row,
            column=col,
            dirty_value=str(dirty_df[col].iloc[row]),
            clean_value=str(clean_df[col].iloc[row]),
        )
        for col in dirty_df.columns
        for row in range(len(dirty_df.index))
        if str(dirty_df[col].iloc[row]) != str(clean_df[col].iloc[row])
    )
    return RealWorldDataset(
        metadata=DATASET_REGISTRY[name].model_copy(
            update={"n_rows": len(dirty_df.index), "n_columns": len(dirty_df.columns)}
        ),
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=tuple(str(c) for c in dirty_df.columns),
        ground_truth=truth,
        dirty_sha256="0" * 64,
        clean_sha256="1" * 64,
    )


class _FlagsValue:
    """Stub detector flagging every cell holding a given value."""

    def __init__(self, target: str) -> None:
        self._target = target

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Flag all cells equal to the target value."""
        issues: list[Issue] = []
        for col in df.columns:
            for row, value in enumerate(df[col]):
                if str(value) == self._target:
                    issues.append(
                        Issue(
                            row=row,
                            column=str(col),
                            issue_type="type_mismatch",
                            severity=Severity.REVIEW,
                            confidence=0.9,
                            actual=str(value),
                            reason="stub",
                        )
                    )
        return issues


class _Silent:
    """Stub detector that never flags."""

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Flag nothing."""
        return []


@pytest.fixture(autouse=True)
def _declare_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Declare the stubs; an undeclared detector is refused."""
    from dataforge.bench.detection import DECLARED_APPLICABILITY

    monkeypatch.setitem(DECLARED_APPLICABILITY, "_FlagsValue", "per_value")
    monkeypatch.setitem(DECLARED_APPLICABILITY, "_Silent", "per_value")


class TestTheUnit:
    """Cells, not distinct values. This is the reason the module exists."""

    def test_a_repeated_flagged_value_counts_once_per_cell(self) -> None:
        """Forty occurrences of one wrong value is forty review items, not one."""
        dirty = {"v": ["bad"] * 4 + ["ok"] * 6}
        clean = {"v": ["good"] * 4 + ["ok"] * 6}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=[_FlagsValue("bad")])
        score = result.per_detector[0].score
        assert score is not None
        assert score.tp == 4, "four cells, not one distinct value"
        assert score.fp == 0
        assert score.precision == 1.0

    def test_a_repeated_clean_value_costs_precision_once_per_cell(self) -> None:
        """The mirror case, and the reason dedup can flatter a detector."""
        dirty = {"v": ["common"] * 8 + ["bad", "ok"]}
        clean = {"v": ["common"] * 8 + ["good", "ok"]}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=[_FlagsValue("common")])
        score = result.per_detector[0].score
        assert score is not None
        assert score.fp == 8, "one wrong distinct value costs eight review items"
        assert score.precision == 0.0

    def test_flag_rate_reports_the_queue_burden(self) -> None:
        dirty = {"a": ["x"] * 5, "b": ["y"] * 5}
        clean = {"a": ["x"] * 5, "b": ["y"] * 5}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=[_FlagsValue("x")])
        score = result.per_detector[0].score
        assert score is not None
        assert score.total_cells == 10
        assert score.cells_flagged == 5
        assert score.flag_rate == 0.5


class TestFailClosed:
    """A number that cannot be valid must not be obtainable."""

    def test_a_corpus_without_frequencies_is_refused(self) -> None:
        """The mirror of the distinct-value harness's refusal."""
        dirty = {"v": ["bad", "ok", "ok"]}
        clean = {"v": ["good", "ok", "ok"]}
        dataset = _dataset(dirty, clean)
        stripped = RealWorldDataset(
            metadata=dataset.metadata.model_copy(update={"frequencies_available": False}),
            dirty_df=dataset.dirty_df,
            clean_df=dataset.clean_df,
            canonical_columns=dataset.canonical_columns,
            ground_truth=dataset.ground_truth,
            dirty_sha256=dataset.dirty_sha256,
            clean_sha256=dataset.clean_sha256,
        )
        with pytest.raises(NotEvaluableError, match="frequencies"):
            measure_cell_detection(stripped, detectors=[_FlagsValue("bad")])

    def test_an_undeclared_detector_is_refused(self) -> None:
        class _Unclassified:
            def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
                return []

        dirty = {"v": ["bad", "ok"]}
        clean = {"v": ["good", "ok"]}
        with pytest.raises(NotEvaluableError, match="undeclared"):
            measure_cell_detection(_dataset(dirty, clean), detectors=[_Unclassified()])

    def test_a_silent_detector_scores_none_not_zero(self) -> None:
        dirty = {"v": ["bad", "ok"]}
        clean = {"v": ["good", "ok"]}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=[_Silent()])
        assert result.per_detector[0].score is None
        assert not result.per_detector[0].fired


class TestLimitsAreCarried:
    """The limits travel on the result, not only in prose."""

    def test_the_scoring_unit_is_recorded(self) -> None:
        dirty = {"v": ["bad", "ok"]}
        clean = {"v": ["good", "ok"]}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=[_FlagsValue("bad")])
        assert result.scoring_unit == "cell"

    def test_the_absence_of_a_debatable_class_is_recorded(self) -> None:
        """RAHA ships no neutral zone, so this scoring is two-way and unidentified."""
        dirty = {"v": ["bad", "ok"]}
        clean = {"v": ["good", "ok"]}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=[_FlagsValue("bad")])
        assert result.debatable_class_available is False

    def test_corpus_provenance_travels_with_the_numbers(self) -> None:
        """A precision measured on injected errors must not lose that fact."""
        dirty = {"v": ["bad", "ok"]}
        clean = {"v": ["good", "ok"]}
        result = measure_cell_detection(
            _dataset(dirty, clean, name="hospital"), detectors=[_FlagsValue("bad")]
        )
        assert result.error_provenance == "injected"
        assert result.tier == "tripwire"

    def test_all_four_applicability_classes_are_evaluable_here(self) -> None:
        """This corpus type is the only valid home for frequency_dependent detectors."""
        from dataforge.bench.detection import DECLARED_APPLICABILITY
        from dataforge.detectors import default_detectors

        dirty = {"v": [str(i) for i in range(12)] + ["bad"]}
        clean = {"v": [str(i) for i in range(12)] + ["good"]}
        result = measure_cell_detection(_dataset(dirty, clean), detectors=default_detectors())
        # No measurement is withheld for applicability reasons; frequencies are present.
        classes = {m.applicability for m in result.per_detector}
        assert "frequency_dependent" in classes
        assert set(DECLARED_APPLICABILITY.values()) >= classes
