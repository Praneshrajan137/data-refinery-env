"""Unit tests for the Task 6 detectors: normalization, outlier, duplicate row."""

from __future__ import annotations

import pandas as pd

from dataforge.detectors.categorical_normalization import (
    CategoricalNormalizationDetector,
    normalization_key,
)
from dataforge.detectors.duplicate_row import DuplicateRowDetector
from dataforge.detectors.outlier import OutlierDetector
from dataforge.repairers.categorical_normalization import CategoricalNormalizationRepairer


class TestCategoricalNormalization:
    def test_flags_case_variants(self) -> None:
        col = ["NY", "NY", "ny", "NY", "CA", "CA", "CA", "NY", "ca", "NY"]
        issues = CategoricalNormalizationDetector().detect(pd.DataFrame({"state": col}))
        assert sorted(i.row for i in issues) == [2, 8]
        assert all(i.issue_type == "categorical_normalization" for i in issues)
        assert {i.expected for i in issues} == {"NY", "CA"}

    def test_ignores_high_cardinality(self) -> None:
        col = [f"id-{i}" for i in range(20)]
        assert CategoricalNormalizationDetector().detect(pd.DataFrame({"id": col})) == []

    def test_repairer_maps_to_canonical(self) -> None:
        col = ["NY", "NY", "ny", "NY", "CA", "CA", "CA", "NY", "ca", "NY"]
        df = pd.DataFrame({"state": col})
        issue = next(i for i in CategoricalNormalizationDetector().detect(df) if i.row == 2)
        fix = CategoricalNormalizationRepairer().propose(issue, df, None)
        assert fix is not None
        assert fix.fix.new_value == "NY"

    def test_normalization_key(self) -> None:
        assert normalization_key("New  YORK") == normalization_key("new york")


class TestOutlier:
    def test_flags_robust_outlier(self) -> None:
        col = [str(x) for x in [10, 11, 9, 12, 10, 11, 13, 9, 10, 12, 11, 4200]]
        issues = OutlierDetector().detect(pd.DataFrame({"v": col}))
        assert [i.row for i in issues] == [11]
        assert issues[0].issue_type == "outlier"

    def test_no_flags_on_tight_distribution(self) -> None:
        col = [str(x) for x in [10, 10, 11, 10, 11, 10, 11, 10, 11, 10, 11, 10]]
        assert OutlierDetector().detect(pd.DataFrame({"v": col})) == []

    def test_ignores_non_numeric_column(self) -> None:
        col = ["apple", "pear", "plum"] * 4
        assert OutlierDetector().detect(pd.DataFrame({"fruit": col})) == []


class TestDuplicateRow:
    def test_flags_exact_duplicate(self) -> None:
        df = pd.DataFrame({"a": ["1", "2", "1"], "b": ["x", "y", "x"]})
        issues = DuplicateRowDetector().detect(df)
        assert [i.row for i in issues] == [2]
        assert issues[0].issue_type == "duplicate_row"

    def test_no_duplicates(self) -> None:
        df = pd.DataFrame({"a": ["1", "2", "3"], "b": ["x", "y", "z"]})
        assert DuplicateRowDetector().detect(df) == []
