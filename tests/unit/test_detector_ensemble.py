"""Unit tests for the detector ensemble: per-cell dedup and expanded taxonomy."""

from __future__ import annotations

import pandas as pd

import dataforge.detectors as detectors_pkg
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Severity


def _issue(row: int, column: str, issue_type: str, confidence: float, severity: Severity) -> Issue:
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,  # type: ignore[arg-type]
        severity=severity,
        confidence=confidence,
        actual="x",
        reason="synthetic",
    )


class _FakeDetector:
    def __init__(self, issues: list[Issue]) -> None:
        self._issues = issues

    def detect(self, df, schema=None):  # noqa: ANN001
        return list(self._issues)


class TestExpandedTaxonomy:
    def test_new_issue_types_are_accepted(self) -> None:
        for issue_type in (
            "missing_value",
            "format_violation",
            "categorical_normalization",
            "outlier",
            "duplicate_row",
        ):
            issue = _issue(0, "c", issue_type, 0.9, Severity.REVIEW)
            assert issue.issue_type == issue_type


class TestEnsembleDedup:
    def test_one_issue_per_cell_first_detector_wins(self, monkeypatch) -> None:  # noqa: ANN001
        # Registration order is priority: the first detector to claim a cell wins,
        # so later (newer, fuzzier) detectors are strictly additive and cannot
        # displace an earlier precise detector - even with higher confidence.
        precise = _issue(0, "c", "fd_violation", 0.50, Severity.UNSAFE)
        newer = _issue(0, "c", "format_violation", 0.90, Severity.REVIEW)
        monkeypatch.setattr(
            detectors_pkg,
            "default_detectors",
            lambda: [_FakeDetector([precise]), _FakeDetector([newer])],
        )
        issues = run_all_detectors(pd.DataFrame({"c": ["1", "2"]}))
        cells = [(i.row, i.column) for i in issues]
        assert cells == [(0, "c")]  # exactly one issue for the cell
        assert issues[0].issue_type == "fd_violation"  # earlier detector won

    def test_newer_detector_fills_only_unclaimed_cells(self, monkeypatch) -> None:  # noqa: ANN001
        precise = _issue(0, "c", "fd_violation", 0.5, Severity.UNSAFE)
        newer_same = _issue(0, "c", "format_violation", 0.9, Severity.REVIEW)
        newer_new = _issue(1, "c", "format_violation", 0.9, Severity.REVIEW)
        monkeypatch.setattr(
            detectors_pkg,
            "default_detectors",
            lambda: [_FakeDetector([precise]), _FakeDetector([newer_same, newer_new])],
        )
        issues = run_all_detectors(pd.DataFrame({"c": ["1", "2"]}))
        by_cell = {(i.row, i.column): i.issue_type for i in issues}
        assert by_cell == {(0, "c"): "fd_violation", (1, "c"): "format_violation"}

    def test_distinct_cells_are_all_kept(self, monkeypatch) -> None:  # noqa: ANN001
        a = _issue(0, "c", "missing_value", 0.8, Severity.REVIEW)
        b = _issue(1, "c", "outlier", 0.7, Severity.REVIEW)
        monkeypatch.setattr(
            detectors_pkg, "default_detectors", lambda: [_FakeDetector([a, b])]
        )
        issues = run_all_detectors(pd.DataFrame({"c": ["1", "2"]}))
        assert {(i.row, i.column) for i in issues} == {(0, "c"), (1, "c")}
