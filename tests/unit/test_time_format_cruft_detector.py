"""Tests for the time-format-cruft detector (detection-only, tier 1, additive)."""

from __future__ import annotations

import pandas as pd

from dataforge.detectors import run_all_detectors
from dataforge.detectors.time_format_cruft import TimeFormatCruftDetector


def test_flags_date_prefixed_time() -> None:
    detector = TimeFormatCruftDetector()
    df = pd.DataFrame({"t": ["12/02/2011 6:55 a.m.", "6:55 a.m.", "5:50 a.m."]})
    issues = detector.detect(df)
    assert [i.row for i in issues] == [0]
    assert issues[0].issue_type == "format_violation"
    assert issues[0].expected == "6:55 a.m."


def test_flags_timezone_suffixed_time() -> None:
    detector = TimeFormatCruftDetector()
    df = pd.DataFrame({"t": ["9:05 a.m. (-00:00)", "9:05 a.m."]})
    issues = detector.detect(df)
    assert [i.row for i in issues] == [0]


def test_does_not_flag_clean_times_or_non_times() -> None:
    detector = TimeFormatCruftDetector()
    df = pd.DataFrame({"t": ["6:55 a.m.", "10:30 p.m.", "hello", "42", ""]})
    assert detector.detect(df) == []


def test_additive_and_dedup_in_ensemble() -> None:
    df = pd.DataFrame({"sched": ["12/02/2011 6:55 a.m.", "6:55 a.m.", "5:50 a.m.", "7:10 a.m."]})
    issues = run_all_detectors(df)
    cells = [(i.row, i.column) for i in issues]
    assert len(cells) == len(set(cells))  # one issue per cell
    assert any(i.row == 0 and i.issue_type == "format_violation" for i in issues)
