"""Tests for the time-format-cruft detector (detection-only, tier 1, additive).

The issue-type assertions below changed on 2026-09-01 from ``format_violation`` to
``time_format_cruft``, and the reason is a write-safety invariant rather than a rename. This
detector's ``Issue.expected`` is a substitutable **value** (``"6:55 a.m."``); the other
emitter of ``format_violation`` puts a shape **mask** (``"9999-99-99"``) there. ``Issue``
carries no detector identity apart from its issue type, so while the two shared an id, routing
this detector's value into the review-suggestion path -- whose only guard is
``expected is None`` -- would have routed masks with it and proposed writing a format
description into a user cell. Separate ids are what make the safe half routable.

These assertions are therefore load-bearing, not incidental: they are what would fail if the
two ids were merged again. See tests/unit/test_expected_value_semantics.py.
"""

from __future__ import annotations

import pandas as pd

from dataforge.detectors import run_all_detectors
from dataforge.detectors.time_format_cruft import TimeFormatCruftDetector


def test_flags_date_prefixed_time() -> None:
    detector = TimeFormatCruftDetector()
    df = pd.DataFrame({"t": ["12/02/2011 6:55 a.m.", "6:55 a.m.", "5:50 a.m."]})
    issues = detector.detect(df)
    assert [i.row for i in issues] == [0]
    assert issues[0].issue_type == "time_format_cruft"
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
    assert any(i.row == 0 and i.issue_type == "time_format_cruft" for i in issues)
