"""Unit tests for DateTranspositionDetector and its propose-not-apply surfacing.

The rotation Y/M/D -> M/D/YY is an EXACT deterministic transform, but detection
is not provable (a valid date is indistinguishable from a transposed one), so the
detector ships with NO repairer: it can never auto-apply. These tests pin that
guarantee (apply-mode leaves the file byte-identical), that the exact rotation is
carried in Issue.expected and surfaced as an unverified suggestion, and that the
detector stays silent on non-date columns (hospital/flights-shaped data).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataforge.detectors import run_all_detectors
from dataforge.detectors.date_transposition import DateTranspositionDetector, rotate_ymd_to_mdy
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.repairers import build_repairers


class TestRotation:
    def test_rotation_matches_measured_examples(self) -> None:
        # These are exact dirty->clean pairs measured on rayyan article_jcreated_at.
        assert rotate_ymd_to_mdy("4/2/15") == "2/15/04"
        assert rotate_ymd_to_mdy("12/1/06") == "1/6/12"
        assert rotate_ymd_to_mdy("1/1/13") == "1/13/01"
        assert rotate_ymd_to_mdy("10/1/15") == "1/15/10"

    def test_rotation_returns_none_for_non_three_part(self) -> None:
        assert rotate_ymd_to_mdy("6:55 a.m.") is None
        assert rotate_ymd_to_mdy("2024-01-01") is None
        assert rotate_ymd_to_mdy("hello") is None


class TestDetection:
    def test_fires_on_ymd_date_column_with_exact_expected(self) -> None:
        df = pd.DataFrame({"created": ["4/2/15", "12/1/06", "1/13/01", "3/9/11"]})
        issues = DateTranspositionDetector().detect(df)
        by_row = {i.row: i for i in issues}
        assert by_row[0].expected == "2/15/04"
        assert by_row[0].actual == "4/2/15"
        assert by_row[0].issue_type == "date_transposition"
        assert all(i.expected is not None for i in issues)

    def test_silent_on_hospital_shaped_text_columns(self) -> None:
        # Codes, names, phone numbers, zip - no 3-part slash dates.
        df = pd.DataFrame(
            {
                "ProviderNumber": ["10018", "10019", "10020"],
                "City": ["BIRMINGHAM", "DOTHAN", "BOAZ"],
                "PhoneNumber": ["2565938310", "3347938701", "2565938310"],
                "ZipCode": ["35957", "36301", "35957"],
            }
        )
        assert DateTranspositionDetector().detect(df) == []

    def test_silent_on_flights_shaped_time_columns(self) -> None:
        df = pd.DataFrame(
            {
                "sched_dep_time": ["7:10 a.m.", "7:45 p.m.", "6:30 p.m."],
                "act_dep_time": ["7:16 a.m.", "7:58 p.m.", "6:54 a.m."],
            }
        )
        assert DateTranspositionDetector().detect(df) == []

    def test_does_not_fire_when_column_is_not_date_dominant(self) -> None:
        # A single stray slash-date among free text must not turn the column
        # into a date column.
        df = pd.DataFrame({"note": ["ok", "fine", "4/2/15", "good", "great", "done"]})
        assert DateTranspositionDetector().detect(df) == []

    def test_does_not_fire_when_month_position_exceeds_twelve(self) -> None:
        # '3/25/14' is only valid as M/D/YY (25 can't be a month), so it is not a
        # transposition candidate and must not be flagged.
        df = pd.DataFrame({"created": ["3/25/14", "6/18/12", "7/30/11", "9/14/10"]})
        assert DateTranspositionDetector().detect(df) == []


class TestNoRepairerNoWritePath:
    def test_date_transposition_has_no_registered_repairer(self) -> None:
        for allow_llm in (False, True):
            registry = build_repairers(cache_dir=None, allow_llm=allow_llm, model="x")
            assert "date_transposition" not in registry

    def test_apply_mode_never_mutates_and_surfaces_suggestion(self, tmp_path: Path) -> None:
        csv = tmp_path / "dates.csv"
        pd.DataFrame({"created": ["4/2/15", "12/1/06", "1/13/01", "3/9/11"]}).to_csv(
            csv, index=False
        )
        original = csv.read_bytes()

        result = run_repair_pipeline(RepairPipelineRequest(source_path=csv, mode="apply"))

        # By construction (no repairer) nothing is applied: file is byte-identical.
        assert csv.read_bytes() == original
        assert all(fix.detector_id != "date_transposition" for fix in result.fixes)
        # The exact rotation is surfaced as an unverified review suggestion.
        transposition = [
            s for s in result.receipt.suggested_fixes if s.detector_id == "date_transposition"
        ]
        assert transposition, "expected a date_transposition review suggestion"
        assert {s.review_reason for s in transposition} == {"unverified_transposition"}
        first = next(s for s in transposition if s.old_value == "4/2/15")
        assert first.new_value == "2/15/04"

    def test_detection_only_type_not_reported_as_abstention(self, tmp_path: Path) -> None:
        csv = tmp_path / "dates.csv"
        pd.DataFrame({"created": ["4/2/15", "12/1/06", "1/13/01", "3/9/11"]}).to_csv(
            csv, index=False
        )
        result = run_repair_pipeline(RepairPipelineRequest(source_path=csv, mode="dry_run"))
        assert not any(
            "No repairer is registered" in reason for reason in result.receipt.failure_reasons
        )


class TestEnsembleAdditivity:
    def test_detector_is_in_default_ensemble(self) -> None:
        df = pd.DataFrame({"created": ["4/2/15", "12/1/06", "1/13/01", "3/9/11"]})
        issues = run_all_detectors(df)
        assert any(i.issue_type == "date_transposition" for i in issues)
