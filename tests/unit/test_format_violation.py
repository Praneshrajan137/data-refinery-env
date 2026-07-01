"""Unit tests for the format-violation detector and canonicalizing repairer."""

from __future__ import annotations

import pandas as pd

from dataforge.detectors.format_violation import FormatViolationDetector, value_shape
from dataforge.repairers.format_violation import FormatViolationRepairer


def _detect(df: pd.DataFrame):
    return FormatViolationDetector().detect(df)


class TestValueShape:
    def test_shapes(self) -> None:
        assert value_shape("2024-01-13") == "9999-99-99"
        assert value_shape("13/01/2024") == "99/99/9999"
        assert value_shape("john@x.com") == "AAAA@A.AAA"


class TestFormatDetector:
    def test_flags_minority_date_shape(self) -> None:
        dates = [f"2024-01-{d:02d}" for d in range(1, 20)] + ["13/01/2024"]
        issues = _detect(pd.DataFrame({"d": dates}))
        assert len(issues) == 1
        assert issues[0].actual == "13/01/2024"
        assert issues[0].issue_type == "format_violation"

    def test_ignores_free_text_columns(self) -> None:
        names = ["John Smith", "Mary Jane", "Bob", "Alice Wong", "Madonna", "X Y Z",
                 "Jean-Luc", "O'Brien", "A B C D", "Sam"]
        # No digit/@ in the dominant shape -> never flagged.
        assert _detect(pd.DataFrame({"name": names})) == []

    def test_ignores_low_dominance(self) -> None:
        mixed = ["2024-01-01", "01/02/2024", "2024-03-03", "04/05/2024",
                 "2024-06-06", "07/08/2024", "2024-09-09", "10/11/2024"]
        # ~50/50 split -> no dominant shape -> no flags.
        assert _detect(pd.DataFrame({"d": mixed})) == []

    def test_flags_zip_missing_leading_zero(self) -> None:
        zips = ["02134"] * 18 + ["2134"]
        issues = _detect(pd.DataFrame({"zip": zips}))
        assert len(issues) == 1
        assert issues[0].actual == "2134"


class TestFormatRepairer:
    def _propose(self, df: pd.DataFrame, row: int, column: str):
        issues = [i for i in _detect(df) if i.row == row and i.column == column]
        assert issues, "expected a format-violation issue at the cell"
        return FormatViolationRepairer().propose(issues[0], df, None)

    def test_reformats_unambiguous_date_to_iso(self) -> None:
        dates = [f"2024-01-{d:02d}" for d in range(1, 20)] + ["25/12/2023"]
        df = pd.DataFrame({"d": dates})
        fix = self._propose(df, 19, "d")
        assert fix is not None
        assert fix.fix.new_value == "2023-12-25"  # DD/MM/YYYY -> ISO

    def test_abstains_on_ambiguous_date(self) -> None:
        dates = [f"2024-01-{d:02d}" for d in range(1, 20)] + ["02/03/2024"]
        df = pd.DataFrame({"d": dates})
        # 02/03/2024 parses as both Feb-3 and Mar-2 -> ambiguous -> abstain.
        fix = self._propose(df, 19, "d")
        assert fix is None

    def test_zero_pads_fixed_width_code(self) -> None:
        zips = ["02134"] * 18 + ["2134"]
        df = pd.DataFrame({"zip": zips})
        fix = self._propose(df, 18, "zip")
        assert fix is not None
        assert fix.fix.new_value == "02134"
