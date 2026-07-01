"""Unit tests for the missing-value detector and FD-derivable repairer."""

from __future__ import annotations

import pandas as pd

from dataforge.cli.common import load_schema
from dataforge.detectors.missing_value import MissingValueDetector
from dataforge.repairers.missing_value import MissingValueRepairer


def _detect(df: pd.DataFrame, schema=None):  # noqa: ANN001
    return MissingValueDetector().detect(df, schema)


class TestMissingValueDetector:
    def test_flags_missing_in_populated_column(self) -> None:
        df = pd.DataFrame({"city": ["NY", "LA", "", "SF", "BOS", "DC", "LA", "NY"]})
        issues = _detect(df)
        assert [i.row for i in issues] == [2]
        assert issues[0].issue_type == "missing_value"

    def test_flags_sentinels(self) -> None:
        df = pd.DataFrame({"city": ["NY", "LA", "N/A", "SF", "BOS", "DC", "LA", "unknown"]})
        rows = sorted(i.row for i in _detect(df))
        assert rows == [2, 7]

    def test_ignores_sparse_column(self) -> None:
        # Mostly empty -> missingness is legitimate, not flagged.
        df = pd.DataFrame({"note": ["", "", "", "", "", "", "x", "y"]})
        assert _detect(df) == []


class TestMissingValueRepairer:
    def _schema(self, tmp_path):  # noqa: ANN001
        path = tmp_path / "schema.yaml"
        path.write_text(
            "columns:\n  zip: str\n  city: str\n"
            "functional_dependencies:\n  - determinant: [zip]\n    dependent: city\n",
            encoding="utf-8",
        )
        return load_schema(path)

    def test_fills_fd_derivable_value(self, tmp_path) -> None:  # noqa: ANN001
        schema = self._schema(tmp_path)
        # zip 10001 -> NY is known from other rows; the missing one is derivable.
        df = pd.DataFrame(
            {
                "zip": ["10001", "10001", "94105", "10001", "94105", "10001", "94105", "10001"],
                "city": ["NY", "NY", "SF", "", "SF", "NY", "SF", "NY"],
            }
        )
        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert issues, "expected a missing_value issue at row 3"
        fix = MissingValueRepairer().propose(issues[0], df, schema)
        assert fix is not None
        assert fix.fix.new_value == "NY"

    def test_abstains_without_fd(self, tmp_path) -> None:  # noqa: ANN001
        # No schema/FD -> cannot derive -> detection-only (abstain).
        df = pd.DataFrame({"city": ["NY", "LA", "", "SF", "BOS", "DC", "LA", "NY"]})
        issues = [i for i in _detect(df) if i.row == 2]
        assert MissingValueRepairer().propose(issues[0], df, None) is None

    def test_abstains_when_fd_value_ambiguous(self, tmp_path) -> None:  # noqa: ANN001
        schema = self._schema(tmp_path)
        # zip 10001 maps to both NY and NJ -> ambiguous -> abstain.
        df = pd.DataFrame(
            {
                "zip": ["10001", "10001", "10001", "10001", "10001", "10001", "10001", "10001"],
                "city": ["NY", "NJ", "NY", "", "NJ", "NY", "NJ", "NY"],
            }
        )
        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert MissingValueRepairer().propose(issues[0], df, schema) is None
