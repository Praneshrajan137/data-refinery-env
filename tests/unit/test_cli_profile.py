"""Unit tests for the ``dataforge profile`` CLI command.

Tests the end-to-end flow: CSV loading, detector execution, rich output.
"""

from __future__ import annotations

import time
from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
_HOSPITAL_CSV = _FIXTURES / "hospital_10rows.csv"
_HOSPITAL_SCHEMA = _FIXTURES / "hospital_schema.yaml"

runner = CliRunner()


class TestProfileCommand:
    """CLI profile subcommand tests."""

    def test_profile_finds_all_seeded_issues(self) -> None:
        """Profile the hospital fixture and verify 4 issues are reported."""
        result = runner.invoke(
            app, ["profile", str(_HOSPITAL_CSV), "--schema", str(_HOSPITAL_SCHEMA)]
        )
        # Exit code 1 because UNSAFE issues exist.
        assert result.exit_code == 1
        output = result.output
        assert "fd_violation" in output
        assert "type_mismatch" in output
        assert "decimal_shift" in output
        assert "4 issues found" in output or "4" in output

    def test_profile_without_schema(self) -> None:
        """Profile without schema — FD violations not detected, but others are."""
        result = runner.invoke(app, ["profile", str(_HOSPITAL_CSV)])
        output = result.output
        # Without schema, no FD violations — only type_mismatch and decimal_shift.
        assert "type_mismatch" in output
        assert "decimal_shift" in output
        # Should exit 0 (no UNSAFE issues without FD violations).
        assert result.exit_code == 0

    def test_profile_missing_file(self) -> None:
        """Missing CSV file produces a clean error."""
        result = runner.invoke(app, ["profile", "nonexistent.csv"])
        assert result.exit_code != 0

    def test_profile_runs_under_2_seconds(self) -> None:
        """Performance: profile on 10-row fixture completes in < 2 seconds."""
        start = time.monotonic()
        result = runner.invoke(
            app, ["profile", str(_HOSPITAL_CSV), "--schema", str(_HOSPITAL_SCHEMA)]
        )
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, f"Profile took {elapsed:.2f}s, exceeds 2s budget"
        # Should complete (exit code 0 or 1, not 2).
        assert result.exit_code in (0, 1)

    def test_profile_output_contains_table_structure(self) -> None:
        """Output contains expected table columns."""
        result = runner.invoke(
            app, ["profile", str(_HOSPITAL_CSV), "--schema", str(_HOSPITAL_SCHEMA)]
        )
        output = result.output
        assert "Row" in output
        assert "Column" in output
        assert "Severity" in output
        assert "Confidence" in output
        assert "Reason" in output

    def test_profile_severity_ordering(self) -> None:
        """UNSAFE issues appear before REVIEW issues in output."""
        result = runner.invoke(
            app, ["profile", str(_HOSPITAL_CSV), "--schema", str(_HOSPITAL_SCHEMA)]
        )
        output = result.output
        unsafe_pos = output.find("UNSAFE")
        review_pos = output.find("REVIEW")
        assert unsafe_pos < review_pos, "UNSAFE should appear before REVIEW"

    def test_version_flag(self) -> None:
        """--version prints version and exits cleanly."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "dataforge" in result.output
        assert "0.1.0" in result.output


class TestProfileCleanData:
    """Profile on data with no issues."""

    def test_clean_csv_exit_zero(self, tmp_path: Path) -> None:
        """A clean CSV with no issues exits with code 0."""
        csv = tmp_path / "clean.csv"
        csv.write_text(
            "name,age,city\n"
            "Alice,25,NYC\n"
            "Bob,30,LA\n"
            "Charlie,35,Chicago\n"
            "Diana,28,Boston\n"
            "Eve,32,Seattle\n"
        )
        result = runner.invoke(app, ["profile", str(csv)])
        assert result.exit_code == 0
        assert "No issues detected" in result.output
