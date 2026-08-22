"""CLI tests for ``dataforge watch``."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app
from tests.support.tables import (
    build_premised_repairable_table,
    build_unpremised_shifted_table,
)

runner = CliRunner()


def _write_repairable_csv(path: Path) -> None:
    """The HELD table: its candidate is detected and never written."""
    build_unpremised_shifted_table(path)


def test_watch_once_profile_json(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    _write_repairable_csv(csv_path)

    result = runner.invoke(app, ["watch", str(csv_path), "--once", "--json"])

    assert result.exit_code == 0
    assert '"event": "profile"' in result.output
    assert '"issues_count": 1' in result.output


def test_watch_once_repair_json_dry_run(tmp_path: Path) -> None:
    """``--action repair`` reports the count of fixes the product stands behind.

    Uses the premised table. On the unpremised ``1020`` table this asserted
    ``fixes_count: 1`` and broke when ``decimal_shift`` left the auto-apply allowlist:
    the count became 0 because the candidate is held, which is correct behaviour that
    this test was never about.
    """
    csv_path = tmp_path / "data.csv"
    table = build_premised_repairable_table(csv_path)

    result = runner.invoke(
        app,
        [
            "watch",
            str(csv_path),
            "--action",
            "repair",
            "--once",
            "--json",
            "--schema",
            str(table.schema_path),
        ],
    )

    assert result.exit_code == 0
    assert '"event": "repair"' in result.output
    assert '"fixes_count": 1' in result.output
    assert not (tmp_path / ".dataforge").exists()


def test_watch_once_repair_reports_zero_fixes_for_a_held_candidate(
    tmp_path: Path,
) -> None:
    """The held path reports zero fixes although the issue WAS detected.

    This pins the disposition the previous version of the test above asserted against by
    accident, so held behaviour is covered deliberately rather than as a side effect of
    which fixture string got pasted. It also documents a known limitation: the repair
    view shows ``fixes_count: 0`` with no sign that a candidate exists -- candidates are
    surfaced in the review queue, not in this summary.
    """
    csv_path = tmp_path / "data.csv"
    _write_repairable_csv(csv_path)

    result = runner.invoke(
        app,
        ["watch", str(csv_path), "--action", "repair", "--once", "--json"],
    )

    assert result.exit_code == 0
    assert '"fixes_count": 0' in result.output
