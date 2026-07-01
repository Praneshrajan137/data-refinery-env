"""BDD-style persona acceptance tests.

These encode the three design personas from the project brief as executable
acceptance criteria, so "the outcomes work" is verified, not asserted:

- Priya (data engineer): `dataforge profile` returns readable output fast.
- Marcus (staff engineer): a fresh user can detect+preview repairs in seconds
  with zero setup, from packaged data.
- Shreya (applied-AI PM): the repo states honest coverage and records decisions.
"""

from __future__ import annotations

import time
from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()
_REPO_ROOT = Path(__file__).resolve().parents[2]


class TestPriyaProfilesQuickly:
    """Priya runs profile on a problematic file and expects fast, readable output."""

    def test_profile_is_fast_and_readable(self, tmp_path: Path) -> None:
        csv = tmp_path / "model.csv"
        csv.write_text(
            "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n6,99\n7,101\n8,97\n",
            encoding="utf-8",
        )
        started = time.perf_counter()
        result = runner.invoke(app, ["profile", str(csv)])
        elapsed = time.perf_counter() - started
        assert result.exit_code in (0, 1)  # 0 = clean, 1 = issues found
        assert elapsed < 5.0, "profile must return in under 5 seconds"
        assert result.output.strip(), "profile must produce human-readable output"


class TestMarcusGetsValueInSeconds:
    """Marcus wants pip-install-then-run value with zero setup."""

    def test_quickstart_zero_config(self) -> None:
        started = time.perf_counter()
        result = runner.invoke(app, ["quickstart"])
        elapsed = time.perf_counter() - started
        assert result.exit_code == 0
        assert elapsed < 10.0
        assert "verified, reversible repair" in result.output


class TestShreyaReadsHonestEvidence:
    """Shreya evaluates the repo's honesty and decision rationale."""

    def test_readme_has_honest_coverage_table(self) -> None:
        readme = (_REPO_ROOT / "README.md").read_text(encoding="utf-8")
        assert "Coverage: what DataForge can and cannot safely fix" in readme
        assert "Detection" in readme and "Correction" in readme

    def test_decisions_log_records_repositioning(self) -> None:
        decisions = (_REPO_ROOT / "DECISIONS.md").read_text(encoding="utf-8")
        assert "verified+calibrated repair" in decisions or "honest coverage" in decisions

    def test_coverage_floors_committed(self) -> None:
        floors = _REPO_ROOT / "eval" / "thresholds" / "coverage_floors.json"
        assert floors.is_file()
