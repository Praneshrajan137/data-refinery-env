"""CLI test for the quickstart command.

The assertions here are on the **count**, not on the sentence. Until 2026-08-25 this file
asserted only ``"verified, reversible repair" in result.output`` -- a sentence the command
prints whether the count is 7 or 0. When ``type_mismatch`` left the calibration-bypass
allowlist the demo silently began reporting zero repairs, and neither this test nor the
persona test noticed, because both were checking that a string was printed rather than that
anything worked.
"""

from __future__ import annotations

import re

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()

#: The count is rendered inside a Rich panel, which wraps and pads, so the digits are read
#: back with a tolerant pattern rather than by matching the whole sentence.
_REPAIR_COUNT = re.compile(r"(\d+)\s+have a verified, reversible repair")


def repair_count(output: str) -> int:
    """Return the repair count the quickstart panel reported."""
    match = _REPAIR_COUNT.search(output)
    assert match is not None, (
        f"quickstart did not report a repair count at all; output was:\n{output}"
    )
    return int(match.group(1))


class TestQuickstart:
    def test_quickstart_runs_on_packaged_data(self) -> None:
        # Runs from packaged fixtures (no working-dir files) -> exit 0, fast.
        result = runner.invoke(app, ["quickstart"])
        assert result.exit_code == 0
        assert "DataForge Quickstart" in result.output

    def test_quickstart_reports_at_least_one_repair(self) -> None:
        """The number, not the sentence. A demo that repairs nothing demonstrates nothing.

        Do NOT weaken this back to a substring check. If it fails, either the packaged
        fixture stopped earning a repair or a detector lost its write path -- both are
        findings to investigate, not assertions to relax.
        """
        result = runner.invoke(app, ["quickstart"])
        assert result.exit_code == 0
        assert repair_count(result.output) >= 1, (
            "the quickstart demo reported zero repairs while still printing that every "
            "fix passed an SMT proof; that is the exact failure this assertion exists to "
            "catch"
        )

    def test_quickstart_detects_more_than_it_repairs(self) -> None:
        """Honesty check: the demo must not imply everything detected is fixable.

        The product's whole position is that it repairs a narrow, provable subset and flags
        the rest. A fixture where every issue is repaired would misrepresent that.
        """
        result = runner.invoke(app, ["quickstart"])
        detected = re.search(r"Detected\s+(\d+)\s+data-quality issue", result.output)
        assert detected is not None
        assert int(detected.group(1)) > repair_count(result.output)

    def test_quickstart_states_that_a_premise_is_required(self) -> None:
        """Nothing writes without a declared premise, so the demo must say so."""
        result = runner.invoke(app, ["quickstart"])
        assert "declared premise" in result.output

    def test_quickstart_is_listed_in_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert "quickstart" in result.output
