"""CLI test for the zero-config quickstart command."""

from __future__ import annotations

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()


class TestQuickstart:
    def test_quickstart_runs_on_packaged_data(self) -> None:
        # Runs from packaged fixtures (no working-dir files) -> exit 0, fast.
        result = runner.invoke(app, ["quickstart"])
        assert result.exit_code == 0
        assert "DataForge Quickstart" in result.output
        assert "verified, reversible repair" in result.output

    def test_quickstart_is_listed_in_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert "quickstart" in result.output
