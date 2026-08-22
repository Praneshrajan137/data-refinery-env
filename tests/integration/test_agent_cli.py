"""CLI-level tests for the selectable agent backends."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app
from tests.support.tables import (
    RepairableTable,
    build_premised_repairable_table,
    build_unpremised_shifted_table,
)

runner = CliRunner()


def _csv(tmp_path: Path) -> Path:
    """The HELD table: detected, never written. Kept for the failure-path test."""
    return build_unpremised_shifted_table(tmp_path / "amounts.csv").csv_path


def _premised(tmp_path: Path) -> RepairableTable:
    """A table whose repair the product stands behind.

    The two success-path tests below assert ``exit_code == 0``, and the ``--agent`` CLI
    branch exits 1 when there are no fixes (``cli/repair.py``: ``code=0 if result.fixes
    else 1``). On the unpremised table the agent now correctly produces zero fixes, so
    those tests were asserting "the CLI ran" via a code path that only ever ran because
    ``decimal_shift`` was auto-appliable.
    """
    return build_premised_repairable_table(tmp_path / "premised.csv")


class TestAgentCli:
    def test_deterministic_policy_runs_offline(self, tmp_path: Path) -> None:
        table = _premised(tmp_path)
        result = runner.invoke(
            app,
            [
                "repair",
                str(table.csv_path),
                "--dry-run",
                "--agent",
                "--policy",
                "deterministic",
                "--schema",
                str(table.schema_path),
            ],
        )
        assert result.exit_code == 0
        assert "Verified Agent Repair" in result.output

    def test_provider_flag_is_accepted(self, tmp_path: Path) -> None:
        # --provider is parsed even though it is ignored for the deterministic policy.
        table = _premised(tmp_path)
        result = runner.invoke(
            app,
            [
                "repair",
                str(table.csv_path),
                "--dry-run",
                "--agent",
                "--policy",
                "deterministic",
                "--provider",
                "gemini",
                "--schema",
                str(table.schema_path),
            ],
        )
        assert result.exit_code == 0

    def test_hosted_without_key_fails_clearly(self, tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        monkeypatch.delenv("AWS_BEARER_TOKEN_BEDROCK", raising=False)
        monkeypatch.delenv("DATAFORGE_LLM_PROVIDER", raising=False)
        result = runner.invoke(
            app, ["repair", str(_csv(tmp_path)), "--dry-run", "--agent", "--policy", "hosted"]
        )
        assert result.exit_code != 0
        assert "API key" in result.output or "Agent repair failed" in result.output
