"""CLI-level tests for the selectable agent backends."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()


def _csv(tmp_path: Path) -> Path:
    path = tmp_path / "amounts.csv"
    path.write_text("id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n", encoding="utf-8")
    return path


class TestAgentCli:
    def test_deterministic_policy_runs_offline(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app,
            ["repair", str(_csv(tmp_path)), "--dry-run", "--agent", "--policy", "deterministic"],
        )
        assert result.exit_code == 0
        assert "Verified Agent Repair" in result.output

    def test_provider_flag_is_accepted(self, tmp_path: Path) -> None:
        # --provider is parsed even though it is ignored for the deterministic policy.
        result = runner.invoke(
            app,
            [
                "repair",
                str(_csv(tmp_path)),
                "--dry-run",
                "--agent",
                "--policy",
                "deterministic",
                "--provider",
                "gemini",
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
