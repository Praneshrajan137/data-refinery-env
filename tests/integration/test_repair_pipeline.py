"""Integration tests for the Week 3 repair pipeline."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()


def _write_csv(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


class TestRepairPipeline:
    """End-to-end Week 3 gating behavior."""

    def test_pipeline_blocks_pii_overwrite_attempt(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "patients.csv"
        schema_path = tmp_path / "schema.yaml"
        _write_csv(
            csv_path,
            "patient_id,phone_number\n1,2175550101\n2,3125550202\n3,not available\n4,6305551010\n",
        )
        schema_path.write_text(
            "columns:\n  patient_id: str\n  phone_number: str\npii_columns:\n  - phone_number\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )

        assert result.exit_code == 1
        assert "attempted but not fixed" in result.output.lower()
        assert "NO_PII_OVERWRITE" in result.output

    def test_pipeline_blocks_fd_violating_fix_after_retries(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "codes.csv"
        schema_path = tmp_path / "schema.yaml"
        _write_csv(
            csv_path,
            "code,name,state\nA,Alpha,IL\nA,Alpha,IL\nA,Beta,NY\n",
        )
        schema_path.write_text(
            "columns:\n"
            "  code: str\n"
            "  name: str\n"
            "  state: str\n"
            "functional_dependencies:\n"
            "  - determinant: [code]\n"
            "    dependent: name\n"
            "  - determinant: [name]\n"
            "    dependent: state\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )

        assert result.exit_code == 1
        assert "attempted but not fixed" in result.output.lower()
        assert "3" in result.output
        assert "functional dependency" in result.output.lower()

    def test_pipeline_accepts_valid_decimal_shift_fix(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "amounts.csv"
        schema_path = tmp_path / "schema.yaml"
        _write_csv(
            csv_path,
            "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        )
        schema_path.write_text(
            "columns:\n"
            "  id: str\n"
            "  amount: float\n"
            "domain_bounds:\n"
            "  amount:\n"
            "    min: 0\n"
            "    max: 5000\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )

        assert result.exit_code == 0
        assert "Applied 1 fix" in result.output
        assert "1020" not in csv_path.read_text(encoding="utf-8")
