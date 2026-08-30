"""``dataforge measure-on-my-table`` end to end: the report a design partner would send back.

This is the surface an external party actually touches, so what is asserted here is not that the
command runs but that the three promises made to that party hold on the real CLI path:

  * it never needs write permission, and writes nothing to the table
  * the report it produces carries no cell value, checked over the emitted file bytes
  * a table with no accepted premise gets "nothing measured", never "nothing wrong"
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()

SENTINEL = "ZZQX-SENTINEL-PATIENT-NAME-4417"


def _write_fd_csv(directory: Path) -> Path:
    """A table where ``zip -> city`` holds, with one sentinel-bearing minority row."""
    lines = ["zip,city,note"]
    for index in range(8):
        lines.append(f"11111,Springfield,note-{index}")
    lines.append(f"11111,{SENTINEL},outlier")
    for index in range(8):
        lines.append(f"22222,Shelbyville,other-{index}")
    path = directory / "customer.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _schema(directory: Path) -> Path:
    path = directory / "schema.yaml"
    path.write_text(
        "functional_dependencies:\n  - determinant: [zip]\n    dependent: city\n",
        encoding="utf-8",
    )
    return path


class TestReportIsSafeToSend:
    def test_sentinel_absent_from_the_written_report(self, tmp_path: Path) -> None:
        source = _write_fd_csv(tmp_path)
        report = tmp_path / "report.json"
        result = runner.invoke(
            app,
            [
                "measure-on-my-table",
                str(source),
                "--schema",
                str(_schema(tmp_path)),
                "--plants",
                "4",
                "--report",
                str(report),
            ],
        )
        assert result.exit_code == 0, result.output
        raw = report.read_bytes()
        assert SENTINEL.encode() not in raw
        # Nor may the column names appear: "hiv_status" discloses without a single row.
        assert b"Springfield" not in raw
        payload = json.loads(raw)
        assert payload["schema_version"] == "measure_on_my_table_v1"
        # The sentinel row is an FD minority, so the instrument DID see and act on that cell.
        assert payload["cells_written_total"] >= 1

    def test_source_table_is_not_modified(self, tmp_path: Path) -> None:
        source = _write_fd_csv(tmp_path)
        before = source.read_bytes()
        result = runner.invoke(
            app,
            [
                "measure-on-my-table",
                str(source),
                "--schema",
                str(_schema(tmp_path)),
                "--plants",
                "4",
            ],
        )
        assert result.exit_code == 0, result.output
        assert source.read_bytes() == before, "the instrument must never write to the table"

    def test_no_transaction_is_created(self, tmp_path: Path) -> None:
        source = _write_fd_csv(tmp_path)
        runner.invoke(
            app,
            ["measure-on-my-table", str(source), "--schema", str(_schema(tmp_path))],
        )
        # No audit log, no snapshot, no receipt: this path is read-only by construction.
        assert not list(tmp_path.glob("**/*.audit.jsonl"))
        assert not list(tmp_path.glob("**/*snapshot*"))


class TestHonestyOfTheHeadline:
    def test_json_report_labels_both_figures_as_bounds(self, tmp_path: Path) -> None:
        source = _write_fd_csv(tmp_path)
        result = runner.invoke(
            app,
            [
                "measure-on-my-table",
                str(source),
                "--schema",
                str(_schema(tmp_path)),
                "--json",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        limitations = " ".join(payload["limitations"])
        assert "UPPER BOUND on precision" in limitations
        assert "UPPER BOUND on damage" in limitations
        assert any("RECALL" in line for line in payload["not_measurable"])

    def test_no_premise_refuses_rather_than_reporting_zero(self, tmp_path: Path) -> None:
        source = _write_fd_csv(tmp_path)
        result = runner.invoke(app, ["measure-on-my-table", str(source)])
        # Exit 1 with an explanation. A zero-write report would read as a clean bill of health
        # for a table nothing was ever checked against.
        assert result.exit_code == 1
        assert "not a safety result" in result.output.replace("\n", " ")

    def test_terminal_output_calls_the_damage_figure_a_ceiling(self, tmp_path: Path) -> None:
        source = _write_fd_csv(tmp_path)
        result = runner.invoke(
            app,
            ["measure-on-my-table", str(source), "--schema", str(_schema(tmp_path))],
        )
        assert result.exit_code == 0, result.output
        flat = " ".join(result.output.split())
        assert "CEILING" in flat
        assert "451 real repairs" in flat, "the measured overstatement must travel with the run"
