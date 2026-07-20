"""CLI tests for the ``dataforge verify-apply`` external-fix command."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dataforge.cli import app

runner = CliRunner()


def _write_csv(path: Path) -> None:
    path.write_text("id,score\n1,10\n2,20\n3,30\n", encoding="utf-8")


def _write_schema(path: Path) -> None:
    path.write_text('{"columns": {"id": "str", "score": "float"}}', encoding="utf-8")


def _write_fixes(path: Path, fixes: list[dict[str, object]]) -> None:
    path.write_text(json.dumps(fixes), encoding="utf-8")


def test_verify_apply_is_registered() -> None:
    result = runner.invoke(app, ["verify-apply", "--help"])
    assert result.exit_code == 0
    assert "--fixes" in result.stdout
    assert "--confirm-escalations" in result.stdout


def test_verify_apply_dry_run_does_not_mutate(tmp_path: Path) -> None:
    csv_path = tmp_path / "t.csv"
    schema_path = tmp_path / "s.json"
    fixes_path = tmp_path / "fixes.json"
    _write_csv(csv_path)
    _write_schema(schema_path)
    _write_fixes(fixes_path, [{"row": 0, "column": "score", "new_value": "15"}])
    original = csv_path.read_bytes()

    result = runner.invoke(
        app,
        [
            "verify-apply",
            str(csv_path),
            "--fixes",
            str(fixes_path),
            "--schema",
            str(schema_path),
            "--dry-run",
            "--confirm-escalations",
            "--json",
        ],
    )
    payload = json.loads(result.stdout)
    assert payload["receipt"]["applied"] is False
    assert csv_path.read_bytes() == original


def test_verify_apply_schema_proven_applies(tmp_path: Path) -> None:
    csv_path = tmp_path / "t.csv"
    schema_path = tmp_path / "s.json"
    fixes_path = tmp_path / "fixes.json"
    _write_csv(csv_path)
    _write_schema(schema_path)
    _write_fixes(fixes_path, [{"row": 0, "column": "score", "new_value": "15"}])

    result = runner.invoke(
        app,
        [
            "verify-apply",
            str(csv_path),
            "--fixes",
            str(fixes_path),
            "--schema",
            str(schema_path),
            "--apply",
            "--confirm-escalations",
            "--proposer",
            "agent-cli",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["receipt"]["applied"] is True
    assert payload["receipt"]["txn_id"]
    assert "15" in csv_path.read_text(encoding="utf-8")


def test_verify_apply_without_schema_is_held(tmp_path: Path) -> None:
    csv_path = tmp_path / "t.csv"
    fixes_path = tmp_path / "fixes.json"
    _write_csv(csv_path)
    _write_fixes(fixes_path, [{"row": 0, "column": "score", "new_value": "15"}])
    original = csv_path.read_bytes()

    result = runner.invoke(
        app,
        [
            "verify-apply",
            str(csv_path),
            "--fixes",
            str(fixes_path),
            "--apply",
            "--confirm-escalations",
            "--json",
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["receipt"]["applied"] is False
    assert csv_path.read_bytes() == original
    assert "floor_cannot_verify" in {
        s["review_reason"] for s in payload["receipt"]["suggested_fixes"]
    }


def test_verify_apply_compare_and_set_rejects_stale(tmp_path: Path) -> None:
    csv_path = tmp_path / "t.csv"
    schema_path = tmp_path / "s.json"
    fixes_path = tmp_path / "fixes.json"
    _write_csv(csv_path)
    _write_schema(schema_path)
    _write_fixes(
        fixes_path,
        [{"row": 1, "column": "score", "new_value": "99", "expected_old_value": "WRONG"}],
    )

    result = runner.invoke(
        app,
        [
            "verify-apply",
            str(csv_path),
            "--fixes",
            str(fixes_path),
            "--schema",
            str(schema_path),
            "--apply",
            "--confirm-escalations",
            "--json",
        ],
    )
    payload = json.loads(result.stdout)
    assert payload["receipt"]["applied"] is False
    assert "stale_precondition" in {
        s["review_reason"] for s in payload["receipt"]["suggested_fixes"]
    }


def test_verify_apply_requires_exactly_one_mode(tmp_path: Path) -> None:
    csv_path = tmp_path / "t.csv"
    fixes_path = tmp_path / "fixes.json"
    _write_csv(csv_path)
    _write_fixes(fixes_path, [{"row": 0, "column": "score", "new_value": "15"}])

    result = runner.invoke(app, ["verify-apply", str(csv_path), "--fixes", str(fixes_path)])
    assert result.exit_code == 2
