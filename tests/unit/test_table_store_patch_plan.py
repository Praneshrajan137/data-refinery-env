"""Table-store patch-plan tests for warehouse repair paths."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

from typer.testing import CliRunner

from dataforge.cli import app
from dataforge.stores import PatchPlan, parse_table_store_uri

runner = CliRunner()


def _duckdb_uri(database_path: Path, relation: str = "items") -> str:
    return (
        "warehouse://duckdb?"
        f"database={quote(database_path.as_posix())}&relation={relation}&row_id=id"
    )


def _write_duckdb_table(database_path: Path) -> None:
    import duckdb

    with duckdb.connect(str(database_path)) as connection:
        connection.execute("CREATE TABLE items (id VARCHAR, amount VARCHAR)")
        connection.execute(
            "INSERT INTO items VALUES "
            "('1', '100'), ('2', '105'), ('3', '98'), ('4', '1020'), ('5', '103')"
        )


def _amount_for(database_path: Path, row_id: str) -> str:
    import duckdb

    with duckdb.connect(str(database_path), read_only=True) as connection:
        return str(
            connection.execute("SELECT amount FROM items WHERE id = ?", [row_id]).fetchone()[0]
        )


def test_parse_table_store_uri_keeps_row_identity_columns(tmp_path: Path) -> None:
    uri = _duckdb_uri(tmp_path / "warehouse.duckdb")

    spec = parse_table_store_uri(uri)

    assert spec.backend == "duckdb"
    assert spec.relation == "items"
    assert spec.row_identity_columns == ("id",)


def test_cloud_warehouse_dry_run_emits_non_mutating_patch_plan() -> None:
    uri = "warehouse://snowflake?relation=PUBLIC.CUSTOMERS&row_id=ID"

    result = runner.invoke(app, ["repair", uri, "--dry-run", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    plan = PatchPlan.model_validate(payload["patch_plan"])
    assert plan.backend == "snowflake"
    assert plan.apply_supported is False
    assert plan.operations == ()


def test_duckdb_repair_apply_audit_and_revert_round_trip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    database_path = tmp_path / "warehouse.duckdb"
    _write_duckdb_table(database_path)
    uri = _duckdb_uri(database_path)

    dry_run = runner.invoke(app, ["repair", uri, "--dry-run", "--json"])
    assert dry_run.exit_code == 0
    dry_payload = json.loads(dry_run.output)
    dry_plan = PatchPlan.model_validate(dry_payload["patch_plan"])
    assert dry_plan.backend == "duckdb"
    assert dry_plan.apply_supported is True
    assert len(dry_plan.operations) == 1
    assert _amount_for(database_path, "4") == "1020"

    apply = runner.invoke(app, ["repair", uri, "--apply", "--json"])
    assert apply.exit_code == 0
    apply_payload = json.loads(apply.output)
    txn_id = apply_payload["apply_receipt"]["txn_id"]
    assert txn_id
    assert _amount_for(database_path, "4") == "102"

    audit = runner.invoke(app, ["audit", txn_id, "--search-root", str(tmp_path), "--json"])
    assert audit.exit_code == 0
    assert json.loads(audit.output)["verdict"] == "verified"

    revert = runner.invoke(app, ["revert", txn_id, "--search-root", str(tmp_path), "--json"])
    assert revert.exit_code == 0
    revert_payload = json.loads(revert.output)
    assert revert_payload["source_kind"] == "table_store"
    assert revert_payload["audit_verdict"] == "verified"
    assert _amount_for(database_path, "4") == "1020"
