"""Table-store patch-plan tests for warehouse repair paths."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

import pytest
from typer.testing import CliRunner

from dataforge.cli import app
from dataforge.stores import PatchPlan, parse_table_store_uri
from dataforge.stores.csv import CSVStore
from dataforge.stores.sql import ensure_safe_relation, quote_identifier, sql_literal

runner = CliRunner()


def _duckdb_uri(database_path: Path, relation: str = "items") -> str:
    return (
        "warehouse://duckdb?"
        f"database={quote(database_path.as_posix())}&relation={relation}&row_id=id"
    )


def _write_duckdb_table(database_path: Path) -> None:
    """Create a relation whose single repair is constraint-checkable.

    Was ``items(id, amount)`` with a ``1020`` decimal shift. That repair is now held on
    every write surface, which emptied the patch plan and collapsed this whole round trip
    -- including its pre- and post-revert assertions, which then agreed trivially because
    nothing had ever changed. A declared FD ``state -> city`` gives a real SQL UPDATE to
    apply, audit, and revert.
    """
    import duckdb

    rows = ", ".join(f"('{i}', 'MA', 'boston')" for i in range(1, 10))
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("CREATE TABLE items (id VARCHAR, state VARCHAR, city VARCHAR)")
        connection.execute(f"INSERT INTO items VALUES {rows}, ('10', 'MA', 'bostonn')")


def _write_duckdb_schema(schema_path: Path) -> Path:
    """The DECLARED premise that makes the city repair checkable."""
    schema_path.write_text(
        "columns:\n  id: string\n  state: string\n  city: string\n"
        "functional_dependencies:\n  - determinant: [state]\n    dependent: city\n",
        encoding="utf-8",
    )
    return schema_path


def _city_for(database_path: Path, row_id: str) -> str:
    import duckdb

    with duckdb.connect(str(database_path), read_only=True) as connection:
        return str(
            connection.execute("SELECT city FROM items WHERE id = ?", [row_id]).fetchone()[0]
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


def test_sql_helpers_reject_unsafe_identifiers_and_escape_literals() -> None:
    assert ensure_safe_relation("main.customers") == "main.customers"
    assert quote_identifier("amount") == '"amount"'
    assert sql_literal("Bob's") == "'Bob''s'"

    with pytest.raises(ValueError, match="Unsafe relation"):
        ensure_safe_relation("main.customers; drop table customers")
    with pytest.raises(ValueError, match="Unsafe column"):
        quote_identifier("amount;drop")


def test_csv_store_apply_requires_transaction_inputs(tmp_path: Path) -> None:
    csv_path = tmp_path / "amounts.csv"
    csv_path.write_text("id,amount\n1,100\n", encoding="utf-8")
    store = CSVStore(csv_path)
    plan = store.build_patch_plan([], schema=None, safety_verdict="allow")

    with pytest.raises(ValueError, match="requires source bytes and fixes"):
        store.apply_patch_plan(plan)


def test_duckdb_repair_apply_audit_and_revert_round_trip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    database_path = tmp_path / "warehouse.duckdb"
    _write_duckdb_table(database_path)
    uri = _duckdb_uri(database_path)
    schema_path = _write_duckdb_schema(tmp_path / "items.schema.yaml")

    dry_run = runner.invoke(
        app, ["repair", uri, "--dry-run", "--json", "--schema", str(schema_path)]
    )
    assert dry_run.exit_code == 0
    dry_payload = json.loads(dry_run.output)
    dry_plan = PatchPlan.model_validate(dry_payload["patch_plan"])
    assert dry_plan.backend == "duckdb"
    assert dry_plan.apply_supported is True
    assert len(dry_plan.operations) == 1
    assert PatchPlan.model_validate_json(dry_plan.canonical_json()).sha256() == dry_plan.sha256()
    assert _city_for(database_path, "10") == "bostonn"

    apply = runner.invoke(app, ["repair", uri, "--apply", "--json", "--schema", str(schema_path)])
    assert apply.exit_code == 0
    apply_payload = json.loads(apply.output)
    txn_id = apply_payload["apply_receipt"]["txn_id"]
    assert txn_id
    assert _city_for(database_path, "10") == "boston"

    audit = runner.invoke(app, ["audit", txn_id, "--search-root", str(tmp_path), "--json"])
    assert audit.exit_code == 0
    assert json.loads(audit.output)["verdict"] == "verified"

    revert = runner.invoke(app, ["revert", txn_id, "--search-root", str(tmp_path), "--json"])
    assert revert.exit_code == 0
    revert_payload = json.loads(revert.output)
    assert revert_payload["source_kind"] == "table_store"
    assert revert_payload["audit_verdict"] == "verified"
    assert _city_for(database_path, "10") == "bostonn"
