"""The DuckDB revert path must verify that it restored, not merely that SQL ran.

This surface had **no test at all**. Grepping the suite for the table store found only
plan-level unit tests (``test_table_store_proven_gate.py``,
``test_warehouse_allowlist_gate.py``, ``test_table_store_patch_plan.py``) and a static
registry scan (``test_surface_uniformity.py``). Nothing exercised
``DuckDBStore.revert_transaction`` against a live database, which is why three checks the
CSV path has had all along could be absent without any gate noticing.

The CSV revert (``dataforge/transactions/revert.py``) verifies the current file against
``post_sha256`` before touching it, and verifies the restored bytes against
``source_sha256`` after. The DuckDB revert fired ``plan.rollback_sql`` blind. Verified
against DuckDB directly: an ``UPDATE`` whose ``WHERE`` matches nothing returns ``[(0,)]``,
so a rollback that changed nothing was indistinguishable from one that worked, and
``append_reverted_event`` recorded it as done either way.

The committed symptom is ``docs/evidence/dbt_duckdb/commands.log``: ``"ok": true`` and
``"audit_verdict": "verified"`` printed beside ``"restored_source_sha256": null``.

The snapshot needed to close the gap was already written and fsynced on every apply and
never read by anything -- ``transactions/revert.py`` returns for
``source_kind == "table_store"`` eight lines before its only reader. So the missing
verification and the unread full-table snapshot were one defect from two ends.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.detectors.base import Schema
from dataforge.repairers.base import ProposedFix
from dataforge.stores.base import TableStoreError
from dataforge.stores.duckdb import DuckDBStore, load_duckdb_transaction
from dataforge.transactions.log import find_transaction_log, load_transaction
from dataforge.transactions.txn import CellFix

pytest.importorskip("duckdb")

_SCHEMA = Schema(
    columns={"id": "int", "state": "str", "city": "str"},
    functional_dependencies=[],
)


def _fix(new_value: str = "boston") -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=1,
            column="city",
            old_value="bostonn",
            new_value=new_value,
            detector_id="fd_violation",
        ),
        reason="declared dependency state -> city",
        confidence=1.0,
        provenance="deterministic",
    )


def _store(tmp_path: Path) -> DuckDBStore:
    import duckdb

    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE items(id INTEGER, state VARCHAR, city VARCHAR)")
        connection.execute("INSERT INTO items VALUES (1,'MA','boston'),(2,'MA','bostonn')")
    return DuckDBStore(
        database_path=database,
        relation="items",
        row_identity_columns=("id",),
    )


def _apply(tmp_path: Path, store: DuckDBStore, fix: ProposedFix) -> tuple[Path, str]:
    """Apply one fix and return the journal path and txn id."""
    plan = store.build_patch_plan([fix], schema=_SCHEMA, safety_verdict="allow")
    receipt = store.apply_patch_plan(plan, state_root=tmp_path)
    return find_transaction_log(receipt.txn_id, search_root=tmp_path), receipt.txn_id


def _fix_row_two() -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=1,
            column="city",
            old_value="bostonn",
            new_value="boston",
            detector_id="fd_violation",
        ),
        reason="declared dependency state -> city",
        confidence=1.0,
        provenance="deterministic",
    )


class TestRevertRestoresAndVerifies:
    def test_a_clean_revert_restores_the_original_rows(self, tmp_path: Path) -> None:
        """Non-vacuity. Without this, a revert that refuses everything looks correct."""
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())

        with store._connect(read_only=True) as connection:
            after_apply = store._relation_rows(connection)
        assert after_apply[1]["city"] == "boston", "the apply must have written something"

        store.revert_transaction(load_transaction(log_path), log_path=log_path)

        with store._connect(read_only=True) as connection:
            after_revert = store._relation_rows(connection)
        assert after_revert[1]["city"] == "bostonn"

    def test_revert_returns_the_restored_digest(self, tmp_path: Path) -> None:
        """The value ``dataforge revert --json`` reports must be computed, not assumed."""
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        transaction = load_transaction(log_path)

        restored = store.revert_transaction(transaction, log_path=log_path)

        assert restored == transaction.source_sha256

    def test_the_journal_records_the_revert(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())

        store.revert_transaction(load_transaction(log_path), log_path=log_path)

        assert load_transaction(log_path).reverted_at is not None


class TestRevertRefusesWhenTheCellMovedUnderIt:
    """The decisive case: the rollback's WHERE clause no longer describes reality."""

    def test_a_third_party_write_makes_revert_refuse(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())

        # Somebody else edits the cell we are about to roll back.
        with store._connect(read_only=False) as connection:
            connection.execute("UPDATE items SET city='cambridge' WHERE id=2")

        with pytest.raises(TableStoreError, match="no longer matches the recorded post-state"):
            store.revert_transaction(load_transaction(log_path), log_path=log_path)

    def test_a_refused_revert_leaves_the_relation_untouched(self, tmp_path: Path) -> None:
        """A refusal must not half-apply the rollback."""
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        with store._connect(read_only=False) as connection:
            connection.execute("UPDATE items SET city='cambridge' WHERE id=2")

        with pytest.raises(TableStoreError):
            store.revert_transaction(load_transaction(log_path), log_path=log_path)

        with store._connect(read_only=True) as connection:
            rows = store._relation_rows(connection)
        assert rows[1]["city"] == "cambridge"

    def test_a_refused_revert_is_not_journaled_as_reverted(self, tmp_path: Path) -> None:
        """The defect that made the gap invisible: success was recorded regardless."""
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        with store._connect(read_only=False) as connection:
            connection.execute("UPDATE items SET city='cambridge' WHERE id=2")

        with pytest.raises(TableStoreError):
            store.revert_transaction(load_transaction(log_path), log_path=log_path)

        assert load_transaction(log_path).reverted_at is None


class TestRollbackRowsChangedIsChecked:
    """A rollback statement must change exactly the one row it claims to.

    Honest scope: with the pre-revert post-state check in place, this guard is
    **defence in depth rather than independently reachable** through the shipped path.
    Any third-party edit to the relation moves ``post_sha256`` and is refused earlier, and
    a tampered journal fails ``verify_transaction_log`` before ``transactions/revert.py``
    dispatches here. Apply also refuses when its own verification query matches other than
    one row, which rules out non-unique row identity arriving through a normal apply.

    It is kept because the cost is one integer comparison and the failure it covers is
    silent: DuckDB returns ``[(0,)]`` for an ``UPDATE`` that matches nothing, so before
    2026-08-29 a rollback that changed no rows and one that worked were indistinguishable,
    and ``append_reverted_event`` recorded both as done. The paired mutant
    (``M22``) removes this check *together with* the post-state check, because removing
    either alone leaves the suite green for a legitimate reason.
    """

    def test_dml_rows_changed_reports_zero_one_and_many(self, tmp_path: Path) -> None:
        """Pin the DuckDB semantics this guard depends on.

        ``cursor.rowcount`` is ``-1`` on DuckDB and unusable; the changed count arrives as
        the statement's single result value. If a DuckDB upgrade changed that, the guard
        would silently compare the wrong number, so the contract is pinned here rather
        than assumed from documentation.
        """
        store = _store(tmp_path)

        with store._connect(read_only=False) as connection:
            assert (
                store._execute_dml_rows_changed(
                    connection, "UPDATE items SET city='x' WHERE id=999"
                )
                == 0
            )
            assert (
                store._execute_dml_rows_changed(connection, "UPDATE items SET city='y' WHERE id=1")
                == 1
            )
            assert (
                store._execute_dml_rows_changed(
                    connection, "UPDATE items SET city='z' WHERE state='MA'"
                )
                == 2
            )

    def test_a_tampered_rollback_statement_is_refused(self, tmp_path: Path) -> None:
        """Editing the journaled plan is refused -- by the snapshot's plan binding.

        Asserting the refusal rather than which message produced it is deliberate: the
        product's guarantee is "this revert does not proceed", and pinning the specific
        gate here would make the test fail on any future reordering that is still correct.
        """
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        transaction = load_transaction(log_path)

        assert transaction.patch_plan is not None
        plan = dict(transaction.patch_plan)
        plan["rollback_sql"] = ["UPDATE items SET city='bostonn' WHERE id=999"]
        tampered = transaction.model_copy(update={"patch_plan": plan})

        with pytest.raises(TableStoreError):
            store.revert_transaction(tampered, log_path=log_path)

        with store._connect(read_only=True) as connection:
            assert store._relation_rows(connection)[1]["city"] == "boston", (
                "a refused revert must not have run any rollback statement"
            )


class TestRevertRequiresATrustworthySnapshot:
    """The snapshot is the only record of the pre-apply state."""

    def _snapshot(self, tmp_path: Path, log_path: Path) -> Path:
        return Path(str(load_transaction(log_path).source_snapshot_path))

    def test_a_missing_snapshot_is_refused(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        self._snapshot(tmp_path, log_path).unlink()

        with pytest.raises(TableStoreError, match="does not exist"):
            store.revert_transaction(load_transaction(log_path), log_path=log_path)

    def test_a_tampered_snapshot_is_refused(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        snapshot = self._snapshot(tmp_path, log_path)
        payload = json.loads(snapshot.read_text(encoding="utf-8"))
        payload["rows"][1]["city"] = "worcester"
        snapshot.write_text(json.dumps(payload), encoding="utf-8")

        with pytest.raises(TableStoreError, match="does not match the recorded source digest"):
            store.revert_transaction(load_transaction(log_path), log_path=log_path)

    def test_a_snapshot_for_another_plan_is_refused(self, tmp_path: Path) -> None:
        """A snapshot is bound to the plan it was taken for, like the CSV bytes check."""
        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        transaction = load_transaction(log_path)
        snapshot = Path(str(transaction.source_snapshot_path))
        payload = json.loads(snapshot.read_text(encoding="utf-8"))
        payload["patch_plan_sha256"] = "0" * 64
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        snapshot.write_bytes(raw)

        from dataforge.transactions.log import sha256_bytes

        moved = transaction.model_copy(update={"source_sha256": sha256_bytes(raw)})

        with pytest.raises(TableStoreError, match="different patch plan"):
            store.revert_transaction(moved, log_path=log_path)


class TestRevertGoesThroughTheHelper:
    """``revert_table_store_transaction`` must inherit the verification, not bypass it."""

    def test_the_helper_reverts_and_reloads(self, tmp_path: Path) -> None:
        from dataforge.stores.revert import revert_table_store_transaction

        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())

        reverted = revert_table_store_transaction(log_path)

        assert reverted.reverted_at is not None
        loaded_store, _ = load_duckdb_transaction(log_path)
        with loaded_store._connect(read_only=True) as connection:
            assert loaded_store._relation_rows(connection)[1]["city"] == "bostonn"

    def test_the_helper_refuses_when_the_cell_moved(self, tmp_path: Path) -> None:
        from dataforge.stores.revert import revert_table_store_transaction

        store = _store(tmp_path)
        log_path, _ = _apply(tmp_path, store, _fix_row_two())
        with store._connect(read_only=False) as connection:
            connection.execute("UPDATE items SET city='cambridge' WHERE id=2")

        with pytest.raises(TableStoreError):
            revert_table_store_transaction(log_path)
