"""Rollback helpers for table-store transactions."""

from __future__ import annotations

from pathlib import Path

from dataforge.stores.base import TableStoreError
from dataforge.stores.duckdb import load_duckdb_transaction
from dataforge.transactions.log import load_transaction
from dataforge.transactions.txn import RepairTransaction


def revert_table_store_transaction(log_path: Path) -> RepairTransaction:
    """Revert a table-store transaction using its recorded backend patch plan.

    ``DuckDBStore.revert_transaction`` verifies the restored relation against the recorded
    snapshot and raises on any mismatch, so a normal return here means the pre-apply state
    was reproduced -- not merely that some SQL ran. That is what lets
    ``dataforge revert --json`` report a restored digest on this branch instead of the
    ``null`` it printed beside ``ok: true`` until 2026-08-29.
    """
    transaction = load_transaction(log_path)
    if transaction.backend == "duckdb":
        store, loaded = load_duckdb_transaction(log_path)
        store.revert_transaction(loaded, log_path=log_path)
        return load_transaction(log_path)
    raise TableStoreError(
        f"Table-store revert is not implemented for backend {transaction.backend!r}."
    )
