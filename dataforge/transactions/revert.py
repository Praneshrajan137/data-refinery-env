"""Revert an applied DataForge transaction."""

from __future__ import annotations

from pathlib import Path

from dataforge.transactions.log import (
    append_reverted_event,
    find_transaction_log,
    load_transaction,
    sha256_file,
)
from dataforge.transactions.txn import RepairTransaction


class TransactionRevertError(Exception):
    """Raised when a transaction cannot be safely reverted."""


def revert_transaction(txn_id: str, *, search_root: Path | None = None) -> RepairTransaction:
    """Revert a previously applied transaction by restoring its source snapshot.

    Args:
        txn_id: Canonical transaction identifier.
        search_root: Optional root directory used to locate the transaction log.

    Returns:
        The replayed transaction state after appending the revert event.

    Raises:
        TransactionRevertError: If the transaction is not revertible or hash checks fail.
    """
    log_path = find_transaction_log(txn_id, search_root=search_root)
    transaction = load_transaction(log_path)

    if not transaction.applied or transaction.post_sha256 is None:
        raise TransactionRevertError(
            f"Transaction '{txn_id}' was recorded but never applied, so there is nothing to revert."
        )
    if transaction.reverted_at is not None:
        raise TransactionRevertError(f"Transaction '{txn_id}' has already been reverted.")

    source_path = Path(transaction.source_path)
    snapshot_path = Path(transaction.source_snapshot_path)

    if not source_path.exists():
        raise TransactionRevertError(f"Source file not found: '{source_path}'.")
    if not snapshot_path.exists():
        raise TransactionRevertError(
            f"Source snapshot not found for transaction '{txn_id}': '{snapshot_path}'."
        )

    current_sha256 = sha256_file(source_path)
    if current_sha256 != transaction.post_sha256:
        raise TransactionRevertError(
            "Refusing to revert because the current file no longer matches the recorded "
            "post-state hash. The file may have been edited after apply."
        )

    source_path.write_bytes(snapshot_path.read_bytes())
    reverted_sha256 = sha256_file(source_path)
    if reverted_sha256 != transaction.source_sha256:
        raise TransactionRevertError(
            f"Revert failed integrity verification for transaction '{txn_id}'."
        )

    append_reverted_event(log_path, txn_id)
    return load_transaction(log_path)
