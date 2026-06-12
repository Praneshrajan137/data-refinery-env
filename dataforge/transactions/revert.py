"""Revert an applied DataForge transaction."""

from __future__ import annotations

from pathlib import Path

from dataforge.transactions.files import (
    SourceLockError,
    atomic_write_bytes,
    source_path_lock,
)
from dataforge.transactions.log import (
    TransactionAuditVerdict,
    append_reverted_event,
    find_transaction_log,
    load_transaction,
    sha256_file,
    verify_transaction_log,
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
    audit_report = verify_transaction_log(txn_id, log_path=log_path)
    if audit_report.verdict not in {
        TransactionAuditVerdict.VERIFIED,
        TransactionAuditVerdict.LEGACY_UNVERIFIED,
    }:
        details = "; ".join(audit_report.errors) or audit_report.verdict.value
        raise TransactionRevertError(
            f"Refusing to revert because transaction audit verification failed: {details}"
        )
    transaction = load_transaction(log_path)

    if not transaction.applied or transaction.post_sha256 is None:
        raise TransactionRevertError(
            f"Transaction '{txn_id}' was recorded but never applied, so there is nothing to revert."
        )
    if transaction.reverted_at is not None:
        raise TransactionRevertError(f"Transaction '{txn_id}' has already been reverted.")

    if transaction.source_kind == "table_store":
        try:
            from dataforge.stores.revert import revert_table_store_transaction

            return revert_table_store_transaction(log_path)
        except Exception as exc:
            raise TransactionRevertError(str(exc)) from exc

    source_path = Path(transaction.source_path)
    snapshot_path = Path(transaction.source_snapshot_path)

    if not source_path.exists():
        raise TransactionRevertError(f"Source file not found: '{source_path}'.")
    if not snapshot_path.exists():
        raise TransactionRevertError(
            f"Source snapshot not found for transaction '{txn_id}': '{snapshot_path}'."
        )

    try:
        with source_path_lock(source_path):
            current_bytes = source_path.read_bytes()
            current_sha256 = sha256_file(source_path)
            if current_sha256 != transaction.post_sha256:
                raise TransactionRevertError(
                    "Refusing to revert because the current file no longer matches the recorded "
                    "post-state hash. The file may have been edited after apply."
                )

            atomic_write_bytes(source_path, snapshot_path.read_bytes())
            reverted_sha256 = sha256_file(source_path)
            if reverted_sha256 != transaction.source_sha256:
                atomic_write_bytes(source_path, current_bytes)
                raise TransactionRevertError(
                    f"Revert failed integrity verification for transaction '{txn_id}'."
                )

            try:
                append_reverted_event(log_path, txn_id)
            except Exception:
                atomic_write_bytes(source_path, current_bytes)
                raise
    except SourceLockError as exc:
        raise TransactionRevertError(str(exc)) from exc
    return load_transaction(log_path)
