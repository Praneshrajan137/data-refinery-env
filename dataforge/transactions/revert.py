"""Revert an applied DataForge transaction."""

from __future__ import annotations

from pathlib import Path

from dataforge.observability import repair_stage_span
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
    with repair_stage_span("revert"):
        log_path = find_transaction_log(txn_id, search_root=search_root)
        audit_report = verify_transaction_log(txn_id, log_path=log_path)
        # LEGACY_UNVERIFIED is admitted deliberately, and the reason is that revert is a
        # RECOVERY operation rather than a mutation that needs a premise. Refusing a v1 log
        # here would not withhold a write; it would strand a user with a modified file and
        # no way back, which is the failure this whole subsystem exists to prevent.
        #
        # What carries the guarantee on the legacy path is NOT the hash chain -- a v1 log has
        # none -- but the three byte-level checks below: the file must still match
        # ``post_sha256``, the restored bytes must equal ``source_sha256``, and a mismatch
        # rolls the restore back. Those hold identically for v1 and v2.
        #
        # The residual threat is an actor who can rewrite the journal but not the data file.
        # That actor gains nothing: to make a forged ``source_sha256`` pass the post-restore
        # check they must also place a snapshot with those bytes, and anyone who can write the
        # snapshot directory can write the source file directly and skip DataForge entirely.
        #
        # `dataforge audit` still exits 1 on this verdict, because a verifier asked "can you
        # cryptographically verify this" must answer no. That is a different question from
        # "may I restore recorded bytes", so the two commands differ by design, not by
        # oversight. Callers that surface a success message MUST report the verdict rather
        # than an unqualified one -- see ``dataforge/cli/revert.py``.
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
