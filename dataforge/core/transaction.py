"""Transaction management for audit trail and reversible repairs."""

import uuid
from datetime import datetime
from typing import Any, Optional

from dataforge.models import Transaction, TransactionLog, TransactionOperation


class TransactionManager:
    """Manages transaction recording and reversal."""

    def __init__(self):
        """Initialize transaction manager."""
        self.log = TransactionLog()

    def create_transaction(
        self,
        operation: TransactionOperation,
        file_path: str,
        changes: list[tuple[int, str, Any, Any]],
        user: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Transaction:
        """
        Create and record a transaction.

        Args:
            operation: Type of operation
            file_path: CSV file affected
            changes: List of (row, column, old_value, new_value) tuples
            user: User who initiated operation
            details: Operation-specific details
            metadata: Additional metadata

        Returns:
            Created Transaction object
        """
        transaction = Transaction(
            id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            operation=operation,
            file_path=file_path,
            user=user,
            details=details or {},
            changes=changes,
            metadata=metadata or {},
        )

        self.log.add_transaction(transaction)
        return transaction

    def get_transaction(self, txn_id: str) -> Optional[Transaction]:
        """Get transaction by ID."""
        return self.log.get_by_id(txn_id)

    def get_recent_transactions(self, count: int = 10) -> list[Transaction]:
        """Get most recent transactions."""
        return self.log.get_recent(count)

    def get_transactions_by_operation(
        self, operation: TransactionOperation
    ) -> list[Transaction]:
        """Get all transactions of a specific type."""
        return self.log.get_by_operation(operation)

    def get_all_transactions(self) -> list[Transaction]:
        """Get all transactions."""
        return self.log.transactions.copy()

    def reverse_transaction(self, txn_id: str) -> Optional[Transaction]:
        """
        Create a reverse transaction for undo/revert.

        Args:
            txn_id: ID of transaction to reverse

        Returns:
            New reverse transaction, or None if not found
        """
        original = self.get_transaction(txn_id)
        if not original:
            return None

        # Reverse the changes (swap old and new values)
        reversed_changes = [
            (row, col, new_val, old_val)
            for row, col, old_val, new_val in original.changes
        ]

        return self.create_transaction(
            operation=TransactionOperation.REVERT,
            file_path=original.file_path,
            changes=reversed_changes,
            user=original.user,
            details={"reverted_transaction": txn_id},
            metadata={"original_txn_id": txn_id},
        )

    def clear_log(self) -> None:
        """Clear all transactions from log."""
        self.log.transactions.clear()
        self.log.last_sync_index = 0

    def get_log_size(self) -> int:
        """Get number of transactions in log."""
        return len(self.log)

    def get_unsync_count(self) -> int:
        """Get number of transactions not yet persisted."""
        return len(self.log) - self.log.last_sync_index

    def mark_synced(self) -> None:
        """Mark current transactions as persisted."""
        self.log.last_sync_index = len(self.log)
