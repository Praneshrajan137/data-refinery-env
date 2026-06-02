"""Audit trail and transaction management service."""

from typing import Optional

from dataforge.core import TransactionManager
from dataforge.io import TransactionLogStore
from dataforge.models import Transaction, TransactionLog, TransactionOperation
from dataforge.types import AuditPath, TableData, CSVPath


class AuditService:
    """Service for managing audit trails and transaction history."""

    def __init__(self):
        """Initialize audit service."""
        self.manager = TransactionManager()
        self.log_path: Optional[AuditPath] = None

    def initialize_log(self, path: AuditPath) -> None:
        """
        Initialize audit log from file.

        Args:
            path: Path to audit log file
        """
        self.log_path = path
        self.manager.log = TransactionLogStore.load(path)

    def record_repair(
        self,
        csv_path: CSVPath,
        changes: list[tuple[int, str, object, object]],
        user: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> Transaction:
        """
        Record a repair transaction.

        Args:
            csv_path: CSV file that was repaired
            changes: List of (row, column, old_value, new_value)
            user: User who performed repair
            details: Operation details

        Returns:
            Created Transaction
        """
        txn = self.manager.create_transaction(
            operation=TransactionOperation.REPAIR,
            file_path=str(csv_path),
            changes=changes,
            user=user,
            details=details or {},
        )

        # Persist to disk if log path is set
        if self.log_path:
            TransactionLogStore.append_transaction(self.log_path, txn)

        return txn

    def get_history(self, count: int = 10) -> list[Transaction]:
        """
        Get recent transactions.

        Args:
            count: Number of recent transactions to retrieve

        Returns:
            List of recent transactions
        """
        return self.manager.get_recent_transactions(count)

    def get_repairs(self, count: int = 10) -> list[Transaction]:
        """
        Get recent repair transactions.

        Args:
            count: Number of recent repairs to retrieve

        Returns:
            List of recent repair transactions
        """
        repairs = self.manager.get_transactions_by_operation(
            TransactionOperation.REPAIR
        )
        return repairs[-count:]

    def get_reverts(self, count: int = 10) -> list[Transaction]:
        """
        Get recent revert transactions.

        Args:
            count: Number of recent reverts to retrieve

        Returns:
            List of recent revert transactions
        """
        reverts = self.manager.get_transactions_by_operation(
            TransactionOperation.REVERT
        )
        return reverts[-count:]

    def can_revert(self, txn_id: str) -> bool:
        """
        Check if transaction can be reverted.

        Args:
            txn_id: Transaction ID

        Returns:
            True if transaction exists and can be reverted
        """
        return self.manager.get_transaction(txn_id) is not None

    def revert_transaction(self, txn_id: str) -> Optional[Transaction]:
        """
        Revert a transaction.

        Args:
            txn_id: ID of transaction to revert

        Returns:
            Reverse transaction, or None if original not found
        """
        reverse_txn = self.manager.reverse_transaction(txn_id)

        # Persist reverse transaction
        if reverse_txn and self.log_path:
            TransactionLogStore.append_transaction(self.log_path, reverse_txn)

        return reverse_txn

    def get_transaction_summary(self, txn: Transaction) -> str:
        """Get human-readable summary of transaction."""
        change_desc = f"{len(txn.changes)} change" + (
            "s" if len(txn.changes) != 1 else ""
        )
        return (
            f"{txn.operation.value.upper()}: {change_desc} "
            f"in {txn.file_path} ({txn.timestamp.strftime('%Y-%m-%d %H:%M:%S')})"
        )

    def get_full_history(self) -> str:
        """Get formatted full history of all transactions."""
        if not self.manager.get_all_transactions():
            return "No transactions recorded"

        lines = ["Audit History:", "=" * 60]
        for txn in self.manager.get_all_transactions():
            lines.append(self.get_transaction_summary(txn))
        return "\n".join(lines)
