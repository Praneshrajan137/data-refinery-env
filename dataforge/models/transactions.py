"""Transaction and audit trail models."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class TransactionOperation(str, Enum):
    """Types of operations recorded in transaction log."""

    REPAIR = "repair"
    REVERT = "revert"
    CONSTRAINT_ADD = "constraint_add"
    CONSTRAINT_REMOVE = "constraint_remove"
    SCHEMA_INFER = "schema_infer"
    SCHEMA_UPDATE = "schema_update"


@dataclass(frozen=True)
class Transaction:
    """
    Immutable transaction record for audit trail.

    Attributes:
        id: Unique transaction identifier
        timestamp: When transaction occurred
        operation: Type of operation
        file_path: CSV file affected
        user: User who initiated operation (optional)
        details: Operation-specific details
        changes: List of (row, column, old_value, new_value) tuples
        metadata: Additional metadata (reason, version, etc.)
    """

    id: str
    timestamp: datetime
    operation: TransactionOperation
    file_path: str
    user: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)
    changes: list[tuple[int, str, Any, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return string representation of transaction."""
        return (
            f"Transaction {self.id}: {self.operation.value} "
            f"({len(self.changes)} changes) at {self.timestamp.isoformat()}"
        )


@dataclass
class TransactionLog:
    """
    Append-only transaction log for audit trail.

    Attributes:
        transactions: List of all transactions in order
        last_sync_index: Index of last persisted transaction
    """

    transactions: list[Transaction] = field(default_factory=list)
    last_sync_index: int = 0

    def add_transaction(self, transaction: Transaction) -> None:
        """Add transaction to log."""
        self.transactions.append(transaction)

    def get_by_id(self, txn_id: str) -> Optional[Transaction]:
        """Get transaction by ID."""
        for txn in self.transactions:
            if txn.id == txn_id:
                return txn
        return None

    def get_recent(self, count: int = 10) -> list[Transaction]:
        """Get most recent transactions."""
        return self.transactions[-count:]

    def get_by_operation(self, operation: TransactionOperation) -> list[Transaction]:
        """Get all transactions of a specific operation type."""
        return [txn for txn in self.transactions if txn.operation == operation]

    def __len__(self) -> int:
        """Return number of transactions in log."""
        return len(self.transactions)

    def __str__(self) -> str:
        """Return string representation of transaction log."""
        return f"TransactionLog({len(self)} transactions)"
