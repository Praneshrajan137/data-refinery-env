"""Transaction log persistence layer."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from dataforge.exceptions import IOError as DataForgeIOError
from dataforge.models import Transaction, TransactionLog, TransactionOperation
from dataforge.types import AuditPath


class TransactionLogStore:
    """Store and retrieve transaction logs."""

    @staticmethod
    def save(log: TransactionLog, path: AuditPath) -> None:
        """
        Save transaction log to JSON file.

        Args:
            log: TransactionLog to save
            path: Path to audit log file

        Raises:
            IOError: If save fails
        """
        path = Path(path)

        try:
            log_data = {
                "transactions": [
                    {
                        "id": txn.id,
                        "timestamp": txn.timestamp.isoformat(),
                        "operation": txn.operation.value,
                        "file_path": txn.file_path,
                        "user": txn.user,
                        "details": txn.details,
                        "changes": txn.changes,
                        "metadata": txn.metadata,
                    }
                    for txn in log.transactions
                ]
            }

            with open(path, "w") as f:
                json.dump(log_data, f, indent=2)

        except Exception as e:
            raise DataForgeIOError(
                f"Failed to save transaction log: {e}",
                context={"path": str(path)},
            )

    @staticmethod
    def load(path: AuditPath) -> TransactionLog:
        """
        Load transaction log from JSON file.

        Args:
            path: Path to audit log file

        Returns:
            Loaded TransactionLog

        Raises:
            IOError: If load fails
        """
        path = Path(path)

        if not path.exists():
            return TransactionLog()  # Empty log if file doesn't exist

        try:
            with open(path, "r") as f:
                data = json.load(f)

            transactions = []
            for txn_data in data.get("transactions", []):
                txn = Transaction(
                    id=txn_data["id"],
                    timestamp=datetime.fromisoformat(txn_data["timestamp"]),
                    operation=TransactionOperation(txn_data["operation"]),
                    file_path=txn_data["file_path"],
                    user=txn_data.get("user"),
                    details=txn_data.get("details", {}),
                    changes=txn_data.get("changes", []),
                    metadata=txn_data.get("metadata", {}),
                )
                transactions.append(txn)

            log = TransactionLog(transactions=transactions)
            log.last_sync_index = len(transactions)
            return log

        except json.JSONDecodeError as e:
            raise DataForgeIOError(
                f"Invalid JSON in audit log: {e}",
                context={"path": str(path)},
            )
        except Exception as e:
            raise DataForgeIOError(
                f"Failed to load transaction log: {e}",
                context={"path": str(path)},
            )

    @staticmethod
    def append_transaction(path: AuditPath, transaction: Transaction) -> None:
        """
        Append single transaction to log file (append-only).

        Args:
            path: Path to audit log file
            transaction: Transaction to append

        Raises:
            IOError: If append fails
        """
        path = Path(path)

        try:
            # Load existing log or create new
            log = TransactionLogStore.load(path)
            log.add_transaction(transaction)

            # Save updated log
            TransactionLogStore.save(log, path)

        except Exception as e:
            raise DataForgeIOError(
                f"Failed to append transaction: {e}",
                context={"path": str(path)},
            )

    @staticmethod
    def get_recent(path: AuditPath, count: int = 10) -> list[Transaction]:
        """
        Get recent transactions from log.

        Args:
            path: Path to audit log file
            count: Number of recent transactions

        Returns:
            List of recent transactions
        """
        log = TransactionLogStore.load(path)
        return log.get_recent(count)

    @staticmethod
    def get_by_operation(
        path: AuditPath, operation: TransactionOperation
    ) -> list[Transaction]:
        """
        Get transactions by operation type.

        Args:
            path: Path to audit log file
            operation: Operation type to filter by

        Returns:
            List of matching transactions
        """
        log = TransactionLogStore.load(path)
        return log.get_by_operation(operation)
