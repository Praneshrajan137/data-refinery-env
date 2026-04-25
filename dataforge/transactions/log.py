"""Append-only JSONL transaction journal for DataForge repairs."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataforge.transactions.txn import RepairTransaction

SCHEMA_VERSION = 1


class TransactionLogError(Exception):
    """Raised when a transaction journal cannot be written or replayed."""


def sha256_bytes(payload: bytes) -> str:
    """Return the SHA-256 digest for the given payload."""
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest for the file at ``path``."""
    return sha256_bytes(path.read_bytes())


def dataforge_root_for(source_path: Path) -> Path:
    """Return the hidden DataForge state directory for a source path."""
    return source_path.resolve().parent / ".dataforge"


def transactions_dir_for(source_path: Path) -> Path:
    """Return the transaction journal directory for a source path."""
    return dataforge_root_for(source_path) / "transactions"


def snapshots_dir_for(source_path: Path) -> Path:
    """Return the snapshot directory for a source path."""
    return dataforge_root_for(source_path) / "snapshots"


def cache_dir_for(source_path: Path) -> Path:
    """Return the cache directory for a source path."""
    return dataforge_root_for(source_path) / "cache"


def snapshot_path_for(source_path: Path, txn_id: str) -> Path:
    """Return the immutable snapshot path for a transaction."""
    return snapshots_dir_for(source_path) / f"{txn_id}.bin"


def transaction_log_path_for(source_path: Path, txn_id: str) -> Path:
    """Return the JSONL log path for a transaction."""
    return transactions_dir_for(source_path) / f"{txn_id}.jsonl"


def _utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


def _write_jsonl_line(path: Path, record: dict[str, Any], *, create: bool = False) -> None:
    """Append or create a JSONL record on disk.

    Args:
        path: The target JSONL log path.
        record: JSON-serializable record to write.
        create: When true, fail if the file already exists.

    Raises:
        TransactionLogError: If the record cannot be written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "x" if create else "a"
    try:
        with path.open(mode, encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")
    except OSError as exc:
        raise TransactionLogError(f"Could not write transaction log '{path}': {exc}") from exc


def append_created_transaction(transaction: RepairTransaction) -> Path:
    """Write the immutable transaction creation event.

    Args:
        transaction: The transaction to serialize.

    Returns:
        The created JSONL log path.
    """
    source_path = Path(transaction.source_path)
    log_path = transaction_log_path_for(source_path, transaction.txn_id)
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "created",
        "occurred_at": transaction.created_at.isoformat(),
        "transaction": transaction.model_dump(mode="json"),
    }
    _write_jsonl_line(log_path, record, create=True)
    return log_path


def append_applied_event(
    log_path: Path,
    txn_id: str,
    post_sha256: str,
    *,
    applied_at: datetime | None = None,
) -> None:
    """Append an ``applied`` event to an existing transaction log."""
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "applied",
        "occurred_at": (applied_at or _utc_now()).isoformat(),
        "txn_id": txn_id,
        "post_sha256": post_sha256,
    }
    _write_jsonl_line(log_path, record, create=False)


def append_reverted_event(
    log_path: Path,
    txn_id: str,
    *,
    reverted_at: datetime | None = None,
) -> None:
    """Append a ``reverted`` event to an existing transaction log."""
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "reverted",
        "occurred_at": (reverted_at or _utc_now()).isoformat(),
        "txn_id": txn_id,
    }
    _write_jsonl_line(log_path, record, create=False)


def load_transaction(log_path: Path) -> RepairTransaction:
    """Replay a transaction log into the latest transaction state.

    Args:
        log_path: Path to the JSONL log file.

    Returns:
        The latest replayed transaction state.

    Raises:
        TransactionLogError: If the log is missing or malformed.
    """
    if not log_path.exists():
        raise TransactionLogError(f"Transaction log not found: {log_path}")

    transaction: RepairTransaction | None = None
    for raw_line in log_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise TransactionLogError(
                f"Unsupported transaction log schema version in '{log_path}'."
            )

        event_type = payload.get("event_type")
        if event_type == "created":
            transaction = RepairTransaction.model_validate(payload["transaction"])
            continue

        if transaction is None:
            raise TransactionLogError(
                f"Transaction log '{log_path}' is missing the initial created event."
            )

        if payload.get("txn_id") != transaction.txn_id:
            raise TransactionLogError(
                f"Transaction log '{log_path}' contains mismatched txn_id values."
            )

        if event_type == "applied":
            transaction = transaction.model_copy(
                update={
                    "applied": True,
                    "post_sha256": payload["post_sha256"],
                }
            )
        elif event_type == "reverted":
            transaction = transaction.model_copy(
                update={
                    "reverted_at": datetime.fromisoformat(payload["occurred_at"]),
                }
            )
        else:
            raise TransactionLogError(
                f"Unknown transaction log event type '{event_type}' in '{log_path}'."
            )

    if transaction is None:
        raise TransactionLogError(f"Transaction log '{log_path}' contained no transaction data.")

    return transaction


def find_transaction_log(txn_id: str, *, search_root: Path | None = None) -> Path:
    """Locate a transaction log by identifier under the working tree.

    Args:
        txn_id: Canonical transaction identifier.
        search_root: Optional root directory to search under.

    Returns:
        The unique matching JSONL log path.

    Raises:
        TransactionLogError: If no log or multiple logs are found.
    """
    root = (search_root or Path.cwd()).resolve()
    direct_candidate = root / ".dataforge" / "transactions" / f"{txn_id}.jsonl"
    if direct_candidate.exists():
        return direct_candidate

    matches: list[Path] = []
    for candidate in root.rglob(f"{txn_id}.jsonl"):
        if candidate.parent.name == "transactions" and candidate.parent.parent.name == ".dataforge":
            matches.append(candidate)

    if not matches:
        raise TransactionLogError(f"Could not find transaction '{txn_id}' under '{root}'.")
    if len(matches) > 1:
        raise TransactionLogError(f"Found multiple transaction logs for '{txn_id}' under '{root}'.")
    return matches[0]
