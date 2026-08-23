"""Append-only JSONL transaction journal for DataForge repairs."""

from __future__ import annotations

import enum
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from dataforge.transactions.txn import RepairTransaction

LEGACY_SCHEMA_VERSION = 1
SCHEMA_VERSION = 2
LEGACY_SCHEMA_NAME = "transaction_journal_v1"
SCHEMA_NAME = "transaction_journal_v2"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class TransactionLogError(Exception):
    """Raised when a transaction journal cannot be written or replayed."""


class TransactionAuditVerdict(enum.Enum):
    """Possible outcomes for transaction log audit verification."""

    VERIFIED = "verified"
    LEGACY_UNVERIFIED = "legacy_unverified"
    UNREVERTIBLE = "unrevertible"
    TAMPERED = "tampered"
    MISSING = "missing"
    MALFORMED = "malformed"


class TransactionAuditReport(BaseModel):
    """Machine-readable result of transaction hash-chain verification."""

    verdict: TransactionAuditVerdict
    log_path: str | None = None
    txn_id: str | None = None
    schema_version: int | None = None
    schema_name: str | None = None
    event_count: int = Field(ge=0)
    head_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    errors: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"frozen": True}


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


def _canonical_event_bytes(record: dict[str, Any]) -> bytes:
    """Serialize an audit event into the canonical hash material.

    ``event_sha256`` is excluded because it is the output; everything else is included.
    The options here are the canonical form, and :func:`_write_jsonl_line` must use the
    same ones -- see the note there.
    """
    unsigned = {key: value for key, value in record.items() if key != "event_sha256"}
    return json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _event_sha256(record: dict[str, Any]) -> str:
    """Return the canonical SHA-256 hash for an event record."""
    return sha256_bytes(_canonical_event_bytes(record))


def _sign_event(record: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``record`` with its canonical event hash attached."""
    signed = dict(record)
    signed["event_sha256"] = _event_sha256(signed)
    return signed


def _write_jsonl_line(path: Path, record: dict[str, Any], *, create: bool = False) -> None:
    """Append or create a JSONL record on disk.

    The serialization options must match :func:`_canonical_event_bytes` exactly. Until
    2026-08-23 they did not: this wrote ``json.dumps(record, sort_keys=True)`` with the
    library's **default** separators (``", "``, ``": "``) and default ``ensure_ascii=True``,
    while the preimage used compact separators and ``ensure_ascii=False``.

    Verification still succeeded, because it re-parses each line and recomputes the hash
    from the parsed object. But the consequence was that **the bytes on disk were not the
    bytes that were hashed**, in a system whose central claim is byte-level verifiability.
    A third party could not hash a line and compare; they had to know to re-parse it with a
    JSON library configured exactly like this one and re-serialize. That puts
    canonicalisation inside the trust boundary, which is the failure
    ``dataforge/attestation/__init__.py`` avoids by signing a DSSE PAE rather than JSON
    text -- and it cited this very log as the counterexample.

    Now the only difference between a line on disk and its preimage is the removal of the
    single ``event_sha256`` key, which is fully specified rather than incidental.

    Older logs remain verifiable unchanged, because verification never depended on the
    on-disk byte layout.

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
            handle.write(
                json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
            )
            handle.write("\n")
    except OSError as exc:
        raise TransactionLogError(f"Could not write transaction log '{path}': {exc}") from exc


def _read_records(log_path: Path) -> list[dict[str, Any]]:
    """Read non-empty JSONL records from a transaction log."""
    records: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(log_path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise TransactionLogError(
                f"Malformed JSON at {log_path}:{line_number}: {exc.msg}"
            ) from exc
        if not isinstance(payload, dict):
            raise TransactionLogError(f"Malformed transaction event at {log_path}:{line_number}.")
        records.append(payload)
    return records


def _log_schema_version(log_path: Path) -> int | None:
    """Return the first event schema version for an existing log."""
    if not log_path.exists():
        return None
    records = _read_records(log_path)
    if not records:
        return None
    raw_version = records[0].get("schema_version")
    return raw_version if isinstance(raw_version, int) else None


def _next_event_metadata(log_path: Path) -> tuple[int, str | None]:
    """Return the next v2 event index and previous hash for ``log_path``."""
    records = _read_records(log_path)
    if not records:
        raise TransactionLogError(f"Transaction log '{log_path}' contained no events.")
    previous = records[-1].get("event_sha256")
    if not isinstance(previous, str) or not _SHA256_RE.fullmatch(previous):
        raise TransactionLogError(
            f"Transaction log '{log_path}' is missing a valid previous event hash."
        )
    return len(records), previous


def _v1_created_record(transaction: RepairTransaction) -> dict[str, Any]:
    """Build a legacy v1 transaction creation event."""
    return {
        "schema_version": LEGACY_SCHEMA_VERSION,
        "event_type": "created",
        "occurred_at": transaction.created_at.isoformat(),
        "transaction": transaction.model_dump(mode="json"),
    }


def _v2_created_record(transaction: RepairTransaction) -> dict[str, Any]:
    """Build a hash-chained v2 transaction creation event."""
    return _sign_event(
        {
            "schema_version": SCHEMA_VERSION,
            "schema_name": SCHEMA_NAME,
            "event_index": 0,
            "event_type": "created",
            "occurred_at": transaction.created_at.isoformat(),
            "previous_event_sha256": None,
            "transaction": transaction.model_dump(mode="json"),
        }
    )


def _v1_applied_record(txn_id: str, post_sha256: str, applied_at: datetime) -> dict[str, Any]:
    """Build a legacy v1 applied event."""
    return {
        "schema_version": LEGACY_SCHEMA_VERSION,
        "event_type": "applied",
        "occurred_at": applied_at.isoformat(),
        "txn_id": txn_id,
        "post_sha256": post_sha256,
    }


def _v2_applied_record(
    log_path: Path,
    txn_id: str,
    post_sha256: str,
    applied_at: datetime,
) -> dict[str, Any]:
    """Build a hash-chained v2 applied event."""
    event_index, previous_hash = _next_event_metadata(log_path)
    return _sign_event(
        {
            "schema_version": SCHEMA_VERSION,
            "schema_name": SCHEMA_NAME,
            "event_index": event_index,
            "event_type": "applied",
            "occurred_at": applied_at.isoformat(),
            "previous_event_sha256": previous_hash,
            "txn_id": txn_id,
            "post_sha256": post_sha256,
        }
    )


def _v1_reverted_record(txn_id: str, reverted_at: datetime) -> dict[str, Any]:
    """Build a legacy v1 reverted event."""
    return {
        "schema_version": LEGACY_SCHEMA_VERSION,
        "event_type": "reverted",
        "occurred_at": reverted_at.isoformat(),
        "txn_id": txn_id,
    }


def _v2_reverted_record(log_path: Path, txn_id: str, reverted_at: datetime) -> dict[str, Any]:
    """Build a hash-chained v2 reverted event."""
    event_index, previous_hash = _next_event_metadata(log_path)
    return _sign_event(
        {
            "schema_version": SCHEMA_VERSION,
            "schema_name": SCHEMA_NAME,
            "event_index": event_index,
            "event_type": "reverted",
            "occurred_at": reverted_at.isoformat(),
            "previous_event_sha256": previous_hash,
            "txn_id": txn_id,
        }
    )


def append_created_transaction(
    transaction: RepairTransaction,
    *,
    log_root: Path | None = None,
) -> Path:
    """Write the immutable transaction creation event.

    Args:
        transaction: The transaction to serialize.

    Returns:
        The created JSONL log path.
    """
    source_path = Path(transaction.source_path)
    log_path = (
        transaction_log_path_for(source_path, transaction.txn_id)
        if log_root is None
        else log_root.resolve() / ".dataforge" / "transactions" / f"{transaction.txn_id}.jsonl"
    )
    _write_jsonl_line(log_path, _v2_created_record(transaction), create=True)
    return log_path


def append_applied_event(
    log_path: Path,
    txn_id: str,
    post_sha256: str,
    *,
    applied_at: datetime | None = None,
) -> None:
    """Append an ``applied`` event to an existing transaction log."""
    occurred_at = applied_at or _utc_now()
    record = (
        _v1_applied_record(txn_id, post_sha256, occurred_at)
        if _log_schema_version(log_path) == LEGACY_SCHEMA_VERSION
        else _v2_applied_record(log_path, txn_id, post_sha256, occurred_at)
    )
    _write_jsonl_line(log_path, record, create=False)


def append_reverted_event(
    log_path: Path,
    txn_id: str,
    *,
    reverted_at: datetime | None = None,
) -> None:
    """Append a ``reverted`` event to an existing transaction log."""
    occurred_at = reverted_at or _utc_now()
    record = (
        _v1_reverted_record(txn_id, occurred_at)
        if _log_schema_version(log_path) == LEGACY_SCHEMA_VERSION
        else _v2_reverted_record(log_path, txn_id, occurred_at)
    )
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
    for payload in _read_records(log_path):
        if payload.get("schema_version") not in {LEGACY_SCHEMA_VERSION, SCHEMA_VERSION}:
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


def verify_transaction_log(
    txn_id: str | None = None,
    *,
    log_path: Path | None = None,
    search_root: Path | None = None,
) -> TransactionAuditReport:
    """Verify a transaction log's local hash chain.

    Legacy v1 logs remain replayable but cannot be cryptographically verified,
    so they return ``legacy_unverified`` instead of ``verified``.
    """
    try:
        resolved_log_path = log_path.resolve() if log_path is not None else None
        if resolved_log_path is None:
            if txn_id is None:
                return TransactionAuditReport(
                    verdict=TransactionAuditVerdict.MISSING,
                    txn_id=txn_id,
                    event_count=0,
                    errors=("txn_id or log_path is required.",),
                )
            resolved_log_path = find_transaction_log(txn_id, search_root=search_root)
    except TransactionLogError as exc:
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.MISSING,
            txn_id=txn_id,
            event_count=0,
            errors=(str(exc),),
        )

    if not resolved_log_path.exists():
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.MISSING,
            log_path=str(resolved_log_path),
            txn_id=txn_id,
            event_count=0,
            errors=(f"Transaction log not found: {resolved_log_path}",),
        )

    try:
        records = _read_records(resolved_log_path)
    except TransactionLogError as exc:
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.MALFORMED,
            log_path=str(resolved_log_path),
            txn_id=txn_id,
            event_count=0,
            errors=(str(exc),),
        )

    if not records:
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.MALFORMED,
            log_path=str(resolved_log_path),
            txn_id=txn_id,
            event_count=0,
            errors=("Transaction log contained no events.",),
        )

    versions = {record.get("schema_version") for record in records}
    if versions == {LEGACY_SCHEMA_VERSION}:
        try:
            transaction = load_transaction(resolved_log_path)
        except TransactionLogError as exc:
            return TransactionAuditReport(
                verdict=TransactionAuditVerdict.MALFORMED,
                log_path=str(resolved_log_path),
                schema_version=LEGACY_SCHEMA_VERSION,
                schema_name=LEGACY_SCHEMA_NAME,
                event_count=len(records),
                errors=(str(exc),),
            )
        if txn_id is not None and transaction.txn_id != txn_id:
            return TransactionAuditReport(
                verdict=TransactionAuditVerdict.TAMPERED,
                log_path=str(resolved_log_path),
                txn_id=transaction.txn_id,
                schema_version=LEGACY_SCHEMA_VERSION,
                schema_name=LEGACY_SCHEMA_NAME,
                event_count=len(records),
                errors=(f"Expected txn_id '{txn_id}', found '{transaction.txn_id}'.",),
            )
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.LEGACY_UNVERIFIED,
            log_path=str(resolved_log_path),
            txn_id=transaction.txn_id,
            schema_version=LEGACY_SCHEMA_VERSION,
            schema_name=LEGACY_SCHEMA_NAME,
            event_count=len(records),
            errors=("Legacy v1 logs do not contain event hashes.",),
        )

    if versions != {SCHEMA_VERSION}:
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.MALFORMED,
            log_path=str(resolved_log_path),
            txn_id=txn_id,
            event_count=len(records),
            errors=(f"Mixed or unsupported schema versions: {sorted(map(str, versions))}.",),
        )

    errors: list[str] = []
    previous_hash: str | None = None
    resolved_txn_id: str | None = None
    head_sha256: str | None = None
    for expected_index, record in enumerate(records):
        if record.get("event_index") != expected_index:
            errors.append(f"Event {expected_index} has event_index {record.get('event_index')!r}.")
        if record.get("previous_event_sha256") != previous_hash:
            errors.append(f"Event {expected_index} previous hash does not match.")

        recorded_hash = record.get("event_sha256")
        if not isinstance(recorded_hash, str) or not _SHA256_RE.fullmatch(recorded_hash):
            errors.append(f"Event {expected_index} is missing a valid event hash.")
        else:
            calculated_hash = _event_sha256(record)
            if calculated_hash != recorded_hash:
                errors.append(f"Event {expected_index} hash does not match its payload.")
            previous_hash = recorded_hash
            head_sha256 = recorded_hash

        event_type = record.get("event_type")
        if event_type == "created":
            raw_transaction = record.get("transaction")
            if not isinstance(raw_transaction, dict):
                errors.append("Created event is missing a transaction payload.")
            else:
                current_txn_id = raw_transaction.get("txn_id")
                if not isinstance(current_txn_id, str):
                    errors.append("Created transaction payload is missing txn_id.")
                elif resolved_txn_id is None:
                    resolved_txn_id = current_txn_id
                elif resolved_txn_id != current_txn_id:
                    errors.append("Created transaction payload changed txn_id.")
        elif event_type in {"applied", "reverted"}:
            current_txn_id = record.get("txn_id")
            if current_txn_id != resolved_txn_id:
                errors.append(
                    f"Event {expected_index} txn_id {current_txn_id!r} does not match created event."
                )
        else:
            errors.append(f"Event {expected_index} has unknown event_type {event_type!r}.")

    if txn_id is not None and resolved_txn_id is not None and resolved_txn_id != txn_id:
        errors.append(f"Expected txn_id '{txn_id}', found '{resolved_txn_id}'.")

    try:
        transaction = load_transaction(resolved_log_path)
    except TransactionLogError as exc:
        errors.append(str(exc))

    if errors:
        return TransactionAuditReport(
            verdict=TransactionAuditVerdict.TAMPERED,
            log_path=str(resolved_log_path),
            txn_id=resolved_txn_id or txn_id,
            schema_version=SCHEMA_VERSION,
            schema_name=SCHEMA_NAME,
            event_count=len(records),
            head_sha256=head_sha256,
            errors=tuple(errors),
        )

    if transaction.applied and transaction.reverted_at is None:
        source_path = Path(transaction.source_path)
        snapshot_path = Path(transaction.source_snapshot_path)
        revert_errors: list[str] = []
        if transaction.source_kind == "file":
            if not source_path.exists():
                revert_errors.append(f"Source file not found: {source_path}")
            elif (
                transaction.post_sha256 is not None
                and sha256_file(source_path) != transaction.post_sha256
            ):
                revert_errors.append("Source file no longer matches the recorded post-state hash.")
        if not snapshot_path.exists():
            revert_errors.append(f"Source snapshot not found: {snapshot_path}")
        if revert_errors:
            return TransactionAuditReport(
                verdict=TransactionAuditVerdict.UNREVERTIBLE,
                log_path=str(resolved_log_path),
                txn_id=resolved_txn_id,
                schema_version=SCHEMA_VERSION,
                schema_name=SCHEMA_NAME,
                event_count=len(records),
                head_sha256=head_sha256,
                errors=tuple(revert_errors),
            )

    return TransactionAuditReport(
        verdict=TransactionAuditVerdict.VERIFIED,
        log_path=str(resolved_log_path),
        txn_id=resolved_txn_id,
        schema_version=SCHEMA_VERSION,
        schema_name=SCHEMA_NAME,
        event_count=len(records),
        head_sha256=head_sha256,
    )
