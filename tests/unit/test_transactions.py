"""Unit tests for DataForge Week 2 transactions and revert flow."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataforge.transactions.log import (
    TransactionLogError,
    append_applied_event,
    append_created_transaction,
    append_reverted_event,
    find_transaction_log,
    load_transaction,
)
from dataforge.transactions.revert import TransactionRevertError, revert_transaction
from dataforge.transactions.txn import CellFix, RepairTransaction


def _sha256_bytes(payload: bytes) -> str:
    """Return the SHA-256 digest for the given payload."""
    return hashlib.sha256(payload).hexdigest()


def _sample_transaction(source_path: Path, snapshot_path: Path) -> RepairTransaction:
    """Build a valid sample transaction for tests."""
    return RepairTransaction(
        txn_id="txn-2026-04-20-a1b2c3",
        created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        source_path=str(source_path.resolve()),
        source_sha256="a" * 64,
        source_snapshot_path=str(snapshot_path.resolve()),
        fixes=[
            CellFix(
                row=1,
                column="amount",
                old_value="1020",
                new_value="102",
                detector_id="decimal_shift",
            )
        ],
        applied=False,
    )


class TestTransactionModels:
    """Validation for transaction models."""

    def test_repair_transaction_accepts_valid_identifier(self, tmp_path: Path) -> None:
        source_path = tmp_path / "data.csv"
        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        source_path.write_text("amount\n1020\n", encoding="utf-8")
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_text("amount\n1020\n", encoding="utf-8")

        txn = _sample_transaction(source_path, snapshot_path)

        assert txn.txn_id == "txn-2026-04-20-a1b2c3"
        assert txn.applied is False
        assert txn.reverted_at is None

    def test_repair_transaction_rejects_invalid_identifier(self, tmp_path: Path) -> None:
        source_path = tmp_path / "data.csv"
        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        source_path.write_text("amount\n1020\n", encoding="utf-8")
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_text("amount\n1020\n", encoding="utf-8")

        with pytest.raises(Exception, match="txn_id"):
            RepairTransaction(
                txn_id="bad-identifier",
                created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
                source_path=str(source_path.resolve()),
                source_sha256="a" * 64,
                source_snapshot_path=str(snapshot_path.resolve()),
                fixes=[],
                applied=False,
            )

    def test_repair_transaction_rejects_non_utc_timestamp(self, tmp_path: Path) -> None:
        source_path = tmp_path / "data.csv"
        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        source_path.write_text("amount\n1020\n", encoding="utf-8")
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_text("amount\n1020\n", encoding="utf-8")

        with pytest.raises(Exception, match="created_at"):
            RepairTransaction(
                txn_id="txn-2026-04-20-a1b2c3",
                created_at=datetime(2026, 4, 20, 12, 0),
                source_path=str(source_path.resolve()),
                source_sha256="a" * 64,
                source_snapshot_path=str(snapshot_path.resolve()),
                fixes=[],
                applied=False,
            )


class TestTransactionJournal:
    """Append-only JSONL journal behavior."""

    def test_journal_replays_created_applied_and_reverted(self, tmp_path: Path) -> None:
        source_path = tmp_path / "data.csv"
        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        source_path.write_text("amount\n1020\n", encoding="utf-8")
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_text("amount\n1020\n", encoding="utf-8")

        txn = _sample_transaction(source_path, snapshot_path)
        log_path = append_created_transaction(txn)
        append_applied_event(
            log_path,
            txn.txn_id,
            post_sha256="b" * 64,
            applied_at=datetime(2026, 4, 20, 12, 1, tzinfo=UTC),
        )
        append_reverted_event(
            log_path,
            txn.txn_id,
            reverted_at=datetime(2026, 4, 20, 12, 2, tzinfo=UTC),
        )

        replayed = load_transaction(log_path)
        lines = log_path.read_text(encoding="utf-8").strip().splitlines()

        assert len(lines) == 3
        assert replayed.applied is True
        assert replayed.post_sha256 == "b" * 64
        assert replayed.reverted_at == datetime(2026, 4, 20, 12, 2, tzinfo=UTC)

    def test_missing_created_event_is_rejected(self, tmp_path: Path) -> None:
        log_path = tmp_path / ".dataforge" / "transactions" / "txn-2026-04-20-a1b2c3.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text(
            '{"schema_version": 1, "event_type": "applied", "txn_id": "txn-2026-04-20-a1b2c3", "occurred_at": "2026-04-20T12:00:00+00:00", "post_sha256": "'
            + ("b" * 64)
            + '"}\n',
            encoding="utf-8",
        )

        with pytest.raises(TransactionLogError, match="missing the initial created event"):
            load_transaction(log_path)

    def test_unknown_event_type_is_rejected(self, tmp_path: Path) -> None:
        log_path = tmp_path / ".dataforge" / "transactions" / "txn-2026-04-20-a1b2c3.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text(
            '{"schema_version": 1, "event_type": "created", "occurred_at": "2026-04-20T12:00:00+00:00", "transaction": {"txn_id": "txn-2026-04-20-a1b2c3", "created_at": "2026-04-20T12:00:00+00:00", "source_path": "C:/tmp/data.csv", "source_sha256": "'
            + ("a" * 64)
            + '", "post_sha256": null, "source_snapshot_path": "C:/tmp/txn.bin", "fixes": [], "applied": false, "reverted_at": null}}\n'
            '{"schema_version": 1, "event_type": "mystery", "occurred_at": "2026-04-20T12:01:00+00:00", "txn_id": "txn-2026-04-20-a1b2c3"}\n',
            encoding="utf-8",
        )

        with pytest.raises(TransactionLogError, match="Unknown transaction log event type"):
            load_transaction(log_path)

    def test_find_transaction_log_errors_for_missing_and_duplicate(self, tmp_path: Path) -> None:
        with pytest.raises(TransactionLogError, match="Could not find transaction"):
            find_transaction_log("txn-2026-04-20-a1b2c3", search_root=tmp_path)

        first = tmp_path / "one" / ".dataforge" / "transactions" / "txn-2026-04-20-a1b2c3.jsonl"
        second = tmp_path / "two" / ".dataforge" / "transactions" / "txn-2026-04-20-a1b2c3.jsonl"
        first.parent.mkdir(parents=True)
        second.parent.mkdir(parents=True)
        first.write_text("", encoding="utf-8")
        second.write_text("", encoding="utf-8")

        with pytest.raises(TransactionLogError, match="multiple transaction logs"):
            find_transaction_log("txn-2026-04-20-a1b2c3", search_root=tmp_path)


class TestRevertFlow:
    """Snapshot-based revert behavior."""

    def test_revert_restores_original_bytes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        source_path = tmp_path / "data.csv"
        source_bytes = b"id,amount\n1,100\n2,1020\n"
        source_path.write_bytes(source_bytes)

        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_bytes(source_bytes)

        post_bytes = b"id,amount\n1,100\n2,102\n"
        source_path.write_bytes(post_bytes)

        txn = RepairTransaction(
            txn_id="txn-2026-04-20-c0ffee",
            created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            source_path=str(source_path.resolve()),
            source_sha256=_sha256_bytes(source_bytes),
            source_snapshot_path=str(snapshot_path.resolve()),
            fixes=[
                CellFix(
                    row=1,
                    column="amount",
                    old_value="1020",
                    new_value="102",
                    detector_id="decimal_shift",
                )
            ],
            applied=False,
        )
        log_path = append_created_transaction(txn)
        append_applied_event(log_path, txn.txn_id, post_sha256=_sha256_bytes(post_bytes))

        reverted = revert_transaction(txn.txn_id)

        assert source_path.read_bytes() == source_bytes
        assert reverted.reverted_at is not None
        replayed = load_transaction(log_path)
        assert replayed.reverted_at is not None

    def test_revert_refuses_when_current_hash_differs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        source_path = tmp_path / "data.csv"
        source_bytes = b"id,amount\n1,100\n2,1020\n"
        source_path.write_bytes(source_bytes)

        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_bytes(source_bytes)

        post_bytes = b"id,amount\n1,100\n2,102\n"
        source_path.write_bytes(b"id,amount\n1,100\n2,999\n")

        txn = RepairTransaction(
            txn_id="txn-2026-04-20-deadbe",
            created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            source_path=str(source_path.resolve()),
            source_sha256=_sha256_bytes(source_bytes),
            source_snapshot_path=str(snapshot_path.resolve()),
            fixes=[
                CellFix(
                    row=1,
                    column="amount",
                    old_value="1020",
                    new_value="102",
                    detector_id="decimal_shift",
                )
            ],
            applied=False,
        )
        log_path = append_created_transaction(txn)
        append_applied_event(log_path, txn.txn_id, post_sha256=_sha256_bytes(post_bytes))

        with pytest.raises(TransactionRevertError, match="post-state hash"):
            revert_transaction(txn.txn_id)

        assert source_path.read_bytes() == b"id,amount\n1,100\n2,999\n"

    def test_revert_missing_source_snapshot_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        source_path = tmp_path / "data.csv"
        source_bytes = b"id,amount\n1,100\n2,1020\n"
        source_path.write_bytes(source_bytes)

        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_bytes(source_bytes)

        post_bytes = b"id,amount\n1,100\n2,102\n"
        source_path.write_bytes(post_bytes)

        txn = RepairTransaction(
            txn_id="txn-2026-04-20-feed01",
            created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            source_path=str(source_path.resolve()),
            source_sha256=_sha256_bytes(source_bytes),
            source_snapshot_path=str(snapshot_path.resolve()),
            fixes=[],
            applied=False,
        )
        log_path = append_created_transaction(txn)
        append_applied_event(log_path, txn.txn_id, post_sha256=_sha256_bytes(post_bytes))
        snapshot_path.unlink()

        with pytest.raises(TransactionRevertError, match="Source snapshot not found"):
            revert_transaction(txn.txn_id)
