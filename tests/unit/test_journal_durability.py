"""The transaction journal must reach disk before the bytes it authorises.

`PRODUCT.md` §8 orders the apply path ``hash-chained journal + immutable source snapshot
-> atomic apply -> byte-for-byte reversible``, and `SPEC_transactions.md` locks
"transaction-first ordering is non-negotiable for applied repairs". Ordering writes in
program order only survives a crash if each write is *durable* when the next one starts.

Two of the three legs already are. `dataforge/engine/repair.py:_write_snapshot_once`
fsyncs the snapshot handle, and `dataforge/transactions/files.py:atomic_write_bytes`
fsyncs both the replacement file and its parent directory before returning. The journal
was the exception: ``_write_jsonl_line`` appended through a buffered handle and returned
with the record still in the page cache.

The consequence is specific and silent. A power loss between the journal append and the
data write can leave the *modified* CSV durable -- it was fsynced -- and the ``created``
or ``applied`` event gone. The transaction id is then unrecoverable, so there is nothing
to pass to ``dataforge revert``, and the user holds a rewritten file with no receipt. That
inverts the ordering guarantee the spec calls non-negotiable, and it fails in the
direction the constitution rates worst: no error is raised anywhere.

**On the shape of these tests.** They assert that ``os.fsync`` is called on the journal
handle, which couples them to the implementation. That cost is deliberate and there is no
cheaper option: durability is only observable by crashing the kernel between two writes,
which no test tier here can do. Asserting the syscall is the strongest available
falsifier, and it is the same trade the repository already accepts for the snapshot fsync.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataforge.transactions.log import (
    append_applied_event,
    append_created_transaction,
    append_reverted_event,
)
from dataforge.transactions.txn import CellFix, RepairTransaction

TXN_ID = "txn-2026-04-20-a1b2c3"
_SOURCE_BYTES = b"id,amount\n1,100\n2,1020\n"
_POST_SHA256 = "b" * 64


@pytest.fixture
def fsynced_fds(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Record every file descriptor passed to ``os.fsync``, still performing the sync."""
    seen: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        seen.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    return seen


def _transaction(tmp_path: Path) -> RepairTransaction:
    """Build a file-backed transaction whose journal lands under ``tmp_path``."""
    source_path = tmp_path / "data.csv"
    source_path.write_bytes(_SOURCE_BYTES)
    snapshot_path = tmp_path / ".dataforge" / "snapshots" / f"{TXN_ID}.bin"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(_SOURCE_BYTES)
    return RepairTransaction(
        txn_id=TXN_ID,
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


class TestEveryJournalEventIsDurableBeforeReturn:
    """One fsync per appended event; a buffered append is not an ordered write."""

    def test_created_event_is_fsynced(self, tmp_path: Path, fsynced_fds: list[int]) -> None:
        transaction = _transaction(tmp_path)

        append_created_transaction(transaction)

        assert fsynced_fds, (
            "append_created_transaction returned without fsyncing the journal, so the "
            "created event can be lost while the data write that follows it survives"
        )

    def test_applied_event_is_fsynced(self, tmp_path: Path, fsynced_fds: list[int]) -> None:
        transaction = _transaction(tmp_path)
        log_path = append_created_transaction(transaction)
        fsynced_fds.clear()

        append_applied_event(log_path, TXN_ID, post_sha256=_POST_SHA256)

        assert fsynced_fds, (
            "append_applied_event returned without fsyncing the journal, so the applied "
            "event can be lost while the mutated source file survives"
        )

    def test_reverted_event_is_fsynced(self, tmp_path: Path, fsynced_fds: list[int]) -> None:
        transaction = _transaction(tmp_path)
        log_path = append_created_transaction(transaction)
        append_applied_event(log_path, TXN_ID, post_sha256=_POST_SHA256)
        fsynced_fds.clear()

        append_reverted_event(log_path, TXN_ID)

        assert fsynced_fds, (
            "append_reverted_event returned without fsyncing the journal, so a completed "
            "revert can look unreverted after a crash"
        )


class TestTheJournalFileItselfIsDiscoverableAfterACrash:
    """A durable record inside an undurable directory entry is still unfindable."""

    def test_creating_the_log_fsyncs_its_parent_directory(
        self, tmp_path: Path, fsynced_fds: list[int]
    ) -> None:
        """``find_transaction_log`` resolves a txn id by directory listing.

        If the new ``<txn_id>.jsonl`` link is not durable, a crash can leave the record
        written and the file absent from its directory -- which is indistinguishable, to
        every DataForge surface, from a transaction that never happened.
        """
        transaction = _transaction(tmp_path)

        log_path = append_created_transaction(transaction)

        directory_fds = len(fsynced_fds)
        assert directory_fds >= 2, (
            "creating a journal fsynced the record but not the directory entry that makes "
            f"{log_path.name} findable, so a crash can hide the transaction entirely"
        )


class TestTheJournalStaysReadable:
    """Durability must not change the wire format the audit re-hashes."""

    def test_appends_still_produce_one_line_per_event(self, tmp_path: Path) -> None:
        transaction = _transaction(tmp_path)
        log_path = append_created_transaction(transaction)
        append_applied_event(log_path, TXN_ID, post_sha256=_POST_SHA256)
        append_reverted_event(log_path, TXN_ID)

        lines = log_path.read_text(encoding="utf-8").splitlines()

        assert len(lines) == 3
