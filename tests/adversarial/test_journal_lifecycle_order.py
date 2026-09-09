"""A validly-hashed transaction journal can still describe an impossible history.

``verify_transaction_log`` checks the *cryptographic* integrity of a v2 journal: every
event carries its own canonical hash, the previous hash links, and ``event_index`` counts
up. Those checks make an *edit* to a recorded event visible. They say nothing about
whether the recorded sequence of events is a history the product can produce, and an
**append** is not an edit: `_v2_applied_record` and `_v2_reverted_record` compute the
chain fields from the log's own tail, so anyone who can write the journal directory can
extend the chain with a perfectly valid hash. `dataforge/transactions/log.py` exports
``append_applied_event`` and ``append_reverted_event`` with no lifecycle guard, so the
same sequences are reachable from the public API by a caller that simply gets the order
wrong -- no attacker required.

Two of these sequences do not merely look wrong; they make an applied transaction
**unrevertible while audit reports health**:

* ``created, applied, reverted, applied`` replays to ``applied=True`` with
  ``reverted_at`` set, which is exactly the condition that skips the revertibility
  block in ``verify_transaction_log`` -- so the source-hash and snapshot-existence
  checks never run -- while ``revert_transaction`` refuses with "already been
  reverted". The user is left with a modified file, no way back, and a ``verified``
  verdict.
* A second ``created`` event replays over the first and resets ``applied`` to
  ``False``, so ``revert_transaction`` reports "recorded but never applied" for a
  file that was in fact rewritten.

That is the failure mode the constitution names as worse than a loud one: silent
collapse behind a green verdict. This tier is the right one because the input is
hostile-shaped journal content rather than a unit contract, and the verifier is the
component required to fail closed (PRODUCT.md first principle 5: a bug in a verifier
must only ever withhold, never wave through).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from dataforge.transactions.log import (
    TransactionAuditVerdict,
    _event_sha256,
    append_applied_event,
    append_created_transaction,
    append_reverted_event,
    verify_transaction_log,
)
from dataforge.transactions.txn import CellFix, RepairTransaction

TXN_ID = "txn-2026-04-20-a1b2c3"
_SOURCE_BYTES = b"id,amount\n1,100\n2,1020\n"
_POST_BYTES = b"id,amount\n1,100\n2,102\n"


def _sha256_bytes(payload: bytes) -> str:
    """Return the SHA-256 digest for bytes."""
    return hashlib.sha256(payload).hexdigest()


def _transaction(source_path: Path, snapshot_path: Path) -> RepairTransaction:
    """Build a sample file-backed transaction rooted at ``source_path``."""
    return RepairTransaction(
        txn_id=TXN_ID,
        created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        source_path=str(source_path.resolve()),
        source_sha256=_sha256_bytes(_SOURCE_BYTES),
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


def _created_log(tmp_path: Path) -> tuple[Path, RepairTransaction]:
    """Write a source file, a snapshot, and a journal holding only ``created``."""
    source_path = tmp_path / "data.csv"
    snapshot_path = tmp_path / ".dataforge" / "snapshots" / f"{TXN_ID}.bin"
    source_path.write_bytes(_SOURCE_BYTES)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(_SOURCE_BYTES)
    transaction = _transaction(source_path, snapshot_path)
    return append_created_transaction(transaction), transaction


def _applied(log_path: Path, minute: int) -> None:
    """Append a valid ``applied`` event through the public API."""
    append_applied_event(
        log_path,
        TXN_ID,
        post_sha256=_sha256_bytes(_POST_BYTES),
        applied_at=datetime(2026, 4, 20, 12, minute, tzinfo=UTC),
    )


def _reverted(log_path: Path, minute: int) -> None:
    """Append a valid ``reverted`` event through the public API."""
    append_reverted_event(
        log_path,
        TXN_ID,
        reverted_at=datetime(2026, 4, 20, 12, minute, tzinfo=UTC),
    )


def _append_second_created(log_path: Path, transaction: RepairTransaction) -> None:
    """Append a correctly hash-chained duplicate ``created`` event.

    Built from the module's own canonical hashing helper rather than by hand, so the
    resulting chain is genuinely valid: the point of the test is that cryptographic
    validity does not imply a possible history. ``append_created_transaction`` cannot be
    reused here because it opens the log with mode ``"x"``.
    """
    records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    record = {
        "schema_version": 2,
        "schema_name": "transaction_journal_v2",
        "event_index": len(records),
        "event_type": "created",
        "occurred_at": transaction.created_at.isoformat(),
        "previous_event_sha256": records[-1]["event_sha256"],
        "transaction": transaction.model_dump(mode="json"),
    }
    record["event_sha256"] = _event_sha256(record)
    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        handle.write("\n")


class TestTheHonestSequenceStillVerifies:
    """Positive control: the fix must withhold only on impossible histories."""

    def test_created_applied_reverted_verifies(self, tmp_path: Path) -> None:
        log_path, _ = _created_log(tmp_path)
        _applied(log_path, 1)
        _reverted(log_path, 2)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is TransactionAuditVerdict.VERIFIED, report.errors

    def test_created_only_verifies(self, tmp_path: Path) -> None:
        """A journalled-but-unapplied transaction is a legitimate resting state."""
        log_path, _ = _created_log(tmp_path)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is TransactionAuditVerdict.VERIFIED, report.errors

    def test_created_applied_verifies(self, tmp_path: Path) -> None:
        log_path, transaction = _created_log(tmp_path)
        Path(transaction.source_path).write_bytes(_POST_BYTES)
        _applied(log_path, 1)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is TransactionAuditVerdict.VERIFIED, report.errors


class TestImpossibleHistoriesAreWithheld:
    """Each sequence below is hash-valid and must still be refused."""

    def test_revert_without_apply_is_not_verified(self, tmp_path: Path) -> None:
        log_path, _ = _created_log(tmp_path)
        _reverted(log_path, 2)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is not TransactionAuditVerdict.VERIFIED
        assert report.errors

    def test_double_apply_is_not_verified(self, tmp_path: Path) -> None:
        """The second ``post_sha256`` silently wins the replay, so this must be refused."""
        log_path, transaction = _created_log(tmp_path)
        Path(transaction.source_path).write_bytes(_POST_BYTES)
        _applied(log_path, 1)
        _applied(log_path, 3)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is not TransactionAuditVerdict.VERIFIED
        assert report.errors

    def test_double_revert_is_not_verified(self, tmp_path: Path) -> None:
        log_path, _ = _created_log(tmp_path)
        _applied(log_path, 1)
        _reverted(log_path, 2)
        _reverted(log_path, 3)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is not TransactionAuditVerdict.VERIFIED
        assert report.errors

    def test_reapply_after_revert_is_not_verified(self, tmp_path: Path) -> None:
        """The sequence that bypasses the revertibility block entirely.

        ``reverted_at`` is set and ``applied`` is true, so the source-hash and
        snapshot-existence checks are skipped -- here the snapshot is deleted and the
        source no longer matches any recorded hash, and the old verifier still said
        ``verified``.
        """
        log_path, transaction = _created_log(tmp_path)
        _applied(log_path, 1)
        _reverted(log_path, 2)
        _applied(log_path, 3)
        Path(transaction.source_snapshot_path).unlink()
        Path(transaction.source_path).write_bytes(b"id,amount\n1,999\n")

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is not TransactionAuditVerdict.VERIFIED
        assert report.errors

    def test_duplicate_created_is_not_verified(self, tmp_path: Path) -> None:
        """A second ``created`` resets ``applied`` to false, losing revertibility."""
        log_path, transaction = _created_log(tmp_path)
        Path(transaction.source_path).write_bytes(_POST_BYTES)
        _applied(log_path, 1)
        _append_second_created(log_path, transaction)

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is not TransactionAuditVerdict.VERIFIED
        assert report.errors

    def test_created_is_required_first(self, tmp_path: Path) -> None:
        """A journal whose first event is not ``created`` has no premise to replay."""
        log_path, transaction = _created_log(tmp_path)
        _applied(log_path, 1)
        records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
        # Drop the created event and re-chain the remainder so the hashes stay valid.
        tail = records[1]
        tail["event_index"] = 0
        tail["previous_event_sha256"] = None
        del tail["event_sha256"]
        tail["event_sha256"] = _event_sha256(tail)
        log_path.write_text(
            json.dumps(tail, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        report = verify_transaction_log(TXN_ID, log_path=log_path)

        assert report.verdict is not TransactionAuditVerdict.VERIFIED
        assert report.errors
