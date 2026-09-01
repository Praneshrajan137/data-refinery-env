"""Revert against a legacy v1 journal, which carries no hash chain.

``revert_transaction`` admits two audit verdicts: ``VERIFIED`` and
``LEGACY_UNVERIFIED`` (``dataforge/transactions/revert.py``). The second is the only
write path in the product authorised by a journal that cannot be cryptographically
verified, and until this file existed it had **no test and no mutant** -- every
``revert_transaction`` call site in the suite built a v2 log, and the committed v1
fixture was only ever fed to ``verify_transaction_log``.

The allowance is deliberate, not an oversight, so these tests pin the reduced
guarantee rather than removing it. What must hold on the legacy path is that the
*byte-level* checks are undiminished: the file must still match ``post_sha256``, the
restored bytes must still equal ``source_sha256``, and a failure must still roll the
restore back. If any of those degrade for v1 logs, ``LEGACY_UNVERIFIED`` becomes an
unguarded write and should be refused instead.

One deliberate choice: the v1 created record is built with ``_v1_created_record``
rather than hand-written JSON. Restating the legacy schema in a test would let the test
and the reader drift apart while both stayed green -- the same defect that
``scripts/ci/gate_population.py`` exists to prevent one level up.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataforge.transactions.log import (
    LEGACY_SCHEMA_VERSION,
    TransactionAuditVerdict,
    _v1_created_record,
    _write_jsonl_line,
    append_applied_event,
    transaction_log_path_for,
    verify_transaction_log,
)
from dataforge.transactions.revert import TransactionRevertError, revert_transaction
from dataforge.transactions.txn import CellFix, RepairTransaction

_SOURCE_BYTES = b"id,amount\n1,100\n2,1020\n"
_POST_BYTES = b"id,amount\n1,100\n2,102\n"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _legacy_applied_transaction(
    tmp_path: Path,
    *,
    txn_id: str,
    snapshot_bytes: bytes = _SOURCE_BYTES,
    recorded_source_sha256: str | None = None,
) -> tuple[Path, Path, Path]:
    """Create a v1 journal for an applied transaction and return its paths.

    Args:
        tmp_path: Test working directory.
        txn_id: Transaction identifier.
        snapshot_bytes: Bytes to place in the snapshot. Defaults to the true source
            bytes; a caller passing something else is simulating a corrupted snapshot.
        recorded_source_sha256: Hash recorded in the journal. Defaults to the digest of
            the true source bytes.

    Returns:
        ``(source_path, snapshot_path, log_path)``.
    """
    source_path = tmp_path / "data.csv"
    source_path.write_bytes(_SOURCE_BYTES)

    snapshot_path = tmp_path / ".dataforge" / "snapshots" / f"{txn_id}.bin"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(snapshot_bytes)

    # The source is now in its post-apply state, as it would be after a real apply.
    source_path.write_bytes(_POST_BYTES)

    txn = RepairTransaction(
        txn_id=txn_id,
        created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        source_path=str(source_path.resolve()),
        source_sha256=recorded_source_sha256 or _sha256(_SOURCE_BYTES),
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

    log_path = transaction_log_path_for(source_path, txn_id)
    _write_jsonl_line(log_path, _v1_created_record(txn), create=True)
    # append_applied_event detects the v1 log and continues in v1 rather than mixing
    # schema versions, so this exercises the real writer instead of a fixture.
    append_applied_event(log_path, txn_id, post_sha256=_sha256(_POST_BYTES))
    return source_path, snapshot_path, log_path


class TestLegacyJournalIsGenuinelyUnverifiable:
    """The premise these tests rest on: a v1 log really has no chain to check."""

    def test_v1_log_audits_as_legacy_unverified_with_no_head_hash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _, _, log_path = _legacy_applied_transaction(tmp_path, txn_id="txn-2026-04-20-1eac01")

        report = verify_transaction_log("txn-2026-04-20-1eac01", log_path=log_path)

        assert report.verdict == TransactionAuditVerdict.LEGACY_UNVERIFIED
        assert report.schema_version == LEGACY_SCHEMA_VERSION
        # Non-vacuity: if this were None because the log were malformed rather than
        # legacy, the tests below would be checking the wrong path.
        assert report.head_sha256 is None
        assert report.event_count == 2


class TestLegacyRevertRestoresBytes:
    """The allowance itself: a legacy transaction remains recoverable."""

    def test_revert_of_legacy_journal_restores_exact_bytes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        source_path, _, log_path = _legacy_applied_transaction(
            tmp_path, txn_id="txn-2026-04-20-1eac02"
        )
        assert source_path.read_bytes() == _POST_BYTES

        reverted = revert_transaction("txn-2026-04-20-1eac02")

        assert source_path.read_bytes() == _SOURCE_BYTES
        assert reverted.reverted_at is not None
        # The revert event must have been appended in v1, not silently upgraded to v2 --
        # a mixed-version log audits as MALFORMED and would be unrevertible thereafter.
        assert (
            verify_transaction_log("txn-2026-04-20-1eac02", log_path=log_path).verdict
            == TransactionAuditVerdict.LEGACY_UNVERIFIED
        )


class TestLegacyRevertKeepsEveryByteLevelCheck:
    """The checks that carry the guarantee when the chain cannot."""

    def test_legacy_revert_still_refuses_when_post_state_hash_differs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        source_path, _, _ = _legacy_applied_transaction(tmp_path, txn_id="txn-2026-04-20-1eac03")
        edited = b"id,amount\n1,100\n2,999\n"
        source_path.write_bytes(edited)

        with pytest.raises(TransactionRevertError, match="post-state hash"):
            revert_transaction("txn-2026-04-20-1eac03")

        # The user's edit survives untouched. A revert that clobbered it would be a
        # data-loss bug reachable only on the legacy path.
        assert source_path.read_bytes() == edited

    def test_legacy_revert_rolls_back_when_snapshot_does_not_match_recorded_hash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        # A snapshot whose bytes disagree with the recorded source hash is exactly what a
        # forged v1 journal would produce, since nothing signs either one.
        source_path, _, _ = _legacy_applied_transaction(
            tmp_path,
            txn_id="txn-2026-04-20-1eac04",
            snapshot_bytes=b"id,amount\n1,666\n2,666\n",
            recorded_source_sha256=_sha256(_SOURCE_BYTES),
        )

        with pytest.raises(TransactionRevertError, match="integrity verification"):
            revert_transaction("txn-2026-04-20-1eac04")

        # Rolled back to the post-apply state, NOT left holding the forged snapshot.
        assert source_path.read_bytes() == _POST_BYTES

    def test_legacy_revert_refuses_twice(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        source_path, _, _ = _legacy_applied_transaction(tmp_path, txn_id="txn-2026-04-20-1eac05")
        revert_transaction("txn-2026-04-20-1eac05")

        with pytest.raises(TransactionRevertError, match="already been reverted"):
            revert_transaction("txn-2026-04-20-1eac05")

        assert source_path.read_bytes() == _SOURCE_BYTES


class TestLegacyRevertIsReportedHonestly:
    """A restore backed by an unverifiable journal must not read as a plain success."""

    def test_cli_names_the_legacy_verdict_and_does_not_claim_verification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typer.testing import CliRunner

        from dataforge.cli import app

        monkeypatch.chdir(tmp_path)
        _legacy_applied_transaction(tmp_path, txn_id="txn-2026-04-20-1eac06")

        result = CliRunner().invoke(app, ["revert", "txn-2026-04-20-1eac06"])

        assert result.exit_code == 0
        output = result.output
        assert "legacy_unverified" in output
        # The specific regression: an unqualified green success that told the user
        # nothing about what backed the restore.
        assert "legacy v1 journal" in output.lower()

    def test_cli_json_receipt_carries_the_legacy_verdict(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import json

        from typer.testing import CliRunner

        from dataforge.cli import app

        monkeypatch.chdir(tmp_path)
        _legacy_applied_transaction(tmp_path, txn_id="txn-2026-04-20-1eac07")

        result = CliRunner().invoke(app, ["revert", "txn-2026-04-20-1eac07", "--json"])

        assert result.exit_code == 0
        receipt = json.loads(result.output)
        assert receipt["ok"] is True
        assert receipt["audit_verdict"] == "legacy_unverified"
        # No chain exists, so there is no revert-event hash to report. Reporting one
        # here would be the vacuous-success shape this file exists to rule out.
        assert receipt["revert_event_sha256"] is None
        assert receipt["restored_source_sha256"] == receipt["expected_source_sha256"]
