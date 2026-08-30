"""The CSV write path must verify reversibility, not merely achieve it by ordering.

``apply_transaction`` enforced two invariants at the mutation primitive -- proven-only and
constraint-checkable-only -- and got reversibility from the *sequence* of its steps:
``create_repair_transaction`` writes a snapshot and journal, then ``_apply_fixes_to_csv``
mutates the file. A correct sequence does produce a recoverable state, so nothing was
wrong. But ordering is not a precondition, and the gap was silent in the worst direction:
a snapshot that never reached disk intact is indistinguishable from a good one until the
revert that needs it, which is the moment the user has no other copy.

``PRODUCT.md`` ranks reversibility above proven-only, and
``docs/trust/write-surface-uniformity.md`` records that Round 1 of the uniformity work
missed precisely "the one with a stronger promise and no test". This is that test.

The warehouse counterpart is ``TestPlanReversibilityIsAPrecondition`` in
``tests/unit/test_table_store_proven_gate.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.engine.repair import (
    create_repair_transaction,
    enforce_snapshot_recoverable,
)
from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix

_CSV = b"id,state,city\n1,MA,boston\n2,MA,bostonn\n"


def _fix() -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=2,
            column="city",
            old_value="bostonn",
            new_value="boston",
            detector_id="fd_violation",
        ),
        reason="test",
        confidence=1.0,
        provenance="deterministic",
    )


def _transaction(tmp_path: Path) -> tuple[object, Path]:
    source = tmp_path / "t.csv"
    source.write_bytes(_CSV)
    transaction, _ = create_repair_transaction(source, [_fix()], _CSV)
    return transaction, Path(transaction.source_snapshot_path)


class TestSnapshotRecoverableGate:
    def test_a_good_snapshot_passes(self, tmp_path: Path) -> None:
        """Non-vacuity. Without this, a gate that refuses everything looks correct."""
        transaction, snapshot = _transaction(tmp_path)

        assert snapshot.is_file()
        enforce_snapshot_recoverable(transaction)

    def test_a_missing_snapshot_is_refused(self, tmp_path: Path) -> None:
        transaction, snapshot = _transaction(tmp_path)
        snapshot.unlink()

        with pytest.raises(Exception, match="does not exist"):
            enforce_snapshot_recoverable(transaction)

    def test_a_truncated_snapshot_is_refused(self, tmp_path: Path) -> None:
        """The reachable failure: the file exists, so an existence check would pass it."""
        transaction, snapshot = _transaction(tmp_path)
        snapshot.write_bytes(_CSV[: len(_CSV) // 2])

        with pytest.raises(Exception, match="does not match the recorded source digest"):
            enforce_snapshot_recoverable(transaction)

    def test_a_snapshot_of_different_content_is_refused(self, tmp_path: Path) -> None:
        """Same length, different bytes -- a size check would not catch this."""
        transaction, snapshot = _transaction(tmp_path)
        snapshot.write_bytes(_CSV.replace(b"boston", b"BOSTON"))

        with pytest.raises(Exception, match="does not match the recorded source digest"):
            enforce_snapshot_recoverable(transaction)

    def test_an_unrecorded_snapshot_path_is_refused(self, tmp_path: Path) -> None:
        transaction, _ = _transaction(tmp_path)
        stripped = transaction.model_copy(update={"source_snapshot_path": None})

        with pytest.raises(Exception, match="records no source snapshot"):
            enforce_snapshot_recoverable(stripped)


class TestApplyTransactionRefusesAnUnrecoverableWrite:
    """The gate must be wired into the primitive, not merely importable."""

    def test_apply_refuses_and_leaves_the_source_untouched(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Simulate a snapshot that does not survive: the user's bytes must not change.

        Patching the snapshot writer is the honest way to reach this state -- the real
        failure is a partial write or a disappearing temp dir, neither of which a test can
        cause reliably.
        """
        import dataforge.engine.repair as repair_module

        source = tmp_path / "t.csv"
        source.write_bytes(_CSV)

        real_writer = repair_module._write_snapshot_once

        def _truncating_writer(snapshot_path: Path, source_bytes: bytes) -> None:
            real_writer(snapshot_path, source_bytes[: len(source_bytes) // 2])

        monkeypatch.setattr(repair_module, "_write_snapshot_once", _truncating_writer)

        with pytest.raises(Exception, match="does not match the recorded source digest"):
            repair_module.apply_transaction(
                source,
                [_fix()],
                _CSV,
                covered_columns=frozenset({"city", "state"}),
            )

        assert source.read_bytes() == _CSV, "a refused apply must not have written anything"
