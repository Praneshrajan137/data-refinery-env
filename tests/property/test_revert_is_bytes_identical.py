"""Property test: apply followed by revert restores exact original bytes."""

from __future__ import annotations

import csv
import hashlib
import secrets
import tempfile
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dataforge.cli.repair import apply_fixes_to_csv
from dataforge.transactions.log import append_applied_event, append_created_transaction
from dataforge.transactions.revert import revert_transaction
from dataforge.transactions.txn import CellFix, RepairTransaction


def _sha256_bytes(payload: bytes) -> str:
    """Return the SHA-256 digest for bytes."""
    return hashlib.sha256(payload).hexdigest()


@st.composite
def _csv_and_fixes(draw: st.DrawFn) -> tuple[bytes, list[CellFix]]:
    alphabet = "abcxyz0123456789"
    headers = draw(
        st.lists(
            st.text(alphabet=alphabet, min_size=1, max_size=6),
            min_size=1,
            max_size=3,
            unique=True,
        )
    )
    row_count = draw(st.integers(min_value=1, max_value=5))
    rows: list[dict[str, str]] = []
    for _ in range(row_count):
        row: dict[str, str] = {}
        for header in headers:
            row[header] = draw(st.text(alphabet=alphabet, min_size=0, max_size=6))
        rows.append(row)

    all_cells = [(row_index, column) for row_index in range(row_count) for column in headers]
    chosen_cells = draw(
        st.lists(
            st.sampled_from(all_cells),
            min_size=1,
            max_size=min(3, len(all_cells)),
            unique=True,
        )
    )

    fixes: list[CellFix] = []
    for row_index, column in chosen_cells:
        old_value = rows[row_index][column]
        new_value = draw(
            st.text(alphabet=alphabet, min_size=1, max_size=6).filter(
                lambda value, old_value=old_value: value != old_value
            )
        )
        fixes.append(
            CellFix(
                row=row_index,
                column=column,
                old_value=old_value,
                new_value=new_value,
                detector_id="property_test",
            )
        )

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8"), fixes


@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(_csv_and_fixes())
def test_revert_is_bytes_identical(csv_and_fixes: tuple[bytes, list[CellFix]]) -> None:
    original_bytes, fixes = csv_and_fixes
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        source_path = temp_path / "data.csv"
        snapshot_path = temp_path / ".dataforge" / "snapshots" / "txn.bin"
        source_path.write_bytes(original_bytes)
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_bytes(original_bytes)

        txn = RepairTransaction(
            txn_id=f"txn-2026-04-20-{secrets.token_hex(3)}",
            created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            source_path=str(source_path.resolve()),
            source_sha256=_sha256_bytes(original_bytes),
            source_snapshot_path=str(snapshot_path.resolve()),
            fixes=fixes,
            applied=False,
        )
        log_path = append_created_transaction(txn)

        post_sha256 = apply_fixes_to_csv(source_path, fixes)
        append_applied_event(log_path, txn.txn_id, post_sha256=post_sha256)

        reverted = revert_transaction(txn.txn_id, search_root=temp_path)

        assert reverted.reverted_at is not None
        assert source_path.read_bytes() == original_bytes
        assert _sha256_bytes(source_path.read_bytes()) == txn.source_sha256
