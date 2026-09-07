"""The bench's subject must be one the FD index can actually index.

## What regressed, and why a stopwatch cannot police it

`DeterminantGroupIndex` caches determinant groupings only when the table can prove it has not
mutated, which means `Table.column_revision`. A pandas frame has no revision counter, so
`dataforge/fd_index.py` degrades to an O(rows) rescan per lookup -- documented, deliberate, and
silent. `_repairs_from_proposed_fixes` was the index's only caller and passed a frame, so the index
never indexed once: 64,227 rescans driving 11.7M pandas scalar lookups, and `propose_fixes` taking
207s on 1,000 rows while the detectors took 0.94s.

`fd_index`'s own docstring retracts an earlier performance claim and concludes that **"only a
deterministic counted metric can decide this"**, having measured three repeats of identical code
spanning 79.8 to 352.2 ms/fix. My own two readings of this pass disagreed by 3x (65s vs 209s). So
these tests assert `builds`/`hits`/`scans` and the stampability of the subject, and assert no
timing at all.
"""

from __future__ import annotations

import pandas as pd

from dataforge.bench.methods import _as_table
from dataforge.fd_index import DeterminantGroupIndex, _stamp
from dataforge.table import Table

_FRAME = pd.DataFrame(
    {
        "zip": ["36301", "36301", "99999"],
        "city": ["dothan", "dothan", "nowhere"],
        "score": ["1", "2", "3"],
    }
)


class TestTheFastPathIsReachable:
    """The invariant that regressed: the bench's subject must be stampable."""

    def test_the_bench_subject_can_be_stamped(self) -> None:
        """This is the whole fix. An unstampable subject silently disables the index."""
        assert _stamp(_as_table(_FRAME), ("zip",)) is not None

    def test_a_pandas_frame_cannot_be_stamped(self) -> None:
        """Pins the reason, so the test above cannot be read as arbitrary."""
        assert _stamp(_FRAME, ("zip",)) is None

    def test_as_table_returns_a_table(self) -> None:
        assert isinstance(_as_table(_FRAME), Table)


class TestCountedMetric:
    """builds/hits/scans, because wall clock on this pass is not reproducible."""

    def test_a_stampable_subject_builds_once_and_then_hits(self) -> None:
        index = DeterminantGroupIndex()
        subject = _as_table(_FRAME)

        for _ in range(5):
            index.rows_for_row(subject, ("zip",), 0)

        assert index.scans == 0, "a stampable subject must never fall back to a rescan"
        assert index.builds == 1, "the grouping must be built once, not per lookup"
        assert index.hits == 4

    def test_an_unstampable_subject_rescans_every_time(self) -> None:
        """The behaviour the bench used to get. Pinned so the contrast is not folklore."""
        index = DeterminantGroupIndex()

        for _ in range(5):
            index.rows_for_row(_FRAME, ("zip",), 0)

        assert index.scans == 5
        assert index.builds == 0
        assert index.hits == 0

    def test_both_subjects_return_the_same_rows(self) -> None:
        """Speed that changes the answer is not speed. The two paths must agree."""
        fast = DeterminantGroupIndex().rows_for_row(_as_table(_FRAME), ("zip",), 0)
        slow = DeterminantGroupIndex().rows_for_row(_FRAME, ("zip",), 0)
        assert fast == slow == [0, 1]


class TestConversionFidelity:
    """A faster subject that alters a cell would move the anchor."""

    def test_every_cell_survives_the_conversion(self) -> None:
        table = _as_table(_FRAME)
        for row in range(len(_FRAME)):
            for column in _FRAME.columns:
                assert table.cell(row, column) == _FRAME.iloc[row][column]

    def test_column_order_is_preserved(self) -> None:
        """CSV column order is part of the output contract."""
        assert list(_as_table(_FRAME).columns) == list(_FRAME.columns)

    def test_mixed_dtype_rows_are_not_coerced(self) -> None:
        """`iterrows` would unify these to one dtype; column-major materialisation must not.

        This is the trap the helper avoids. Both current corpora are uniformly `object`, so the
        bug would have surfaced only on a future dataset -- which is exactly how the index came to
        stop indexing without anyone noticing.
        """
        mixed = pd.DataFrame({"text": ["a", "b"], "number": [1, 2]})
        table = _as_table(mixed)
        assert table.cell(0, "number") == "1"
        assert table.cell(0, "text") == "a"

    def test_the_table_does_not_share_storage_with_the_frame(self) -> None:
        """The three copy(deep=True) calls this replaced existed to guarantee isolation."""
        frame = _FRAME.copy(deep=True)
        table = _as_table(frame)
        table.set_cell(0, "city", "MUTATED")
        assert frame.iloc[0]["city"] == "dothan"
