"""One-pass determinant grouping, reused across the flags that share a group.

## Why this exists

An FD violation flags **every row** of a violating group, so the number of flags grows with the
table: 28,679 flags on 1,000 rows of ``hospital``, collapsing to 7,905 distinct cells. Each flag
then rebuilt *the same* group with its own full scan, in three places -- the repairer's
``_matching_group``, the SMT verifier's footprint computation, and ``DirectVerifier``'s FD check.
That is O(rows) work repeated O(rows) times to recompute an identical answer, which is where the
measured quadratic behaviour comes from.

The grouping itself is one hash pass. This is the standard structure from the FD-discovery
literature -- the partition / position list index of TANE (Huhtala et al., *The Computer Journal*
42(2), 1999), where validity testing is fast "even for a large number of tuples" precisely because
rows are partitioned by value rather than compared pairwise.

## The invalidation contract, which is the whole difficulty

``docs/trust/fd-repair-scalability.md`` declined this optimisation on 2026-08-28 for a real
reason: the table is **mutable on the write path**, and "a stale group would propose a repair from
data that has since changed". Caching over a mutating table is unsound, so the cache needs a
proof, not a hope. Two facts make one available:

1. **Group membership depends only on the determinant columns.** An FD repair writes the
   *dependent* column, so it cannot move any row between groups keyed on the determinant. The
   overwhelmingly common write therefore does not invalidate anything.
2. **Chained FDs are the exception that matters.** If ``B`` is the dependent of ``FD1`` and a
   determinant of ``FD2``, then repairing ``FD1`` *does* change ``FD2``'s grouping. This is not
   hypothetical: on ``hospital``'s oracle premise all 13 dependent columns are also determinants
   of some other dependency.

So the cache is stamped with ``Table.column_revision`` for each determinant column and rebuilt
when any of them changes. Only row *groupings* are cached; **dependent values are always read live
from the table**, so a repair is visible to the next flag immediately.

``TableLike`` also admits pandas DataFrames, which carry no revision counter. For those the index
degrades to building per call -- the previous behaviour, no worse -- rather than guessing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from dataforge.table import TableLike, cell_value, column_values, row_count

#: How a caller materialises one column. Injectable so a caller that already memoises its columns
#: keeps that memo instead of losing it behind this index.
ColumnReader = Callable[[TableLike, str], list[Any]]


@runtime_checkable
class _Revisioned(Protocol):
    """A table that can prove whether one of its columns has changed."""

    def column_revision(self, column: str) -> int: ...


def _stamp(df: TableLike, columns: tuple[str, ...]) -> tuple[int, ...] | None:
    """Return a per-column write stamp, or ``None`` when the table cannot provide one."""
    if not isinstance(df, _Revisioned):
        return None
    try:
        return tuple(df.column_revision(column) for column in columns)
    except KeyError:
        return None


class DeterminantGroupIndex:
    """Groups row indices by determinant tuple, rebuilt only when a determinant changes.

    Held per repairer/verifier instance, which is per repair pass. Nothing is shared across
    passes, so a stale entry cannot outlive the table it describes.

    ``column_reader`` lets a caller supply its own column materialisation -- typically one that
    memoises per instance. Without it, a caller that already cached its columns would lose that
    cache by routing through here, which measured as a **4x whole-suite slowdown** when this class
    was first wired in: the test suite passes pandas frames, which carry no revision counter, so
    every call rebuilt a full group dict *and* re-materialised every determinant column.
    """

    def __init__(
        self,
        column_reader: ColumnReader | None = None,
        *,
        cache_groups: bool = True,
    ) -> None:
        self._read_column: ColumnReader = column_reader or column_values
        self._cache_groups = cache_groups
        self._groups: dict[tuple[str, ...], dict[tuple[str, ...], list[int]]] = {}
        self._stamps: dict[tuple[str, ...], tuple[int, ...]] = {}
        self._table_id: int | None = None
        self.builds = 0
        self.hits = 0
        self.scans = 0

    def _reset_if_new_table(self, df: TableLike) -> None:
        """Drop everything when handed a different table object.

        ``id()`` is used only to detect a *change* of table, never as a cache key: a recycled
        address can make two different tables look identical, so it is paired with the revision
        stamp rather than trusted alone. A false "same table" reading still has to pass the stamp
        check, and a false "different table" reading only costs a rebuild.
        """
        table_id = id(df)
        if self._table_id != table_id:
            self._groups.clear()
            self._stamps.clear()
            self._table_id = table_id

    def groups_for(
        self,
        df: TableLike,
        determinant: tuple[str, ...],
        stamp: tuple[int, ...],
    ) -> dict[tuple[str, ...], list[int]]:
        """Return ``determinant value tuple -> row indices``, built at most once per stamp."""
        self._reset_if_new_table(df)
        cached = self._groups.get(determinant)
        if cached is not None and self._stamps.get(determinant) == stamp:
            self.hits += 1
            return cached

        columns = [self._read_column(df, column) for column in determinant]
        groups: dict[tuple[str, ...], list[int]] = {}
        for index in range(row_count(df)):
            key = tuple(str(values[index]) for values in columns)
            groups.setdefault(key, []).append(index)

        self._groups[determinant] = groups
        self._stamps[determinant] = stamp
        self.builds += 1
        return groups

    def rows_for_key(
        self,
        df: TableLike,
        determinant: tuple[str, ...],
        key: tuple[str, ...],
    ) -> list[int]:
        """Return the rows whose determinant tuple equals ``key``, empty when absent.

        With a revision stamp this is a dict lookup into a grouping built once. Without one --
        a pandas frame, which cannot prove it has not mutated -- it falls back to a targeted scan
        that collects only matching rows. That is the pre-index behaviour: no faster, but it does
        not pay to build a full grouping that can never be reused.

        ``cache_groups=False`` forces the scan even when a stamp is available. Building all groups
        pays off only if the instance outlives the call, and that is an argument from lifetime, not
        from a stopwatch. The repairer's index does outlive the call -- one per repair pass, and on
        a ``Table`` that measures 1 build, 200 reuses, 0 rescans with propose cost flat at about
        0.01 ms/flag from 1,000 to 16,000 rows. ``SchemaToSMT`` is rebuilt per fix, so for it a
        full grouping over every row can never be reused while a targeted scan collects only the
        rows the constraint needs.

        An earlier version of this docstring justified that with "65.5 ms/fix with caching against
        16.0 ms with the scan on ``hospital``". **That comparison was invalid** and is retracted:
        the 65.5 ms figure was taken on a ``Table`` and the 16.0 ms figure on a pandas frame, and
        caching is only reachable on ``Table``, so the two runs differed in representation as well
        as configuration. Re-measuring by wall clock did not settle it either -- three repeats of
        identical code spanned 79.8 to 352.2 ms/fix. Only a deterministic counted metric can decide
        this, and until one exists the flag rests on the lifetime argument.
        """
        stamp = _stamp(df, determinant) if self._cache_groups else None
        if stamp is not None:
            return self.groups_for(df, determinant, stamp).get(key, [])

        self.scans += 1
        columns = [self._read_column(df, column) for column in determinant]
        return [
            index
            for index in range(row_count(df))
            if all(
                str(values[index]) == expected
                for values, expected in zip(columns, key, strict=True)
            )
        ]

    def key_for_row(
        self,
        df: TableLike,
        determinant: tuple[str, ...],
        row: int,
    ) -> tuple[str, ...]:
        """Return one row's determinant tuple with O(len(determinant)) cell reads.

        Deliberately not via ``column_values``: materialising whole columns to read a single row
        is how an index that was supposed to remove O(rows) work reintroduces it.
        """
        return tuple(cell_value(df, row, column) for column in determinant)

    def rows_for_row(
        self,
        df: TableLike,
        determinant: tuple[str, ...],
        row: int,
    ) -> list[int]:
        """Return the rows sharing ``row``'s determinant tuple, including ``row`` itself."""
        return self.rows_for_key(df, determinant, self.key_for_row(df, determinant, row))
