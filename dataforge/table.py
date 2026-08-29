"""Small string-preserving table primitives for DataForge core paths.

The CLI hot path should not need pandas just to profile or repair a CSV.
This module provides the narrow DataFrame-like surface that DataForge's
detectors, repairers, and verifier actually need.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any, Protocol, cast, overload


class TableLike(Protocol):
    """Protocol for the tabular surface consumed by DataForge core logic.

    Two implementations satisfy this: `Table` below, and `pandas.DataFrame`. They are **not**
    interchangeable for performance, and the split in practice is total:

    * the CLI reads CSV through `read_csv`, so user-facing runs always use `Table`;
    * benchmarks and evaluation harnesses in `scripts/` build a `pandas.DataFrame`.

    The consequence worth knowing about is `column_revision`, which `Table` provides and
    `DataFrame` does not. `DeterminantGroupIndex` uses it as a cache stamp, so a `DataFrame`
    silently takes the uncached branch -- one scan per fix -- while `Table` reuses the grouping.
    A harness measuring a `DataFrame` is therefore not measuring the shipped path.

    Verdict parity between the two is asserted by `tests/property/test_representation_parity.py`
    and must stay asserted; cost parity does not hold and is not claimed. New performance
    measurements should use `Table`. See `docs/trust/fd-repair-scalability.md` for which published
    figures are affected.
    """

    @property
    def columns(self) -> Any: ...

    @property
    def index(self) -> Any: ...

    @property
    def at(self) -> Any: ...

    def __getitem__(self, key: str) -> Any: ...

    def copy(self, deep: bool = True) -> Any: ...

    def to_csv(
        self,
        buffer: io.StringIO,
        *,
        index: bool = False,
        lineterminator: str = "\n",
    ) -> None: ...


class ColumnView(Sequence[str]):
    """Read-only column view with the small API repairers expect."""

    def __init__(self, values: Sequence[str]) -> None:
        self._values = values

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    @overload
    def __getitem__(self, index: int) -> str: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[str]: ...

    def __getitem__(self, index: int | slice) -> str | Sequence[str]:
        return self._values[index]

    def tolist(self) -> list[str]:
        """Return a list copy, matching pandas Series enough for detectors."""
        return list(self._values)


class _AtIndexer:
    """``table.at[row, column]`` getter/setter compatibility shim."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def __getitem__(self, key: tuple[int, str]) -> str:
        row, column = key
        return self._table.cell(row, column)

    def __setitem__(self, key: tuple[int, str], value: object) -> None:
        row, column = key
        self._table.set_cell(row, column, value)


class Table:
    """In-memory CSV table with string-preserving cells."""

    def __init__(self, columns: Sequence[str], rows: Iterable[dict[str, object]]) -> None:
        self._columns = [str(column) for column in columns]
        # Membership is checked on EVERY cell read and write. `column not in self._columns` is a
        # linear scan of a list, so at 100 columns it cost 100 string comparisons to fetch one
        # value -- and it multiplied every per-row loop in the detectors, the repairers and both
        # verifiers. The set is derived from the same list in the same place, so the two cannot
        # disagree; `_columns` is kept because CSV column ORDER is part of the output contract.
        self._column_set = frozenset(self._columns)
        self._rows: list[dict[str, str]] = [
            {
                column: "" if row.get(column) is None else str(row.get(column, ""))
                for column in self._columns
            }
            for row in rows
        ]
        # Per-column write counter, so a cache over this table can prove it is not stale instead
        # of hoping. An index that groups rows by a set of columns is invalidated by a write to
        # one of THOSE columns and by nothing else -- notably, an FD repair writes the dependent
        # column, which cannot change a grouping keyed on the determinant. Without a stamp the
        # only safe options are rebuilding per call (no reuse) or caching over a mutable table
        # (unsound), which is exactly why docs/trust/fd-repair-scalability.md declined the index.
        self._column_revision: dict[str, int] = dict.fromkeys(self._columns, 0)
        self.at = _AtIndexer(self)

    @property
    def columns(self) -> list[str]:
        """Return column names in CSV order."""
        return list(self._columns)

    @property
    def index(self) -> range:
        """Return zero-based row positions."""
        return range(len(self._rows))

    @property
    def empty(self) -> bool:
        """Return whether the table has no rows."""
        return not self._rows

    @overload
    def __getitem__(self, key: str) -> ColumnView: ...

    @overload
    def __getitem__(self, key: list[str]) -> Table: ...

    @overload
    def __getitem__(self, key: tuple[str, ...]) -> Table: ...

    def __getitem__(self, key: str | list[str] | tuple[str, ...]) -> ColumnView | Table:
        if isinstance(key, str):
            if key not in self._column_set:
                raise KeyError(key)
            return ColumnView([row.get(key, "") for row in self._rows])
        columns = [str(column) for column in key]
        for column in columns:
            if column not in self._column_set:
                raise KeyError(column)
        return Table(
            columns, ({column: row.get(column, "") for column in columns} for row in self._rows)
        )

    def __len__(self) -> int:
        return len(self._rows)

    def copy(self, deep: bool = True) -> Table:
        """Return an independent table copy."""
        del deep
        return Table(self._columns, (dict(row) for row in self._rows))

    def cell(self, row: int, column: str) -> str:
        """Return a cell value."""
        if column not in self._column_set:
            raise KeyError(column)
        return self._rows[row].get(column, "")

    def set_cell(self, row: int, column: str, value: object) -> None:
        """Set a cell value after validating the column."""
        if column not in self._column_set:
            raise KeyError(column)
        self._rows[row][column] = "" if value is None else str(value)
        self._column_revision[column] = self._column_revision.get(column, 0) + 1

    def column_revision(self, column: str) -> int:
        """Return how many times this column has been written.

        Monotonic and per column. A cache keyed on a tuple of these for the columns it depends on
        is invalidated exactly when one of those columns changes.
        """
        if column not in self._column_set:
            raise KeyError(column)
        return self._column_revision.get(column, 0)

    def iter_records(self, columns: Sequence[str] | None = None) -> Iterator[dict[str, str]]:
        """Yield row dictionaries in table order."""
        selected = self._columns if columns is None else [str(column) for column in columns]
        for row in self._rows:
            yield {column: row.get(column, "") for column in selected}

    def to_dict(self, orient: str = "records") -> list[dict[str, str]]:
        """Return records in the pandas-compatible orientation used by DataForge."""
        if orient != "records":
            raise ValueError("Only orient='records' is supported.")
        return list(self.iter_records())

    def to_csv(
        self, buffer: io.StringIO, *, index: bool = False, lineterminator: str = "\n"
    ) -> None:
        """Write the table as CSV to a text buffer."""
        if index:
            raise ValueError("Table.to_csv does not support index=True.")
        writer = csv.DictWriter(buffer, fieldnames=self._columns, lineterminator=lineterminator)
        writer.writeheader()
        for row in self._rows:
            writer.writerow({column: row.get(column, "") for column in self._columns})


def read_csv(path: Path) -> Table:
    """Read a CSV as a string-preserving ``Table``."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        return Table(columns, reader)


def table_to_csv_bytes(table: TableLike) -> bytes:
    """Serialize a table-like object to UTF-8 CSV bytes."""
    output = io.StringIO()
    if isinstance(table, Table):
        table.to_csv(output, index=False, lineterminator="\n")
    else:
        # pandas-compatible fallback for tests and optional integrations.
        table.to_csv(output, index=False, lineterminator="\n")
    return output.getvalue().encode("utf-8")


def column_names(table: TableLike) -> list[str]:
    """Return table column names as strings."""
    return [str(column) for column in table.columns]


def row_count(table: TableLike) -> int:
    """Return the number of rows in a table-like object."""
    return len(table.index)


def column_values(table: TableLike, column: str) -> list[Any]:
    """Return all values for one column."""
    values = table[column]
    if hasattr(values, "tolist"):
        return list(values.tolist())
    return list(values)


def cell_value(table: TableLike, row: int, column: str) -> str:
    """Return a cell value as a string."""
    return str(table.at[row, column])


def set_cell_value(table: TableLike, row: int, column: str, value: object) -> None:
    """Set a cell value on a table-like object."""
    table.at[row, column] = value


def copy_table(table: TableLike) -> TableLike:
    """Return a deep copy of a table-like object."""
    copied = table.copy(deep=True)
    return cast(TableLike, copied)
