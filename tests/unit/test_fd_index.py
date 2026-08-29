"""The grouping cache must be provably fresh, on the only path the product ships.

``DeterminantGroupIndex`` caches a row grouping keyed on determinant columns, and the table it
describes is **mutated while repairs are applied**. `docs/trust/fd-repair-scalability.md` declined
this optimisation once for exactly that reason: "a stale group would propose a repair from data that
has since changed". It was allowed back only with an invalidation contract -- a per-column write
counter on ``Table`` -- and the argument that FD repairs write the *dependent* column, which cannot
move a row between groups keyed on the *determinant*.

That argument is sound. It was also, until this file existed, **entirely unexecuted**: there was no
test for ``dataforge/fd_index.py`` at all. Worse, the caching branch is the one the shipped CLI
*always* takes, because ``read_csv`` returns a ``Table``, while every harness and almost every test
in the repository passes a ``pandas.DataFrame``, which has no write counter and so silently takes
the uncached scan branch. The tested path and the shipped path were disjoint.

The chained-FD case is the one that can actually bite, and it is not hypothetical: on hospital's
oracle premise all 13 dependent columns are also determinants of some other dependency. If ``FD1``
writes ``B`` and ``FD2`` is keyed on ``B``, then repairing ``FD1`` genuinely does change ``FD2``'s
grouping, and a cache that did not notice would repair against stale evidence.

Every test here asserts on ``builds`` / ``hits`` / ``scans``, which are counters rather than
timings, so they are reproducible on any machine.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.fd_index import DeterminantGroupIndex
from dataforge.table import Table, set_cell_value


def _table() -> Table:
    """Two determinant groups on ``code``, so a grouping is non-trivial."""
    rows = [
        {"code": "A", "city": "Paris", "zone": "north"},
        {"code": "A", "city": "Paris", "zone": "north"},
        {"code": "A", "city": "Lyon", "zone": "north"},
        {"code": "B", "city": "Nice", "zone": "south"},
        {"code": "B", "city": "Nice", "zone": "south"},
    ]
    return Table(["code", "city", "zone"], rows)


def test_a_table_supports_the_stamp_and_a_dataframe_does_not() -> None:
    """The precondition for everything else, and the reason the shipped path differs from tests."""
    table = _table()
    assert table.column_revision("code") == 0

    frame = pd.DataFrame({"code": ["A", "A"], "city": ["Paris", "Lyon"]})
    assert not hasattr(frame, "column_revision"), (
        "if pandas ever grows this method the fallback branch becomes unreachable and these tests "
        "would silently stop covering it"
    )


def test_the_grouping_is_built_once_and_then_reused() -> None:
    """The whole point: one pass, then lookups."""
    table = _table()
    index = DeterminantGroupIndex()

    first = index.rows_for_key(table, ("code",), ("A",))
    assert first == [0, 1, 2]
    assert (index.builds, index.hits, index.scans) == (1, 0, 0)

    for _ in range(10):
        assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    assert index.builds == 1, "rebuilt a grouping nothing invalidated"
    assert index.hits == 10
    assert index.scans == 0


def test_writing_the_dependent_column_does_not_invalidate() -> None:
    """The load-bearing half of the contract, and the reason caching is worth anything.

    An FD repair writes the dependent column. If that invalidated the determinant grouping, the
    cache would be rebuilt on every applied fix and the optimisation would be worthless.
    """
    table = _table()
    index = DeterminantGroupIndex()
    index.rows_for_key(table, ("code",), ("A",))
    assert index.builds == 1

    set_cell_value(table, 2, "city", "Marseille")

    assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    assert index.builds == 1, (
        "a write to 'city' rebuilt a grouping keyed on 'code'. Group membership cannot depend on a "
        "column the key does not include."
    )
    assert index.hits == 1


def test_writing_a_determinant_column_does_invalidate_and_regroup() -> None:
    """The safety half: the cache must notice, and must return the NEW grouping."""
    table = _table()
    index = DeterminantGroupIndex()
    assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    assert index.builds == 1

    # Row 2 leaves group A and joins group B.
    set_cell_value(table, 2, "code", "B")

    assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1], "stale membership returned"
    assert index.builds == 2, "a write to the determinant did not invalidate the grouping"
    assert index.rows_for_key(table, ("code",), ("B",)) == [2, 3, 4]


def test_chained_fd_write_invalidates_the_downstream_grouping() -> None:
    """The case the contract exists for, and the one that is not hypothetical.

    ``code -> city`` and ``city -> zone`` are chained. Repairing the first writes ``city``, which is
    the determinant of the second, so the second's grouping genuinely changes. A cache that treated
    "a dependent was written" as "nothing to do" would hand the next flag a stale group.
    """
    table = _table()
    index = DeterminantGroupIndex()

    assert index.rows_for_key(table, ("city",), ("Paris",)) == [0, 1]
    assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    assert index.builds == 2

    # Repairing code -> city rewrites row 2's city from Lyon to Paris.
    set_cell_value(table, 2, "city", "Paris")

    assert index.rows_for_key(table, ("city",), ("Paris",)) == [0, 1, 2], (
        "the downstream grouping keyed on 'city' is stale: row 2 joined the Paris group when the "
        "upstream FD was repaired, and this is the chained-FD case the invalidation contract exists "
        "for"
    )
    assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    assert index.builds == 3, "expected exactly the city grouping to be rebuilt"


def test_composite_determinants_invalidate_on_any_member() -> None:
    """A grouping keyed on two columns is stale if either changes."""
    table = _table()
    index = DeterminantGroupIndex()
    assert index.rows_for_key(table, ("code", "zone"), ("A", "north")) == [0, 1, 2]
    assert index.builds == 1

    set_cell_value(table, 2, "zone", "south")

    assert index.rows_for_key(table, ("code", "zone"), ("A", "north")) == [0, 1]
    assert index.builds == 2


def test_a_different_table_object_does_not_inherit_the_cache() -> None:
    """Two tables with identical column names must not share a grouping."""
    first = _table()
    index = DeterminantGroupIndex()
    assert index.rows_for_key(first, ("code",), ("A",)) == [0, 1, 2]

    second = Table(["code", "city", "zone"], [{"code": "A", "city": "Rome", "zone": "east"}])
    assert index.rows_for_key(second, ("code",), ("A",)) == [0], "cache leaked across tables"


def test_a_dataframe_scans_every_time_and_never_caches() -> None:
    """Without a write counter the index cannot prove freshness, so it must not cache."""
    frame = pd.DataFrame(
        {"code": ["A", "A", "A", "B", "B"], "city": ["Paris", "Paris", "Lyon", "Nice", "Nice"]}
    )
    index = DeterminantGroupIndex()
    for _ in range(4):
        assert index.rows_for_key(frame, ("code",), ("A",)) == [0, 1, 2]
    assert (index.builds, index.hits) == (0, 0), (
        "a pandas frame cannot prove it has not mutated, so caching it would be unsound"
    )
    assert index.scans == 4


def test_cache_groups_false_forces_scanning_even_on_a_table() -> None:
    """The escape hatch SchemaToSMT uses must actually bypass the cache."""
    table = _table()
    index = DeterminantGroupIndex(cache_groups=False)
    for _ in range(3):
        assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    assert (index.builds, index.hits) == (0, 0)
    assert index.scans == 3


def test_the_guard_bites_when_the_stamp_is_defeated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-vacuity: prove these tests would FAIL if invalidation stopped working.

    A freshness test that cannot fail is worse than no test, because it manufactures confidence.
    Here the stamp is pinned to a constant, which is exactly what a broken revision counter would
    look like, and the determinant-invalidation assertion must then break.
    """
    import dataforge.fd_index as module

    monkeypatch.setattr(module, "_stamp", lambda df, columns: (0,))

    table = _table()
    index = DeterminantGroupIndex()
    assert index.rows_for_key(table, ("code",), ("A",)) == [0, 1, 2]
    set_cell_value(table, 2, "code", "B")

    stale = index.rows_for_key(table, ("code",), ("A",))
    assert stale == [0, 1, 2], (
        "with the stamp defeated the index SHOULD return the stale grouping. If it does not, the "
        "invalidation tests above are passing for some other reason and prove nothing."
    )
    assert index.builds == 1
