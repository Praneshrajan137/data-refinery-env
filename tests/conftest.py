"""Pytest fixture layer over :mod:`tests.support.tables`.

The domain objects and builders live in ``tests/support/tables.py`` so they are importable
by test modules that need the ``RepairableTable`` type for annotations, or that build
tables at paths pytest does not choose. This file only adapts them to pytest.

See ``tests/support/tables.py`` for why the suite needs a shared notion of "a repairable
table" at all -- short version: the literal ``id,amount\\n…4,1020\\n…`` was copy-pasted
into thirteen files under six names, encoding exactly one detector, so removing that
detector from the auto-apply allowlist broke thirteen tests and silently hollowed out six
more.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from tests.support import tree_integrity
from tests.support.tables import (
    RepairableTable,
    build_premised_repairable_table,
    build_unpremised_shifted_table,
)

# A fast profile for the inner loop, REGISTERED BUT NEVER LOADED HERE.
#
# Hypothesis ships its own `ci` profile -- `derandomize=True, deadline=None, database=None,
# print_blob=True` -- and auto-loads it when `is_in_ci()` is true. A `settings.load_profile(...)`
# call in this file would run afterwards and override that, silently weakening CI while looking
# like a local convenience. So the profile is only registered; `--hypothesis-profile dev` on the
# command line does the loading. The suite's 1,030 examples stay at full strength everywhere
# except when a developer explicitly asks for the fast path.
try:  # pragma: no cover - absent only if the dev extra is not installed
    from hypothesis import settings as _hypothesis_settings
except ImportError:  # pragma: no cover
    pass
else:
    if "dev" not in _hypothesis_settings._profiles:
        _hypothesis_settings.register_profile("dev", max_examples=10)


@pytest.fixture(scope="session", autouse=True)
def working_tree_is_not_modified() -> Iterator[None]:
    """Fail the session if any test changed the working tree.

    See ``tests/support/tree_integrity.py`` for why this is a guard rather than a convention.
    Short version: a ``finally`` that restores a repository file is correct serially, unsafe in
    parallel, and does not survive a hard kill -- which is how an inverted write-safety allowlist
    once sat in the working tree undetected.

    Session-scoped, so under ``-n`` each worker checks its own share independently. The
    comparison is start-versus-end, so a tree that was already dirty is not blamed on a test.
    """
    before = tree_integrity.snapshot()
    yield
    if before is None:
        warnings.warn(
            "Working-tree integrity guard did not run: 'git status' was unavailable. A test "
            "that writes the repository would not be detected in this session.",
            stacklevel=1,
        )
        return
    after = tree_integrity.snapshot()
    if after is None:
        warnings.warn(
            "Working-tree integrity guard could not read the final tree state.", stacklevel=1
        )
        return
    problems = tree_integrity.diff(before, after)
    if problems:
        raise AssertionError(tree_integrity.failure_message(problems))


@pytest.fixture
def premised_repairable_table(tmp_path: Path) -> RepairableTable:
    """A table whose repair the product stands behind. Default for "a write happened"."""
    return build_premised_repairable_table(tmp_path / "premised.csv")


@pytest.fixture
def make_premised_repairable_table() -> Callable[[Path], RepairableTable]:
    """Factory for :func:`premised_repairable_table` at a caller-chosen path."""
    return build_premised_repairable_table


@pytest.fixture
def unpremised_shifted_table(tmp_path: Path) -> RepairableTable:
    """A table whose only candidate repair is HELD, never written."""
    return build_unpremised_shifted_table(tmp_path / "unpremised.csv")


@pytest.fixture
def make_unpremised_shifted_table() -> Callable[[Path], RepairableTable]:
    """Factory for :func:`unpremised_shifted_table` at a caller-chosen path."""
    return build_unpremised_shifted_table
