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

from collections.abc import Callable
from pathlib import Path

import pytest

from tests.support.tables import (
    RepairableTable,
    build_premised_repairable_table,
    build_unpremised_shifted_table,
)


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
