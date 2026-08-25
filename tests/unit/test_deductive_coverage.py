"""Tests for the deductive-coverage measurement.

The measurement's whole value is that it distinguishes three decision rules and two premise
sources. A test suite that never constructs a case where they disagree would pass while the
measurement collapsed into one number, so the divergence cases are asserted explicitly.
"""

from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest

from dataforge.detectors.base import FunctionalDependency
from scripts.bench.measure_deductive_coverage import (
    MIN_GROUP_SIZE,
    _fd_label,
    _rule_choice,
    discover_oracle_fds,
    fd_holds_on_clean,
)


def _frame(rows: list[dict[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, dtype=str)


class TestFunctionalDependencyDiscovery:
    """Discovery must admit only dependencies that hold, and only useful ones."""

    def test_admits_a_dependency_that_holds_exactly(self) -> None:
        clean = _frame(
            [
                {"zip": "1", "city": "A", "note": "p"},
                {"zip": "1", "city": "A", "note": "q"},
                {"zip": "2", "city": "B", "note": "r"},
                {"zip": "2", "city": "B", "note": "s"},
            ]
        )
        found = discover_oracle_fds(clean, columns=("zip", "city", "note"))
        assert FunctionalDependency(determinant=("zip",), dependent="city") in found

    def test_rejects_a_dependency_with_a_single_exception(self) -> None:
        """One violating row is enough. A premise that is nearly true is not a premise."""
        clean = _frame(
            [
                {"zip": "1", "city": "A"},
                {"zip": "1", "city": "A"},
                {"zip": "1", "city": "DIFFERENT"},
            ]
        )
        assert discover_oracle_fds(clean, columns=("zip", "city")) == ()

    def test_rejects_a_constant_dependent(self) -> None:
        """A single-valued column is determined by everything and would inflate coverage."""
        clean = _frame(
            [
                {"zip": "1", "country": "X"},
                {"zip": "1", "country": "X"},
                {"zip": "2", "country": "X"},
            ]
        )
        found = discover_oracle_fds(clean, columns=("zip", "country"))
        assert all(fd.dependent != "country" for fd in found)

    def test_rejects_a_determinant_whose_groups_are_all_singletons(self) -> None:
        """A key determines everything and can never produce a vote, so it cannot repair."""
        clean = _frame(
            [
                {"id": "1", "city": "A"},
                {"id": "2", "city": "B"},
                {"id": "3", "city": "C"},
            ]
        )
        assert discover_oracle_fds(clean, columns=("id", "city")) == ()

    def test_min_group_size_is_the_smallest_group_that_can_vote(self) -> None:
        assert MIN_GROUP_SIZE == 2

    def test_fd_holds_on_clean_matches_discovery(self) -> None:
        clean = _frame(
            [
                {"zip": "1", "city": "A"},
                {"zip": "1", "city": "A"},
                {"zip": "2", "city": "B"},
                {"zip": "2", "city": "B"},
            ]
        )
        holds = FunctionalDependency(determinant=("zip",), dependent="city")
        fails = FunctionalDependency(determinant=("city",), dependent="zip")
        assert fd_holds_on_clean(clean, holds)
        # This one happens to hold too on this frame; assert the negative on a frame where it can't.
        broken = _frame([{"zip": "1", "city": "A"}, {"zip": "1", "city": "B"}])
        assert not fd_holds_on_clean(broken, holds)
        assert fd_holds_on_clean(clean, fails)

    def test_missing_columns_do_not_raise(self) -> None:
        clean = _frame([{"a": "1"}])
        absent = FunctionalDependency(determinant=("nope",), dependent="a")
        assert not fd_holds_on_clean(clean, absent)


class TestDecisionRules:
    """The three rules must genuinely differ, or the counterfactuals measure nothing."""

    def test_plurality_and_majority_diverge_on_a_split_group(self) -> None:
        """Three distinct values, top holds 2 of 5: a plurality but not a majority.

        This is the case that occurs 1732 times on flights, where the shipped plurality rule
        writes and a majority rule holds.
        """
        counts = Counter({"A": 2, "B": 2, "C": 1})
        assert _rule_choice("shipped", counts, "C") is None, "a tie is not a plurality"

        counts = Counter({"A": 2, "B": 1, "C": 1, "D": 1})
        assert _rule_choice("shipped", counts, "D") == "A"
        assert _rule_choice("majority", counts, "D") is None, "2 of 5 is not a majority"

    def test_majority_requires_more_than_half(self) -> None:
        assert _rule_choice("majority", Counter({"A": 3, "B": 2}), "B") == "A"
        assert _rule_choice("majority", Counter({"A": 2, "B": 2}), "B") is None
        assert _rule_choice("majority", Counter({"A": 2, "B": 1, "C": 1}), "C") is None

    def test_unanimity_excludes_the_cells_own_value(self) -> None:
        """The property that makes unanimity dangerous: the cell loses its own vote."""
        counts = Counter({"A": 3, "B": 1})
        assert _rule_choice("unanimity", counts, "B") == "A"
        # With two other values present, nothing is entailed.
        assert _rule_choice("unanimity", Counter({"A": 2, "B": 1, "C": 1}), "C") is None

    def test_unanimity_can_fire_where_the_cell_was_the_only_dissenter(self) -> None:
        """Measured consequence: this overwrites a clean cell 3 times on hospital-oracle."""
        counts = Counter({"A": 4, "CLEAN_BUT_RARE": 1})
        assert _rule_choice("unanimity", counts, "CLEAN_BUT_RARE") == "A"
        # Plurality and majority agree here, so unanimity's extra corruption is not from the
        # choice of winner but from the cells it is willing to act on at all.
        assert _rule_choice("shipped", counts, "CLEAN_BUT_RARE") == "A"

    def test_a_uniform_group_is_returned_by_the_shipped_rule(self) -> None:
        """Mirrors ``_deterministic_choice``'s ``len(ranked) < 2`` branch."""
        assert _rule_choice("shipped", Counter({"A": 3}), "A") == "A"

    def test_empty_counts_choose_nothing(self) -> None:
        for rule in ("shipped", "majority", "unanimity"):
            assert _rule_choice(rule, Counter(), "x") is None

    def test_unknown_rule_refuses_rather_than_defaulting(self) -> None:
        with pytest.raises(ValueError, match="unknown rule"):
            _rule_choice("strictest", Counter({"A": 2, "B": 1}), "B")


def test_fd_label_is_stable_and_readable() -> None:
    single = FunctionalDependency(determinant=("zip",), dependent="city")
    multi = FunctionalDependency(determinant=("a", "b"), dependent="c")
    assert _fd_label(single) == "zip -> city"
    assert _fd_label(multi) == "a + b -> c"
