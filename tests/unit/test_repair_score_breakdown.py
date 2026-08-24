"""Tests for the repair-score decomposition.

Offline arithmetic. The load-bearing property is **exact reconciliation**: the decomposed terms
must sum back to `RepairScore.fp` and `RepairScore.fn` on every input, including the awkward
ones. A decomposition that merely approximates the totals would be a second, competing set of
numbers rather than a view of the existing ones.

Property-based over random inputs as well as by example, because the reconciliation must hold
for every combination of the four outcomes and not just the ones I thought to write down.
"""

from __future__ import annotations

import random

import pytest

from dataforge.bench.core import (
    BenchmarkRepair,
    RepairScoreBreakdown,
    decompose_repair_score,
    score_repairs,
)
from dataforge.datasets.real_world import GroundTruthCell


def _truth(*cells: tuple[int, str, str]) -> list[GroundTruthCell]:
    """Build ground truth from (row, column, clean_value) triples."""
    return [
        GroundTruthCell(row=row, column=column, dirty_value="dirty", clean_value=clean)
        for row, column, clean in cells
    ]


def _repair(row: int, column: str, new_value: str) -> BenchmarkRepair:
    """Build one repair prediction."""
    return BenchmarkRepair(row=row, column=column, new_value=new_value, reason="test")


class TestReconciliation:
    """The decomposition is a view of RepairScore, not a rival to it."""

    def test_the_four_outcomes_reconcile_exactly(self) -> None:
        truth = _truth((0, "a", "fixed"), (1, "a", "fixed"), (2, "a", "fixed"))
        repairs = [
            _repair(0, "a", "fixed"),  # correct
            _repair(1, "a", "wrong"),  # wrong value on a real error
            _repair(9, "a", "anything"),  # repaired a clean cell
            # row 2 untouched -> abstained on a real error
        ]
        score = score_repairs(truth, repairs)
        breakdown = decompose_repair_score(truth, repairs)

        assert breakdown.correct == score.tp == 1
        assert breakdown.repaired_a_clean_cell == 1
        assert breakdown.wrong_value_on_a_real_error == 1
        assert breakdown.abstained_on_a_real_error == 1
        assert breakdown.false_positives == score.fp == 2
        assert breakdown.false_negatives == score.fn == 2

    def test_a_wrong_value_is_counted_in_both_totals(self) -> None:
        """Not a bug in the decomposition; a property of score_repairs, now visible."""
        truth = _truth((0, "a", "fixed"))
        repairs = [_repair(0, "a", "wrong")]
        score = score_repairs(truth, repairs)
        breakdown = decompose_repair_score(truth, repairs)
        assert score.fp == 1
        assert score.fn == 1
        assert breakdown.wrong_value_on_a_real_error == 1
        assert breakdown.repaired_a_clean_cell == 0
        assert breakdown.abstained_on_a_real_error == 0
        # One cell, two penalties.
        assert breakdown.false_positives == 1
        assert breakdown.false_negatives == 1

    def test_last_write_wins_is_applied_identically(self) -> None:
        """Both functions must collapse duplicate predictions the same way."""
        truth = _truth((0, "a", "fixed"))
        repairs = [_repair(0, "a", "wrong"), _repair(0, "a", "fixed")]
        score = score_repairs(truth, repairs)
        breakdown = decompose_repair_score(truth, repairs)
        assert score.tp == 1
        assert breakdown.correct == 1
        assert breakdown.wrong_value_on_a_real_error == 0
        assert breakdown.false_positives == score.fp == 0

    @pytest.mark.parametrize("seed", range(25))
    def test_reconciliation_holds_on_random_inputs(self, seed: int) -> None:
        rng = random.Random(seed)
        n_truth = rng.randrange(0, 12)
        truth_cells = [(row, "c", f"clean{row}") for row in range(n_truth)]
        truth = _truth(*truth_cells)
        repairs: list[BenchmarkRepair] = []
        for _ in range(rng.randrange(0, 18)):
            row = rng.randrange(0, 16)
            correct = rng.random() < 0.4
            value = f"clean{row}" if correct else f"other{rng.randrange(100)}"
            repairs.append(_repair(row, "c", value))
        score = score_repairs(truth, repairs)
        breakdown = decompose_repair_score(truth, repairs)
        assert breakdown.false_positives == score.fp
        assert breakdown.false_negatives == score.fn
        assert breakdown.correct == score.tp


class TestDegenerateCases:
    """Silence, and nothing to be silent about."""

    def test_total_abstention_produces_no_damage_rate(self) -> None:
        """A corrector that wrote nothing has no damage rate; 0.0 would read as safety."""
        truth = _truth((0, "a", "fixed"), (1, "a", "fixed"))
        breakdown = decompose_repair_score(truth, [])
        assert breakdown.correct == 0
        assert breakdown.false_positives == 0
        assert breakdown.abstained_on_a_real_error == 2
        assert breakdown.cells_touched == 0
        assert breakdown.damage_rate is None

    def test_an_empty_corpus_makes_every_write_damage(self) -> None:
        breakdown = decompose_repair_score([], [_repair(0, "a", "x"), _repair(1, "a", "y")])
        assert breakdown.repaired_a_clean_cell == 2
        assert breakdown.abstained_on_a_real_error == 0
        assert breakdown.damage_rate == 1.0

    def test_a_perfect_corrector_has_zero_damage(self) -> None:
        truth = _truth((0, "a", "fixed"), (1, "b", "also"))
        repairs = [_repair(0, "a", "fixed"), _repair(1, "b", "also")]
        breakdown = decompose_repair_score(truth, repairs)
        assert breakdown.correct == 2
        assert breakdown.false_positives == 0
        assert breakdown.false_negatives == 0
        assert breakdown.damage_rate == 0.0

    def test_nothing_at_all_reconciles(self) -> None:
        breakdown = decompose_repair_score([], [])
        assert breakdown.false_positives == 0
        assert breakdown.false_negatives == 0
        assert breakdown.damage_rate is None


class TestTheExistingScoreIsUnchanged:
    """Committed artifacts and the promotion gate depend on RepairScore's fields."""

    def test_repair_score_has_no_new_fields(self) -> None:
        from dataforge.bench.core import RepairScore

        assert set(RepairScore.model_fields) == {
            "tp",
            "fp",
            "fn",
            "precision",
            "recall",
            "f1",
        }

    def test_the_breakdown_is_a_separate_type(self) -> None:
        """It must not be mistaken for, or substituted into, a RepairScore."""
        from dataforge.bench.core import RepairScore

        assert not issubclass(RepairScoreBreakdown, RepairScore)
        assert "precision" not in RepairScoreBreakdown.model_fields
        assert "f1" not in RepairScoreBreakdown.model_fields

    def test_the_breakdown_is_frozen(self) -> None:
        breakdown = decompose_repair_score([], [])
        with pytest.raises(Exception, match="frozen|immutable|Instance is frozen"):
            breakdown.correct = 5  # type: ignore[misc]
