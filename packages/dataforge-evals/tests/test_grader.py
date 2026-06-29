"""Grader correctness tests - the grader is the sole source of truth.

Test cases derived from SPEC_dataforge_evals.md Appendix A and the
implementation plan's required coverage list.
"""

from __future__ import annotations

from dataforge_evals.agents.base import Fix, GroundTruthCell
from dataforge_evals.grader import Grade, grade_fixes, normalize_fixes


class TestNormalizeFixes:
    """Fix normalization must be deterministic and last-write-wins."""

    def test_duplicate_cell_uses_last_prediction(self) -> None:
        """Spec A.4: second fix for the same cell overwrites the first."""
        fixes = [
            Fix(row=0, column="Score", new_value="4.0", reason="first"),
            Fix(row=0, column="Score", new_value="4.5", reason="second"),
        ]

        result = normalize_fixes(fixes)

        assert result == [Fix(row=0, column="Score", new_value="4.5", reason="second")]

    def test_different_cells_preserved(self) -> None:
        """Fixes targeting different cells are all retained."""
        fixes = [
            Fix(row=0, column="Score", new_value="4.5"),
            Fix(row=1, column="Phone", new_value="555"),
        ]

        result = normalize_fixes(fixes)

        assert len(result) == 2

    def test_empty_list_returns_empty(self) -> None:
        """Edge case: no fixes yields no output."""
        assert normalize_fixes([]) == []

    def test_triple_duplicate_keeps_last(self) -> None:
        """Three fixes to the same cell: only the last survives."""
        fixes = [
            Fix(row=0, column="Score", new_value="1.0", reason="first"),
            Fix(row=0, column="Score", new_value="2.0", reason="second"),
            Fix(row=0, column="Score", new_value="3.0", reason="third"),
        ]

        result = normalize_fixes(fixes)

        assert result == [Fix(row=0, column="Score", new_value="3.0", reason="third")]


class TestGradeFixes:
    """The grader must be the sole exact-match source of truth."""

    def test_perfect_match_scores_one(self) -> None:
        """Spec A.1: correct fix on exact cell with exact value."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)
        fixes = [Fix(row=0, column="Score", new_value="4.5", reason="correct")]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 1
        assert grade.fp == 0
        assert grade.fn == 0
        assert grade.precision == 1.0
        assert grade.recall == 1.0
        assert grade.f1 == 1.0

    def test_empty_prediction_counts_all_false_negative(self) -> None:
        """Spec A.2: no predictions means all ground truth is missed."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)

        grade = grade_fixes(truth, [])

        assert grade.tp == 0
        assert grade.fp == 0
        assert grade.fn == 1
        assert grade.precision == 0.0
        assert grade.recall == 0.0
        assert grade.f1 == 0.0

    def test_wrong_value_on_right_cell_counts_fp_and_fn(self) -> None:
        """Spec A.3: right cell, wrong value - both FP and FN."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)
        fixes = [Fix(row=0, column="Score", new_value="5.0", reason="wrong")]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 0
        assert grade.fp == 1
        assert grade.fn == 1
        assert grade.f1 == 0.0

    def test_wrong_cell_fix_is_pure_false_positive(self) -> None:
        """Spec A.5: fix targets a cell with no ground-truth issue."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)
        fixes = [Fix(row=1, column="Phone", new_value="555", reason="wrong cell")]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 0
        assert grade.fp == 1
        assert grade.fn == 1

    def test_extra_false_positive_reduces_precision_not_recall(self) -> None:
        """Spec A.6: one correct fix plus one spurious fix."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)
        fixes = [
            Fix(row=0, column="Score", new_value="4.5", reason="correct"),
            Fix(row=1, column="Phone", new_value="217-555-0101", reason="extra"),
        ]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 1
        assert grade.fp == 1
        assert grade.fn == 0
        assert grade.precision == 0.5
        assert grade.recall == 1.0
        assert grade.f1 == 0.6667

    def test_duplicate_fix_uses_last_write_wins_for_grading(self) -> None:
        """Duplicate fix to same cell: last value is graded."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)
        fixes = [
            Fix(row=0, column="Score", new_value="WRONG", reason="first"),
            Fix(row=0, column="Score", new_value="4.5", reason="corrected"),
        ]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 1
        assert grade.fp == 0
        assert grade.fn == 0
        assert grade.f1 == 1.0

    def test_whitespace_normalization(self) -> None:
        """Trailing whitespace in new_value should not cause a miss."""
        truth = (GroundTruthCell(row=0, column="Score", dirty_value="45", clean_value="4.5"),)
        fixes = [Fix(row=0, column="Score", new_value="4.5  ", reason="trailing space")]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 1
        assert grade.f1 == 1.0

    def test_empty_ground_truth_with_predictions(self) -> None:
        """No ground truth + predictions = all FP, zero FN."""
        truth: tuple[GroundTruthCell, ...] = ()
        fixes = [
            Fix(row=0, column="Score", new_value="4.5"),
            Fix(row=1, column="Phone", new_value="555"),
        ]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 0
        assert grade.fp == 2
        assert grade.fn == 0
        assert grade.precision == 0.0

    def test_empty_ground_truth_no_predictions(self) -> None:
        """No ground truth + no predictions = perfect trivial score."""
        grade = grade_fixes((), [])

        assert grade.tp == 0
        assert grade.fp == 0
        assert grade.fn == 0
        assert grade.precision == 0.0
        assert grade.recall == 0.0
        assert grade.f1 == 0.0

    def test_large_batch_correctness(self) -> None:
        """100+ fixes to guard against accumulation bugs."""
        n = 120
        truth = tuple(
            GroundTruthCell(row=i, column="Value", dirty_value=str(i * 10), clean_value=str(i))
            for i in range(n)
        )
        fixes = [Fix(row=i, column="Value", new_value=str(i), reason="batch") for i in range(n)]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == n
        assert grade.fp == 0
        assert grade.fn == 0
        assert grade.precision == 1.0
        assert grade.recall == 1.0
        assert grade.f1 == 1.0

    def test_multi_cell_mixed_correctness(self) -> None:
        """Mixed bag: some correct, some wrong value, some missing, some extra."""
        truth = (
            GroundTruthCell(row=0, column="A", dirty_value="x", clean_value="a"),
            GroundTruthCell(row=1, column="B", dirty_value="y", clean_value="b"),
            GroundTruthCell(row=2, column="C", dirty_value="z", clean_value="c"),
        )
        fixes = [
            Fix(row=0, column="A", new_value="a", reason="correct"),  # TP
            Fix(row=1, column="B", new_value="WRONG", reason="wrong"),  # FP
            # row=2, C is not predicted -> FN
            Fix(row=3, column="D", new_value="extra", reason="spurious"),  # FP
        ]

        grade = grade_fixes(truth, fixes)

        assert grade.tp == 1
        assert grade.fp == 2
        assert grade.fn == 2
        # precision = 1/3, recall = 1/3
        assert grade.precision == 0.3333
        assert grade.recall == 0.3333

    def test_grade_is_frozen(self) -> None:
        """Grade model must be immutable."""
        grade = Grade(tp=1, fp=0, fn=0, precision=1.0, recall=1.0, f1=1.0)
        try:
            grade.tp = 2  # type: ignore[misc]
            raised = False
        except Exception:
            raised = True
        assert raised
