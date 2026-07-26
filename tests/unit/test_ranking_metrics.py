"""Unit tests for the pure review-queue ranking metrics."""

from __future__ import annotations

from dataforge.bench.ranking_metrics import (
    precision_at_k,
    queue_precision_lift,
    recall_at_k,
    roc_auc,
)


class TestPrecisionAtK:
    def test_hand_computed(self) -> None:
        ranked = [(0.9, True), (0.8, False), (0.7, True), (0.6, False)]
        assert precision_at_k(ranked, 2) == 0.5  # top2: T, F
        assert precision_at_k(ranked, 4) == 0.5  # 2 of 4
        assert precision_at_k(ranked, 1) == 1.0  # top1: T

    def test_k_exceeds_length_uses_all(self) -> None:
        ranked = [(0.9, True), (0.1, False)]
        assert precision_at_k(ranked, 10) == 0.5

    def test_empty_and_nonpositive_k(self) -> None:
        assert precision_at_k([], 3) == 0.0
        assert precision_at_k([(0.9, True)], 0) == 0.0


class TestRecallAtK:
    def test_hand_computed(self) -> None:
        ranked = [(0.9, True), (0.8, False), (0.7, True), (0.6, False)]
        assert recall_at_k(ranked, 1) == 0.5  # 1 of 2 true captured
        assert recall_at_k(ranked, 3) == 1.0  # both true in top 3

    def test_no_true_errors(self) -> None:
        assert recall_at_k([(0.9, False), (0.1, False)], 2) == 0.0


class TestRocAuc:
    def test_perfect_separation(self) -> None:
        assert roc_auc([(0.9, True), (0.8, True), (0.2, False), (0.1, False)]) == 1.0

    def test_reversed_is_zero(self) -> None:
        assert roc_auc([(0.1, True), (0.2, True), (0.8, False), (0.9, False)]) == 0.0

    def test_ties_are_half(self) -> None:
        assert roc_auc([(0.5, True), (0.5, False)]) == 0.5

    def test_undefined_returns_half(self) -> None:
        assert roc_auc([]) == 0.5
        assert roc_auc([(0.9, True), (0.8, True)]) == 0.5  # no negatives
        assert roc_auc([(0.9, False)]) == 0.5  # no positives

    def test_partial_separation(self) -> None:
        # T outranks two F but one F outranks the other T.
        auc = roc_auc([(0.9, True), (0.6, False), (0.5, True), (0.4, False)])
        # pairs: (T0.9>F0.6)1, (T0.9>F0.4)1, (T0.5<F0.6)0, (T0.5>F0.4)1 => 3/4
        assert auc == 0.75


class TestQueuePrecisionLift:
    def test_eightfold_lift(self) -> None:
        # top-2 precision 0.5 over base rate 0.0625 -> 8x
        ranked = [(0.9, True), (0.8, False), (0.1, False), (0.05, False)]
        assert queue_precision_lift(ranked, 2, 0.0625) == 8.0

    def test_zero_base_rate(self) -> None:
        assert queue_precision_lift([(0.9, True)], 1, 0.0) == 0.0
