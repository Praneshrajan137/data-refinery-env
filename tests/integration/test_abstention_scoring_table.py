"""Executable counterpart to ``specs/SPEC_abstention_scoring.md``.

Every property and limit named in that spec is asserted here. The spec's decision
table is a claim; this file is what makes it falsifiable.

Two disciplines this module is written to, both because of shipped defects in this
repository:

* **No assertion may pass by proving nothing.** Every equality that could hold at
  zero or empty carries an explicit precondition first.
* **Boundaries get their own cases.** Off-by-one mutants have survived nineteen
  tests here before, because no fixture sat exactly on the boundary.
"""

from __future__ import annotations

import pytest

from dataforge.bench.abstention import (
    AbstentionScoringError,
    ThreeWayScore,
    aggregate_three_way,
    detection_risk_coverage_frontier,
    score_detection_three_way,
)
from dataforge.conformal import RISK_COVERAGE_GRID

# The spec's worked example, from Auto-Test's own benchmark README: a month column
# whose only unambiguous error is the misspelling, plus a quarter column where
# "total" is the canonical debatable value.
_MONTHS = ("january", "febuary", "march", "april", "may", "june", "july")
_MONTH_ERRORS = ("febuary",)

_QUARTERS = ("Q1", "Q2", "Q3", "Q4", "total")
_QUARTER_DEBATABLE = ("total",)


class TestRuleArithmetic:
    """TP/FP/FN as defined in the spec."""

    def test_perfect_detection_on_unambiguous_error(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=_MONTH_ERRORS,
        )
        assert score.tp == 1
        assert score.fp == 0
        assert score.fn == 0
        assert score.precision == 1.0
        assert score.recall == 1.0
        assert score.f1 == 1.0

    def test_flagging_a_clean_value_is_a_precision_loss(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=("febuary", "march"),
        )
        assert score.tp == 1
        assert score.fp == 1, "a value in neither G nor D must cost precision"
        assert score.precision == 0.5

    def test_missing_an_unambiguous_error_is_a_recall_loss(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=("febuary", "march"),
            predicted=("febuary",),
        )
        assert score.tp == 1
        assert score.fn == 1
        assert score.recall == 0.5


class TestNeutralZone:
    """P1-P4: the properties that make abstention free inside D."""

    def test_p1_flagging_a_debatable_value_costs_nothing(self) -> None:
        """A prediction inside D must not appear in FP."""
        without = score_detection_three_way(
            distinct_values=_QUARTERS,
            ground_truth=(),
            debatable=_QUARTER_DEBATABLE,
            predicted=(),
        )
        with_prediction = score_detection_three_way(
            distinct_values=_QUARTERS,
            ground_truth=(),
            debatable=_QUARTER_DEBATABLE,
            predicted=_QUARTER_DEBATABLE,
        )
        assert with_prediction.fp == 0
        assert with_prediction.fp == without.fp
        # Precondition: the neutral zone must actually have been exercised, or the
        # equality above holds vacuously for a scorer that ignores D entirely.
        assert with_prediction.debatable_predicted == 1

    def test_p2_missing_a_debatable_value_costs_nothing(self) -> None:
        """A miss inside D must not appear in FN."""
        score = score_detection_three_way(
            distinct_values=_QUARTERS,
            ground_truth=(),
            debatable=_QUARTER_DEBATABLE,
            predicted=(),
        )
        assert score.fn == 0
        assert score.debatable_missed == 1, "precondition: D must be non-empty"

    def test_p1_and_p2_together_make_the_two_policies_score_identically(self) -> None:
        """The whole point: flag-D and abstain-on-D are indistinguishable.

        This is the property that makes 'I abstained because it was ambiguous' a
        measurable claim. Under a two-way rule these two scores differ.
        """
        common = {
            "distinct_values": (*_QUARTERS, "Q5x"),
            "ground_truth": ("Q5x",),
            "debatable": _QUARTER_DEBATABLE,
        }
        flags_debatable = score_detection_three_way(**common, predicted=("Q5x", "total"))
        abstains = score_detection_three_way(**common, predicted=("Q5x",))

        assert flags_debatable.precision == abstains.precision
        assert flags_debatable.recall == abstains.recall
        assert flags_debatable.f1 == abstains.f1
        # Preconditions: both must be real scores, and the two policies must in fact
        # have differed. Without these the equality could hold at None == None.
        assert abstains.precision == 1.0
        assert flags_debatable.debatable_predicted == 1
        assert abstains.debatable_predicted == 0

    def test_p3_debatable_is_not_reclassified_as_clean(self) -> None:
        """If D were folded into 'clean', flagging it would become a precision loss."""
        as_debatable = score_detection_three_way(
            distinct_values=_QUARTERS,
            ground_truth=(),
            debatable=_QUARTER_DEBATABLE,
            predicted=_QUARTER_DEBATABLE,
        )
        as_clean = score_detection_three_way(
            distinct_values=_QUARTERS,
            ground_truth=(),
            debatable=(),
            predicted=_QUARTER_DEBATABLE,
        )
        assert as_debatable.fp == 0
        assert as_clean.fp == 1, "precondition: the two treatments must differ"

    def test_p4_overlapping_labels_raise(self) -> None:
        """A value cannot be both unambiguous and debatable."""
        with pytest.raises(AbstentionScoringError, match="disjoint"):
            score_detection_three_way(
                distinct_values=_MONTHS,
                ground_truth=("febuary",),
                debatable=("febuary",),
                predicted=(),
            )


class TestUndefinedDiscipline:
    """The undefined-value table. Every filler here would flatter a non-decider."""

    def test_flagging_nothing_yields_no_precision_not_perfect_precision(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=(),
        )
        assert score.precision is None, "an unflagged column has no precision to report"
        assert score.f1 is None

    def test_an_all_abstain_system_scores_recall_zero_not_none(self) -> None:
        """Non-vacuity requirement 1. Abstaining is free only inside D."""
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=(),
        )
        assert score.recall == 0.0
        assert score.recall is not None
        assert score.fn == 1, "precondition: there was an error to miss"

    def test_a_column_with_no_errors_has_no_recall_obligation(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=(),
            predicted=(),
        )
        assert score.recall is None
        assert score.precision is None
        assert score.f1 is None

    def test_f1_is_zero_not_none_when_both_defined_and_disjoint(self) -> None:
        """Boundary: precision + recall == 0 with both defined is a real 0.0."""
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=("febuary",),
            predicted=("march",),
        )
        assert score.precision == 0.0
        assert score.recall == 0.0
        assert score.f1 == 0.0, "0.0 is defined here; None would hide a total miss"


class TestAggregation:
    """Pooled counts, and the refusal to aggregate nothing."""

    def test_aggregate_pools_counts_rather_than_averaging_rates(self) -> None:
        small = score_detection_three_way(
            distinct_values=("a", "bx"),
            ground_truth=("bx",),
            predicted=("bx",),
        )
        large = score_detection_three_way(
            distinct_values=tuple(f"v{i}" for i in range(100)) + ("badx",),
            ground_truth=("badx",),
            predicted=tuple(f"v{i}" for i in range(9)) + ("badx",),
        )
        pooled = aggregate_three_way([small, large])

        assert pooled.tp == 2
        assert pooled.fp == 9
        assert pooled.precision == round(2 / 11, 4)
        # A macro average of the two per-column precisions would be (1.0 + 0.1)/2
        # = 0.55, which weights a 2-value column like a 101-value one.
        assert pooled.precision != 0.55
        assert pooled.columns_scored == 2

    def test_aggregating_zero_columns_raises_rather_than_returning_zeros(self) -> None:
        """Non-vacuity requirement 3."""
        with pytest.raises(AbstentionScoringError, match="zero columns"):
            aggregate_three_way([])

    def test_a_none_precision_column_does_not_contribute_a_free_perfect_score(self) -> None:
        undecided = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=(),
        )
        wrong = score_detection_three_way(
            distinct_values=("a", "bx"),
            ground_truth=("bx",),
            predicted=("a",),
        )
        assert undecided.precision is None, "precondition"
        pooled = aggregate_three_way([undecided, wrong])
        assert pooled.precision == 0.0, "a None must not average up a wrong column"


class TestAbstentionReporting:
    """Coverage and abstention rate are reported, not inferred."""

    def test_coverage_and_abstention_rate_are_complementary(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=("febuary", "march"),
        )
        assert score.n_distinct_values == 7
        assert score.n_predicted == 2
        assert score.coverage == round(2 / 7, 4)
        assert score.abstention_rate == round(1.0 - round(2 / 7, 4), 4)

    def test_full_abstention_reports_zero_coverage(self) -> None:
        score = score_detection_three_way(
            distinct_values=_MONTHS,
            ground_truth=_MONTH_ERRORS,
            predicted=(),
        )
        assert score.coverage == 0.0
        assert score.abstention_rate == 1.0


class TestRiskCoverageFrontier:
    """The frontier, and its one-definition-of-risk guarantee."""

    def test_frontier_reports_a_bound_above_the_point_estimate(self) -> None:
        samples = [(0.95, True)] * 10 + [(0.55, False)] * 2
        frontier = detection_risk_coverage_frontier(samples)
        assert frontier, "precondition: frontier must be non-empty"
        for point in frontier:
            assert point["risk_upper"] >= point["selective_risk"], (
                "an upper bound below its point estimate is not a bound"
            )

    def test_a_perfect_record_still_carries_nonzero_risk_upper(self) -> None:
        """Why the bound exists: ten-for-ten is not proof of zero risk."""
        frontier = detection_risk_coverage_frontier([(0.99, True)] * 10)
        assert frontier, "precondition"
        top = frontier[0]
        assert top["selective_risk"] == 0.0
        assert top["risk_upper"] > 0.0
        assert top["risk_upper"] < 1.0

    def test_thresholds_accepting_nothing_are_omitted_not_scored_as_safe(self) -> None:
        frontier = detection_risk_coverage_frontier([(0.10, False)])
        thresholds = [point["threshold"] for point in frontier]
        assert 0.99 not in thresholds, (
            "a threshold nothing cleared must not be reported at risk 0.0"
        )
        assert thresholds, "precondition: some threshold must have accepted"

    def test_grid_is_pre_specified_and_descending(self) -> None:
        """A grid read off the labels is a validity weakness, not a power one."""
        assert tuple(sorted(RISK_COVERAGE_GRID, reverse=True)) == RISK_COVERAGE_GRID
        assert len(RISK_COVERAGE_GRID) >= 10

    def test_boundary_confidence_exactly_at_threshold_is_accepted(self) -> None:
        """Boundary case: >= not >. Off-by-one mutants have survived here before."""
        frontier = detection_risk_coverage_frontier([(0.90, True)], grid=(0.90,))
        assert len(frontier) == 1
        assert frontier[0]["n_accepted"] == 1, "confidence == threshold must be accepted"

    def test_empty_samples_yield_an_empty_frontier(self) -> None:
        assert detection_risk_coverage_frontier([]) == []


class TestSpecLimits:
    """L1-L4 are structural, so they are asserted as shape, not as prose."""

    def test_score_carries_no_correction_field(self) -> None:
        """L3: this axis is detection only. A repair number here would be fabricated."""
        fields = set(ThreeWayScore.model_fields)
        forbidden = {"new_value", "clean_value", "exact_match", "correction_precision"}
        assert not (fields & forbidden), (
            "RT-bench/ST-bench ship no clean values; a correction field would invite "
            "a fabricated number"
        )

    def test_duplicate_values_collapse_because_dist_val_is_distinct(self) -> None:
        """L1: multiplicity is lost. Asserted so the limit cannot be forgotten."""
        score = score_detection_three_way(
            distinct_values=("a", "a", "a", "bx"),
            ground_truth=("bx",),
            predicted=("bx",),
        )
        assert score.n_distinct_values == 2, (
            "a value occurring three times counts once; this is not cell-level precision"
        )
