"""Tests for corrector-specific calibration (C3).

The corrector's confidence is a self-consistency agreement fraction, which is
only trustworthy once it has been calibrated against measured correctness. The
honest default is therefore *propose-not-apply*: a corrector fix is surfaced as
a human-review suggestion unless a per-class threshold -- fit to a >= 0.95
precision floor on benchmark data -- says it is safe to auto-apply.
"""

from __future__ import annotations

from dataforge.calibration import (
    corrector_default_policy,
    policy_from_corrector_samples,
)


class TestProposeNotApplyDefault:
    def test_default_policy_never_auto_applies(self) -> None:
        policy = corrector_default_policy()

        for issue_type in (
            "categorical_normalization",
            "missing_value",
            "format_violation",
            "type_mismatch",
            "outlier",
        ):
            assert policy.action_for(issue_type, 1.0) == "review"

    def test_default_policy_target_precision_is_high(self) -> None:
        assert corrector_default_policy().target_precision >= 0.95


class TestFitFromSamples:
    def test_high_confidence_correct_class_becomes_auto_applicable(self) -> None:
        # A class whose high-confidence corrector outputs are consistently
        # correct earns an auto-apply threshold at that confidence.
        samples = {
            "categorical_normalization": [(1.0, True)] * 20,
        }

        policy = policy_from_corrector_samples(samples, target_precision=0.95)

        assert policy.action_for("categorical_normalization", 1.0) == "auto_apply"

    def test_unreliable_class_stays_propose_not_apply(self) -> None:
        # A class whose corrector outputs are wrong as often as right can never
        # clear the precision floor, so it stays review-only.
        samples = {
            "missing_value": [(1.0, i % 2 == 0) for i in range(20)],
        }

        policy = policy_from_corrector_samples(samples, target_precision=0.95)

        assert policy.action_for("missing_value", 1.0) == "review"

    def test_low_support_class_stays_propose_not_apply(self) -> None:
        samples = {"outlier": [(1.0, True), (1.0, True)]}

        policy = policy_from_corrector_samples(samples, target_precision=0.95)

        assert policy.action_for("outlier", 1.0) == "review"

    def test_unlisted_class_defaults_to_propose_not_apply(self) -> None:
        policy = policy_from_corrector_samples(
            {"categorical_normalization": [(1.0, True)] * 20},
            target_precision=0.95,
        )

        # A class with no fitted threshold must not auto-apply by default.
        assert policy.action_for("some_other_class", 1.0) == "review"

    def test_threshold_separates_high_and_low_confidence(self) -> None:
        # High-confidence outputs correct, low-confidence outputs wrong: the
        # fitted threshold should auto-apply the former and review the latter.
        samples = {
            "format_violation": [(0.9, True)] * 12 + [(0.3, False)] * 12,
        }

        policy = policy_from_corrector_samples(samples, target_precision=0.95)

        assert policy.action_for("format_violation", 0.9) == "auto_apply"
        assert policy.action_for("format_violation", 0.3) == "review"
