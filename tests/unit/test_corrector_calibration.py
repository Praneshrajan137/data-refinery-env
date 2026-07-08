"""Tests for corrector-specific calibration (C3).

The corrector's confidence is a self-consistency agreement fraction, which is
only trustworthy once it has been calibrated against measured correctness. The
honest default is therefore *propose-not-apply*: a corrector fix is surfaced as
a human-review suggestion unless a per-class threshold -- fit to a >= 0.95
precision floor on benchmark data -- says it is safe to auto-apply.
"""

from __future__ import annotations

from dataforge.calibration import (
    conformal_corrector_policy,
    corrector_default_policy,
    guard_policy_for_drift,
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


class TestConformalCorrectorPolicy:
    """Distribution-free (conformal) auto-apply policy replacing in-sample fit."""

    def test_reliable_class_is_certified(self) -> None:
        # A large, clean high-confidence class earns a certified auto-apply.
        samples = {"categorical_normalization": [(0.99, True)] * 200}
        policy = conformal_corrector_policy(samples, alpha=0.1, delta=0.05, min_support=30)
        assert policy.action_for("categorical_normalization", 0.99) == "auto_apply"

    def test_unreliable_class_stays_propose_not_apply(self) -> None:
        samples = {"missing_value": [(0.99, i % 2 == 0) for i in range(200)]}
        policy = conformal_corrector_policy(samples, alpha=0.1, delta=0.05, min_support=30)
        assert policy.action_for("missing_value", 0.99) == "review"

    def test_low_support_class_cannot_certify(self) -> None:
        samples = {"outlier": [(0.99, True)] * 5}
        policy = conformal_corrector_policy(samples, alpha=0.1, delta=0.05, min_support=30)
        assert policy.action_for("outlier", 0.99) == "review"

    def test_target_precision_reflects_alpha(self) -> None:
        policy = conformal_corrector_policy({}, alpha=0.05)
        assert policy.target_precision == 0.95


class TestDriftGuard:
    """Distribution-shift guard: downgrade auto-apply when exchangeability breaks."""

    def _certified_policy(self) -> object:
        samples = {"categorical_normalization": [(0.99, True)] * 200}
        return conformal_corrector_policy(samples, alpha=0.1, delta=0.05, min_support=30)

    def test_no_drift_keeps_certified_policy(self) -> None:
        policy = self._certified_policy()
        ref = [0.99] * 500
        live = [0.99] * 500
        guarded = guard_policy_for_drift(policy, ref, live, psi_threshold=0.2)
        assert guarded.action_for("categorical_normalization", 0.99) == "auto_apply"

    def test_drift_downgrades_to_propose_not_apply(self) -> None:
        policy = self._certified_policy()
        ref = [0.2 + 0.0005 * i for i in range(500)]  # low-confidence calibration
        live = [0.95 + 0.0001 * i for i in range(500)]  # high-confidence live
        guarded = guard_policy_for_drift(policy, ref, live, psi_threshold=0.2)
        # Guarantee void under drift => nothing auto-applies.
        assert guarded.action_for("categorical_normalization", 0.99) == "review"
