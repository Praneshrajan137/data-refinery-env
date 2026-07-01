"""Unit tests for the calibration and abstention policy."""

from __future__ import annotations

from dataforge.calibration import (
    AbstentionPolicy,
    default_policy,
    fit_thresholds,
    severity_for_action,
)


class TestAbstentionPolicy:
    def test_default_policy_makes_fuzzy_classes_detection_only(self) -> None:
        policy = default_policy()
        # Detection-only families never auto-apply (threshold > 1.0).
        assert policy.action_for("missing_value", 1.0) == "review"
        assert policy.action_for("outlier", 1.0) == "review"
        assert policy.action_for("duplicate_row", 1.0) == "review"

    def test_precise_families_auto_apply_above_threshold(self) -> None:
        policy = default_policy()
        assert policy.action_for("decimal_shift", 0.75) == "auto_apply"
        assert policy.action_for("decimal_shift", 0.50) == "review"
        assert policy.action_for("format_violation", 0.95) == "auto_apply"
        assert policy.action_for("format_violation", 0.80) == "review"

    def test_unknown_type_uses_default_threshold(self) -> None:
        policy = AbstentionPolicy(default_threshold=0.9)
        assert policy.action_for("brand_new", 0.95) == "auto_apply"
        assert policy.action_for("brand_new", 0.5) == "review"

    def test_severity_mapping(self) -> None:
        assert severity_for_action("auto_apply") == "safe"
        assert severity_for_action("review") == "review"


class TestFitThresholds:
    def test_fit_achieves_target_precision(self) -> None:
        # High-confidence samples are correct; low-confidence are wrong.
        samples = {
            "format_violation": [(0.95, True)] * 20 + [(0.60, False)] * 20,
        }
        thresholds = fit_thresholds(samples, target_precision=0.95)
        # The threshold must exclude the wrong low-confidence band.
        assert thresholds["format_violation"] > 0.60
        policy = AbstentionPolicy(auto_apply_thresholds=thresholds)
        # Verify precision of the auto-applied set meets the target.
        applied = [
            correct
            for conf, correct in samples["format_violation"]
            if policy.action_for("format_violation", conf) == "auto_apply"
        ]
        assert applied and sum(applied) / len(applied) >= 0.95

    def test_low_support_is_detection_only(self) -> None:
        thresholds = fit_thresholds({"rare": [(0.9, True)] * 3}, min_support=10)
        assert thresholds["rare"] == 1.01

    def test_unreachable_target_is_detection_only(self) -> None:
        # Even the highest-confidence band is only 50% correct -> never reaches 0.95.
        samples = {"noisy": [(0.9, True), (0.9, False)] * 20}
        thresholds = fit_thresholds(samples, target_precision=0.95)
        assert thresholds["noisy"] == 1.01

    def test_handles_tied_confidences_correctly(self) -> None:
        # All samples share one confidence; the group is scored together.
        samples = {"tied": [(0.8, True)] * 19 + [(0.8, False)]}  # 95% correct
        thresholds = fit_thresholds(samples, target_precision=0.95)
        assert thresholds["tied"] == 0.8
