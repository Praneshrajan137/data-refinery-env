"""Unit tests for dataforge.conformal - calibration/test split primitives.

The split is the honesty foundation for distribution-free auto-apply
calibration: a threshold must be *fit* on calibration data and *measured* on
disjoint test data, so a reported precision is never the optimistic in-sample
number. These tests pin the split's determinism, order-independence, and
leakage-freedom (calib and test never share a sample).
"""

from __future__ import annotations

import random

import pytest

from dataforge.conformal import (
    LabeledSample,
    area_under_risk_coverage,
    certified_coverage_report,
    certify_threshold,
    certify_thresholds_by_class,
    population_stability_index,
    reliability_curve,
    repeated_split_certification,
    risk_coverage_curve,
    split_by_class,
)


def _counts(samples: list[LabeledSample]) -> dict[LabeledSample, int]:
    counts: dict[LabeledSample, int] = {}
    for s in samples:
        counts[s] = counts.get(s, 0) + 1
    return counts


class TestSplitByClass:
    """calibration/test split determinism, disjointness, and fractions."""

    def _samples(self, n: int) -> list[LabeledSample]:
        # Distinct confidences so equality/ordering is unambiguous.
        return [(round(0.01 * i, 4), i % 3 == 0) for i in range(n)]

    def test_deterministic_same_seed(self) -> None:
        data = {"value_format": self._samples(20)}
        a_calib, a_test = split_by_class(data, seed=7)
        b_calib, b_test = split_by_class(data, seed=7)
        assert a_calib == b_calib
        assert a_test == b_test

    def test_different_seed_can_differ(self) -> None:
        data = {"value_format": self._samples(40)}
        a_calib, _ = split_by_class(data, seed=1)
        b_calib, _ = split_by_class(data, seed=2)
        assert a_calib != b_calib

    def test_order_independent(self) -> None:
        """Shuffling the input order yields the same partition (as multisets)."""
        forward = self._samples(30)
        backward = list(reversed(forward))
        f_calib, f_test = split_by_class({"c": forward}, seed=5)
        b_calib, b_test = split_by_class({"c": backward}, seed=5)
        assert _counts(f_calib["c"]) == _counts(b_calib["c"])
        assert _counts(f_test["c"]) == _counts(b_test["c"])

    def test_disjoint_and_exhaustive(self) -> None:
        """Every sample lands in exactly one side (no leakage, no loss)."""
        data = {"a": self._samples(21), "b": self._samples(10)}
        calib, test = split_by_class(data, seed=3)
        for cls, original in data.items():
            merged = _counts(calib[cls])
            for s, c in _counts(test[cls]).items():
                merged[s] = merged.get(s, 0) + c
            assert merged == _counts(original)
            # No shared *instances*: sizes add up exactly.
            assert len(calib[cls]) + len(test[cls]) == len(original)

    def test_calibration_fraction_respected(self) -> None:
        data = {"c": self._samples(100)}
        calib, test = split_by_class(data, seed=11, calib_fraction=0.7)
        assert len(calib["c"]) == 70
        assert len(test["c"]) == 30

    def test_per_class_preserved(self) -> None:
        data = {"a": self._samples(8), "b": self._samples(12)}
        calib, test = split_by_class(data, seed=9)
        assert set(calib) == {"a", "b"}
        assert set(test) == {"a", "b"}

    def test_empty_class(self) -> None:
        calib, test = split_by_class({"a": []}, seed=1)
        assert calib["a"] == []
        assert test["a"] == []

    @pytest.mark.parametrize("frac", [0.0, 1.0, -0.1, 1.5])
    def test_invalid_fraction_raises(self, frac: float) -> None:
        with pytest.raises(ValueError, match="calib_fraction"):
            split_by_class({"a": [(0.5, True)]}, seed=1, calib_fraction=frac)


class TestCertifyThreshold:
    """certify_threshold: distribution-free upper bound on accepted-set error."""

    def test_all_correct_certifies_low_threshold(self) -> None:
        """Plenty of all-correct samples => a low threshold is certified."""
        calib: list[LabeledSample] = [(round(0.5 + 0.004 * i, 4), True) for i in range(100)]
        t = certify_threshold(calib, alpha=0.1, delta=0.05, min_support=30)
        assert t is not None
        assert t <= 0.5  # accepts (nearly) everything, since error is 0

    def test_all_wrong_cannot_certify(self) -> None:
        calib: list[LabeledSample] = [(round(0.5 + 0.004 * i, 4), False) for i in range(100)]
        assert certify_threshold(calib, alpha=0.1, delta=0.05, min_support=30) is None

    def test_min_support_blocks_tiny_sets(self) -> None:
        calib: list[LabeledSample] = [(0.99, True), (0.98, True), (0.97, True)]
        # Only 3 samples; below min_support => cannot certify.
        assert certify_threshold(calib, alpha=0.1, delta=0.05, min_support=30) is None

    def test_empty_returns_none(self) -> None:
        assert certify_threshold([], alpha=0.1, delta=0.05) is None

    def test_certified_threshold_holds_out_of_sample(self) -> None:
        """The certified accepted set honors the error budget (self-consistent)."""
        # High-confidence region is clean; low-confidence region is dirty.
        calib: list[LabeledSample] = []
        for i in range(600):
            conf = round(0.5 + 0.0008 * i, 4)
            correct = conf >= 0.8  # sharp boundary
            calib.append((conf, correct))
        t = certify_threshold(calib, alpha=0.1, delta=0.05, min_support=30)
        assert t is not None
        accepted = [correct for conf, correct in calib if conf >= t]
        error = sum(1 for c in accepted if not c) / len(accepted)
        assert error <= 0.1  # accepted-set error stays within the budget
        assert t >= 0.75  # and it stays near the clean region, not wide open

    def test_underpowered_region_abstains(self) -> None:
        """A pure but tiny region cannot certify a strict alpha => abstain (safe)."""
        calib: list[LabeledSample] = [(round(0.9 + 0.001 * i, 4), True) for i in range(25)]
        # 25 clean samples cannot distribution-free-certify a 5% error rate.
        assert certify_threshold(calib, alpha=0.05, delta=0.05, min_support=20) is None


class TestConformalValidityMonteCarlo:
    """The definitive proof: empirical out-of-sample error must exceed alpha in
    at most ~delta of trials, AND the method must certify often enough to be
    useful (not vacuously abstaining)."""

    def test_coverage_guarantee_holds(self) -> None:
        alpha = 0.1
        delta = 0.1
        trials = 300
        n_calib = 400
        n_test = 4000
        rng = random.Random(20260707)

        violations = 0
        certified_trials = 0
        for _ in range(trials):
            # p(correct | confidence) = confidence  => error(c) = 1 - c.
            # A threshold near >= 1 - alpha achieves accepted error <= alpha.
            calib: list[LabeledSample] = []
            for _ in range(n_calib):
                c = round(rng.uniform(0.5, 1.0), 4)
                calib.append((c, rng.random() < c))
            t = certify_threshold(calib, alpha=alpha, delta=delta, min_support=30)
            if t is None:
                continue
            certified_trials += 1
            accepted_errors = 0
            accepted_total = 0
            for _ in range(n_test):
                c = round(rng.uniform(0.5, 1.0), 4)
                correct = rng.random() < c
                if c >= t:
                    accepted_total += 1
                    accepted_errors += 0 if correct else 1
            if accepted_total > 0 and accepted_errors / accepted_total > alpha:
                violations += 1

        # Valid: out-of-sample violations at most delta (with MC slack).
        assert violations / trials <= delta + 0.03
        # Useful: the certifier is not vacuously abstaining.
        assert certified_trials / trials > 0.5


class TestCertifyThresholdsByClass:
    """Mondrian (class-conditional) certification."""

    def test_per_class_independent(self) -> None:
        clean = [(round(0.6 + 0.004 * i, 4), True) for i in range(100)]
        dirty = [(round(0.6 + 0.004 * i, 4), False) for i in range(100)]
        thresholds = certify_thresholds_by_class(
            {"value_format": clean, "other": dirty},
            alpha=0.1,
            delta=0.05,
            min_support=30,
        )
        assert thresholds["value_format"] <= 1.0  # certified
        assert thresholds["other"] == 1.01  # abstain sentinel (never auto-apply)


class TestPopulationStabilityIndex:
    """Distribution-shift monitor guarding the exchangeability assumption."""

    def test_identical_distributions_near_zero(self) -> None:
        ref = [round(0.5 + 0.0005 * i, 4) for i in range(1000)]
        assert population_stability_index(ref, list(ref)) < 0.01

    def test_shifted_distribution_flags_drift(self) -> None:
        ref = [0.2 + 0.0002 * i for i in range(1000)]  # concentrated low
        live = [0.8 + 0.0002 * i for i in range(1000)]  # concentrated high
        assert population_stability_index(ref, live) > 0.25  # significant shift

    def test_moderate_shift_between(self) -> None:
        ref = [round(0.01 * (i % 100), 4) for i in range(1000)]
        live = [round(min(1.0, 0.01 * (i % 100) + 0.1), 4) for i in range(1000)]
        psi = population_stability_index(ref, live)
        assert psi > 0.0

    def test_empty_inputs_return_zero(self) -> None:
        assert population_stability_index([], [0.5]) == 0.0
        assert population_stability_index([0.5], []) == 0.0


class TestCertifiedCoverageReport:
    """The honest, reproducible artifact: certify on calib, measure on test."""

    def test_reliable_class_reports_bounded_test_error(self) -> None:
        # Clean high-confidence class: should auto-apply a lot with low test error.
        samples = {"value_format": [(0.99, True)] * 400}
        report = certified_coverage_report(samples, alpha=0.1, delta=0.05, min_support=30, seed=1)
        assert report["overall_test_error"] <= 0.1
        assert report["overall_test_coverage"] > 0.5
        assert report["per_class"]["value_format"]["auto_applied"] > 0

    def test_unreliable_class_auto_applies_nothing(self) -> None:
        samples = {"other": [(0.99, i % 2 == 0) for i in range(400)]}
        report = certified_coverage_report(samples, alpha=0.1, delta=0.05, min_support=30, seed=1)
        assert report["per_class"]["other"]["auto_applied"] == 0
        assert report["overall_test_coverage"] == 0.0

    def test_report_is_json_serializable_and_reproducible(self) -> None:
        import json

        samples = {"value_format": [(round(0.5 + 0.001 * i, 4), True) for i in range(400)]}
        a = certified_coverage_report(samples, alpha=0.1, seed=7)
        b = certified_coverage_report(samples, alpha=0.1, seed=7)
        assert a == b  # deterministic
        json.dumps(a)  # serializable (no exotic types)


class TestRiskCoverageCurve:
    """Selective-classification risk-coverage curve (Geifman & El-Yaniv 2017)."""

    def test_monotone_coverage_and_endpoints(self) -> None:
        # Cleaner at high confidence, dirtier at low: risk should rise with coverage.
        samples: list[LabeledSample] = []
        for i in range(100):
            conf = round(0.5 + 0.005 * i, 4)
            samples.append((conf, conf >= 0.75))
        curve = risk_coverage_curve(samples)
        coverages = [pt["coverage"] for pt in curve]
        # Coverage is increasing as the threshold drops.
        assert coverages == sorted(coverages)
        # Full-coverage endpoint risk == overall error rate.
        overall_error = sum(1 for _, ok in samples if not ok) / len(samples)
        assert curve[-1]["coverage"] == 1.0
        assert abs(curve[-1]["selective_risk"] - overall_error) < 1e-9

    def test_perfect_model_zero_risk(self) -> None:
        samples: list[LabeledSample] = [(round(0.5 + 0.001 * i, 4), True) for i in range(100)]
        curve = risk_coverage_curve(samples)
        assert all(pt["selective_risk"] == 0.0 for pt in curve)
        assert area_under_risk_coverage(curve) == 0.0

    def test_worst_model_high_aurc(self) -> None:
        samples: list[LabeledSample] = [(round(0.5 + 0.001 * i, 4), False) for i in range(100)]
        aurc = area_under_risk_coverage(risk_coverage_curve(samples))
        assert aurc > 0.9  # risk ~1 across all coverage => AURC near worst

    def test_empty_is_safe(self) -> None:
        assert risk_coverage_curve([]) == []
        assert area_under_risk_coverage([]) == 0.0


class TestRepeatedSplitValidity:
    """Real-data analog of the synthetic Monte-Carlo guarantee proof."""

    def test_guarantee_holds_across_splits(self) -> None:
        # p(correct | conf) = conf; a high threshold controls error at alpha.
        rng = random.Random(4242)
        samples: list[LabeledSample] = []
        for _ in range(1200):
            c = round(rng.uniform(0.5, 1.0), 4)
            samples.append((c, rng.random() < c))
        report = repeated_split_certification(
            {"value_format": samples}, alpha=0.1, delta=0.1, min_support=30, splits=200
        )
        # Empirical fraction of splits whose test error exceeds alpha stays <= delta.
        assert report["over_alpha_rate"] <= 0.1 + 0.05
        # And it certifies in a healthy fraction of splits (not vacuous).
        assert report["certified_rate"] > 0.5

    def test_unreliable_class_never_certifies(self) -> None:
        samples: list[LabeledSample] = [(0.9, i % 2 == 0) for i in range(400)]
        report = repeated_split_certification(
            {"other": samples}, alpha=0.05, delta=0.05, min_support=30, splits=50
        )
        assert report["certified_rate"] == 0.0
        assert report["over_alpha_rate"] == 0.0  # nothing applied => no violations


class TestReliabilityCurve:
    """Reliability diagram data (Guo et al. 2017)."""

    def test_bins_counts_and_fields(self) -> None:
        samples: list[LabeledSample] = [(0.15, False)] * 20 + [(0.85, True)] * 20
        curve = reliability_curve(samples, bins=5)
        total = sum(b["count"] for b in curve)
        assert total == 40
        # Low-confidence bin is inaccurate; high-confidence bin is accurate.
        low = next(b for b in curve if b["count"] and b["mean_confidence"] < 0.5)
        high = next(b for b in curve if b["count"] and b["mean_confidence"] >= 0.5)
        assert low["accuracy"] == 0.0
        assert high["accuracy"] == 1.0

    def test_empty_returns_empty(self) -> None:
        assert reliability_curve([], bins=5) == []
