"""Distribution-free calibration for the DataForge auto-apply gate.

Background
----------
DataForge auto-applies a repair only when a proposal's confidence clears a
threshold. Historically that threshold was fit *in-sample*
(:func:`dataforge.calibration.fit_thresholds`): it picked the lowest confidence
whose precision met the target on the very samples used to measure it. An
in-sample threshold overstates precision on new data, so a "0.95" auto-apply
boundary can silently fall below 0.95 in production. For a system whose whole
promise is "never make the data worse", that is the wrong kind of wrong.

This module replaces guesswork with a distribution-free, finite-sample
guarantee, following split conformal prediction and conformal risk control:

- Angelopoulos & Bates, "A Gentle Introduction to Conformal Prediction and
  Distribution-Free Uncertainty Quantification", arXiv:2107.07511.
- Angelopoulos, Bates, Fisch, Lei, Schuster, "Conformal Risk Control",
  arXiv:2208.02814.

The foundation (this file, step one) is an honest calibration/test split: any
threshold is *fit* on a calibration split and *measured* on a disjoint test
split, so a reported precision is never the optimistic in-sample number.
Step two (:func:`certify_threshold`) adds the guarantee itself.
"""

from __future__ import annotations

import hashlib
import math
import random
from collections.abc import Mapping, Sequence

__all__ = [
    "LabeledSample",
    "ABSTAIN_THRESHOLD",
    "split_by_class",
    "certify_threshold",
    "certify_thresholds_by_class",
    "certified_coverage_report",
    "population_stability_index",
    "risk_coverage_curve",
    "area_under_risk_coverage",
    "repeated_split_certification",
    "reliability_curve",
]

# A confidence-labeled outcome: (confidence in [0, 1], was_correct).
LabeledSample = tuple[float, bool]

# Sentinel threshold meaning "never auto-apply" (matches AbstentionPolicy's
# convention where a threshold of 1.01 is unreachable by any confidence <= 1.0).
ABSTAIN_THRESHOLD = 1.01


def _class_seed(seed: int, error_class: str) -> int:
    """Derive a stable per-class RNG seed (independent of PYTHONHASHSEED).

    Python's built-in ``hash`` is salted per process, so it cannot produce a
    reproducible split across runs. We hash the class name with SHA-256 and mix
    it with the caller's seed so each class shuffles independently yet
    deterministically.
    """
    digest = hashlib.sha256(error_class.encode("utf-8")).digest()[:8]
    return seed ^ int.from_bytes(digest, "big")


def split_by_class(
    samples_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    seed: int,
    calib_fraction: float = 0.5,
) -> tuple[dict[str, list[LabeledSample]], dict[str, list[LabeledSample]]]:
    """Split labeled samples into disjoint calibration and test halves per class.

    The split is:

    - **Deterministic** - the same ``seed`` yields the same partition.
    - **Order-independent** - samples are canonically sorted before shuffling,
      so the partition does not depend on the order the caller collected them.
    - **Leakage-free** - each sample instance lands in exactly one side; the two
      sides are disjoint and together reconstruct the input.

    Args:
        samples_by_class: ``{error_class: [(confidence, was_correct), ...]}``.
        seed: Base RNG seed; mixed with a stable per-class hash.
        calib_fraction: Fraction of each class routed to calibration, in (0, 1).

    Returns:
        ``(calibration_by_class, test_by_class)``.

    Raises:
        ValueError: If ``calib_fraction`` is not strictly between 0 and 1.
    """
    if not 0.0 < calib_fraction < 1.0:
        raise ValueError(f"calib_fraction must be in (0, 1); got {calib_fraction}")

    calibration: dict[str, list[LabeledSample]] = {}
    test: dict[str, list[LabeledSample]] = {}
    for error_class, samples in samples_by_class.items():
        # Canonical sort makes the split independent of input ordering; the
        # seeded shuffle then makes it a uniformly random (but reproducible) split.
        ordered = sorted(samples)
        rng = random.Random(_class_seed(seed, error_class))
        rng.shuffle(ordered)
        n_calib = round(len(ordered) * calib_fraction)
        calibration[error_class] = ordered[:n_calib]
        test[error_class] = ordered[n_calib:]
    return calibration, test


def certify_threshold(
    calibration: Sequence[LabeledSample],
    *,
    alpha: float,
    delta: float = 0.05,
    min_support: int = 30,
) -> float | None:
    """Certify the lowest confidence threshold whose auto-apply error <= alpha.

    Selective-risk control via fixed sequential testing (Bates et al. 2021,
    "Distribution-Free, Risk-Controlling Prediction Sets"; Angelopoulos et al.
    2021, "Learn then Test"). Candidate thresholds are tested in a fixed order -
    purest (highest) threshold first, descending toward larger accepted sets. At
    each threshold we test H0: accepted-set error > alpha with an exact one-sided
    Clopper-Pearson upper confidence bound on the binomial error rate; testing in
    a fixed sequence and stopping at the first non-rejection controls the
    family-wise error at ``delta`` with no Bonferroni penalty (hence far more
    powerful than a union bound over the grid).

    Guarantee: with probability at least ``1 - delta`` over the calibration draw,
    every certified threshold's true accepted-set error rate is <= ``alpha``
    (valid for data exchangeable with the calibration sample; see the module
    docstring). We return the lowest certified threshold to maximize coverage.

    Args:
        calibration: ``(confidence, was_correct)`` pairs from a held-out
            calibration split - never the samples used to report final metrics.
        alpha: Maximum tolerated accepted-set error rate (``1 - target_precision``).
        delta: Failure probability of the guarantee.
        min_support: Minimum accepted-sample count before a threshold is eligible
            to certify (guards against certifying on a starved set).

    Returns:
        The lowest certified confidence threshold, or ``None`` if none can be
        certified (the honest signal to never auto-apply this class).
    """
    if not 0.0 < alpha < 1.0:
        raise ValueError(f"alpha must be in (0, 1); got {alpha}")
    if not 0.0 < delta < 1.0:
        raise ValueError(f"delta must be in (0, 1); got {delta}")
    if not calibration:
        return None

    certified: float | None = None
    # Fixed sequence: purest (highest) threshold first, descending.
    for threshold in sorted({conf for conf, _ in calibration}, reverse=True):
        accepted = [correct for conf, correct in calibration if conf >= threshold]
        n = len(accepted)
        if n < min_support:
            continue  # too small to test yet; keep descending toward larger sets
        errors = sum(1 for correct in accepted if not correct)
        if _clopper_pearson_upper(errors, n, delta) <= alpha:
            certified = threshold  # keep lowering; last certified = max coverage
        else:
            break  # first non-rejection halts the fixed sequence (FWER control)
    return certified


def _clopper_pearson_upper(errors: int, n: int, delta: float) -> float:
    """Exact one-sided (upper) Clopper-Pearson bound on a binomial rate.

    Returns the largest error rate ``p`` consistent with observing ``errors`` in
    ``n`` trials at confidence ``1 - delta``: the ``p`` solving
    ``P(Bin(n, p) <= errors) = delta``. Distribution-free and finite-sample
    exact. Implemented without SciPy via bisection on the binomial CDF (which is
    monotone decreasing in ``p``).
    """
    if errors >= n:
        return 1.0
    if errors == 0:
        # Closed form: P(Bin(n,p) <= 0) = (1-p)^n = delta  =>  p = 1 - delta^(1/n).
        return 1.0 - float(delta ** (1.0 / n))
    low, high = errors / n, 1.0
    for _ in range(80):  # 80 bisections => ~1e-24 precision, plenty
        mid = (low + high) / 2.0
        if _binom_cdf(errors, n, mid) > delta:
            low = mid  # CDF too high => true p is larger
        else:
            high = mid
    return high


def _binom_cdf(k: int, n: int, p: float) -> float:
    """P(Bin(n, p) <= k), computed in log space for numerical stability."""
    if p <= 0.0:
        return 1.0
    if p >= 1.0:
        return 0.0
    log_p = math.log(p)
    log_q = math.log1p(-p)
    log_terms = [
        math.lgamma(n + 1)
        - math.lgamma(i + 1)
        - math.lgamma(n - i + 1)
        + i * log_p
        + (n - i) * log_q
        for i in range(k + 1)
    ]
    peak = max(log_terms)
    return math.exp(peak) * sum(math.exp(t - peak) for t in log_terms)


def certify_thresholds_by_class(
    calibration_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    alpha: float,
    delta: float = 0.05,
    min_support: int = 30,
) -> dict[str, float]:
    """Class-conditional (Mondrian) certification of auto-apply thresholds.

    Certifies each error class independently so the error guarantee is
    *per class*, not merely marginal across classes - the honest granularity for
    a safety gate. Classes that cannot be certified receive
    :data:`ABSTAIN_THRESHOLD` (never auto-apply).

    Args:
        calibration_by_class: ``{error_class: [(confidence, was_correct), ...]}``
            from the calibration split.
        alpha: Maximum tolerated per-class accepted-set error rate.
        delta: Per-class failure probability of the guarantee.
        min_support: Minimum accepted-sample count to certify a class.

    Returns:
        ``{error_class: threshold}``; ``ABSTAIN_THRESHOLD`` means detection-only.
    """
    thresholds: dict[str, float] = {}
    for error_class, samples in calibration_by_class.items():
        certified = certify_threshold(samples, alpha=alpha, delta=delta, min_support=min_support)
        thresholds[error_class] = ABSTAIN_THRESHOLD if certified is None else certified
    return thresholds


def min_samples_for_certification(alpha: float, delta: float = 0.05) -> int:
    """Smallest all-correct accepted-sample count that can certify precision ``1 - alpha``.

    With zero observed errors the Clopper-Pearson upper bound is ``1 - delta**(1/n)``
    (see :func:`_clopper_pearson_upper`); certification needs that bound ``<= alpha``,
    i.e. ``n >= ln(delta) / ln(1 - alpha)``. This is the *floor* even for a perfect
    corrector, so it is the honest data budget for any future auto-apply.

    Example: at ``alpha = delta = 0.05`` a class needs at least 59 accepted-and-correct
    samples above the threshold before any distribution-free 95% guarantee is possible.
    """
    if not 0.0 < alpha < 1.0 or not 0.0 < delta < 1.0:
        raise ValueError("alpha and delta must be in (0, 1)")
    return math.ceil(math.log(delta) / math.log(1.0 - alpha))


def certification_reason(
    samples: Sequence[LabeledSample],
    *,
    alpha: float,
    delta: float = 0.05,
    min_support: int = 30,
) -> str | None:
    """Explain why a class could NOT be certified, or ``None`` if it was.

    Turns the opaque ``ABSTAIN_THRESHOLD`` (1.01) sentinel into a machine- and
    human-readable reason so a disabled auto-apply class is self-documenting rather
    than a magic number. Distinguishes the two distinct causes: too few labelled
    outcomes (fixable by collecting data) versus a corrector too imprecise to certify
    even with the data on hand (fixable only by a better corrector).
    """
    n = len(samples)
    if certify_threshold(samples, alpha=alpha, delta=delta, min_support=min_support) is not None:
        return None
    needed = min_samples_for_certification(alpha, delta)
    if n < min_support:
        return (
            f"insufficient_support: n={n} < min_support={min_support} "
            f"(need >= {needed} all-correct accepted samples to certify {1.0 - alpha:.0%})"
        )
    errors = sum(1 for _, correct in samples if not correct)
    cp_upper = _clopper_pearson_upper(errors, n, delta)
    return (
        f"precision_below_target: n={n} but Clopper-Pearson upper error {cp_upper:.3f} "
        f"> alpha {alpha} (corrector too imprecise; need >= {needed} all-correct at threshold)"
    )


def uncertified_reasons_by_class(
    calibration_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    alpha: float,
    delta: float = 0.05,
    min_support: int = 30,
) -> dict[str, str]:
    """Reason string for every class that cannot be certified (certifiable classes omitted)."""
    reasons: dict[str, str] = {}
    for error_class, samples in calibration_by_class.items():
        reason = certification_reason(samples, alpha=alpha, delta=delta, min_support=min_support)
        if reason is not None:
            reasons[error_class] = reason
    return reasons


def certified_coverage_report(
    samples_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    alpha: float = 0.05,
    delta: float = 0.05,
    min_support: int = 30,
    seed: int = 0,
    calib_fraction: float = 0.5,
) -> dict[str, object]:
    """Certify thresholds on a calibration split and *measure* them on the test split.

    This is the honest, reproducible artifact the project needs: it never reports
    an in-sample number. Thresholds are certified on the calibration half via
    conformal risk control, then the auto-applied set is scored on the disjoint
    test half. The reported per-class ``test_error`` is what a user would actually
    experience; ``test_coverage`` is the fraction of that class auto-applied.

    Args:
        samples_by_class: ``{error_class: [(confidence, was_correct), ...]}`` -
            the full labeled sample from a benchmark run.
        alpha: Target auto-apply error budget (``1 - target_precision``).
        delta: Failure probability of the guarantee.
        min_support: Minimum accepted count to certify a class.
        seed: Split seed (reproducible).
        calib_fraction: Fraction routed to calibration.

    Returns:
        A JSON-friendly report: overall auto-applied ``test_error`` and
        ``test_coverage`` plus per-class detail and the certified thresholds.
    """
    calibration, test = split_by_class(samples_by_class, seed=seed, calib_fraction=calib_fraction)
    thresholds = certify_thresholds_by_class(
        calibration, alpha=alpha, delta=delta, min_support=min_support
    )
    per_class: dict[str, dict[str, float | int]] = {}
    total_applied = 0
    total_errors = 0
    total_support = 0
    for error_class, test_samples in test.items():
        threshold = thresholds.get(error_class, ABSTAIN_THRESHOLD)
        applied = [correct for conf, correct in test_samples if conf >= threshold]
        n_applied = len(applied)
        n_errors = sum(1 for correct in applied if not correct)
        support = len(test_samples)
        total_applied += n_applied
        total_errors += n_errors
        total_support += support
        per_class[error_class] = {
            "threshold": round(threshold, 4),
            "test_support": support,
            "auto_applied": n_applied,
            "test_error": round(n_errors / n_applied, 4) if n_applied else 0.0,
            "test_coverage": round(n_applied / support, 4) if support else 0.0,
        }
    return {
        "alpha": alpha,
        "delta": delta,
        "min_support": min_support,
        "seed": seed,
        "calib_fraction": calib_fraction,
        "overall_test_error": round(total_errors / total_applied, 4) if total_applied else 0.0,
        "overall_test_coverage": (
            round(total_applied / total_support, 4) if total_support else 0.0
        ),
        "auto_applied_total": total_applied,
        "test_total": total_support,
        "per_class": per_class,
    }


def population_stability_index(
    reference: Sequence[float],
    live: Sequence[float],
    *,
    bins: int = 10,
) -> float:
    """Population Stability Index between a reference and a live score sample.

    The conformal guarantee is only valid for data *exchangeable* with the
    calibration sample. In production a user's table may be nothing like the
    calibration benchmark, silently voiding the guarantee. PSI is a cheap,
    standard drift signal on the confidence-score distribution: roughly, < 0.1
    means no meaningful shift, 0.1-0.25 moderate, > 0.25 significant. When drift
    is significant the caller should downgrade auto-apply to review, because the
    exchangeability the certificate rests on no longer holds.

    Args:
        reference: Calibration-time scores (e.g. proposal confidences).
        live: Scores observed on the table being repaired now.
        bins: Number of equal-width bins over [0, 1].

    Returns:
        PSI >= 0.0; 0.0 when either sample is empty (no signal, stay safe by
        making no drift claim).
    """
    if not reference or not live:
        return 0.0
    epsilon = 1e-6
    ref_counts = [0] * bins
    live_counts = [0] * bins
    for value in reference:
        ref_counts[min(int(min(max(value, 0.0), 1.0) * bins), bins - 1)] += 1
    for value in live:
        live_counts[min(int(min(max(value, 0.0), 1.0) * bins), bins - 1)] += 1
    n_ref, n_live = len(reference), len(live)
    psi = 0.0
    for index in range(bins):
        ref_frac = ref_counts[index] / n_ref + epsilon
        live_frac = live_counts[index] / n_live + epsilon
        psi += (live_frac - ref_frac) * math.log(live_frac / ref_frac)
    return round(psi, 6)


def risk_coverage_curve(samples: Sequence[LabeledSample]) -> list[dict[str, float]]:
    """Risk-coverage curve for the auto-apply gate (Geifman & El-Yaniv, 2017).

    Framing the gate as selective classification: as the confidence threshold
    drops, more proposals are accepted (coverage rises) and the selective risk
    (fraction of accepted proposals that are wrong = 1 - precision) typically
    rises. A model with a usable safe operating point has low risk at meaningful
    coverage; a miscalibrated one has high risk at every coverage > 0.

    Args:
        samples: ``(confidence, was_correct)`` pairs.

    Returns:
        Points ``{"threshold", "coverage", "selective_risk"}`` ordered by
        increasing coverage (one point per distinct confidence, plus the
        full-coverage endpoint). Empty for empty input.
    """
    if not samples:
        return []
    total = len(samples)
    ordered = sorted(samples, key=lambda s: s[0], reverse=True)
    curve: list[dict[str, float]] = []
    accepted = 0
    errors = 0
    index = 0
    n = len(ordered)
    while index < n:
        confidence = ordered[index][0]
        while index < n and ordered[index][0] == confidence:
            accepted += 1
            errors += 0 if ordered[index][1] else 1
            index += 1
        curve.append(
            {
                "threshold": round(confidence, 6),
                "coverage": round(accepted / total, 6),
                "selective_risk": round(errors / accepted, 6),
            }
        )
    return curve


def area_under_risk_coverage(curve: Sequence[dict[str, float]]) -> float:
    """Area under the risk-coverage curve (AURC); lower is better.

    Trapezoidal integral of selective risk over coverage. 0.0 for an empty
    curve or a perfect model; approaches 1.0 when risk stays ~1 across coverage
    (no safe operating point). This single number summarizes how usable a
    proposer is under a reject option.
    """
    points = [pt for pt in curve if pt.get("coverage", 0.0) > 0.0]
    if not points:
        return 0.0
    points = sorted(points, key=lambda pt: pt["coverage"])
    area = 0.0
    prev_cov = 0.0
    prev_risk = points[0]["selective_risk"]
    for pt in points:
        width = pt["coverage"] - prev_cov
        area += width * (pt["selective_risk"] + prev_risk) / 2.0
        prev_cov = pt["coverage"]
        prev_risk = pt["selective_risk"]
    return round(area / prev_cov, 6) if prev_cov > 0 else 0.0


def repeated_split_certification(
    samples_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    alpha: float,
    delta: float = 0.05,
    min_support: int = 30,
    splits: int = 200,
) -> dict[str, float]:
    """Validate the conformal guarantee on real data over many random splits.

    This is the real-data analog of the synthetic Monte-Carlo validity proof:
    for each of ``splits`` seeds, certify per-class thresholds on the calibration
    half and *measure* the auto-applied error on the disjoint test half. A valid
    procedure keeps the fraction of splits whose test error exceeds ``alpha`` at
    or below ``delta``.

    Args:
        samples_by_class: ``{error_class: [(confidence, was_correct), ...]}``.
        alpha: Target auto-apply error budget.
        delta: Failure probability of the guarantee.
        min_support: Minimum accepted count to certify a class.
        splits: Number of random calibration/test splits.

    Returns:
        ``{"splits", "certified_rate", "over_alpha_rate", "mean_test_coverage",
        "mean_test_error"}``.
    """
    certified = 0
    over_alpha = 0
    applied_splits = 0
    coverage_sum = 0.0
    error_sum = 0.0
    for seed in range(splits):
        calibration, test = split_by_class(samples_by_class, seed=seed)
        thresholds = certify_thresholds_by_class(
            calibration, alpha=alpha, delta=delta, min_support=min_support
        )
        any_certified = any(t <= 1.0 for t in thresholds.values())
        if any_certified:
            certified += 1
        applied = 0
        errors = 0
        support = 0
        for error_class, test_samples in test.items():
            threshold = thresholds.get(error_class, ABSTAIN_THRESHOLD)
            for conf, correct in test_samples:
                support += 1
                if conf >= threshold:
                    applied += 1
                    errors += 0 if correct else 1
        if applied > 0:
            applied_splits += 1
            split_error = errors / applied
            coverage_sum += applied / support if support else 0.0
            error_sum += split_error
            if split_error > alpha:
                over_alpha += 1
    return {
        "splits": float(splits),
        "certified_rate": round(certified / splits, 6) if splits else 0.0,
        "over_alpha_rate": round(over_alpha / splits, 6) if splits else 0.0,
        "mean_test_coverage": round(coverage_sum / applied_splits, 6) if applied_splits else 0.0,
        "mean_test_error": round(error_sum / applied_splits, 6) if applied_splits else 0.0,
    }


def reliability_curve(samples: Sequence[LabeledSample], *, bins: int = 5) -> list[dict[str, float]]:
    """Reliability-diagram data: per-bin mean confidence vs empirical accuracy.

    The standard calibration visualization (Guo et al., 2017). A well-calibrated
    model has accuracy ~= mean confidence in every bin; the gap is the per-bin
    calibration error that ECE aggregates.

    Args:
        samples: ``(confidence, was_correct)`` pairs.
        bins: Number of equal-width confidence bins over [0, 1].

    Returns:
        One dict per non-empty bin: ``{"bin_lower", "bin_upper",
        "mean_confidence", "accuracy", "count"}``. Empty for empty input.
    """
    if not samples:
        return []
    conf_sum = [0.0] * bins
    correct = [0] * bins
    counts = [0] * bins
    for confidence, was_correct in samples:
        clamped = min(max(confidence, 0.0), 1.0)
        index = min(int(clamped * bins), bins - 1)
        conf_sum[index] += clamped
        correct[index] += 1 if was_correct else 0
        counts[index] += 1
    curve: list[dict[str, float]] = []
    for index in range(bins):
        if counts[index] == 0:
            continue
        curve.append(
            {
                "bin_lower": round(index / bins, 4),
                "bin_upper": round((index + 1) / bins, 4),
                "mean_confidence": round(conf_sum[index] / counts[index], 6),
                "accuracy": round(correct[index] / counts[index], 6),
                "count": counts[index],
            }
        )
    return curve
