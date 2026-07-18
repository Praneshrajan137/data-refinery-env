"""Post-hoc probability calibration for the DataForge auto-apply gate.

Why this exists
---------------
The LLM corrector reports a *confidence* that is really a self-consistency
agreement fraction (how often k independent samples agreed). Agreement is a
useful ranking signal but a poor *probability*: measured Expected Calibration
Error (ECE) on corrector output is high (~0.8), so a reported "0.9" does not
mean "correct 90% of the time". Two things depend on a trustworthy probability:

1. Honest reporting -- a user who sees a confidence should be able to read it as
   a probability of correctness.
2. The selective auto-apply gate -- :mod:`dataforge.conformal` certifies a
   per-class threshold; feeding it a *calibrated* score (rather than raw
   agreement) makes the operating point interpretable and the reliability
   diagram flat.

This module fits a **post-hoc, monotonic** calibration map from raw confidence
to calibrated probability, per error class, on a **calibration split only**
(never the split used to report metrics), following the standard references:

- Zadrozny & Elkan (2002), isotonic-regression calibration.
- Platt (1999), sigmoid/logistic calibration.
- Guo et al. (2017), "On Calibration of Modern Neural Networks" (ECE, reliability
  diagrams).

Both fitters are pure-Python (no numpy/sklearn), matching the dependency-free
style of :mod:`dataforge.conformal`. Isotonic is the default: it is
nonparametric, order-preserving (so it never re-ranks proposals), and fit with
the Pool Adjacent Violators Algorithm (PAVA). Platt is offered for small
calibration sets where a 2-parameter sigmoid is less variance-prone.

Calibration is *advisory to the safety stack*: it only rescales a score. The SMT
verifier, safety constitution, and provable-only auto-apply gate remain hard
gates beneath it, so a miscalibrated or adversarial map can only change which
plausibility-only fixes are surfaced for review -- never wave through a
corrupting write.
"""

from __future__ import annotations

import bisect
import math
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from dataforge.conformal import LabeledSample

__all__ = [
    "CalibrationMap",
    "CalibrationMethod",
    "identity_map",
    "fit_isotonic",
    "fit_platt",
    "fit_calibration_map",
    "fit_calibration_map_by_class",
    "calibrate_samples_by_class",
]

CalibrationMethod = Literal["isotonic", "platt", "identity"]

# Below this many calibration samples a fitted map overfits, so we fall back to
# the identity map (report the raw score unchanged) rather than invent one.
_DEFAULT_MIN_SUPPORT = 30


def _clamp_unit(value: float) -> float:
    """Clamp a score into the closed unit interval."""
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


class CalibrationMap(BaseModel):
    """A fitted, monotone map from raw confidence to calibrated probability.

    One of three shapes depending on ``method``:

    * ``isotonic`` -- a piecewise-linear step function defined by non-decreasing
      knots ``(x_knots, y_knots)``; predictions interpolate between knots and
      clamp at the ends.
    * ``platt`` -- a logistic ``sigmoid(a * confidence + b)``.
    * ``identity`` -- returns the (clamped) input unchanged; the honest fallback
      when there is too little data to fit anything.

    The model is frozen and JSON-serializable so a fitted map can be persisted
    alongside a benchmark artifact and re-applied deterministically.
    """

    method: CalibrationMethod
    x_knots: tuple[float, ...] = Field(default_factory=tuple)
    y_knots: tuple[float, ...] = Field(default_factory=tuple)
    a: float = 0.0
    b: float = 0.0

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    def predict(self, confidence: float) -> float:
        """Map a raw confidence in [0, 1] to a calibrated probability in [0, 1]."""
        score = _clamp_unit(float(confidence))
        if self.method == "identity":
            return score
        if self.method == "platt":
            return _sigmoid(self.a * score + self.b)
        return self._predict_isotonic(score)

    def _predict_isotonic(self, score: float) -> float:
        """Piecewise-linear interpolation over the isotonic knots, clamped."""
        xs = self.x_knots
        ys = self.y_knots
        if not xs:
            return score
        if score <= xs[0]:
            return _clamp_unit(ys[0])
        if score >= xs[-1]:
            return _clamp_unit(ys[-1])
        # Find the knot interval [xs[i-1], xs[i]] containing score.
        index = bisect.bisect_right(xs, score)
        x_lo, x_hi = xs[index - 1], xs[index]
        y_lo, y_hi = ys[index - 1], ys[index]
        if x_hi == x_lo:
            return _clamp_unit(y_hi)
        weight = (score - x_lo) / (x_hi - x_lo)
        return _clamp_unit(y_lo + weight * (y_hi - y_lo))


def _sigmoid(value: float) -> float:
    """Numerically stable logistic sigmoid."""
    if value >= 0.0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def identity_map() -> CalibrationMap:
    """Return the identity calibration map (report the raw score unchanged)."""
    return CalibrationMap(method="identity")


def fit_isotonic(samples: Sequence[LabeledSample]) -> CalibrationMap:
    """Fit an isotonic (monotone, nonparametric) calibration map via PAVA.

    Aggregates duplicate confidences, then runs the Pool Adjacent Violators
    Algorithm to produce the least-squares non-decreasing fit of empirical
    accuracy against confidence. The result is order-preserving: it never
    changes the *ranking* of proposals, only rescales the scores so that a
    calibrated value reads as a probability of correctness.

    Args:
        samples: ``(confidence, was_correct)`` pairs from the calibration split.

    Returns:
        An ``isotonic`` :class:`CalibrationMap`, or the identity map when the
        sample is empty.
    """
    if not samples:
        return identity_map()

    # Aggregate duplicate confidences into (mean accuracy, weight) in x order.
    grouped: OrderedDict[float, list[float]] = OrderedDict()
    for confidence, was_correct in sorted(samples, key=lambda s: s[0]):
        grouped.setdefault(_clamp_unit(float(confidence)), []).append(1.0 if was_correct else 0.0)
    xs = list(grouped.keys())
    means = [sum(vals) / len(vals) for vals in grouped.values()]
    weights = [float(len(vals)) for vals in grouped.values()]

    # PAVA: maintain a stack of pooled blocks [value, weight, span]; merge left
    # while the previous block's value violates monotonicity (>= current).
    stack: list[list[float]] = []
    for value, weight in zip(means, weights, strict=True):
        current = [value, weight, 1.0]
        while stack and stack[-1][0] >= current[0]:
            prev = stack.pop()
            pooled_weight = prev[1] + current[1]
            pooled_value = (prev[0] * prev[1] + current[0] * current[1]) / pooled_weight
            current = [pooled_value, pooled_weight, prev[2] + current[2]]
        stack.append(current)

    # Expand pooled block values back to one fitted value per unique confidence.
    y_fit: list[float] = []
    for value, _weight, span in stack:
        y_fit.extend([value] * int(span))
    return CalibrationMap(method="isotonic", x_knots=tuple(xs), y_knots=tuple(y_fit))


def fit_platt(
    samples: Sequence[LabeledSample],
    *,
    max_iterations: int = 100,
    tolerance: float = 1e-9,
) -> CalibrationMap:
    """Fit a Platt (logistic) calibration map ``sigmoid(a * conf + b)``.

    Uses Platt's (1999) target smoothing to avoid overfitting on separable data
    and a damped Newton-Raphson solve on the two parameters. Falls back to the
    identity map when the sample is degenerate (empty, single class, or the
    solve fails to stay finite).

    Args:
        samples: ``(confidence, was_correct)`` pairs from the calibration split.
        max_iterations: Maximum Newton steps.
        tolerance: Stop when the objective improves by less than this.

    Returns:
        A ``platt`` :class:`CalibrationMap`, or the identity map on degeneracy.
    """
    n_pos = sum(1 for _, correct in samples if correct)
    n_neg = len(samples) - n_pos
    if n_pos == 0 or n_neg == 0:
        return identity_map()

    # Platt target smoothing: shrink hard 0/1 labels toward the interior.
    hi_target = (n_pos + 1.0) / (n_pos + 2.0)
    lo_target = 1.0 / (n_neg + 2.0)
    points = [
        (_clamp_unit(float(conf)), hi_target if correct else lo_target) for conf, correct in samples
    ]

    a = 0.0
    b = math.log((n_neg + 1.0) / (n_pos + 1.0))

    def neg_log_likelihood(a_val: float, b_val: float) -> float:
        total = 0.0
        for x, target in points:
            logit = a_val * x + b_val
            # log(1 + exp(logit)) computed stably.
            softplus = max(logit, 0.0) + math.log1p(math.exp(-abs(logit)))
            total += softplus - target * logit
        return total

    loss = neg_log_likelihood(a, b)
    for _ in range(max_iterations):
        grad_a = grad_b = 0.0
        hess_aa = hess_ab = hess_bb = 0.0
        for x, target in points:
            prob = _sigmoid(a * x + b)
            error = prob - target
            weight = prob * (1.0 - prob)
            grad_a += error * x
            grad_b += error
            hess_aa += weight * x * x
            hess_ab += weight * x
            hess_bb += weight
        # Small ridge keeps the 2x2 Hessian invertible near separation.
        hess_aa += 1e-12
        hess_bb += 1e-12
        det = hess_aa * hess_bb - hess_ab * hess_ab
        if abs(det) < 1e-18:
            break
        step_a = (hess_bb * grad_a - hess_ab * grad_b) / det
        step_b = (hess_aa * grad_b - hess_ab * grad_a) / det
        # Backtracking line search guarantees monotone decrease.
        scale = 1.0
        new_loss = loss
        for _ in range(20):
            cand_a = a - scale * step_a
            cand_b = b - scale * step_b
            new_loss = neg_log_likelihood(cand_a, cand_b)
            if new_loss <= loss:
                break
            scale *= 0.5
        if not math.isfinite(new_loss) or loss - new_loss < tolerance:
            a -= scale * step_a
            b -= scale * step_b
            loss = new_loss
            break
        a -= scale * step_a
        b -= scale * step_b
        loss = new_loss

    if not (math.isfinite(a) and math.isfinite(b)):
        return identity_map()
    return CalibrationMap(method="platt", a=a, b=b)


def fit_calibration_map(
    samples: Sequence[LabeledSample],
    *,
    method: CalibrationMethod = "isotonic",
    min_support: int = _DEFAULT_MIN_SUPPORT,
) -> CalibrationMap:
    """Fit one calibration map, falling back to identity below ``min_support``.

    Args:
        samples: ``(confidence, was_correct)`` calibration-split pairs.
        method: ``"isotonic"`` (default), ``"platt"``, or ``"identity"``.
        min_support: Minimum samples required to fit; below this the honest
            choice is the identity map (report the raw score).

    Returns:
        A fitted :class:`CalibrationMap`.
    """
    if method == "identity" or len(samples) < min_support:
        return identity_map()
    if method == "platt":
        return fit_platt(samples)
    return fit_isotonic(samples)


def fit_calibration_map_by_class(
    calibration_by_class: Mapping[str, Sequence[LabeledSample]],
    *,
    method: CalibrationMethod = "isotonic",
    min_support: int = _DEFAULT_MIN_SUPPORT,
) -> dict[str, CalibrationMap]:
    """Fit a per-class calibration map (Mondrian calibration).

    Each error class is calibrated independently, mirroring the per-class
    conformal certification in :func:`dataforge.conformal.certify_thresholds_by_class`,
    so a class with well-calibrated agreement is not dragged by a poorly
    calibrated one. Low-support classes get the identity map.

    Args:
        calibration_by_class: ``{error_class: [(confidence, was_correct), ...]}``
            from the calibration split only.
        method: Calibration method applied to every class.
        min_support: Minimum per-class samples to fit.

    Returns:
        ``{error_class: CalibrationMap}``.
    """
    return {
        error_class: fit_calibration_map(samples, method=method, min_support=min_support)
        for error_class, samples in calibration_by_class.items()
    }


def calibrate_samples_by_class(
    maps_by_class: Mapping[str, CalibrationMap],
    samples_by_class: Mapping[str, Sequence[LabeledSample]],
) -> dict[str, list[LabeledSample]]:
    """Apply per-class calibration maps to labeled samples for downstream gating.

    Rescales each ``(confidence, was_correct)`` pair's confidence through its
    class map (identity for any class without a fitted map), preserving the
    correctness label. The result feeds the conformal auto-apply gate on a
    *calibrated* score. The caller must pass a split disjoint from the one used
    to fit the maps to keep the downstream guarantee leakage-free.

    Args:
        maps_by_class: Fitted maps from :func:`fit_calibration_map_by_class`.
        samples_by_class: Samples to rescale (e.g. the test split).

    Returns:
        ``{error_class: [(calibrated_confidence, was_correct), ...]}``.
    """
    calibrated: dict[str, list[LabeledSample]] = {}
    for error_class, samples in samples_by_class.items():
        calibration_map = maps_by_class.get(error_class)
        if calibration_map is None:
            calibrated[error_class] = [(_clamp_unit(float(c)), ok) for c, ok in samples]
            continue
        calibrated[error_class] = [(calibration_map.predict(c), ok) for c, ok in samples]
    return calibrated
