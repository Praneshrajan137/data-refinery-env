"""Calibration and abstention policy for DataForge repairs.

This is the product differentiator made measurable: DataForge auto-applies a
repair only when the proposing detector's calibrated confidence clears a
threshold fit to a target precision (default 0.95). Below that, the repair is
*proposed for review*, never silently applied. The tool is therefore broad and
safe at once - coverage rises while auto-apply precision stays high.

The policy is advisory to the detection/repair stack: a repairer marks a
proposed fix's confidence, and :meth:`AbstentionPolicy.action_for` decides
whether it is eligible for auto-apply. The existing SMT verifier and safety
constitution remain hard gates underneath - abstention only ever makes the
system *more* conservative, never less.

Thresholds are fit empirically (:func:`fit_thresholds`) against labeled
``(confidence, was_correct)`` samples from a benchmark run, so the auto-apply
boundary is grounded in measured precision, not guesswork.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import BaseModel, Field

__all__ = [
    "AbstentionAction",
    "AbstentionPolicy",
    "conformal_corrector_policy",
    "corrector_default_policy",
    "default_policy",
    "fit_thresholds",
    "guard_policy_for_drift",
    "policy_from_corrector_samples",
    "severity_for_action",
]

AbstentionAction = Literal["auto_apply", "review"]

# Conservative defaults. Detector families whose deterministic proposals are
# provably exact (decimal_shift uses an arithmetic inverse; fd_violation a
# strict majority) auto-apply at lower confidence; fuzzier families require
# higher confidence before auto-apply.
_DEFAULT_THRESHOLDS: dict[str, float] = {
    "decimal_shift": 0.70,
    "fd_violation": 0.80,
    "type_mismatch": 0.80,
    "format_violation": 0.90,
    "categorical_normalization": 0.90,
    "missing_value": 1.01,  # detection-only by default: never auto-apply
    "outlier": 1.01,  # detection-only: flag, do not auto-fix
    "duplicate_row": 1.01,  # detection-only: row deletes are constitution-blocked
}


class AbstentionPolicy(BaseModel):
    """Maps an issue's calibrated confidence to an auto-apply / review decision.

    Args:
        target_precision: The precision the auto-apply thresholds were fit for.
        auto_apply_thresholds: Per-issue-type minimum confidence to auto-apply.
        default_threshold: Threshold for issue types not listed.
    """

    target_precision: float = Field(default=0.95, ge=0.0, le=1.0)
    auto_apply_thresholds: dict[str, float] = Field(default_factory=dict)
    default_threshold: float = Field(default=0.90, ge=0.0, le=1.01)

    model_config = {"frozen": True}

    def threshold_for(self, issue_type: str) -> float:
        """Return the auto-apply confidence threshold for an issue type."""
        return self.auto_apply_thresholds.get(issue_type, self.default_threshold)

    def action_for(self, issue_type: str, confidence: float) -> AbstentionAction:
        """Decide whether a proposed fix may auto-apply or must be reviewed.

        Args:
            issue_type: The detector issue type.
            confidence: The proposing detector's calibrated confidence in [0, 1].

        Returns:
            ``"auto_apply"`` if confidence clears the threshold, else ``"review"``.
        """
        return "auto_apply" if confidence >= self.threshold_for(issue_type) else "review"


def default_policy() -> AbstentionPolicy:
    """Return the conservative default abstention policy."""
    return AbstentionPolicy(
        target_precision=0.95,
        auto_apply_thresholds=dict(_DEFAULT_THRESHOLDS),
        default_threshold=0.90,
    )


def severity_for_action(action: AbstentionAction) -> str:
    """Map an abstention action to the detector severity label."""
    return "safe" if action == "auto_apply" else "review"


def corrector_default_policy() -> AbstentionPolicy:
    """Return the honest default policy for the LLM corrector: propose-not-apply.

    Until per-class thresholds are fit from measured corrector correctness (see
    :func:`policy_from_corrector_samples`), every corrector proposal is surfaced
    as a human-review suggestion and never auto-applied. The high target
    precision is recorded so the intent of the boundary is explicit.
    """
    return AbstentionPolicy(
        target_precision=0.95,
        auto_apply_thresholds={},
        default_threshold=1.01,
    )


def policy_from_corrector_samples(
    samples_by_class: dict[str, list[tuple[float, bool]]],
    *,
    target_precision: float = 0.95,
    min_support: int = 10,
) -> AbstentionPolicy:
    """Build a corrector abstention policy from labeled correctness samples.

    Fits a per-class auto-apply threshold to the precision floor (reusing
    :func:`fit_thresholds`) and keeps the propose-not-apply default for any
    class that is unlisted, low-support, or cannot reach the floor. The result
    auto-applies only where measured precision justifies it; everything else
    becomes a review suggestion.
    """
    thresholds = fit_thresholds(
        samples_by_class,
        target_precision=target_precision,
        min_support=min_support,
    )
    return AbstentionPolicy(
        target_precision=target_precision,
        auto_apply_thresholds=thresholds,
        default_threshold=1.01,
    )


def fit_thresholds(
    samples_by_class: dict[str, list[tuple[float, bool]]],
    *,
    target_precision: float = 0.95,
    min_support: int = 10,
) -> dict[str, float]:
    """Fit per-class auto-apply confidence thresholds to a target precision.

    For each class, finds the lowest confidence threshold ``t`` such that the
    predictions with ``confidence >= t`` achieve at least ``target_precision``.
    This maximizes recall subject to the precision floor. Classes with too few
    samples, or that cannot reach the target at any threshold, get ``1.01``
    (never auto-apply) - the honest, conservative default.

    Args:
        samples_by_class: ``{issue_type: [(confidence, was_correct), ...]}``.
        target_precision: Minimum precision the threshold must guarantee.
        min_support: Minimum labeled samples required to fit a class.

    Returns:
        ``{issue_type: threshold}``. A threshold of 1.01 means detection-only.
    """
    thresholds: dict[str, float] = {}
    for issue_type, samples in samples_by_class.items():
        if len(samples) < min_support:
            thresholds[issue_type] = 1.01
            continue
        # Candidate thresholds are the observed confidences (descending): adding
        # each next-lower confidence grows the auto-apply set. Precision is only
        # evaluated at confidence-group boundaries so tied confidences (which a
        # threshold includes together) are scored together.
        ordered = sorted(samples, key=lambda s: s[0], reverse=True)
        best_threshold = 1.01
        applied = 0
        correct = 0
        index = 0
        n = len(ordered)
        while index < n:
            confidence = ordered[index][0]
            while index < n and ordered[index][0] == confidence:
                applied += 1
                correct += 1 if ordered[index][1] else 0
                index += 1
            if correct / applied >= target_precision:
                best_threshold = confidence
        thresholds[issue_type] = round(best_threshold, 4)
    return thresholds


def conformal_corrector_policy(
    calibration_by_class: Mapping[str, Sequence[tuple[float, bool]]],
    *,
    alpha: float = 0.05,
    delta: float = 0.05,
    min_support: int = 30,
) -> AbstentionPolicy:
    """Build an auto-apply policy with a *distribution-free* per-class guarantee.

    This is the rigorous replacement for :func:`policy_from_corrector_samples`.
    Where that function fits a threshold to a precision floor **in-sample** (which
    overstates precision on new data), this certifies each class's threshold with
    conformal risk control (:func:`dataforge.conformal.certify_thresholds_by_class`):
    with probability >= ``1 - delta``, an auto-applied class's true error rate is
    <= ``alpha`` on data exchangeable with the calibration sample. Classes that
    cannot be certified stay propose-not-apply.

    The caller MUST pass the **calibration split only** and reserve a disjoint
    test split for honest metric reporting (see
    :func:`dataforge.conformal.split_by_class`). The SMT verifier and safety
    constitution remain hard gates beneath this policy - it only ever narrows
    what is eligible for auto-apply.

    Args:
        calibration_by_class: ``{issue_type: [(confidence, was_correct), ...]}``
            from the calibration split.
        alpha: Maximum tolerated per-class auto-apply error (``1 - target_precision``).
        delta: Failure probability of the guarantee.
        min_support: Minimum accepted-sample count to certify a class.

    Returns:
        An :class:`AbstentionPolicy` auto-applying only certified classes.
    """
    from dataforge.conformal import certify_thresholds_by_class

    thresholds = certify_thresholds_by_class(
        calibration_by_class,
        alpha=alpha,
        delta=delta,
        min_support=min_support,
    )
    return AbstentionPolicy(
        target_precision=1.0 - alpha,
        auto_apply_thresholds=thresholds,
        default_threshold=1.01,
    )


def guard_policy_for_drift(
    policy: AbstentionPolicy,
    reference_confidences: Sequence[float],
    live_confidences: Sequence[float],
    *,
    psi_threshold: float = 0.2,
) -> AbstentionPolicy:
    """Downgrade to propose-not-apply when the live distribution has drifted.

    A conformal certificate is valid only for data exchangeable with the
    calibration sample. If the confidence distribution on the table being
    repaired differs materially from calibration (Population Stability Index
    above ``psi_threshold``), the guarantee no longer holds, so we return the
    conservative propose-not-apply policy instead of the certified one. When
    distributions agree, the certified policy is returned unchanged.

    Args:
        policy: The certified policy to use when no drift is detected.
        reference_confidences: Calibration-time confidence scores.
        live_confidences: Confidence scores on the current table.
        psi_threshold: PSI above which auto-apply is downgraded (default 0.2).

    Returns:
        ``policy`` if stable, else :func:`corrector_default_policy`.
    """
    from dataforge.conformal import population_stability_index

    psi = population_stability_index(reference_confidences, live_confidences)
    return corrector_default_policy() if psi > psi_threshold else policy
