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

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

if TYPE_CHECKING:
    from dataforge.calibration_map import CalibrationMap
    from dataforge.table import TableLike

__all__ = [
    "AbstentionAction",
    "AbstentionPolicy",
    "CalibrationScope",
    "calibrated_conformal_corrector_policy",
    "conformal_corrector_policy",
    "corrector_default_policy",
    "default_policy",
    "fit_thresholds",
    "guard_policy_for_drift",
    "guard_policy_for_drift_by_class",
    "guard_policy_for_scope",
    "load_calibration_scope",
    "load_corrector_calibration",
    "policy_from_corrector_samples",
    "severity_for_action",
    "table_fingerprint",
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
        uncertified_classes: Per-issue-type reason a class is disabled (threshold
            >= 1.01). Makes the "never auto-apply" sentinel self-documenting:
            distinguishes detection-only-by-design from data/precision limits.
    """

    target_precision: float = Field(default=0.95, ge=0.0, le=1.0)
    auto_apply_thresholds: dict[str, float] = Field(default_factory=dict)
    default_threshold: float = Field(default=0.90, ge=0.0, le=1.01)
    uncertified_classes: dict[str, str] = Field(default_factory=dict)

    # ``extra="forbid"`` is a write-safety property, not tidiness. Added 2026-08-26 after
    # measuring what a plausible future wiring of `SessionCertification` would do.
    #
    # The certificate that carries per-table certified thresholds is printed and discarded, and
    # `dataforge repair` reads this four-block artifact instead. The known incompatibility is that
    # a certificate has no ``policy`` key, so the loader raises. The obvious fix -- wrap it in one
    # -- was measured, and it does not raise. A certificate-shaped block was ACCEPTED, its
    # certified ``thresholds`` silently dropped as an unknown field, and ``default_threshold`` fell
    # back to 0.90. At confidence 0.95 that flips the decision from ``review`` to ``auto_apply``:
    # a write against a threshold nobody certified, reached with no error and no log line. The
    # conservative default this path is supposed to hold is 1.01, meaning never.
    #
    # This is a guard being weakest exactly where it is most needed. The permissive model sits at
    # the boundary a well-intentioned wiring attempt arrives through, so silence there converts an
    # abstention into a write. A field this model does not recognise must be an error, because the
    # only alternative to refusing it is guessing a threshold.
    model_config = ConfigDict(extra="forbid", frozen=True)

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
    from dataforge.conformal import certify_thresholds_by_class, uncertified_reasons_by_class

    thresholds = certify_thresholds_by_class(
        calibration_by_class,
        alpha=alpha,
        delta=delta,
        min_support=min_support,
    )
    uncertified = uncertified_reasons_by_class(
        calibration_by_class,
        alpha=alpha,
        delta=delta,
        min_support=min_support,
    )
    return AbstentionPolicy(
        target_precision=1.0 - alpha,
        auto_apply_thresholds=thresholds,
        default_threshold=1.01,
        uncertified_classes=uncertified,
    )


def calibrated_conformal_corrector_policy(
    calibration_by_type: Mapping[str, Sequence[tuple[float, bool]]],
    *,
    method: Literal["isotonic", "platt"] = "isotonic",
    alpha: float = 0.05,
    delta: float = 0.05,
    min_support: int = 30,
) -> tuple[AbstentionPolicy, dict[str, CalibrationMap]]:
    """Fit per-issue-type calibration maps, then certify thresholds on calibrated scores.

    This is the auto-apply-ready counterpart of :func:`conformal_corrector_policy`.
    It (1) fits a post-hoc calibration map per issue type on the calibration split,
    (2) rescales the samples through those maps, and (3) certifies distribution-free
    per-class thresholds on the *calibrated* scores. The returned maps MUST be applied
    to a fix's raw confidence at inference (before :meth:`AbstentionPolicy.action_for`)
    so the threshold and the score live on the same calibrated scale.

    Samples and maps are keyed by **issue_type** (``CellFix.detector_id``), which is the
    key the engine uses at auto-apply time -- not the ground-truth error class. The SMT
    verifier, safety constitution, and provable-only gate remain hard gates beneath this
    policy; calibration only ever narrows what is eligible for auto-apply.

    Args:
        calibration_by_type: ``{issue_type: [(confidence, was_correct), ...]}`` from the
            calibration split only.
        method: Post-hoc calibration family (``"isotonic"`` default, or ``"platt"``).
        alpha: Maximum tolerated per-class auto-apply error (``1 - target_precision``).
        delta: Failure probability of the conformal guarantee.
        min_support: Minimum accepted-sample count to fit a map and certify a class.

    Returns:
        ``(policy, maps_by_issue_type)``. Classes that cannot be certified stay
        propose-not-apply (threshold ``1.01``); classes below ``min_support`` get an
        identity map.
    """
    from dataforge.calibration_map import calibrate_samples_by_class, fit_calibration_map_by_class

    maps = fit_calibration_map_by_class(calibration_by_type, method=method, min_support=min_support)
    calibrated = calibrate_samples_by_class(maps, calibration_by_type)
    policy = conformal_corrector_policy(
        calibrated, alpha=alpha, delta=delta, min_support=min_support
    )
    return policy, maps


def load_corrector_calibration(
    path: Path,
) -> tuple[AbstentionPolicy, dict[str, CalibrationMap], dict[str, list[float]]]:
    """Load a persisted certified corrector policy, per-issue-type maps, and drift reference.

    The artifact is the committed output of the calibration pipeline. Its ``policy``
    block reconstructs an :class:`AbstentionPolicy` (certified per-issue-type thresholds),
    its ``maps`` block reconstructs one :class:`CalibrationMap` per issue type, and its
    ``reference_confidences`` block carries the raw calibration-split confidences per issue
    type. The engine applies the maps to a fix's raw confidence before the policy decides
    auto-apply (so both live on the same calibrated scale) and PSI-compares the live
    confidence distribution against the reference to downgrade auto-apply under drift.

    Args:
        path: Path to the certified corrector-calibration JSON artifact.

    Returns:
        ``(policy, maps_by_issue_type, reference_confidences_by_issue_type)`` ready to
        pass into ``RepairPipelineRequest``. ``reference_confidences`` is ``{}`` for
        older artifacts that predate the drift-guard field.

    Raises:
        ValueError: If the artifact is malformed.
    """
    from dataforge.calibration_map import CalibrationMap

    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Corrector calibration artifact must be a JSON object.")
    policy_block = raw.get("policy")
    if not isinstance(policy_block, dict):
        raise ValueError("Corrector calibration artifact is missing a 'policy' object.")
    try:
        policy = AbstentionPolicy.model_validate(policy_block)
    except ValidationError as exc:
        # Named explicitly because the reachable way to get here is wrapping a
        # `SessionCertification` in a `policy` block, which is the obvious fix for the fact that
        # `dataforge calibrate` prints a certificate this loader cannot read. Before
        # `AbstentionPolicy` forbade extra fields that payload was ACCEPTED, its certified
        # thresholds silently dropped, and `default_threshold` fell back to 0.90 -- flipping a
        # 0.95-confidence fix from `review` to `auto_apply` against a threshold nobody certified.
        # The certificate and this artifact are different quantities on different statistical
        # bases; translating between them is a decision, not a shape change.
        raise ValueError(
            "Corrector calibration artifact's 'policy' block is not an AbstentionPolicy: "
            f"{exc.errors()[0].get('loc')} {exc.errors()[0].get('msg')}. If this came from "
            "`dataforge calibrate --certify`, that certificate is NOT a corrector calibration "
            "artifact and must not be reshaped into one. See PRODUCT.md section 1.3."
        ) from exc
    maps_block = raw.get("maps", {})
    if not isinstance(maps_block, dict):
        raise ValueError("Corrector calibration artifact 'maps' must be an object.")
    maps = {
        str(issue_type): CalibrationMap.model_validate(dump, strict=False)
        for issue_type, dump in maps_block.items()
    }
    reference_block = raw.get("reference_confidences", {})
    if not isinstance(reference_block, dict):
        raise ValueError(
            "Corrector calibration artifact 'reference_confidences' must be an object."
        )
    reference_confidences = {
        str(issue_type): [float(c) for c in confs] for issue_type, confs in reference_block.items()
    }
    return policy, maps, reference_confidences


def guard_policy_for_drift_by_class(
    policy: AbstentionPolicy,
    reference_confidences: Mapping[str, Sequence[float]],
    live_confidences_by_class: Mapping[str, Sequence[float]],
    *,
    psi_threshold: float = 0.2,
    min_live: int = 5,
) -> tuple[AbstentionPolicy, dict[str, float]]:
    """Downgrade only the issue types whose confidence distribution has drifted.

    :func:`guard_policy_for_drift` pools every class into one PSI comparison, which
    discards precisely the Mondrian structure that :func:`certify_thresholds_by_class`
    builds: certification is *per class*, so enforcement should be too. Pooling has two
    failure modes, both silent. A shift confined to one class can be masked by the pooled
    histogram, leaving a drifted class auto-applying; and a shift in one class can trip the
    pooled test, needlessly disabling every other class.

    Classes with fewer than ``min_live`` live confidences are left untouched, because PSI
    on a handful of points is noise rather than evidence.

    Args:
        policy: The certified policy to start from.
        reference_confidences: Calibration-time confidences per issue type.
        live_confidences_by_class: This run's raw LLM confidences per issue type.
        psi_threshold: PSI above which a class is downgraded (default 0.2).
        min_live: Minimum live samples before a class is judged at all.

    Returns:
        ``(guarded_policy, psi_by_class)``. Drifted classes are set to
        :data:`~dataforge.conformal.ABSTAIN_THRESHOLD` so they can never auto-apply, and
        are added to ``uncertified_classes`` so the reason surfaces truthfully. Undrifted
        classes keep their certified thresholds.
    """
    from dataforge.conformal import ABSTAIN_THRESHOLD, population_stability_index

    psi_by_class: dict[str, float] = {}
    drifted: list[str] = []
    for issue_type, live in live_confidences_by_class.items():
        reference = reference_confidences.get(issue_type)
        if not reference or len(live) < min_live:
            continue
        psi = population_stability_index(list(reference), list(live))
        psi_by_class[issue_type] = round(psi, 6)
        if psi > psi_threshold:
            drifted.append(issue_type)

    if not drifted:
        return policy, psi_by_class

    thresholds = dict(policy.auto_apply_thresholds)
    for issue_type in drifted:
        thresholds[issue_type] = ABSTAIN_THRESHOLD
    uncertified = dict(policy.uncertified_classes)
    for issue_type in drifted:
        uncertified[issue_type] = (
            f"drift_downgraded: PSI {psi_by_class[issue_type]:.3f} > {psi_threshold} "
            "against the calibration reference for this class"
        )
    return (
        policy.model_copy(
            update={"auto_apply_thresholds": thresholds, "uncertified_classes": uncertified}
        ),
        psi_by_class,
    )


class CalibrationScope(BaseModel):
    """The table a calibration artifact was fitted on, so it cannot be misapplied.

    A conformal certificate is valid only for data exchangeable with its calibration
    sample. Before this existed, nothing stopped a user pointing
    ``--corrector-calibration`` at an artifact fitted on a different table entirely: the
    loader validated JSON shape and nothing else, and the only runtime defence was a PSI
    check on the confidence histogram that is a no-op for artifacts without a reference.
    A schema fingerprint is a cheap, decidable necessary condition -- it cannot prove
    exchangeability, but it catches the blatant case of applying one dataset's certificate
    to another.
    """

    # ``extra="forbid"`` for the same reason as :class:`AbstentionPolicy`. A foreign block that
    # records its table identity under a different key -- ``SessionCertification`` spells it
    # ``table_fingerprint``, not ``fingerprint`` -- would otherwise be accepted with
    # ``fingerprint=None``. :func:`guard_policy_for_scope` fails closed on that, so the outcome is
    # safe, but the user is told the artifact "records no table scope" when it records one under a
    # name this model did not read. Refusing is safe AND legible; a silent downgrade is only safe.
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: str | None = Field(default=None, description="Dataset name recorded at fit time.")
    columns: tuple[str, ...] = Field(default=(), description="Sorted column names at fit time.")
    fingerprint: str | None = Field(
        default=None, description="Stable hash of the sorted column/dtype pairs."
    )


def table_fingerprint(df: TableLike) -> str:
    """Return a stable hash of a table's column set.

    Deliberately excludes row count and cell values: a certificate should survive a table
    growing or its rows changing, but not its *shape* changing.

    **Honest limit.** This is a *necessary* condition, not a sufficient one. Two tables
    with identical columns can still be non-exchangeable (different populations, units, or
    eras), so a matching fingerprint does not establish that a certificate applies -- it
    only rules out the blatant case of applying one dataset's certificate to a structurally
    different table. Distribution drift remains the responsibility of the PSI guards.
    """
    import hashlib

    from dataforge.table import column_names

    parts = sorted(str(name) for name in column_names(df))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def load_calibration_scope(path: Path) -> CalibrationScope | None:
    """Read the optional ``scope`` block from a calibration artifact.

    Returns ``None`` when absent, which is the case for every artifact written before this
    field existed. ``None`` means "unknown scope", and callers must treat that as *not
    verifiable* rather than as *verified*.
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Corrector calibration artifact must be a JSON object.")
    block = raw.get("scope")
    if not isinstance(block, dict):
        return None
    return CalibrationScope.model_validate(block)


def guard_policy_for_scope(
    policy: AbstentionPolicy,
    scope: CalibrationScope | None,
    df: TableLike,
) -> tuple[AbstentionPolicy, str | None]:
    """Refuse a certified policy whose calibration table does not match this one.

    Fails **closed on unknown**: an artifact with no recorded scope cannot be shown to
    apply here, so auto-apply is downgraded rather than assumed valid. That is the
    conservative reading of the exchangeability precondition, and it is the opposite of
    the previous behaviour, which accepted any artifact against any table.

    Args:
        policy: The certified policy from the artifact.
        scope: The artifact's recorded scope, or ``None`` if it has none.
        df: The table about to be repaired.

    Returns:
        ``(policy, None)`` when the scope matches, else
        ``(corrector_default_policy(), reason)``.
    """
    if policy.auto_apply_thresholds == {} and policy.default_threshold > 1.0:
        return policy, None  # already fully disabled; nothing to guard
    if scope is None or scope.fingerprint is None:
        return (
            corrector_default_policy(),
            "calibration artifact records no table scope, so it cannot be shown to apply "
            "to this table; auto-apply downgraded to propose-only",
        )
    actual = table_fingerprint(df)
    if actual != scope.fingerprint:
        return (
            corrector_default_policy(),
            f"calibration artifact was fitted on a different table shape "
            f"(scope fingerprint {scope.fingerprint}, this table {actual}"
            + (f", scope dataset {scope.dataset!r}" if scope.dataset else "")
            + "); auto-apply downgraded to propose-only",
        )
    return policy, None


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
