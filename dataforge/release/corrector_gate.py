"""Release gate for the optional LLM corrector's auto-apply promotion.

The product guarantee is that no LLM-proposed correction auto-applies until
measured evidence earns it (distribution-free certification, calibrated
confidence). This gate makes that guarantee an enforced, committed-artifact
invariant: it inspects the committed corrector calibration artifact and fails
the release if any issue type is enabled for auto-apply (its threshold is
reachable, i.e. <= 1.0) without a recorded promotion justification that clears
the bar. Today every class carries the disabled ``1.01`` sentinel, so nothing
auto-applies and the gate passes -- but the moment someone commits an artifact
that enables a class, they must also commit the evidence.

The promotion bar itself (precision_at_auto_apply >= 0.95, ECE <= 0.10, enough
certified samples) is defined by :func:`dataforge.bench.corrector_promotion_verdict`
and :func:`dataforge.conformal.min_samples_for_certification`; this gate enforces
that a shipped auto-apply is backed by that evidence.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

__all__ = [
    "CorrectorReleaseGateResult",
    "check_corrector_release_gate",
]

# A confidence threshold is in [0, 1]; a threshold strictly above 1.0 can never
# be cleared, so it disables auto-apply. The canonical disabled sentinel emitted
# by the conformal procedure when it cannot certify is 1.01.
_MAX_REACHABLE_CONFIDENCE = 1.0

_DEFAULT_ARTIFACT = (
    Path(__file__).resolve().parents[2] / "eval" / "results" / "corrector_calibration.json"
)

# Promotion bar (mirrors dataforge.bench.corrector_promotion_verdict defaults).
_MIN_PRECISION_AT_AUTO_APPLY = 0.95
_MAX_ECE = 0.10


@dataclass(frozen=True)
class CorrectorReleaseGateResult:
    """Outcome of the corrector auto-apply promotion gate."""

    passed: bool
    reason: str
    artifact_path: str
    enabled_classes: list[str] = field(default_factory=list)


def _promotion_evidence_ok(evidence: object) -> bool:
    """Return whether a per-class promotion evidence block clears the bar."""
    if not isinstance(evidence, dict):
        return False
    precision = evidence.get("precision_at_auto_apply")
    ece = evidence.get("ece")
    if not isinstance(precision, int | float) or precision < _MIN_PRECISION_AT_AUTO_APPLY:
        return False
    return isinstance(ece, int | float) and ece <= _MAX_ECE


def check_corrector_release_gate(
    artifact_path: Path | None = None,
) -> CorrectorReleaseGateResult:
    """Enforce that no corrector class auto-applies without committed evidence.

    Args:
        artifact_path: Optional path to the corrector calibration artifact.
            Defaults to the committed ``eval/results/corrector_calibration.json``.

    Returns:
        A :class:`CorrectorReleaseGateResult`. ``passed`` is ``True`` when no LLM
        class is promoted to auto-apply, or every promoted class carries evidence
        clearing the promotion bar. A missing artifact passes (nothing to
        promote); a malformed artifact fails closed.
    """
    path = artifact_path or _DEFAULT_ARTIFACT
    if not path.exists():
        return CorrectorReleaseGateResult(
            passed=True,
            reason="No committed corrector calibration artifact; nothing to promote.",
            artifact_path=str(path),
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return CorrectorReleaseGateResult(
            passed=False,
            reason=f"Corrector artifact could not be read as JSON (fail-closed): {exc}",
            artifact_path=str(path),
        )

    policy = payload.get("policy")
    if not isinstance(policy, dict):
        return CorrectorReleaseGateResult(
            passed=False,
            reason="Corrector artifact has no policy block (fail-closed).",
            artifact_path=str(path),
        )
    thresholds = policy.get("auto_apply_thresholds")
    if not isinstance(thresholds, dict):
        return CorrectorReleaseGateResult(
            passed=True,
            reason="Corrector policy declares no per-class auto-apply thresholds; nothing promoted.",
            artifact_path=str(path),
        )

    evidence_by_class = payload.get("promotion_evidence")
    evidence_map = evidence_by_class if isinstance(evidence_by_class, dict) else {}

    enabled = [
        str(issue_type)
        for issue_type, threshold in thresholds.items()
        if isinstance(threshold, int | float) and threshold <= _MAX_REACHABLE_CONFIDENCE
    ]
    if not enabled:
        return CorrectorReleaseGateResult(
            passed=True,
            reason=(
                "No LLM corrector class is promoted to auto-apply "
                "(every threshold is the disabled sentinel); propose-not-apply holds."
            ),
            artifact_path=str(path),
        )

    unjustified = [cls for cls in enabled if not _promotion_evidence_ok(evidence_map.get(cls))]
    if unjustified:
        return CorrectorReleaseGateResult(
            passed=False,
            reason=(
                f"Corrector classes {sorted(unjustified)} are enabled for auto-apply without "
                f"committed promotion evidence meeting precision>={_MIN_PRECISION_AT_AUTO_APPLY} "
                f"and ECE<={_MAX_ECE}. Commit the evidence or restore the disabled sentinel."
            ),
            artifact_path=str(path),
            enabled_classes=sorted(enabled),
        )
    return CorrectorReleaseGateResult(
        passed=True,
        reason=(
            f"Corrector classes {sorted(enabled)} are promoted to auto-apply with committed "
            "promotion evidence clearing the bar."
        ),
        artifact_path=str(path),
        enabled_classes=sorted(enabled),
    )
