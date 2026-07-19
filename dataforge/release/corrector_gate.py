"""Release gate for the optional LLM corrector's auto-apply promotion.

The product guarantee is that no LLM-proposed correction auto-applies until
MEASURED evidence earns it. This gate makes that an enforced, committed-artifact
invariant, wired to the *real* verdict source: it inspects the committed corrector
calibration policy artifact, and if any issue type is enabled for auto-apply (its
threshold is reachable, i.e. <= 1.0), it requires that a committed measured
corrector benchmark record clears the promotion bar under the canonical
:func:`dataforge.bench.corrector_promotion_verdict` (precision_at_auto_apply
>= 0.95, ECE <= 0.10, auto_apply_count >= 1).

Today every class carries the disabled ``1.01`` sentinel and every committed
measured record REJECTS (gemini precision 0.16 / ECE 0.79; gpt-5-mini 0.077 /
0.82), so nothing auto-applies and the gate passes. The moment someone enables a
class, the gate demands a committed measurement that actually clears the bar --
not a hand-authored field. It fails closed on a malformed policy artifact.
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
# be cleared, so it disables auto-apply. The conformal procedure emits 1.01 as the
# canonical "cannot certify" disabled sentinel.
_MAX_REACHABLE_CONFIDENCE = 1.0

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_ARTIFACT = _PROJECT_ROOT / "eval" / "results" / "corrector_calibration.json"
_DEFAULT_RESULTS_DIR = _PROJECT_ROOT / "eval" / "results"


@dataclass(frozen=True)
class CorrectorReleaseGateResult:
    """Outcome of the corrector auto-apply promotion gate."""

    passed: bool
    reason: str
    artifact_path: str
    enabled_classes: list[str] = field(default_factory=list)
    passing_measurements: list[str] = field(default_factory=list)


def _passing_measured_records(results_dir: Path) -> list[str]:
    """Return committed corrector benchmark records that CLEAR the promotion bar.

    Reconstructs each committed ``corrector_*.json`` benchmark record and runs the
    canonical ``corrector_promotion_verdict`` -- the same function the bench uses
    -- so the gate can never drift from the real promotion definition.
    """
    # Lazy imports keep the release package import-light.
    from dataforge.bench.core import SeedBenchmarkResult
    from dataforge.bench.methods import corrector_promotion_verdict

    passing: list[str] = []
    if not results_dir.is_dir():
        return passing
    for path in sorted(results_dir.glob("corrector_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        records = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            continue
        for entry in records:
            if not isinstance(entry, dict) or entry.get("method") != "llm_corrector":
                continue
            try:
                record = SeedBenchmarkResult.model_validate(entry)
            except Exception:  # noqa: BLE001 - a malformed record simply cannot promote
                continue
            passed, _reasons = corrector_promotion_verdict(record)
            if passed:
                passing.append(f"{path.name}:{entry.get('dataset', '?')}")
    return passing


def check_corrector_release_gate(
    artifact_path: Path | None = None,
    *,
    results_dir: Path | None = None,
) -> CorrectorReleaseGateResult:
    """Enforce that no corrector class auto-applies without a passing measurement.

    Args:
        artifact_path: Corrector calibration policy artifact. Defaults to the
            committed ``eval/results/corrector_calibration.json``.
        results_dir: Directory of committed corrector benchmark records. Defaults
            to ``eval/results``.

    Returns:
        A :class:`CorrectorReleaseGateResult`. ``passed`` is ``True`` when no class
        is enabled for auto-apply, or a committed measured record clears the
        promotion bar. A missing artifact passes (nothing to promote); a malformed
        artifact fails closed.
    """
    path = artifact_path or _DEFAULT_ARTIFACT
    results = results_dir or _DEFAULT_RESULTS_DIR
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

    enabled = sorted(
        str(issue_type)
        for issue_type, threshold in thresholds.items()
        if isinstance(threshold, int | float) and threshold <= _MAX_REACHABLE_CONFIDENCE
    )
    if not enabled:
        return CorrectorReleaseGateResult(
            passed=True,
            reason=(
                "No LLM corrector class is promoted to auto-apply "
                "(every threshold is the disabled sentinel); propose-not-apply holds."
            ),
            artifact_path=str(path),
        )

    passing = _passing_measured_records(results)
    if passing:
        return CorrectorReleaseGateResult(
            passed=True,
            reason=(
                f"Corrector classes {enabled} are enabled and a committed measurement clears "
                f"the promotion bar: {passing}."
            ),
            artifact_path=str(path),
            enabled_classes=enabled,
            passing_measurements=passing,
        )
    return CorrectorReleaseGateResult(
        passed=False,
        reason=(
            f"Corrector classes {enabled} are enabled for auto-apply but NO committed measured "
            "record clears corrector_promotion_verdict (precision>=0.95, ECE<=0.10, "
            "auto_apply_count>=1). Commit a passing measurement or restore the disabled sentinel."
        ),
        artifact_path=str(path),
        enabled_classes=enabled,
    )
