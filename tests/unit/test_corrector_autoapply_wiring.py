"""Tests for the calibrated + conformally-certified corrector auto-apply wiring.

Pins the never-corrupt invariants of the new path:
- A schema-PROVEN, high-(calibrated)-confidence LLM fix auto-applies under a
  certified policy.
- The SAME fix is HELD as plausibility_only when there is no authoritative schema.
- A fix whose calibrated confidence falls below the certified threshold is HELD.
- Deterministic fixes are unaffected (always auto-apply); no maps -> raw confidence.
- The certified-policy builder and the artifact loader round-trip.
"""

from __future__ import annotations

import json
from pathlib import Path

from dataforge.calibration import (
    AbstentionPolicy,
    calibrated_conformal_corrector_policy,
    load_corrector_calibration,
)
from dataforge.calibration_map import CalibrationMap
from dataforge.conformal import certification_reason, min_samples_for_certification
from dataforge.engine.repair import _guard_corrector_policy_for_drift, _partition_auto_apply
from dataforge.repairers.base import ProposedFix
from dataforge.transactions.txn import CellFix


def _llm_fix(confidence: float, issue_type: str = "missing_value") -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=0,
            column="c",
            old_value="x",
            new_value="y",
            detector_id=issue_type,
            operation="update",
        ),
        reason="llm corrector",
        confidence=confidence,
        provenance="llm_live",
    )


def _deterministic_fix(confidence: float = 0.99) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=1,
            column="c",
            old_value="a",
            new_value="b",
            detector_id="fd_violation",
            operation="update",
        ),
        reason="deterministic",
        confidence=confidence,
        provenance="deterministic",
    )


_POLICY = AbstentionPolicy(
    target_precision=0.95,
    auto_apply_thresholds={"missing_value": 0.8},
    default_threshold=1.01,
)
_RAISING = CalibrationMap(method="isotonic", x_knots=(0.0, 1.0), y_knots=(0.85, 0.99))
_LOWERING = CalibrationMap(method="isotonic", x_knots=(0.0, 1.0), y_knots=(0.0, 0.5))


class TestPartitionAutoApply:
    def test_schema_proven_high_confidence_auto_applies(self) -> None:
        fix = _llm_fix(0.9)
        auto, held, plausibility = _partition_auto_apply(
            [fix],
            _POLICY,
            authoritative_schema_present=True,
            allow_unproven_autoapply=False,
            calibration_map_by_class={"missing_value": _RAISING},
        )
        assert auto == [fix]
        assert held == [] and plausibility == []

    def test_no_schema_holds_as_plausibility_only(self) -> None:
        fix = _llm_fix(0.99)
        auto, held, plausibility = _partition_auto_apply(
            [fix],
            _POLICY,
            authoritative_schema_present=False,
            allow_unproven_autoapply=False,
            calibration_map_by_class={"missing_value": _RAISING},
        )
        assert plausibility == [fix]
        assert auto == [] and held == []

    def test_calibration_below_threshold_holds(self) -> None:
        # Raw 0.9 would clear 0.8, but the lowering map pulls it to 0.45 -> held.
        fix = _llm_fix(0.9)
        auto, held, _ = _partition_auto_apply(
            [fix],
            _POLICY,
            authoritative_schema_present=True,
            allow_unproven_autoapply=False,
            calibration_map_by_class={"missing_value": _LOWERING},
        )
        assert held == [fix]
        assert auto == []

    def test_deterministic_fix_always_auto_applies(self) -> None:
        fix = _deterministic_fix()
        auto, held, plausibility = _partition_auto_apply(
            [fix],
            _POLICY,
            authoritative_schema_present=False,
            allow_unproven_autoapply=False,
            calibration_map_by_class={"missing_value": _LOWERING},
        )
        assert auto == [fix]
        assert held == [] and plausibility == []

    def test_no_maps_uses_raw_confidence(self) -> None:
        # Backward compatible: without maps, raw 0.9 clears the 0.8 threshold.
        fix = _llm_fix(0.9)
        auto, _, _ = _partition_auto_apply(
            [fix],
            _POLICY,
            authoritative_schema_present=True,
            allow_unproven_autoapply=False,
        )
        assert auto == [fix]


class TestCalibratedPolicyBuilder:
    def test_builds_policy_and_maps_keyed_by_type(self) -> None:
        # 60 samples for missing_value: high confidence => mostly correct.
        rng_correct = [(0.95, True)] * 55 + [(0.95, False)] * 5
        samples_by_type = {"missing_value": rng_correct}
        policy, maps = calibrated_conformal_corrector_policy(
            samples_by_type, alpha=0.1, delta=0.1, min_support=30
        )
        assert isinstance(policy, AbstentionPolicy)
        assert "missing_value" in maps
        assert isinstance(maps["missing_value"], CalibrationMap)

    def test_thin_support_stays_propose_not_apply(self) -> None:
        policy, _ = calibrated_conformal_corrector_policy(
            {"outlier": [(0.9, True), (0.9, False)]}, min_support=30
        )
        assert policy.threshold_for("outlier") == 1.01


class TestArtifactRoundTrip:
    def test_load_corrector_calibration(self, tmp_path: Path) -> None:
        artifact = {
            "policy": _POLICY.model_dump(),
            "maps": {"missing_value": _RAISING.model_dump()},
            "reference_confidences": {"missing_value": [0.9, 0.92, 0.88]},
        }
        path = tmp_path / "corrector_calibration_engine.json"
        path.write_text(json.dumps(artifact), encoding="utf-8")
        policy, maps, reference = load_corrector_calibration(path)
        assert policy.threshold_for("missing_value") == 0.8
        assert maps["missing_value"].predict(1.0) == _RAISING.predict(1.0)
        assert reference["missing_value"] == [0.9, 0.92, 0.88]

    def test_load_tolerates_missing_reference(self, tmp_path: Path) -> None:
        # Older artifacts predate the drift-guard field: reference defaults to {}.
        artifact = {"policy": _POLICY.model_dump(), "maps": {}}
        path = tmp_path / "old_artifact.json"
        path.write_text(json.dumps(artifact), encoding="utf-8")
        _policy, _maps, reference = load_corrector_calibration(path)
        assert reference == {}


class TestDriftGuard:
    def test_drift_downgrades_policy_and_holds(self) -> None:
        # Reference confidences cluster high (~0.9); live cluster low (~0.1) -> PSI
        # drift -> policy downgraded to propose-not-apply -> the fix is HELD.
        reference = {"missing_value": [0.9] * 40 + [0.95] * 40}
        fix = _llm_fix(0.1)
        guarded = _guard_corrector_policy_for_drift(_POLICY, [fix], reference)
        auto, held, _ = _partition_auto_apply(
            [_llm_fix(0.9)],  # a fix that WOULD clear the raw 0.8 threshold
            guarded,
            authoritative_schema_present=True,
            allow_unproven_autoapply=False,
        )
        assert auto == []
        assert len(held) == 1

    def test_no_drift_keeps_policy(self) -> None:
        reference = {"missing_value": [0.9] * 40 + [0.92] * 40}
        live_fix = _llm_fix(0.9)
        guarded = _guard_corrector_policy_for_drift(_POLICY, [live_fix], reference)
        # Same policy object semantics: a 0.9 fix still auto-applies.
        auto, _, _ = _partition_auto_apply(
            [live_fix],
            guarded,
            authoritative_schema_present=True,
            allow_unproven_autoapply=False,
        )
        assert auto == [live_fix]

    def test_no_reference_is_noop(self) -> None:
        assert _guard_corrector_policy_for_drift(_POLICY, [_llm_fix(0.9)], None) is _POLICY
        assert _guard_corrector_policy_for_drift(_POLICY, [_llm_fix(0.9)], {}) is _POLICY

    def test_no_llm_fixes_is_noop(self) -> None:
        reference = {"missing_value": [0.1] * 80}
        assert (
            _guard_corrector_policy_for_drift(_POLICY, [_deterministic_fix()], reference) is _POLICY
        )


_COMMITTED_ARTIFACT = (
    Path(__file__).resolve().parents[2] / "eval" / "results" / "corrector_calibration.json"
)


class TestRealArtifactSelectsNothing:
    """The shipped gpt-5-mini artifact must auto-apply NOTHING and say why.

    Locks in the honest operative reality (all thresholds are the 1.01 disabled
    sentinel) so a future edit cannot silently enable auto-apply from a weak,
    uncertified corrector. This is the regression anchor for the trust thesis.
    """

    def test_committed_artifact_auto_applies_nothing(self) -> None:
        policy, maps, _reference = load_corrector_calibration(_COMMITTED_ARTIFACT)
        # A maximally confident LLM fix on the dominant class, calibrated through
        # the shipped map, must still be HELD (calibrated score < 1.01 sentinel).
        fix = _llm_fix(1.0, issue_type="fd_violation")
        auto, held, _ = _partition_auto_apply(
            [fix],
            policy,
            authoritative_schema_present=True,
            allow_unproven_autoapply=False,
            calibration_map_by_class=maps,
        )
        assert auto == []
        assert held == [fix]

    def test_committed_artifact_documents_every_disabled_class(self) -> None:
        # Invariant: any class parked at the 1.01 sentinel MUST carry a reason,
        # so the disabled state is never an opaque magic number.
        policy, _maps, _reference = load_corrector_calibration(_COMMITTED_ARTIFACT)
        for issue_type, threshold in policy.auto_apply_thresholds.items():
            if threshold >= 1.01:
                assert issue_type in policy.uncertified_classes, issue_type
                assert policy.uncertified_classes[issue_type]


class TestCertificationReasons:
    def test_min_samples_floor_is_59_at_95_percent(self) -> None:
        # The honest data budget: even a PERFECT corrector needs >= 59 all-correct
        # accepted samples to certify 95% precision at delta=0.05.
        assert min_samples_for_certification(0.05, 0.05) == 59

    def test_insufficient_support_reason(self) -> None:
        reason = certification_reason([(1.0, True)] * 3, alpha=0.05, min_support=30)
        assert reason is not None
        assert "insufficient_support" in reason
        assert "n=3" in reason

    def test_precision_below_target_reason(self) -> None:
        # Enough support (>= min_support) but a near-0-accuracy corrector.
        samples = [(1.0, i < 2) for i in range(40)]
        reason = certification_reason(samples, alpha=0.05, min_support=30)
        assert reason is not None
        assert "precision_below_target" in reason
        assert "n=40" in reason

    def test_certifiable_class_has_no_reason(self) -> None:
        # A perfect corrector with ample support IS certifiable -> reason None.
        samples = [(1.0, True)] * 80
        assert certification_reason(samples, alpha=0.05, min_support=30) is None


class TestCalibrationPlateauBehavior:
    """Isotonic maps are piecewise-constant, not strictly monotone.

    Documents that two distinct raw confidences on the same plateau collapse to
    one calibrated value -- the exact reason calibration is NOT a pure no-op at the
    threshold boundary (it can move a live score across a fixed threshold).
    """

    def test_plateau_collapses_distinct_confidences(self) -> None:
        flat = CalibrationMap(method="isotonic", x_knots=(0.0, 0.5, 1.0), y_knots=(0.2, 0.2, 0.9))
        # 0.1 and 0.4 both sit on the first flat segment -> identical calibrated value.
        assert flat.predict(0.1) == flat.predict(0.4)
        # A value past the plateau rises -> monotone non-decreasing preserved.
        assert flat.predict(1.0) > flat.predict(0.4)
