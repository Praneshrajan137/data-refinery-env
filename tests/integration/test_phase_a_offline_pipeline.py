"""Phase A end-to-end offline acceptance test.

Proves the calibration-critical Phase B/C pipeline runs end-to-end with NO API
keys and NO GPU: ground-truth target derivation -> v10 calibration curriculum ->
calibration samples -> conformal certified-coverage artifact. This is the honest
definition of "Phase A done": the future spend has a working, deterministic path.
"""

from __future__ import annotations

from dataforge.bench.calibration_artifact import build_calibration_artifact
from dataforge.calibration_targets import calibration_samples, derive_targets_for_fixes
from dataforge.conformal import certified_coverage_report
from scripts.data.build_calibration_curriculum import build_calibration_curriculum


def _v9_submit(trajectory_id: str, column: str, value: str) -> dict[str, object]:
    return {
        "trajectory_id": trajectory_id,
        "dataset": "hospital",
        "inferability": "deterministic_normalization",
        "curriculum_version": "expert_v9_action_envelope",
        "fix": [{"row": 0, "column": column, "new_value": value}],
        "completion": (
            f'{{"action":"submit_repairs","repairs":'
            f'[{{"column":"{column}","new_value":"{value}","row":0}}]}}'
        ),
    }


def _v9_abstain(trajectory_id: str) -> dict[str, object]:
    return {
        "trajectory_id": trajectory_id,
        "dataset": "flights",
        "inferability": "not_inferable_from_prompt",
        "curriculum_version": "expert_v9_action_envelope",
        "fix": [{"row": 0, "column": "act_dep_time", "new_value": "9:32 a.m."}],
        "completion": '{"action":"finish","repairs":[]}',
    }


def test_target_derivation_to_conformal_artifact_runs_offline() -> None:
    # 1. Ground-truth-derived calibration targets (Task 1): a mix of correct
    #    high-confidence repairs, wrong low-confidence repairs, and abstentions.
    proposed = {(r, "value"): ("right" if r % 2 == 0 else "wrong") for r in range(80)}
    clean = {(r, "value"): "right" for r in range(80)}
    inferability = {(r, "value"): "context_derivable" for r in range(80)}
    targets = derive_targets_for_fixes(
        proposed_by_cell=proposed,
        clean_by_cell=clean,
        inferability_by_cell=inferability,  # type: ignore[arg-type]
    )
    samples = calibration_samples(targets.values())
    assert samples  # (confidence, correct) pairs, GT-derived

    # 2. The conformal report runs on the derived samples (no model, no keys).
    report = certified_coverage_report(
        {"context_derivable": samples}, alpha=0.05, delta=0.05, min_support=30
    )
    assert "overall_test_coverage" in report

    # 3. The flagship calibration artifact assembles end-to-end.
    record = {
        "provider": "offline_fake",
        "model": "deterministic",
        "dataset": "hospital",
        "precision": 0.5,
        "recall": 0.5,
        "f1": 0.5,
        "ece": 0.4,
        "precision_at_auto_apply": 0.5,
        "auto_apply_count": 0,
        "calibration_samples_by_class": {"context_derivable": [list(s) for s in samples]},
    }
    artifact = build_calibration_artifact({"offline_dry_run": record}, splits=10, min_support=30)
    assert artifact["conditions"]["offline_dry_run"]["certified_coverage"] is not None
    assert "conclusion" in artifact


def test_v10_curriculum_feeds_calibrated_completions() -> None:
    # v10 (Task 4) propagates GT-grounded confidence into the training completion
    # and correctly abstains on the non-inferable slice.
    records = [
        _v9_submit("a", "state", "AL"),
        _v9_submit("b", "county", "Jefferson"),
        _v9_abstain("c"),
    ]
    selected, report = build_calibration_curriculum(records)
    assert report["ok"] is True
    assert report["metrics"]["confidence_coverage"] == 1.0
    assert report["metrics"]["abstention_records"] == 1
    # Every calibrated submit completion carries a confidence target.
    submits = [r for r in selected if r["should_abstain"] is False and r["fix"]]
    assert submits and all('"confidence"' in str(r["completion"]) for r in submits)
