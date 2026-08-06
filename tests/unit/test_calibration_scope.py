"""A calibration certificate must not be applicable to an arbitrary table.

Before `guard_policy_for_scope` existed, `load_corrector_calibration` validated JSON shape
and nothing else, so a user could point `--corrector-calibration` at an artifact fitted on
one dataset and have its certified thresholds applied to a completely different table. The
only runtime defence was a PSI check on the confidence histogram, which is a no-op for
artifacts lacking a `reference_confidences` block.

These tests lock the two properties that close that hole:

* an artifact with **no** recorded scope fails **closed** -- unknown is not treated as
  verified;
* a scope whose fingerprint does not match the table being repaired is refused.

They also lock the per-class drift guard, which replaced a pooled PSI test that discarded
the Mondrian structure certification is built on.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from dataforge.calibration import (
    AbstentionPolicy,
    CalibrationScope,
    corrector_default_policy,
    guard_policy_for_drift_by_class,
    guard_policy_for_scope,
    load_calibration_scope,
    table_fingerprint,
)
from dataforge.conformal import ABSTAIN_THRESHOLD


def _table(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame({column: ["a", "b"] for column in columns})


def _certified(**thresholds: float) -> AbstentionPolicy:
    """A policy with at least one reachable threshold, i.e. auto-apply actually enabled."""
    return AbstentionPolicy(
        auto_apply_thresholds=dict(thresholds) or {"format_violation": 0.9},
        default_threshold=ABSTAIN_THRESHOLD,
    )


class TestTableFingerprint:
    def test_is_stable_across_row_changes(self) -> None:
        """A certificate should survive the table growing, but not reshaping."""
        one = pd.DataFrame({"a": ["x"], "b": ["y"]})
        many = pd.DataFrame({"a": ["x", "z", "q"], "b": ["y", "w", "r"]})
        assert table_fingerprint(one) == table_fingerprint(many)

    def test_is_order_independent(self) -> None:
        assert table_fingerprint(_table(["a", "b"])) == table_fingerprint(
            pd.DataFrame({"b": ["a", "b"], "a": ["a", "b"]})
        )

    def test_differs_when_columns_differ(self) -> None:
        assert table_fingerprint(_table(["a", "b"])) != table_fingerprint(_table(["a", "c"]))


class TestScopeGuardFailsClosed:
    def test_missing_scope_downgrades_auto_apply(self) -> None:
        """Unknown scope must not be read as verified scope."""
        policy, reason = guard_policy_for_scope(_certified(), None, _table(["a"]))
        assert policy == corrector_default_policy()
        assert reason is not None and "no table scope" in reason

    def test_scope_without_fingerprint_downgrades(self) -> None:
        policy, reason = guard_policy_for_scope(
            _certified(), CalibrationScope(dataset="hospital"), _table(["a"])
        )
        assert policy == corrector_default_policy()
        assert reason is not None

    def test_mismatched_fingerprint_is_refused(self) -> None:
        scope = CalibrationScope(
            dataset="hospital",
            columns=("a", "b"),
            fingerprint=table_fingerprint(_table(["a", "b"])),
        )
        policy, reason = guard_policy_for_scope(_certified(), scope, _table(["x", "y"]))
        assert policy == corrector_default_policy()
        assert reason is not None and "different table shape" in reason
        assert "hospital" in reason

    def test_matching_fingerprint_is_allowed_through(self) -> None:
        df = _table(["a", "b"])
        scope = CalibrationScope(dataset="hospital", fingerprint=table_fingerprint(df))
        certified = _certified()
        policy, reason = guard_policy_for_scope(certified, scope, df)
        assert policy == certified
        assert reason is None

    def test_already_disabled_policy_is_left_alone(self) -> None:
        """A fully-disabled policy has nothing to guard, so it must pass through unchanged."""
        disabled = AbstentionPolicy(auto_apply_thresholds={}, default_threshold=ABSTAIN_THRESHOLD)
        policy, reason = guard_policy_for_scope(disabled, None, _table(["a"]))
        assert policy == disabled
        assert reason is None


class TestPerClassDriftGuard:
    def test_only_the_drifted_class_is_disabled(self) -> None:
        """Pooling used to punish every class for one class's drift; it must not now."""
        policy = _certified(stable=0.8, drifted=0.8)
        reference = {"stable": [0.8] * 40, "drifted": [0.8] * 40}
        live = {"stable": [0.8] * 20, "drifted": [0.05] * 20}
        guarded, psi = guard_policy_for_drift_by_class(policy, reference, live)
        assert guarded.auto_apply_thresholds["stable"] == pytest.approx(0.8)
        assert guarded.auto_apply_thresholds["drifted"] == pytest.approx(ABSTAIN_THRESHOLD)
        assert psi["drifted"] > psi["stable"]

    def test_a_drifted_class_records_its_reason(self) -> None:
        policy = _certified(drifted=0.8)
        guarded, _psi = guard_policy_for_drift_by_class(
            policy, {"drifted": [0.9] * 40}, {"drifted": [0.1] * 20}
        )
        assert "drift_downgraded" in guarded.uncertified_classes["drifted"]

    def test_thin_live_samples_are_not_judged(self) -> None:
        """PSI on a handful of points is noise; it must not disable a certified class."""
        policy = _certified(thin=0.8)
        guarded, psi = guard_policy_for_drift_by_class(
            policy, {"thin": [0.9] * 40}, {"thin": [0.1, 0.1]}
        )
        assert psi == {}
        assert guarded.auto_apply_thresholds["thin"] == pytest.approx(0.8)

    def test_stable_distributions_are_untouched(self) -> None:
        policy = _certified(stable=0.8)
        reference = {"stable": [0.5 + i * 0.01 for i in range(40)]}
        live = {"stable": [0.5 + i * 0.01 for i in range(40)]}
        guarded, _psi = guard_policy_for_drift_by_class(policy, reference, live)
        assert guarded == policy


class TestLoadCalibrationScope:
    def test_absent_scope_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "cal.json"
        path.write_text(json.dumps({"policy": {}}), encoding="utf-8")
        assert load_calibration_scope(path) is None

    def test_scope_round_trips(self, tmp_path: Path) -> None:
        path = tmp_path / "cal.json"
        path.write_text(
            json.dumps(
                {
                    "policy": {},
                    "scope": {
                        "dataset": "hospital",
                        "columns": ["a", "b"],
                        "fingerprint": "abc123",
                    },
                }
            ),
            encoding="utf-8",
        )
        scope = load_calibration_scope(path)
        assert scope is not None
        assert scope.dataset == "hospital"
        assert scope.fingerprint == "abc123"

    def test_non_object_artifact_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "cal.json"
        path.write_text(json.dumps([1, 2]), encoding="utf-8")
        with pytest.raises(ValueError, match="must be a JSON object"):
            load_calibration_scope(path)
