"""The corrector scope guard has to actually run, and has to actually refuse.

`guard_policy_for_scope` and `load_calibration_scope` were written, exported, documented --
and never called. `guard_policy_for_scope`'s own docstring described fixing the fact that
"the only runtime defence was a PSI check on the confidence histogram that is a no-op for
artifacts without a reference", but the replacement was never wired into
`run_repair_pipeline`. A certificate fitted on one table was accepted against any other.

That is the worst kind of gap, because the guard's existence reads as protection. Nothing
failed, so nothing revealed it. These tests exist so the wiring cannot silently come loose
again: they assert the guard runs in the pipeline, not merely that the function works.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataforge.calibration import (
    AbstentionPolicy,
    CalibrationScope,
    corrector_default_policy,
    guard_policy_for_scope,
    table_fingerprint,
)
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

_EMPTY = pd.DataFrame({"id": [1], "city": ["Boston"]})


def _permissive_policy() -> AbstentionPolicy:
    """A policy that WILL auto-apply, so a downgrade is observable."""
    return AbstentionPolicy(auto_apply_thresholds={"missing_value": 0.0}, default_threshold=0.0)


def _csv(tmp_path: Path) -> Path:
    source = tmp_path / "data.csv"
    source.write_text(
        "id,city\n1,Boston\n2,Denver\n3,Austin\n4,Reno\n5,Miami\n6,Chicago\n7,Dallas\n8,\n",
        encoding="utf-8",
    )
    return source


class TestGuardIsReachableFromThePipeline:
    """Unit-testing the function proved nothing while nothing called it."""

    def test_unscoped_policy_is_downgraded_and_recorded(self, tmp_path: Path) -> None:
        source = _csv(tmp_path)
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=source,
                mode="dry_run",
                corrector_policy=_permissive_policy(),
            )
        )
        withdrawn = [note for note in result.receipt.limitations if "auto-apply withdrawn" in note]
        assert withdrawn, (
            "a permissive policy with no recorded scope was accepted; the scope guard is "
            "not wired into run_repair_pipeline"
        )
        assert "records no table scope" in withdrawn[0]

    def test_mismatched_scope_is_downgraded(self, tmp_path: Path) -> None:
        source = _csv(tmp_path)
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=source,
                mode="dry_run",
                corrector_policy=_permissive_policy(),
                corrector_calibration_scope=CalibrationScope(
                    dataset="somewhere_else", fingerprint="0" * 32
                ),
            )
        )
        withdrawn = [note for note in result.receipt.limitations if "auto-apply withdrawn" in note]
        assert withdrawn
        assert "different table shape" in withdrawn[0]
        assert "somewhere_else" in withdrawn[0]

    def test_matching_scope_is_left_alone(self, tmp_path: Path) -> None:
        """The guard must not fire on a legitimate certificate, or it is useless."""
        source = _csv(tmp_path)
        fingerprint = table_fingerprint(pd.read_csv(source, dtype=str))
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=source,
                mode="dry_run",
                corrector_policy=_permissive_policy(),
                corrector_calibration_scope=CalibrationScope(fingerprint=fingerprint),
            )
        )
        assert not [n for n in result.receipt.limitations if "auto-apply withdrawn" in n]

    def test_explicit_verification_bypasses_the_guard(self, tmp_path: Path) -> None:
        """In-process callers should not have to fabricate a scope."""
        source = _csv(tmp_path)
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=source,
                mode="dry_run",
                corrector_policy=_permissive_policy(),
                corrector_scope_verified=True,
            )
        )
        assert not [n for n in result.receipt.limitations if "auto-apply withdrawn" in n]

    def test_unproven_optin_is_not_gated_by_certificate_scope(self, tmp_path: Path) -> None:
        """That mode claims no certificate, so there is no scope to keep it inside."""
        source = _csv(tmp_path)
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=source,
                mode="dry_run",
                corrector_policy=_permissive_policy(),
                allow_unproven_autoapply=True,
            )
        )
        assert not [n for n in result.receipt.limitations if "auto-apply withdrawn" in n]

    def test_the_default_disabled_policy_is_not_flagged(self, tmp_path: Path) -> None:
        """`enabled_classes` ships empty, so the common path must stay quiet."""
        source = _csv(tmp_path)
        result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="dry_run"))
        assert not [n for n in result.receipt.limitations if "auto-apply withdrawn" in n]


class TestGuardSemantics:
    def test_fails_closed_on_unknown_scope(self) -> None:
        """None means 'not verifiable', which must not be read as 'verified'."""
        guarded, reason = guard_policy_for_scope(_permissive_policy(), None, _EMPTY)
        assert reason is not None
        assert guarded == corrector_default_policy()

    def test_a_scope_without_a_fingerprint_is_also_unknown(self) -> None:
        guarded, reason = guard_policy_for_scope(
            _permissive_policy(), CalibrationScope(dataset="x"), _EMPTY
        )
        assert reason is not None
        assert guarded == corrector_default_policy()

    def test_an_already_disabled_policy_is_returned_untouched(self) -> None:
        disabled = corrector_default_policy()
        guarded, reason = guard_policy_for_scope(disabled, None, _EMPTY)
        assert guarded is disabled
        assert reason is None

    def test_the_downgrade_removes_every_auto_apply_threshold(self) -> None:
        """A partial downgrade would leave some classes silently certified."""
        guarded, _ = guard_policy_for_scope(_permissive_policy(), None, _EMPTY)
        assert guarded.auto_apply_thresholds == {}
