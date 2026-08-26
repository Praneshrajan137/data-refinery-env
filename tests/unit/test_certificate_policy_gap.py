"""Tests for the silent-drop hazard in the certificate-to-policy gap.

Why this file exists, dated 2026-08-26. ``dataforge calibrate --certify`` prints a
``SessionCertification`` carrying per-table certified auto-apply thresholds, and discards it.
``dataforge repair`` reads a different four-block artifact. PRODUCT.md section 1.3 records that no
certified threshold has ever influenced a byte, and states the known incompatibility: a certificate
has no ``policy`` key, so :func:`load_corrector_calibration` raises.

The obvious fix -- wrap the certificate in a ``policy`` block -- was measured, and it did not
raise. ``AbstentionPolicy`` was frozen but permitted extra fields, so a certificate-shaped payload
was ACCEPTED, its certified ``thresholds`` silently dropped as unrecognised, and
``default_threshold`` fell back to 0.90. At confidence 0.95 that flips the decision from ``review``
to ``auto_apply``: a write against a threshold nobody certified, with no error and no log line.

These tests assert the THRESHOLD VALUE and the DECISION, not merely that an exception is raised.
The danger was never the exception type; it was the number that appeared in its absence. A test
that only asserted ``pytest.raises(ValidationError)`` would still pass if someone replaced the
refusal with a lenient default, which is exactly the change this file exists to prevent.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from dataforge.calibration import (
    AbstentionPolicy,
    CalibrationScope,
    corrector_default_policy,
    load_calibration_scope,
    load_corrector_calibration,
)

#: The fields a ``SessionCertification`` actually carries, as of 2026-08-26. Not the whole model --
#: enough of it that the payload is recognisably a certificate and shares no field with
#: ``AbstentionPolicy``, which is the property that made the silent drop total rather than partial.
CERTIFICATE_SHAPED = {
    "alpha": 0.05,
    "delta": 0.05,
    "grid": [0.99, 0.95, 0.9],
    "min_support": 30,
    "thresholds": {"fd_violation": 0.99, "missing_value": 0.99},
    "reasons": {},
    "certified_classes": ["fd_violation", "missing_value"],
    "repair_labels_used": 200,
    "label_source": "human",
    "table_fingerprint": "a" * 32,
}


class TestACertificateCannotBecomeAPolicy:
    """The certificate and the policy are different quantities. Reshaping is a decision."""

    def test_a_certificate_shaped_policy_block_is_refused(self) -> None:
        """The measured hazard, pinned. Before ``extra="forbid"`` this returned a policy."""
        with pytest.raises(ValidationError):
            AbstentionPolicy.model_validate(CERTIFICATE_SHAPED)

    def test_the_refusal_is_not_a_lenient_default(self) -> None:
        """The assertion that matters: no 0.90 threshold is reachable from that payload.

        Written as a negative on the VALUE because the failure mode was never an absent exception
        -- it was a number appearing where an abstention belonged. If a future change makes the
        payload loadable again, this fails on the threshold even if it raises nothing.
        """
        try:
            policy = AbstentionPolicy.model_validate(CERTIFICATE_SHAPED)
        except ValidationError:
            return
        pytest.fail(
            f"a certificate-shaped payload produced a policy with "
            f"default_threshold={policy.default_threshold} and "
            f"thresholds={policy.auto_apply_thresholds}; the certified thresholds were dropped"
        )

    def test_the_dropped_threshold_would_have_flipped_a_decision(self) -> None:
        """Why the silent drop was a write hazard and not untidiness.

        Constructed explicitly rather than by validation, so the arithmetic is visible: 0.90 is
        below a 0.95-confidence fix and 1.01 is above it. The gap between the permissive default
        and the conservative one is the gap between writing and abstaining.
        """
        permissive = AbstentionPolicy(auto_apply_thresholds={}, default_threshold=0.90)
        conservative = corrector_default_policy()

        assert permissive.action_for("fd_violation", 0.95) == "auto_apply"
        assert conservative.action_for("fd_violation", 0.95) == "review"
        assert conservative.threshold_for("fd_violation") > 1.0

    def test_the_loader_names_the_certificate_in_its_error(self, tmp_path: Path) -> None:
        """A refusal a reader cannot act on invites the reshape it refused.

        The error must say that the certificate is not this artifact, because the person hitting it
        is mid-way through the exact wiring attempt that produced the hazard.
        """
        artifact = tmp_path / "calibration.json"
        artifact.write_text(json.dumps({"policy": CERTIFICATE_SHAPED}), encoding="utf-8")

        with pytest.raises(ValueError) as caught:
            load_corrector_calibration(artifact)

        message = str(caught.value)
        assert "calibrate --certify" in message
        assert "must not be reshaped" in message

    def test_the_committed_artifact_still_loads(self) -> None:
        """Non-vacuity. A refusal that also refuses the real artifact is a broken loader."""
        policy, maps, reference = load_corrector_calibration(
            Path("eval/results/corrector_calibration.json")
        )

        assert policy.default_threshold > 1.0
        assert maps
        assert reference

    def test_an_unknown_policy_field_is_refused_rather_than_ignored(self) -> None:
        """Generalised past the certificate: any unrecognised field is an error.

        Pinned separately so the guard survives a future change to ``SessionCertification``'s
        field names. What must hold is that this model never guesses a threshold, whatever the
        unrecognised key happens to be called.
        """
        with pytest.raises(ValidationError):
            AbstentionPolicy.model_validate({"default_threshold": 0.9, "auto_apply_thresold": {}})


class TestScopeRefusesAForeignBlock:
    """The exchangeability guard, which fails closed but used to do so illegibly."""

    def test_a_foreign_fingerprint_key_is_refused(self) -> None:
        """``SessionCertification`` spells it ``table_fingerprint``, not ``fingerprint``.

        Accepting the block with ``fingerprint=None`` is SAFE -- ``guard_policy_for_scope``
        downgrades on unknown scope -- but it tells the user the artifact "records no table scope"
        when it records one under a name the model did not read. Safe and legible beats safe.
        """
        with pytest.raises(ValidationError):
            CalibrationScope.model_validate({"table_fingerprint": "b" * 32})

    def test_a_real_scope_block_still_loads(self, tmp_path: Path) -> None:
        """Non-vacuity for the test above."""
        artifact = tmp_path / "calibration.json"
        artifact.write_text(
            json.dumps(
                {
                    "policy": {"default_threshold": 1.01},
                    "scope": {
                        "dataset": "hospital",
                        "columns": ["a", "b"],
                        "fingerprint": "c" * 32,
                    },
                }
            ),
            encoding="utf-8",
        )

        scope = load_calibration_scope(artifact)

        assert scope is not None
        assert scope.fingerprint == "c" * 32
        assert scope.dataset == "hospital"

    def test_scope_still_carries_every_declared_field(self) -> None:
        """Regression guard for a slip made while adding the config to this class.

        The edit that introduced ``extra="forbid"`` deleted the ``dataset`` field in passing. With
        extras forbidden, a dropped field turns every artifact that records it into a hard load
        failure -- so the two changes interact, and the field list is worth asserting directly.
        """
        assert set(CalibrationScope.model_fields) == {"dataset", "columns", "fingerprint"}
