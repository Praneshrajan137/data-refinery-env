"""A certificate must not outlive the model that earned it.

Corrector accuracy is model-specific and — this is the part that makes it dangerous — it does
**not** track model capability. On hospital, Azure `gpt-5-mini` measured
`precision_at_auto_apply` 0.077 while a smaller Gemini model measured 0.16
(`eval/results/corrector_gpt5mini_hospital.json`,
`eval/results/corrector_gemini_hospital.json`). Frontier capability bought worse calibration.

So "the model changed, but it's a better one" is not an argument for reusing a certificate.
These tests pin the consequence: a certificate names its model, refuses to span two models,
and fails closed when the model is unknown.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.calibration_session import (
    build_calibration_session,
    certificate_model_mismatch,
    certify_from_session,
    label_repair_sample,
)
from dataforge.detectors.base import Issue, Severity

_SHA = "c" * 64


def _session(count: int = 70) -> object:
    issues = [
        Issue(
            row=i,
            column="c",
            issue_type="missing_value",
            severity=Severity.REVIEW,
            confidence=0.9,
            actual="x",
            reason="r",
        )
        for i in range(count)
    ]
    return build_calibration_session(
        issues,
        source_path=Path("t.csv"),
        source_sha256=_SHA,
        row_count=count,
        columns=["c"],
        table_fingerprint="fp",
        fd_detection_source="none",
        per_class=count,
    )


def _label_all(artifact, *, provider="azure", model="gpt-5-mini"):  # type: ignore[no-untyped-def]
    for sample in list(artifact.samples):
        artifact = label_repair_sample(
            artifact,
            row=sample.row,
            column=sample.column,
            decision="correct",
            proposed_repair="v",
            repair_confidence=0.97,
            corrector_provider=provider,
            corrector_model=model,
        )
    return artifact


class TestSessionRecordsTheModel:
    def test_the_model_is_recorded_from_repair_verdicts(self) -> None:
        artifact = _label_all(_session(5))
        assert artifact.corrector_model == "gpt-5-mini"
        assert artifact.corrector_provider == "azure"

    def test_a_fresh_session_names_no_model(self) -> None:
        """No repair has been proposed, so claiming a model would be a fabrication."""
        artifact = _session(5)
        assert artifact.corrector_model is None
        assert artifact.corrector_provider is None

    def test_two_models_in_one_session_are_refused(self) -> None:
        """An averaged threshold would describe neither model."""
        artifact = _label_all(_session(5), model="gpt-5-mini")
        with pytest.raises(ValueError, match="cannot span two models"):
            label_repair_sample(
                artifact,
                row=artifact.samples[0].row,
                column="c",
                decision="correct",
                proposed_repair="v",
                repair_confidence=0.97,
                corrector_model="gemini-3.1-flash-lite",
            )

    def test_relabelling_with_the_same_model_is_allowed(self) -> None:
        artifact = _label_all(_session(5), model="gpt-5-mini")
        again = label_repair_sample(
            artifact,
            row=artifact.samples[0].row,
            column="c",
            decision="error",
            corrector_model="gpt-5-mini",
        )
        assert again.corrector_model == "gpt-5-mini"


class TestCertificateCarriesTheModel:
    def test_the_certificate_names_the_model(self) -> None:
        result = certify_from_session(_label_all(_session()))
        assert result.corrector_model == "gpt-5-mini"
        assert result.corrector_provider == "azure"

    def test_the_same_model_matches(self) -> None:
        result = certify_from_session(_label_all(_session()))
        assert certificate_model_mismatch(result, provider="azure", model="gpt-5-mini") is None

    def test_a_different_model_is_refused(self) -> None:
        result = certify_from_session(_label_all(_session()))
        reason = certificate_model_mismatch(
            result, provider="gemini", model="gemini-3.1-flash-lite"
        )
        assert reason is not None
        assert "re-run calibration" in reason

    def test_a_different_provider_on_the_same_model_name_is_refused(self) -> None:
        """Same name on another provider is a different deployment, not the same model."""
        result = certify_from_session(_label_all(_session(), provider="azure"))
        reason = certificate_model_mismatch(result, provider="bedrock", model="gpt-5-mini")
        assert reason is not None

    def test_an_unknown_running_model_is_refused(self) -> None:
        result = certify_from_session(_label_all(_session()))
        assert certificate_model_mismatch(result, provider=None, model=None) is not None

    def test_a_certificate_with_no_model_fails_closed(self) -> None:
        """Fails closed on unknown, matching guard_policy_for_scope."""
        artifact = _session()
        for sample in list(artifact.samples):
            artifact = label_repair_sample(
                artifact,
                row=sample.row,
                column="c" if sample.column == "c" else sample.column,
                decision="correct",
                proposed_repair="v",
                repair_confidence=0.97,
            )
        result = certify_from_session(artifact)
        assert result.corrector_model is None
        reason = certificate_model_mismatch(result, provider="azure", model="gpt-5-mini")
        assert reason is not None
        assert "records no corrector model" in reason
