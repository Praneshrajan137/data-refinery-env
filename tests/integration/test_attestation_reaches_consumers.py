"""The portable attestation must reach an agent, and verify without trusting DataForge.

Until 2026-08-29 the string ``attest`` appeared nowhere in the ``dataforge-mcp`` package.
Every tool returned ``repair_receipt_v1``, ``dataforge repair --apply`` emitted no
attestation, and ``dataforge attest build`` was a manual third command needing three
hand-supplied arguments. So an in-toto/DSSE statement that a third party can verify offline
-- no network, no solver, no side-channel schema, checked against 18 committed conformance
vectors of which 14 are rejections -- existed and could not reach the surface it was built
for.

The distinction that makes this worth wiring: a receipt is a DataForge artifact an agent must
trust us about; an attestation is one it can check.

These tests assert the property rather than the plumbing. It is not enough that a field is
populated -- the emitted bytes must survive the normative verifier, and must be refused when
the data does not match.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.attestation import verify_attestation

FIXTURES = Path(__file__).resolve().parents[2] / "dataforge" / "fixtures"
SOURCE = "premised_fd_10rows.csv"
SCHEMA = "premised_fd_10rows.schema.yaml"


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    (tmp_path / SOURCE).write_bytes((FIXTURES / SOURCE).read_bytes())
    (tmp_path / SCHEMA).write_bytes((FIXTURES / SCHEMA).read_bytes())
    return tmp_path


def _apply(workspace: Path) -> dict[str, object]:
    """Apply the one provable repair through the shipped pipeline and return the payload."""
    from dataforge.cli.common import load_schema
    from dataforge.cli.repair import _attest_result
    from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

    source = workspace / SOURCE
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="apply",
            schema=load_schema(workspace / SCHEMA),
            allow_llm=False,
        )
    )
    assert result.receipt.applied, result.receipt.reason
    assert result.receipt.fixes_count == 1, "the fixture must produce exactly one repair"
    return _attest_result(result, source_path=source, schema_path=workspace / SCHEMA)


class TestRepairEmitsAnAttestationWithoutASecondCommand:
    def test_an_applied_repair_produces_an_attestation(self, workspace: Path) -> None:
        payload = _apply(workspace)

        assert "attestation" in payload, payload.get("attestation_unavailable")
        statement = payload["attestation"]
        assert isinstance(statement, dict)
        assert statement["_type"] == "https://in-toto.io/Statement/v1"
        assert statement["predicateType"] == "https://dataforge.dev/RepairAttestation/v1"

    def test_the_emitted_attestation_verifies_against_the_data(self, workspace: Path) -> None:
        """The property, not the plumbing.

        A populated field proves wiring. This proves the bytes are a valid attestation over
        the file that was actually written, checked by the normative verifier.
        """
        payload = _apply(workspace)
        statement = payload["attestation"]
        assert isinstance(statement, dict)

        report = verify_attestation(statement, data_bytes=(workspace / SOURCE).read_bytes())

        assert report.ok, [check.detail for check in report.failures]
        assert not report.skipped, "with --data nothing should be skipped"

    def test_verification_needs_no_solver_and_no_schema(self, workspace: Path) -> None:
        """The offline property, which is the whole reason this artifact is worth shipping.

        Verified by passing ONLY the statement and the file bytes: no schema, no journal, no
        constraint artifact, no z3. If the verifier needed any of them this call would fail.
        """
        payload = _apply(workspace)
        statement = payload["attestation"]
        assert isinstance(statement, dict)

        report = verify_attestation(
            json.loads(json.dumps(statement)),
            data_bytes=(workspace / SOURCE).read_bytes(),
        )

        assert report.ok

    def test_a_tampered_file_is_refused(self, workspace: Path) -> None:
        """Non-vacuity. Without this, a verifier that returns ok unconditionally would pass."""
        payload = _apply(workspace)
        statement = payload["attestation"]
        assert isinstance(statement, dict)

        report = verify_attestation(statement, data_bytes=b"id,state,city\n1,MA,boston\n")

        assert not report.ok
        assert any("identity" in check.name for check in report.failures)

    def test_a_dry_run_is_not_attested_and_says_why(self, workspace: Path) -> None:
        """An attestation over an unchanged file invites being read as a repair certificate."""
        from dataforge.cli.common import load_schema
        from dataforge.cli.repair import _attest_result
        from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=workspace / SOURCE,
                mode="dry_run",
                schema=load_schema(workspace / SCHEMA),
                allow_llm=False,
            )
        )

        payload = _attest_result(
            result, source_path=workspace / SOURCE, schema_path=workspace / SCHEMA
        )

        assert "attestation" not in payload
        assert "no post-state to attest" in str(payload["attestation_unavailable"])


class TestUnavailabilityIsNeverSilent:
    """An agent receiving nothing cannot tell "not attestable" from "not implemented"."""

    def test_a_receipt_that_cannot_be_attested_reports_the_reason(self) -> None:
        from dataforge.attestation import attest_repair

        emission = attest_repair(
            {"schema_version": "repair_receipt_v1"},
            subject_name="t.csv",
            tool_version="0.0.0",
            produced_at="2026-08-29T00:00:00+00:00",
        )

        assert emission.ok is False
        assert emission.reason
        assert emission.as_dict() == {"attestation_unavailable": emission.reason}

    def test_a_build_failure_does_not_raise_into_the_repair(self) -> None:
        """A repair that succeeded must not be reported as failed because attesting failed.

        The attestation is evidence about the write, not part of it. Raising here would make
        an optional artifact able to fail a completed mutation.
        """
        from dataforge.attestation import attest_repair

        emission = attest_repair(
            {"not": "a receipt"},
            subject_name="t.csv",
            tool_version="0.0.0",
            produced_at="2026-08-29T00:00:00+00:00",
        )

        assert emission.envelope is None
        assert emission.reason is not None
