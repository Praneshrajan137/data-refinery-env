"""Tests for the repair attestation (``dataforge.repair.attestation/v1``).

Every case in the spec's Appendix A toy-case table has a test here, and every rejection
path is exercised. The acceptance criterion is not "the happy path works" -- it is that
each distinct way of lying is caught, and caught by a named check.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from dataforge.attestation import (
    ATTESTATION_VERSION,
    PAYLOAD_TYPE,
    PREDICATE_TYPE,
    STATEMENT_TYPE,
    build_attestation,
    pae,
    sign_attestation,
    verify_attestation,
)
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

DATA = b"amount\n100\n104\n98\n103\n101\n106\n99\n1020\n"


def _statement(**predicate_overrides: Any) -> dict[str, Any]:
    """A minimal VALID applied attestation, then selectively broken by each test."""
    digest = "b" * 64
    predicate: dict[str, Any] = {
        "attestation_version": ATTESTATION_VERSION,
        "tool": {"name": "dataforge", "version": "0.1.0", "contract_version": "repair_contract_v2"},
        "produced_at": "2026-08-13T00:00:00Z",
        "mode": "apply",
        "applied": True,
        "reversible": True,
        "source": {"digest": {"sha256": "a" * 64}},
        "post": {"digest": {"sha256": digest}},
        "authority": {
            "authoritative_columns": [],
            "accepted_constraint_ids": [],
            "constraints_digest": None,
            "constraints": None,
        },
        "fixes": [
            {
                "row": 7,
                "column": "amount",
                "detector_id": "decimal_shift",
                "provenance": "deterministic",
                "verification_strength": "proven",
            }
        ],
        "held": [],
        "verdicts": {
            "verifier": "accept",
            "safety": "allow",
            "independent_verification": "not_run",
        },
        "journal": {"txn_id": "txn-x", "head_sha256": "c" * 64},
        "revert_command": "dataforge revert txn-x",
        "model": None,
        "limitations": [],
        "verification": {"checks_available": []},
    }
    predicate.update(predicate_overrides)
    subject_digest = digest
    post = predicate.get("post")
    if predicate.get("applied") and isinstance(post, dict):
        subject_digest = post["digest"]["sha256"]
    elif not predicate.get("applied"):
        subject_digest = predicate["source"]["digest"]["sha256"]
    return {
        "_type": STATEMENT_TYPE,
        "subject": [{"name": "data.csv", "digest": {"sha256": subject_digest}}],
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def _failed(result: Any) -> set[str]:
    return {check.name for check in result.failures}


# --- Acceptance ---------------------------------------------------------------


def test_a_deterministic_applied_repair_verifies() -> None:
    result = verify_attestation(_statement())
    assert result.ok, [check.as_dict() for check in result.failures]


def test_external_fix_on_a_covered_column_verifies() -> None:
    statement = _statement(
        authority={
            "authoritative_columns": ["amount"],
            "accepted_constraint_ids": ["c1"],
            "constraints_digest": {"sha256": "d" * 64},
            "constraints": {"columns": {"amount": "int"}},
        },
        fixes=[
            {
                "row": 7,
                "column": "amount",
                "detector_id": "external",
                "provenance": "external",
                "verification_strength": "proven",
            }
        ],
    )
    result = verify_attestation(statement)
    assert result.ok, [check.as_dict() for check in result.failures]


# --- Rejection: one test per way of lying -------------------------------------


def test_external_fix_off_authority_is_rejected() -> None:
    statement = _statement(
        authority={
            "authoritative_columns": ["id"],
            "accepted_constraint_ids": ["c1"],
            "constraints_digest": {"sha256": "d" * 64},
            "constraints": {"columns": {"id": "int"}},
        },
        fixes=[
            {
                "row": 0,
                "column": "city",
                "detector_id": "external",
                "provenance": "external",
                "verification_strength": "proven",
            }
        ],
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "strength_is_earned" in _failed(result)


def test_entity_consensus_without_authority_is_rejected() -> None:
    statement = _statement(
        fixes=[
            {
                "row": 0,
                "column": "city",
                "detector_id": "entity_consensus",
                "provenance": "entity_consensus",
                "verification_strength": "proven",
            }
        ]
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "strength_is_earned" in _failed(result)


def test_an_honest_plausibility_only_downgrade_is_respected() -> None:
    """Derivation must never upgrade a receipt that honestly claims less."""
    statement = _statement(
        authority={
            "authoritative_columns": ["amount"],
            "accepted_constraint_ids": ["c1"],
            "constraints_digest": {"sha256": "d" * 64},
            "constraints": {"columns": {"amount": "int"}},
        },
        fixes=[
            {
                "row": 7,
                "column": "amount",
                "detector_id": "corrector",
                "provenance": "llm_live",
                "verification_strength": "plausibility_only",
            }
        ],
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "strength_is_earned" in _failed(result)


def test_unknown_provenance_is_rejected() -> None:
    statement = _statement(
        fixes=[
            {
                "row": 0,
                "column": "amount",
                "detector_id": "x",
                "provenance": "some_future_corrector",
                "verification_strength": "proven",
            }
        ]
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "vocabulary_closed" in _failed(result)


def test_unknown_review_reason_is_rejected() -> None:
    statement = _statement(
        held=[{"row": 1, "column": "amount", "review_reason": "because_i_said_so"}]
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "vocabulary_closed" in _failed(result)


def test_missing_verification_strength_is_rejected() -> None:
    """Not nullable. An absent label previously read as proven."""
    statement = _statement(
        fixes=[
            {
                "row": 0,
                "column": "amount",
                "detector_id": "x",
                "provenance": "deterministic",
                "verification_strength": None,
            }
        ]
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "vocabulary_closed" in _failed(result)


def test_unrecognised_version_is_rejected() -> None:
    result = verify_attestation(_statement(attestation_version="2"))
    assert result.ok is False
    assert "version_recognised" in _failed(result)


def test_unrecognised_predicate_type_is_rejected() -> None:
    statement = _statement()
    statement["predicateType"] = "https://example.com/SomethingElse/v1"
    result = verify_attestation(statement)
    assert result.ok is False
    assert "envelope_recognised" in _failed(result)


def test_missing_tool_version_is_rejected() -> None:
    result = verify_attestation(
        _statement(tool={"name": "dataforge", "contract_version": "repair_contract_v2"})
    )
    assert result.ok is False
    assert "schema_complete" in _failed(result)


def test_subject_not_matching_post_state_is_rejected() -> None:
    statement = _statement()
    statement["subject"][0]["digest"]["sha256"] = "9" * 64
    result = verify_attestation(statement)
    assert result.ok is False
    assert "subject_matches_post_state" in _failed(result)


def test_applied_without_revert_command_is_rejected() -> None:
    result = verify_attestation(_statement(revert_command=None))
    assert result.ok is False
    assert "reversibility_complete" in _failed(result)


def test_rejected_verifier_verdict_is_rejected() -> None:
    result = verify_attestation(
        _statement(
            verdicts={
                "verifier": "reject",
                "safety": "allow",
                "independent_verification": "not_run",
            }
        )
    )
    assert result.ok is False
    assert "verdicts_accepting" in _failed(result)


def test_schema_proven_write_without_embedded_constraints_is_rejected() -> None:
    """A dangling pointer is not evidence: the constraints must travel with the claim."""
    statement = _statement(
        authority={
            "authoritative_columns": ["amount"],
            "accepted_constraint_ids": ["c1"],
            "constraints_digest": {"sha256": "d" * 64},
            "constraints": None,
        },
        fixes=[
            {
                "row": 7,
                "column": "amount",
                "detector_id": "external",
                "provenance": "external",
                "verification_strength": "proven",
            }
        ],
    )
    result = verify_attestation(statement)
    assert result.ok is False
    assert "constraints_present" in _failed(result)


# --- Data identity ------------------------------------------------------------


def test_data_identity_is_reported_as_skipped_when_no_data_is_supplied() -> None:
    """Skipped must never be folded into ok: fewer checks is not the same as passing."""
    result = verify_attestation(_statement())
    skipped = {check.name for check in result.skipped}
    assert "data_identity" in skipped


def test_data_identity_detects_tampered_bytes(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    source.write_bytes(DATA)
    outcome = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    receipt = outcome.receipt.model_dump(mode="json")
    statement = build_attestation(
        receipt,
        tool_version="0.1.0",
        produced_at="2026-08-13T00:00:00Z",
        subject_name=source.name,
    )

    good = verify_attestation(statement, data_bytes=source.read_bytes())
    assert good.ok, [check.as_dict() for check in good.failures]

    bad = verify_attestation(statement, data_bytes=source.read_bytes() + b"9\n")
    assert bad.ok is False
    assert "data_identity" in _failed(bad)


def test_build_from_a_real_pipeline_run_records_what_the_receipt_never_did(
    tmp_path: Path,
) -> None:
    source = tmp_path / "data.csv"
    source.write_bytes(DATA)
    outcome = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    statement = build_attestation(
        outcome.receipt.model_dump(mode="json"),
        tool_version="0.1.0",
        produced_at="2026-08-13T00:00:00Z",
        subject_name=source.name,
        journal_head_sha256="c" * 64,
    )
    predicate = statement["predicate"]
    # The three facts the receipt has never carried.
    assert predicate["tool"]["version"] == "0.1.0"
    assert predicate["produced_at"] == "2026-08-13T00:00:00Z"
    assert predicate["journal"]["head_sha256"] == "c" * 64
    # And authority, recorded per column.
    assert "authoritative_columns" in predicate["authority"]


# --- DSSE signing -------------------------------------------------------------


def test_pae_matches_the_dsse_test_vector() -> None:
    """The published DSSE example, so the encoding is right by construction."""
    assert pae("http://example.com/HelloWorld", b"hello world") == (
        b"DSSEv1 29 http://example.com/HelloWorld 11 hello world"
    )


def test_signed_attestation_verifies_with_the_matching_key() -> None:
    key = Ed25519PrivateKey.generate()
    envelope = sign_attestation(_statement(), private_key=key)
    assert envelope["payloadType"] == PAYLOAD_TYPE
    result = verify_attestation(envelope, public_key_raw=key.public_key().public_bytes_raw())
    assert result.ok, [check.as_dict() for check in result.failures]
    signature = next(check for check in result.checks if check.name == "signature")
    assert signature.ok is True
    assert signature.skipped is False


def test_a_wrong_key_rejects() -> None:
    envelope = sign_attestation(_statement(), private_key=Ed25519PrivateKey.generate())
    other = Ed25519PrivateKey.generate()
    result = verify_attestation(envelope, public_key_raw=other.public_key().public_bytes_raw())
    assert result.ok is False
    assert "signature" in _failed(result)


def test_a_tampered_payload_rejects() -> None:
    key = Ed25519PrivateKey.generate()
    envelope = sign_attestation(_statement(), private_key=key)
    payload = json.loads(base64.b64decode(envelope["payload"]))
    payload["predicate"]["applied"] = False  # rewrite history
    envelope["payload"] = base64.b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    result = verify_attestation(envelope, public_key_raw=key.public_key().public_bytes_raw())
    assert result.ok is False
    assert "signature" in _failed(result)


def test_no_public_key_reports_unsigned_never_authentic() -> None:
    """Absence of a key must not be reported as a verified signature."""
    envelope = sign_attestation(_statement(), private_key=Ed25519PrivateKey.generate())
    result = verify_attestation(envelope)
    signature = next(check for check in result.checks if check.name == "signature")
    assert signature.skipped is True
    assert "authenticity was NOT established" in signature.detail


def test_signing_rejects_a_non_ed25519_key() -> None:
    with pytest.raises(TypeError):
        sign_attestation(_statement(), private_key="not-a-key")
