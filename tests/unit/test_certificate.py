"""Tests for the self-verifiable trust certificate."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from dataforge.calibration import AbstentionPolicy
from dataforge.certificate import reverify_certificate, verify_certificate
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline


def _decimal_shift_csv(path: Path) -> None:
    # Clustered values with one clear 10x outlier -> deterministic decimal_shift
    # fix that auto-applies through the verified gate.
    path.write_text(
        "amount\n100\n104\n98\n103\n101\n106\n99\n1020\n",
        encoding="utf-8",
    )


def test_certificate_verifies_against_repaired_data(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    _decimal_shift_csv(source)
    result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    receipt = result.receipt.model_dump(mode="json")

    verification = verify_certificate(receipt, data_bytes=source.read_bytes())
    assert verification.ok, [c for c in verification.checks if not c.ok]
    # The applied set is deterministic -> proven, not a plausible LLM write.
    proven = {c.name: c.ok for c in verification.checks}
    assert proven["auto_apply_is_proven_deterministic"] is True
    assert proven["data_identity"] is True


def test_tampered_data_is_detected(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    _decimal_shift_csv(source)
    result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    receipt = result.receipt.model_dump(mode="json")

    tampered = source.read_bytes() + b"9\n"
    verification = verify_certificate(receipt, data_bytes=tampered)
    assert verification.ok is False
    assert any(c.name == "data_identity" and not c.ok for c in verification.checks)


def test_tampered_receipt_hash_is_detected(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    _decimal_shift_csv(source)
    result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    receipt = result.receipt.model_dump(mode="json")

    receipt["post_sha256"] = "0" * 64  # forge the recorded output hash
    verification = verify_certificate(receipt, data_bytes=source.read_bytes())
    assert verification.ok is False


def test_dry_run_certificate_verifies_against_source(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    _decimal_shift_csv(source)
    original = source.read_bytes()
    result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="dry_run"))
    receipt = result.receipt.model_dump(mode="json")

    assert source.read_bytes() == original  # dry run never mutates
    verification = verify_certificate(receipt, data_bytes=original)
    assert verification.ok, [c for c in verification.checks if not c.ok]


def test_llm_applied_is_flagged_not_proven() -> None:
    # A hand-built receipt describing a policy-permitted LLM auto-apply must be
    # reported honestly as NOT proven-deterministic.
    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "candidate_provenance": ["llm_live"],
    }
    verification = verify_certificate(receipt)
    assert verification.ok is False
    assert any(
        c.name == "auto_apply_is_proven_deterministic" and not c.ok for c in verification.checks
    )


def test_entity_consensus_applied_is_flagged_not_proven() -> None:
    """``entity_consensus`` is untrusted, so a certificate must not call it proven.

    Sibling-row agreement is evidence, not proof: a majority can be wrong. The engine
    lists it in ``_UNTRUSTED_PROVENANCE`` for exactly that reason. This certificate
    check is the artifact a third party reads, so if it omits the provenance the
    certificate says "proven" about a value nothing proved -- a truthfulness
    violation, which is the product claim itself.

    This is the same drift class that already shipped twice: once in the browser's
    ``LLM_PROVENANCE`` and once in ``REVIEW_REASON_COPY``. A hand-maintained copy of a
    closed vocabulary is a copy that will disagree.
    """
    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "candidate_provenance": ["entity_consensus"],
    }
    verification = verify_certificate(receipt)
    assert verification.ok is False, (
        "entity_consensus was accepted as proven-deterministic; the certificate "
        "overstates proof for an untrusted provenance"
    )
    assert any(
        c.name == "auto_apply_is_proven_deterministic" and not c.ok for c in verification.checks
    )


def test_unknown_provenance_fails_closed() -> None:
    """An unrecognised provenance must be treated as untrusted, not as trusted.

    Membership-testing against a hardcoded set of *untrusted* names fails OPEN: any
    provenance nobody thought of reads as deterministic. A trust artifact must fail
    closed, so the check is written against the known-trusted set instead.
    """
    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "candidate_provenance": ["some_future_corrector"],
    }
    verification = verify_certificate(receipt)
    assert verification.ok is False, (
        "an unknown provenance was accepted as proven; the check fails open"
    )


def test_missing_verification_strength_fails_closed() -> None:
    """A fix with an untrusted provenance and no strength must not read as proven.

    ``verification_strength`` is stamped late and is frequently absent, so a check
    that only rejects the explicit string ``plausibility_only`` treats every unstamped
    fix as proven. Strength must be derived from provenance when it is missing --
    the same reason ``enforce_proven_only`` computes it rather than reading it.
    """
    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "applied_fixes": [
            {
                "row": 0,
                "column": "city",
                "old_value": "x",
                "new_value": "y",
                "detector_id": "entity_consensus",
                "reason": "siblings agree",
                "confidence": 0.9,
                "provenance": "entity_consensus",
                "verification_strength": None,
            }
        ],
    }
    verification = verify_certificate(receipt)
    assert verification.ok is False, (
        "an unstamped untrusted fix was accepted as proven; the strength check fails open"
    )


def test_authority_is_credited_per_column_not_per_table() -> None:
    """An untrusted write is proven only on a column the schema actually covers.

    This is the 2026-08-09 defect made checkable from the certificate alone: accepting
    one ``column_type`` constraint on ``id`` once granted blanket authority, so an
    ``external`` garbage value on the unrelated column ``city`` was applied AND stamped
    ``proven``. A reader holding only the certificate could not have caught that,
    because the certificate never recorded which columns the authority covered.
    """
    base = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "authoritative_columns": ["id"],
    }
    fix = {
        "row": 0,
        "column": "city",
        "old_value": "Springfield",
        "new_value": "ZZZ_GARBAGE",
        "detector_id": "external",
        "reason": "agent proposed",
        "confidence": 0.99,
        "provenance": "external",
        "verification_strength": "proven",
    }

    off_authority = verify_certificate({**base, "applied_fixes": [fix]})
    assert off_authority.ok is False, (
        "an external write to a column outside the schema's authority was accepted "
        "as proven; authority is being read as table-level"
    )
    detail = next(
        c.detail for c in off_authority.checks if c.name == "auto_apply_is_proven_deterministic"
    )
    assert "city" in detail

    # The same fix on a covered column is genuinely proven -- the verification layer's
    # whole purpose -- and must still verify.
    on_authority = verify_certificate(
        {**base, "authoritative_columns": ["id", "city"], "applied_fixes": [fix]}
    )
    assert on_authority.ok, [c for c in on_authority.checks if not c.ok]


def test_an_honest_downgrade_in_the_receipt_is_respected() -> None:
    """A receipt that records ``plausibility_only`` must be believed, even over authority.

    Derivation is used to catch a receipt claiming MORE than it earned. It must never be
    used to upgrade a receipt that honestly claims LESS. The engine downgrades to
    plausibility_only for reasons a certificate reader cannot see -- a drift monitor
    firing, a policy withdrawal, a scope check refusing a calibration artifact -- and
    each of those is recorded rather than printed precisely so it travels with the data.

    Found by mutation: disabling the ``recorded == "plausibility_only"`` branch left
    every other test green, because in those fixtures derivation independently reached
    the same verdict. Here it does not: the column IS covered, so derivation alone would
    say ``proven``.
    """
    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "authoritative_columns": ["city"],
        "applied_fixes": [
            {
                "row": 0,
                "column": "city",
                "old_value": "x",
                "new_value": "y",
                "detector_id": "corrector",
                "reason": "llm proposed",
                "confidence": 0.9,
                "provenance": "llm_live",
                "verification_strength": "plausibility_only",
            }
        ],
    }
    verification = verify_certificate(receipt)
    assert verification.ok is False, (
        "a receipt honestly recording plausibility_only was upgraded to proven by "
        "derivation; the certificate must never claim more than the receipt"
    )
    detail = next(
        c.detail for c in verification.checks if c.name == "auto_apply_is_proven_deterministic"
    )
    assert "plausibility_only" in detail


# --- Deep re-verification (reverify_certificate) ------------------------------


def _permissive_policy() -> AbstentionPolicy:
    return AbstentionPolicy(
        target_precision=0.95,
        auto_apply_thresholds={"missing_value": 0.0},
        default_threshold=0.0,
    )


def test_reverify_re_derives_accept_for_deterministic_run(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    _decimal_shift_csv(source)
    result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    receipt = result.receipt.model_dump(mode="json")

    verification = reverify_certificate(receipt, data_bytes=source.read_bytes())
    assert verification.ok, [c for c in verification.checks if not c.ok]
    names = {c.name: c.ok for c in verification.checks}
    assert names["reverify_constraints_accept"] is True
    assert names["reverify_recorded_strength_truthful"] is True
    assert names["reverify_parse_integrity"] is True


def test_reverify_detects_tampered_data(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    _decimal_shift_csv(source)
    result = run_repair_pipeline(RepairPipelineRequest(source_path=source, mode="apply"))
    receipt = result.receipt.model_dump(mode="json")

    tampered = source.read_bytes().replace(b"amount", b"amount") + b"55\n"
    verification = reverify_certificate(receipt, data_bytes=tampered)
    assert verification.ok is False  # base data_identity (post hash) mismatch


def test_reverify_catches_a_lying_strength_label() -> None:
    # A receipt that claims an unverified LLM value is "proven" (no schema) must
    # be rejected -- the certificate is not allowed to lie.
    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_path": "x.csv",
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "revert_command": "dataforge revert txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "candidate_provenance": ["llm_live"],
        "applied_fixes": [
            {
                "row": 0,
                "column": "city",
                "old_value": "",
                "new_value": "Atlantis",
                "detector_id": "missing_value",
                "provenance": "llm_live",
                "verification_strength": "proven",
            }
        ],
    }
    # Provide data that matches the forged post hash is impossible here; skip the
    # hash check by not passing data that must match, and assert the label lie is
    # caught. We craft data_bytes matching post_sha256 is not needed: the strength
    # check runs regardless.
    verification = reverify_certificate(receipt, data_bytes=b"city\nAtlantis\n")
    assert verification.ok is False
    assert any(
        c.name == "reverify_recorded_strength_truthful" and not c.ok for c in verification.checks
    )


def test_reverify_cross_checks_with_diverse_verifier_on_authoritative_schema() -> None:
    # With an authoritative schema, reverify re-derives ACCEPT using TWO
    # independently-written checkers (SMT + Direct) and records their agreement.
    from dataforge.verifier.schema import DomainBound, Schema

    receipt = {
        "schema_version": "repair_receipt_v1",
        "applied": True,
        "reversible": True,
        "source_path": "x.csv",
        "source_sha256": "a" * 64,
        "post_sha256": "b" * 64,
        "txn_id": "txn-x",
        "verifier_verdict": "accept",
        "safety_verdict": "allow",
        "candidate_provenance": ["deterministic"],
        "applied_fixes": [
            {
                "row": 0,
                "column": "amount",
                "old_value": "500",
                "new_value": "50",
                "detector_id": "decimal_shift",
                "provenance": "deterministic",
                "verification_strength": "proven",
            }
        ],
    }
    schema = Schema(
        columns={"amount": "int"},
        domain_bounds=(DomainBound(column="amount", min_value=0.0, max_value=100.0),),
    )
    verification = reverify_certificate(receipt, data_bytes=b"amount\n50\n20\n30\n", schema=schema)
    names = {c.name: c.ok for c in verification.checks}
    assert names["reverify_constraints_accept"] is True
    assert names["reverify_independent_agreement"] is True


def test_reverify_accepts_honest_unproven_optin(tmp_path: Path) -> None:
    # An LLM fill auto-applied via the explicit opt-in, recorded truthfully as
    # plausibility_only, must RE-VERIFY as valid: constraints re-derive ACCEPT and
    # the labels are truthful. Honest-unproven is a valid certificate.
    source = tmp_path / "data.csv"
    source.write_text(
        "id,city\n1,Boston\n2,Denver\n3,Austin\n4,Reno\n5,Miami\n6,Chicago\n7,Dallas\n8,\n",
        encoding="utf-8",
    )
    with patch("dataforge.repairers.llm_corrector.complete", _make_complete("Seattle")):
        result = run_repair_pipeline(
            RepairPipelineRequest(
                source_path=source,
                mode="apply",
                allow_llm=True,
                confirm_escalations=True,
                corrector_policy=_permissive_policy(),
                allow_unproven_autoapply=True,
            )
        )
    assert result.receipt.applied is True
    receipt = result.receipt.model_dump(mode="json")
    verification = reverify_certificate(receipt, data_bytes=source.read_bytes())
    assert verification.ok, [c for c in verification.checks if not c.ok]
    strengths = {f["verification_strength"] for f in receipt["applied_fixes"]}
    assert strengths == {"plausibility_only"}


def _make_complete(value: str):  # noqa: ANN202
    async def _complete(messages: object, *, model: str, temperature: float) -> str:
        return value

    return _complete
