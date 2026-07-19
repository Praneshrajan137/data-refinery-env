"""Tests for verify_and_apply: the external-fix prove-and-reverse gate.

Covers the full trust contract: schema-proven external fixes apply and certify;
untrusted (no-schema / unconfirmed) fixes are held; compare-and-set rejects stale
writes; invalid targets and no-ops are handled; and every applied change is
byte-for-byte reversible and re-verifiable.
"""

from __future__ import annotations

from pathlib import Path

from dataforge.certificate import reverify_certificate, verify_certificate
from dataforge.detectors.base import Schema
from dataforge.engine.repair import ExternalFix, VerifyAndApplyRequest, verify_and_apply
from dataforge.transactions.revert import revert_transaction

_SCHEMA = Schema(columns={"id": "str", "score": "float"})


def _csv(tmp_path: Path) -> Path:
    source = tmp_path / "t.csv"
    source.write_text("id,score\n1,10\n2,20\n3,30\n", encoding="utf-8")
    return source


def test_schema_proven_external_fix_applies_and_certifies(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    before = source.read_bytes()
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=0, column="score", new_value="15")],
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
            proposer="agent-x",
        )
    )
    assert result.receipt.applied is True
    assert result.receipt.txn_id
    assert [f.verification_strength for f in result.receipt.applied_fixes] == ["proven"]
    post = source.read_bytes()
    assert verify_certificate(result.receipt.model_dump(mode="json"), data_bytes=post).ok
    assert reverify_certificate(
        result.receipt.model_dump(mode="json"), data_bytes=post, schema=_SCHEMA
    ).ok
    # Reversible.
    revert_transaction(result.receipt.txn_id, search_root=tmp_path)
    assert source.read_bytes() == before


def test_no_schema_external_fix_is_held_not_applied(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    before = source.read_bytes()
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=0, column="score", new_value="15")],
            mode="apply",
            confirm_escalations=True,
        )
    )
    assert result.receipt.applied is False
    assert source.read_bytes() == before
    assert "floor_cannot_verify" in {s.review_reason for s in result.receipt.suggested_fixes}


def test_unconfirmed_external_write_escalates(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=0, column="score", new_value="15")],
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=False,
        )
    )
    assert result.receipt.applied is False
    assert "safety_escalation" in {s.review_reason for s in result.receipt.suggested_fixes}


def test_compare_and_set_rejects_stale_and_applies_fresh(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    stale = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=1, column="score", new_value="99", expected_old_value="WRONG")],
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
        )
    )
    assert stale.receipt.applied is False
    assert "stale_precondition" in {s.review_reason for s in stale.receipt.suggested_fixes}

    fresh = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=1, column="score", new_value="25", expected_old_value="20")],
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
        )
    )
    assert fresh.receipt.applied is True


def test_invalid_target_and_duplicate_are_rejected(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[
                ExternalFix(row=0, column="nope", new_value="x"),  # unknown column
                ExternalFix(row=99, column="score", new_value="1"),  # out of range
                ExternalFix(row=0, column="score", new_value="11"),  # ok
                ExternalFix(row=0, column="score", new_value="12"),  # duplicate cell
            ],
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
        )
    )
    reasons = {s.review_reason for s in result.receipt.suggested_fixes}
    assert "invalid_target" in reasons
    assert result.receipt.applied is True  # the one valid fix applied


def test_noop_is_skipped(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    before = source.read_bytes()
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=0, column="score", new_value="10")],  # already 10
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
        )
    )
    assert result.receipt.applied is False
    assert source.read_bytes() == before
    assert any("no-op" in limit for limit in result.receipt.limitations)


def test_dry_run_never_mutates(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    before = source.read_bytes()
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=0, column="score", new_value="15")],
            mode="dry_run",
            schema=_SCHEMA,
            confirm_escalations=True,
        )
    )
    assert result.receipt.applied is False
    assert source.read_bytes() == before


def test_allow_unproven_optin_applies_but_records_plausibility(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[ExternalFix(row=0, column="score", new_value="15")],
            mode="apply",
            confirm_escalations=True,
            allow_unproven_autoapply=True,  # no schema, but opt-in
        )
    )
    assert result.receipt.applied is True
    assert [f.verification_strength for f in result.receipt.applied_fixes] == ["plausibility_only"]
    # Honesty: the certificate reports the auto-applied set is NOT proven.
    verification = verify_certificate(
        result.receipt.model_dump(mode="json"), data_bytes=source.read_bytes()
    )
    assert verification.ok is False
    assert any(
        c.name == "auto_apply_is_proven_deterministic" and not c.ok for c in verification.checks
    )
