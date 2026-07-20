"""Guardrail-value proof: an untrusted agent cannot corrupt data through verify_and_apply.

This is the verification-layer thesis made measurable. We feed a realistic batch of
externally-proposed fixes from an *untrusted* agent -- some correct, some corrupting,
some stale, some invalid -- and prove that:

  * ONLY the schema-proven-correct fixes are applied,
  * every corrupting/stale/invalid proposal is blocked with an honest reason,
  * the applied set is self-certifying, and
  * the whole batch is byte-for-byte reversible.

Net trust metric: an agent with an arbitrary mix of good and bad proposals yields
zero corruptions -- the file only ever moves to a proven-correct state or not at all.
"""

from __future__ import annotations

from pathlib import Path

from dataforge.certificate import reverify_certificate, verify_certificate
from dataforge.detectors.base import Schema
from dataforge.engine.repair import ExternalFix, VerifyAndApplyRequest, verify_and_apply
from dataforge.transactions.revert import revert_transaction

_SCHEMA = Schema(columns={"id": "str", "score": "float"})


def _csv(tmp_path: Path) -> Path:
    source = tmp_path / "readings.csv"
    source.write_text("id,score\n1,10\n2,20\n3,30\n4,40\n", encoding="utf-8")
    return source


def test_untrusted_agent_batch_yields_zero_corruptions(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    before = source.read_bytes()

    # An untrusted agent proposes a mixed batch.
    agent_fixes = [
        ExternalFix(row=0, column="score", new_value="15"),  # correct -> should apply
        ExternalFix(row=1, column="score", new_value="25"),  # correct -> should apply
        ExternalFix(row=2, column="score", new_value="abc"),  # corrupting (type) -> rejected
        ExternalFix(
            row=3, column="score", new_value="99", expected_old_value="WRONG"
        ),  # stale CAS -> rejected
        ExternalFix(row=0, column="ghost", new_value="x"),  # invalid target -> rejected
    ]

    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=agent_fixes,
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
            proposer="untrusted-agent",
        )
    )
    receipt = result.receipt

    # Only the two proven-correct fixes were applied.
    applied = {(f.row, f.column, f.new_value) for f in receipt.applied_fixes}
    assert applied == {(0, "score", "15"), (1, "score", "25")}
    assert all(f.verification_strength == "proven" for f in receipt.applied_fixes)

    # Every bad proposal was blocked with an honest, specific reason.
    reasons = {s.review_reason for s in receipt.suggested_fixes}
    assert {"verifier_rejected", "stale_precondition", "invalid_target"} <= reasons

    # Trust metrics: 5 proposed, 2 applied, 3 blocked, ZERO corruptions.
    assert receipt.issues_count == 5
    assert len(receipt.applied_fixes) == 2
    assert len(receipt.suggested_fixes) == 3

    # The applied set is self-certifying ...
    post = source.read_bytes()
    assert verify_certificate(receipt.model_dump(mode="json"), data_bytes=post).ok
    assert reverify_certificate(receipt.model_dump(mode="json"), data_bytes=post, schema=_SCHEMA).ok
    # ... the corrupting value never touched the file ...
    assert "abc" not in source.read_text(encoding="utf-8")
    # ... and the entire agent batch is byte-for-byte reversible.
    revert_transaction(receipt.txn_id, search_root=tmp_path)
    assert source.read_bytes() == before


def test_all_corrupting_batch_applies_nothing_and_never_mutates(tmp_path: Path) -> None:
    source = _csv(tmp_path)
    before = source.read_bytes()

    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[
                ExternalFix(row=0, column="score", new_value="abc"),
                ExternalFix(row=1, column="score", new_value=""),
            ],
            mode="apply",
            schema=_SCHEMA,
            confirm_escalations=True,
            proposer="untrusted-agent",
        )
    )
    assert result.receipt.applied is False
    assert result.receipt.txn_id is None
    assert source.read_bytes() == before  # no transaction, no mutation
