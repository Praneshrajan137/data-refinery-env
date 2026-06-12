"""Tests for the public DataForge repair engine contract."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.engine.repair import (
    CandidateFix,
    CandidateRepair,
    ProofObligation,
    RepairPipelineRequest,
    RootCause,
    TransactionApplyError,
    _lock_path_for,
    apply_transaction,
    create_repair_transaction,
    propose_repairs,
    run_repair_pipeline,
    source_path_lock,
)
from dataforge.repairers.base import ProposedFix
from dataforge.safety import SafetyContext, SafetyFilter, SafetyResult, SafetyVerdict
from dataforge.schema_inference import (
    ConstraintReviewArtifact,
    ConstraintReviewError,
    build_constraint_review_artifact,
    infer_schema,
)
from dataforge.table import read_csv
from dataforge.transactions.revert import revert_transaction
from dataforge.transactions.txn import CellFix
from dataforge.verifier import VerificationResult, VerificationVerdict


def _write_repairable_csv(path: Path) -> None:
    """Write a small CSV with a deterministic decimal-shift repair."""
    path.write_text(
        "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        encoding="utf-8",
    )


def _proposed_fix() -> ProposedFix:
    """Return a direct decimal-shift proposal for transaction tests."""
    return ProposedFix(
        fix=CellFix(
            row=3,
            column="amount",
            old_value="1020",
            new_value="102",
            detector_id="decimal_shift",
        ),
        reason="decimal-shift repair",
        confidence=0.9,
        provenance="deterministic",
    )


def _issue() -> Issue:
    """Return a decimal-shift issue for engine proposal tests."""
    return Issue(
        row=3,
        column="amount",
        issue_type="decimal_shift",
        severity=Severity.REVIEW,
        confidence=0.9,
        expected="102",
        actual="1020",
        reason="decimal shift",
    )


def _write_fd_repairable_csv(path: Path) -> None:
    """Write an FD violation with a strict majority repair."""
    path.write_text(
        "code,name\n"
        "A,Alpha\n"
        "A,Alpha\n"
        "A,Alfa\n"
        "B,Beta\n"
        "B,Beta\n"
        "C,Gamma\n"
        "C,Gamma\n"
        "D,Delta\n"
        "D,Delta\n"
        "E,Echo\n",
        encoding="utf-8",
    )


def _constraint_artifact_for(
    csv_path: Path,
    *,
    accept_fd: bool = False,
    accept_column: str | None = None,
) -> ConstraintReviewArtifact:
    """Build a reviewed constraint artifact for repair tests."""
    source_sha256 = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    artifact = build_constraint_review_artifact(
        infer_schema(read_csv(csv_path)),
        source_path=csv_path,
        source_sha256=source_sha256,
    )
    reviewed = []
    for candidate in artifact.candidates:
        should_accept_fd = (
            accept_fd
            and candidate.candidate.kind == "functional_dependency"
            and candidate.candidate.columns == ("code",)
            and candidate.candidate.dependent == "name"
        )
        should_accept_column = (
            accept_column is not None
            and candidate.candidate.kind == "column_type"
            and candidate.candidate.columns == (accept_column,)
        )
        reviewed.append(
            candidate.model_copy(
                update={
                    "decision": "accepted"
                    if should_accept_fd or should_accept_column
                    else candidate.decision
                }
            )
        )
    return artifact.model_copy(update={"candidates": reviewed})


class _NoneRepairer:
    def propose(self, *_args: object, **_kwargs: object) -> None:
        return None


class _SequenceRepairer:
    def __init__(self, fixes: list[ProposedFix]) -> None:
        self._fixes = fixes
        self._index = 0

    def propose(self, *_args: object, **_kwargs: object) -> ProposedFix:
        fix = self._fixes[min(self._index, len(self._fixes) - 1)]
        self._index += 1
        return fix


def test_candidate_fix_is_strict() -> None:
    with pytest.raises(ValidationError):
        CandidateFix.model_validate(
            {
                "row": "3",
                "column": "amount",
                "old_value": "1020",
                "new_value": "102",
                "detector_id": "decimal_shift",
                "reason": "repair",
                "confidence": 0.9,
                "provenance": "deterministic",
            }
        )


def test_oss_v1_contract_models_are_strict() -> None:
    root_cause = RootCause(
        row=3,
        column="amount",
        issue_type="decimal_shift",
        category="decimal_shift",
        confidence=0.9,
        reason="power-of-ten anomaly",
    )
    candidate = CandidateRepair(
        row=3,
        column="amount",
        old_value="1020",
        new_value="102",
        detector_id="decimal_shift",
        reason="repair",
        confidence=0.9,
        provenance="deterministic",
        verifier_reason="accepted",
    )
    proof = ProofObligation(
        obligation_id="smt::decimal_shift::3::amount",
        verifier="smt",
        status="accepted",
        reason="accepted",
    )

    assert root_cause.category == "decimal_shift"
    assert candidate.verifier_reason == "accepted"
    assert proof.status == "accepted"

    with pytest.raises(ValidationError):
        RootCause.model_validate(
            {
                "row": "3",
                "column": "amount",
                "issue_type": "decimal_shift",
                "category": "decimal_shift",
                "confidence": 0.9,
                "reason": "bad row type",
            }
        )


def test_run_repair_pipeline_dry_run_returns_ephemeral_receipt(tmp_path: Path) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    original = csv_path.read_bytes()

    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=csv_path,
            mode="dry_run",
            schema=None,
            create_dry_run_transaction=True,
        )
    )

    assert result.receipt.applied is False
    assert result.receipt.receipt_version == "repair_receipt_v1"
    assert result.transaction is not None
    assert re.fullmatch(r"txn-\d{4}-\d{2}-\d{2}-[0-9a-f]{6}", result.transaction.txn_id)
    assert result.fixes
    assert csv_path.read_bytes() == original
    assert result.receipt.root_causes
    assert result.receipt.candidate_repairs
    assert result.receipt.proof_obligations
    assert result.receipt.patch_plan_sha256 is not None
    assert result.receipt.revert_command == f"dataforge revert {result.transaction.txn_id}"
    assert result.receipt.limitations
    assert result.receipt.root_causes[0].category == "decimal_shift"
    assert result.receipt.proof_obligations[0].status == "accepted"


def test_repair_pipeline_uses_only_accepted_constraints(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    _write_fd_repairable_csv(csv_path)
    source_sha256 = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    artifact = _constraint_artifact_for(csv_path, accept_fd=True)

    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=csv_path,
            mode="dry_run",
            constraints=artifact,
            constraints_artifact_sha256="a" * 64,
        )
    )

    assert result.receipt.source_sha256 == source_sha256
    assert result.receipt.accepted_constraint_ids
    assert result.receipt.constraints_artifact_sha256 == "a" * 64
    assert [fix.detector_id for fix in result.fixes] == ["fd_violation"]
    assert result.fixes[0].old_value == "Alfa"
    assert result.fixes[0].new_value == "Alpha"


def test_repair_pipeline_ignores_pending_constraints(tmp_path: Path) -> None:
    csv_path = tmp_path / "fd.csv"
    _write_fd_repairable_csv(csv_path)
    artifact = _constraint_artifact_for(csv_path, accept_fd=False)

    result = run_repair_pipeline(
        RepairPipelineRequest(source_path=csv_path, mode="dry_run", constraints=artifact)
    )

    assert result.receipt.accepted_constraint_ids == []
    assert result.issues == []
    assert result.fixes == []


def test_repair_pipeline_rejects_conflicting_accepted_constraints(tmp_path: Path) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    artifact = _constraint_artifact_for(csv_path, accept_column="amount")
    accepted_id = artifact.accepted_candidate_ids()[0]

    with pytest.raises(ConstraintReviewError, match=accepted_id):
        run_repair_pipeline(
            RepairPipelineRequest(
                source_path=csv_path,
                mode="dry_run",
                schema=Schema(columns={"amount": "float"}),
                constraints=artifact,
            )
        )


def test_apply_transaction_reverts_byte_for_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    original = csv_path.read_bytes()

    result = run_repair_pipeline(RepairPipelineRequest(source_path=csv_path, mode="apply"))

    assert result.receipt.applied is True
    assert result.receipt.txn_id is not None
    assert csv_path.read_bytes() != original

    reverted = revert_transaction(result.receipt.txn_id, search_root=tmp_path)

    assert reverted.reverted_at is not None
    assert csv_path.read_bytes() == original


def test_apply_transaction_rejects_stale_source(tmp_path: Path) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    source_bytes = csv_path.read_bytes()
    csv_path.write_text("id,amount\n1,100\n2,105\n3,98\n4,9999\n5,103\n", encoding="utf-8")

    with pytest.raises(TransactionApplyError, match="source file changed"):
        apply_transaction(csv_path, [_proposed_fix()], source_bytes)


def test_duplicate_transaction_id_is_rejected(tmp_path: Path) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    source_bytes = csv_path.read_bytes()
    txn_id = "txn-2026-04-20-abcdef"

    create_repair_transaction(csv_path, [_proposed_fix()], source_bytes, txn_id=txn_id)

    with pytest.raises(TransactionApplyError, match="snapshot already exists"):
        create_repair_transaction(csv_path, [_proposed_fix()], source_bytes, txn_id=txn_id)


def test_apply_failure_restores_original_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    source_bytes = csv_path.read_bytes()

    def fail_append(*args: object, **kwargs: object) -> None:
        raise OSError("journal append failed")

    monkeypatch.setattr("dataforge.engine.repair.append_applied_event", fail_append)

    with pytest.raises(OSError, match="journal append failed"):
        apply_transaction(csv_path, [_proposed_fix()], source_bytes)

    assert csv_path.read_bytes() == source_bytes


def test_source_path_lock_times_out_and_recovers_stale_lock(tmp_path: Path) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)

    with (
        source_path_lock(csv_path),
        pytest.raises(TransactionApplyError, match="Timed out"),
        source_path_lock(csv_path, timeout_seconds=0.01),
    ):
        pass

    lock_path = _lock_path_for(csv_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("stale", encoding="utf-8")
    old_time = 1
    lock_path.touch()
    import os

    os.utime(lock_path, (old_time, old_time))

    with source_path_lock(csv_path, stale_after_seconds=0.0):
        assert True


def test_propose_repairs_records_missing_and_empty_repairers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})

    monkeypatch.setattr("dataforge.engine.repair.build_repairers", lambda **_kwargs: {})
    accepted, attempts = propose_repairs(
        [_issue()],
        csv_path,
        df.copy(deep=True),
        None,
        allow_llm=False,
        model="gemini-2.0-flash",
        allow_pii=False,
        confirm_pii=False,
        confirm_escalations=False,
        interactive=False,
    )
    assert accepted == []
    assert attempts[0][0].status == "attempted_not_fixed"

    monkeypatch.setattr(
        "dataforge.engine.repair.build_repairers",
        lambda **_kwargs: {"decimal_shift": _NoneRepairer()},
    )
    _accepted, attempts = propose_repairs(
        [_issue()],
        csv_path,
        df.copy(deep=True),
        None,
        allow_llm=False,
        model="gemini-2.0-flash",
        allow_pii=False,
        confirm_pii=False,
        confirm_escalations=False,
        interactive=False,
    )
    assert attempts[0][0].reason.startswith("No repair proposal")


def test_propose_repairs_retries_after_safety_denial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
    first = _proposed_fix().model_copy(
        update={"fix": _proposed_fix().fix.model_copy(update={"new_value": "999"})}
    )
    second = _proposed_fix()
    repairer = _SequenceRepairer([first, second])

    monkeypatch.setattr(
        "dataforge.engine.repair.build_repairers",
        lambda **_kwargs: {"decimal_shift": repairer},
    )
    monkeypatch.setattr(
        "dataforge.engine.repair.SafetyFilter.evaluate",
        lambda *_args, **_kwargs: SafetyResult(verdict=SafetyVerdict.ALLOW, reason="ok"),
    )
    safety_results = iter(
        [
            SafetyResult(verdict=SafetyVerdict.DENY, reason="blocked"),
            SafetyResult(verdict=SafetyVerdict.ALLOW, reason="ok"),
        ]
    )
    monkeypatch.setattr(
        "dataforge.engine.repair.SafetyFilter.evaluate",
        lambda *_args, **_kwargs: next(safety_results),
    )
    monkeypatch.setattr(
        "dataforge.engine.repair.SMTVerifier.verify",
        lambda *_args, **_kwargs: VerificationResult(
            verdict=VerificationVerdict.ACCEPT,
            reason="verified",
        ),
    )

    accepted, attempts = propose_repairs(
        [_issue()],
        csv_path,
        df.copy(deep=True),
        None,
        allow_llm=False,
        model="gemini-2.0-flash",
        allow_pii=False,
        confirm_pii=False,
        confirm_escalations=False,
        interactive=False,
    )

    assert accepted == [second]
    assert [attempt.status for attempt in attempts[0]] == ["denied", "accepted"]


def test_propose_repairs_resolves_interactive_escalation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_path = tmp_path / "amounts.csv"
    _write_repairable_csv(csv_path)
    df = pd.DataFrame({"amount": ["100", "105", "98", "1020", "103"]})
    monkeypatch.setattr(
        "dataforge.engine.repair.build_repairers",
        lambda **_kwargs: {"decimal_shift": _SequenceRepairer([_proposed_fix()])},
    )
    monkeypatch.setattr(
        "dataforge.engine.repair.SafetyFilter.evaluate",
        lambda *_args, **_kwargs: SafetyResult(
            verdict=SafetyVerdict.ESCALATE,
            reason="needs review",
        ),
    )
    monkeypatch.setattr(
        "dataforge.engine.repair.SMTVerifier.verify",
        lambda *_args, **_kwargs: VerificationResult(
            verdict=VerificationVerdict.ACCEPT,
            reason="verified",
        ),
    )

    def resolver(
        candidate: ProposedFix,
        schema: object,
        context: SafetyContext,
        safety_filter: SafetyFilter,
        safety_result: SafetyResult,
    ) -> tuple[SafetyContext, SafetyResult]:
        del candidate, schema, safety_filter, safety_result
        return context, SafetyResult(verdict=SafetyVerdict.ALLOW, reason="confirmed")

    accepted, attempts = propose_repairs(
        [_issue()],
        csv_path,
        df.copy(deep=True),
        None,
        allow_llm=False,
        model="gemini-2.0-flash",
        allow_pii=False,
        confirm_pii=False,
        confirm_escalations=False,
        interactive=True,
        escalation_resolver=resolver,
    )

    assert accepted
    assert attempts[0][0].status == "accepted"
