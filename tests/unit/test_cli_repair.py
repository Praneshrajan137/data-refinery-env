"""CLI tests for Week 2 repair and revert commands."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from rich.console import Console
from typer.testing import CliRunner

from dataforge.cli import app
from dataforge.cli.repair import (
    _propose_repairs,
    _render_attempt_summary,
    _resolve_escalation,
)
from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.engine.repair import _apply_fixes_to_csv
from dataforge.repairers.base import ProposedFix, RepairAttempt
from dataforge.safety import SafetyContext, SafetyResult, SafetyVerdict
from dataforge.schema_inference import (
    build_constraint_review_artifact,
    dump_constraint_review_artifact,
    infer_schema,
)
from dataforge.table import read_csv
from dataforge.transactions.log import append_created_transaction
from dataforge.transactions.txn import CellFix, RepairTransaction
from dataforge.verifier import VerificationResult, VerificationVerdict

runner = CliRunner()


def _write_repairable_csv(path: Path) -> None:
    """Write a small CSV with a deterministic decimal-shift issue.

    NOTE: as of 2026-08-22 a decimal-shift issue is **detected but not auto-applied**.
    `decimal_shift` infers a repair from the column's own distribution and measured
    precision 0.0000 on hospital, flights and rayyan, so it is not in
    `CONSTRAINT_CHECKABLE_DETECTORS` and must clear a calibrated threshold like any other
    fallible source. Use `_write_premised_repairable` when a test needs a fix that
    actually applies.
    """
    path.write_text(
        "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        encoding="utf-8",
    )


def _write_premised_repairable(csv_path: Path) -> Path:
    """Write a CSV whose repair is checkable against a DECLARED premise, plus that schema.

    Returns the schema path. `state -> city` is declared, holds on every row but one, and
    `fd_violation` is constraint-checkable -- so this exercises a write the product
    actually stands behind, rather than a schema-free distributional guess.
    """
    rows = "\n".join(f"{i},MA,boston" for i in range(1, 10))
    csv_path.write_text(f"id,state,city\n{rows}\n10,MA,bostonn\n", encoding="utf-8")
    schema_path = csv_path.with_suffix(".schema.yaml")
    schema_path.write_text(
        "columns:\n"
        "  id: string\n"
        "  state: string\n"
        "  city: string\n"
        "functional_dependencies:\n"
        "  - determinant: [state]\n"
        "    dependent: city\n",
        encoding="utf-8",
    )
    return schema_path


def _write_fd_repairable_csv(path: Path) -> None:
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


def _issue(*, issue_type: str = "fd_violation", row: int = 3, column: str = "amount") -> Issue:
    """A candidate issue.

    Default changed from ``decimal_shift`` to ``fd_violation`` on 2026-08-22.
    ``decimal_shift`` is a distributional inference (measured precision 0.0000 on
    hospital, flights and rayyan) and is deliberately no longer in
    ``CONSTRAINT_CHECKABLE_DETECTORS``, so a fix carrying it is held for review rather
    than auto-applied. Tests that patch ``propose_repairs`` to exercise the *write* path
    need a detector the product actually stands behind.
    """
    return Issue(
        row=row,
        column=column,
        issue_type=issue_type,  # type: ignore[arg-type]
        severity=Severity.REVIEW,
        confidence=0.9,
        expected="102",
        actual="1020",
        reason="candidate issue",
    )


def _proposed_fix(
    *,
    row: int = 3,
    column: str = "amount",
    old_value: str = "1020",
    new_value: str = "102",
    detector_id: str = "fd_violation",
) -> ProposedFix:
    return ProposedFix(
        fix=CellFix(
            row=row,
            column=column,
            old_value=old_value,
            new_value=new_value,
            detector_id=detector_id,
        ),
        reason="candidate",
        confidence=0.9,
        provenance="deterministic",
    )


class TestRepairCommand:
    """Repair CLI behavior."""

    def test_apply_fixes_to_csv_rejects_invalid_targets(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)

        with pytest.raises(ValueError, match="Column 'missing'"):
            _apply_fixes_to_csv(
                csv_path,
                [
                    CellFix(
                        row=0,
                        column="missing",
                        old_value="100",
                        new_value="101",
                        detector_id="test",
                    )
                ],
            )

        with pytest.raises(ValueError, match="Row 99"):
            _apply_fixes_to_csv(
                csv_path,
                [
                    CellFix(
                        row=99,
                        column="amount",
                        old_value="100",
                        new_value="101",
                        detector_id="test",
                    )
                ],
            )

        with pytest.raises(ValueError, match="stale fix"):
            _apply_fixes_to_csv(
                csv_path,
                [
                    CellFix(
                        row=0,
                        column="amount",
                        old_value="999",
                        new_value="101",
                        detector_id="test",
                    )
                ],
            )

        with pytest.raises(ValueError, match="Unsupported repair operation"):
            _apply_fixes_to_csv(
                csv_path,
                [
                    CellFix(
                        row=0,
                        column="amount",
                        old_value="100",
                        new_value="101",
                        detector_id="test",
                        operation="delete_row",
                    )
                ],
            )

    def test_requires_exactly_one_mode(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)

        result = runner.invoke(app, ["repair", str(csv_path)])

        assert result.exit_code == 2
        assert "Choose exactly one" in result.output

    def test_dry_run_shows_diff_and_creates_no_artifacts(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        schema_path = _write_premised_repairable(csv_path)

        result = runner.invoke(
            app, ["repair", str(csv_path), "--dry-run", "--schema", str(schema_path)]
        )

        assert result.exit_code == 0
        assert "Proposed Repairs" in result.output
        assert "fd_violation" in result.output
        assert not (tmp_path / ".dataforge").exists()

    def test_dry_run_json(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        schema_path = _write_premised_repairable(csv_path)

        result = runner.invoke(
            app,
            ["repair", str(csv_path), "--dry-run", "--json", "--schema", str(schema_path)],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["receipt"]["receipt_version"] == "repair_receipt_v1"
        assert '"mode": "dry_run"' in result.output
        assert '"fixes_count": 1' in result.output

    def test_decimal_shift_is_detected_but_never_auto_applied(self, tmp_path: Path) -> None:
        """The finding survives; the write does not.

        This replaces what the two tests above used to assert. ``decimal_shift`` is a
        distributional inference with measured precision 0.0000 on hospital, flights and
        rayyan, and on error-free TPC-H it would have rewritten 263,428 monetary values.
        It must still be reported -- suppressing a finding would be its own defect -- but
        it must never reach the write path without a calibrated threshold.
        """
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        original = csv_path.read_bytes()

        profiled = runner.invoke(app, ["profile", str(csv_path)])
        assert "decimal_shift" in profiled.output, "the finding must still be surfaced"

        applied = runner.invoke(app, ["repair", str(csv_path), "--apply"])
        assert csv_path.read_bytes() == original, (
            "decimal_shift was auto-applied; it bypassed the calibration gate. "
            f"exit={applied.exit_code}"
        )

    def test_dry_run_json_uses_accepted_constraints_artifact(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "fd.csv"
        constraints_path = tmp_path / "constraints.json"
        _write_fd_repairable_csv(csv_path)
        artifact = build_constraint_review_artifact(
            infer_schema(read_csv(csv_path)),
            source_path=csv_path,
            source_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        )
        reviewed = [
            candidate.model_copy(
                update={
                    "decision": "accepted"
                    if (
                        candidate.candidate.kind == "functional_dependency"
                        and candidate.candidate.columns == ("code",)
                        and candidate.candidate.dependent == "name"
                    )
                    else candidate.decision
                }
            )
            for candidate in artifact.candidates
        ]
        artifact = artifact.model_copy(update={"candidates": reviewed})
        constraints_path.write_text(dump_constraint_review_artifact(artifact), encoding="utf-8")

        result = runner.invoke(
            app,
            [
                "repair",
                str(csv_path),
                "--constraints",
                str(constraints_path),
                "--dry-run",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["receipt"]["accepted_constraint_ids"] == artifact.accepted_candidate_ids()
        assert (
            payload["receipt"]["constraints_artifact_sha256"]
            == hashlib.sha256(constraints_path.read_bytes()).hexdigest()
        )
        assert payload["fixes"][0]["detector_id"] == "fd_violation"

    def test_dry_run_returns_one_when_no_fixes_exist(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "clean.csv"
        csv_path.write_text("id,amount\n1,100\n2,101\n3,102\n4,103\n5,104\n", encoding="utf-8")

        result = runner.invoke(app, ["repair", str(csv_path), "--dry-run"])

        assert result.exit_code == 1
        assert "No fixes proposed" in result.output
        assert not (tmp_path / ".dataforge").exists()

    def test_apply_writes_transaction_before_mutating_source(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "data.csv"
        # The half-migrated one: the mutated bytes below were already switched to the
        # premised (state,city) shape while the fixture was left as the shifted table.
        schema_path = _write_premised_repairable(csv_path)
        original_bytes = csv_path.read_bytes()

        def fake_apply(path: Path, fixes: list[object]) -> str:
            log_files = list((tmp_path / ".dataforge" / "transactions").glob("*.jsonl"))
            assert len(log_files) == 1
            assert path.read_bytes() == original_bytes
            mutated = b"id,state,city\n1,MA,boston\n"
            path.write_bytes(mutated)
            return hashlib.sha256(mutated).hexdigest()

        with patch("dataforge.engine.repair._apply_fixes_to_csv", side_effect=fake_apply):
            result = runner.invoke(
                app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
            )

        assert result.exit_code == 0, result.output
        assert "Transaction ID" in result.output

    def test_apply_then_revert_round_trip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "data.csv"
        schema_path = _write_premised_repairable(csv_path)
        original_bytes = csv_path.read_bytes()

        apply_result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )
        txn_match = re.search(r"txn-\d{4}-\d{2}-\d{2}-[0-9a-f]{6}", apply_result.output)

        assert apply_result.exit_code == 0
        assert txn_match is not None
        assert csv_path.read_bytes() != original_bytes

        revert_result = runner.invoke(app, ["revert", txn_match.group(0)])

        assert revert_result.exit_code == 0
        assert csv_path.read_bytes() == original_bytes
        assert "restored" in revert_result.output.lower()

    def test_revert_search_root_json_works_outside_transaction_tree(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_dir = tmp_path / "data"
        outside_dir = tmp_path / "outside"
        data_dir.mkdir()
        outside_dir.mkdir()
        csv_path = data_dir / "data.csv"
        schema_path = _write_premised_repairable(csv_path)
        original_bytes = csv_path.read_bytes()

        apply_result = runner.invoke(
            app, ["repair", str(csv_path), "--apply", "--schema", str(schema_path)]
        )
        txn_match = re.search(r"txn-\d{4}-\d{2}-\d{2}-[0-9a-f]{6}", apply_result.output)
        assert apply_result.exit_code == 0
        assert txn_match is not None

        monkeypatch.chdir(outside_dir)
        revert_result = runner.invoke(
            app,
            [
                "revert",
                txn_match.group(0),
                "--search-root",
                str(data_dir),
                "--json",
            ],
        )

        assert revert_result.exit_code == 0
        payload = json.loads(revert_result.output)
        assert payload["schema_version"] == "repair_revert_receipt_v1"
        assert payload["ok"] is True
        assert payload["txn_id"] == txn_match.group(0)
        assert payload["audit_verdict"] == "verified"
        assert payload["restored_source_sha256"] == hashlib.sha256(original_bytes).hexdigest()
        assert payload["revert_event_sha256"]
        assert csv_path.read_bytes() == original_bytes

    def test_transaction_log_write_failure_leaves_source_untouched(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        original_bytes = csv_path.read_bytes()

        with patch(
            "dataforge.engine.repair.append_created_transaction", side_effect=OSError("disk full")
        ):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code != 0
        assert csv_path.read_bytes() == original_bytes

    def test_invalid_schema_exits_two(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        schema_path = tmp_path / "bad.yaml"
        _write_repairable_csv(csv_path)
        schema_path.write_text("- not-a-mapping\n", encoding="utf-8")

        result = runner.invoke(
            app,
            ["repair", str(csv_path), "--dry-run", "--schema", str(schema_path)],
        )

        assert result.exit_code == 2
        assert "YAML mapping" in result.output

    def test_safety_denial_exits_one(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)

        with patch(
            "dataforge.engine.repair.SafetyFilter.evaluate",
            return_value=SafetyResult(
                verdict=SafetyVerdict.DENY,
                reason="blocked by safety",
            ),
        ):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code == 1
        assert "blocked by safety" in result.output

    def test_verifier_reject_exits_one(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)

        with (
            patch(
                "dataforge.engine.repair.SafetyFilter.evaluate",
                return_value=SafetyResult(
                    verdict=SafetyVerdict.ALLOW,
                    reason="ok",
                ),
            ),
            patch(
                "dataforge.engine.repair.SMTVerifier.verify",
                return_value=VerificationResult(
                    verdict=VerificationVerdict.REJECT,
                    reason="verifier rejected",
                ),
            ),
        ):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code == 1
        assert "verifier rejected" in result.output

    def test_apply_failure_after_file_mutation_restores_source(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        original_bytes = csv_path.read_bytes()

        def fake_append_applied(*args: object, **kwargs: object) -> None:
            raise OSError("append failed")

        with patch("dataforge.engine.repair.append_applied_event", side_effect=fake_append_applied):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code == 1
        assert csv_path.read_bytes() == original_bytes

    def test_revert_not_applied_transaction_exits_one(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "data.csv"
        snapshot_path = tmp_path / ".dataforge" / "snapshots" / "txn.bin"
        csv_bytes = b"id,amount\n1,100\n"
        csv_path.write_bytes(csv_bytes)
        snapshot_path.parent.mkdir(parents=True)
        snapshot_path.write_bytes(csv_bytes)

        txn = RepairTransaction(
            txn_id="txn-2026-04-20-123abc",
            created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            source_path=str(csv_path.resolve()),
            source_sha256=hashlib.sha256(csv_bytes).hexdigest(),
            source_snapshot_path=str(snapshot_path.resolve()),
            fixes=[],
            applied=False,
        )
        append_created_transaction(txn)

        result = runner.invoke(app, ["revert", txn.txn_id])

        assert result.exit_code == 1
        assert "nothing to revert" in result.output.lower()

    def test_revert_missing_transaction_exits_two(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["revert", "txn-2026-04-20-ffffff"])

        assert result.exit_code == 2
        assert "could not find transaction" in result.output.lower()

    def test_batch_safety_failure_exits_one(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        accepted_fix = _proposed_fix()

        with (
            patch(
                "dataforge.engine.repair.propose_repairs",
                return_value=(
                    [accepted_fix],
                    [
                        [
                            RepairAttempt(
                                issue=_issue(),
                                attempt_number=1,
                                fix=accepted_fix,
                                status="accepted",
                                reason="ok",
                            )
                        ]
                    ],
                ),
            ),
            patch(
                "dataforge.engine.repair.SafetyFilter.evaluate_batch",
                return_value=SafetyResult(
                    verdict=SafetyVerdict.DENY,
                    reason="batch blocked",
                    rule_ids=("NO_CONFLICTING_CELL_WRITES",),
                ),
            ),
        ):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code == 1
        assert "batch blocked" in result.output

    def test_apply_reports_partial_success_summary(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        accepted_fix = _proposed_fix()
        failed_attempt = RepairAttempt(
            issue=_issue(issue_type="fd_violation", row=1, column="amount"),
            attempt_number=3,
            fix=accepted_fix,
            status="attempted_not_fixed",
            reason="Issue was attempted but not fixed after 3 attempt(s).",
            unsat_core=("fd::code::name::row::1",),
        )

        with (
            patch(
                "dataforge.engine.repair.propose_repairs",
                return_value=(
                    [accepted_fix],
                    [
                        [
                            RepairAttempt(
                                issue=_issue(),
                                attempt_number=1,
                                fix=accepted_fix,
                                status="accepted",
                                reason="ok",
                            )
                        ],
                        [failed_attempt],
                    ],
                ),
            ),
            patch(
                "dataforge.engine.repair.SafetyFilter.evaluate_batch",
                return_value=SafetyResult(verdict=SafetyVerdict.ALLOW, reason="ok"),
            ),
            patch(
                "dataforge.engine.repair.apply_transaction", return_value="txn-2026-04-21-abcdef"
            ),
        ):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code == 0
        assert "Week 3 Summary" in result.output
        assert "attempted but not fixed" in result.output.lower()


class _StaticRepairer:
    def __init__(self, proposed_fix: ProposedFix | None) -> None:
        self._proposed_fix = proposed_fix

    def propose(
        self,
        issue: Issue,
        df: pd.DataFrame,
        schema: Schema | None,
        retry_context: object | None = None,
    ) -> ProposedFix | None:
        del issue, df, schema, retry_context
        return self._proposed_fix


class TestRepairHelpers:
    """Direct coverage for Week 3 helper branches."""

    def test_propose_repairs_marks_missing_repairer(self, tmp_path: Path) -> None:
        path = tmp_path / "data.csv"
        path.write_text("amount\n1020\n", encoding="utf-8")

        with patch("dataforge.engine.repair.build_repairers", return_value={}):
            accepted, attempts = _propose_repairs(
                [_issue()],
                path,
                pd.DataFrame({"amount": ["1020"]}),
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
        assert "No repairer" in attempts[0][0].reason

    def test_propose_repairs_records_unconfirmed_escalation(self, tmp_path: Path) -> None:
        path = tmp_path / "data.csv"
        path.write_text("amount\n1020\n", encoding="utf-8")
        candidate = _proposed_fix(row=0)

        with (
            patch(
                "dataforge.engine.repair.build_repairers",
                return_value={"fd_violation": _StaticRepairer(candidate)},
            ),
            patch(
                "dataforge.engine.repair.SafetyFilter.evaluate",
                return_value=SafetyResult(
                    verdict=SafetyVerdict.ESCALATE,
                    reason="needs confirmation",
                    rule_ids=("NO_AGGREGATE_BREAK",),
                ),
            ),
        ):
            accepted, attempts = _propose_repairs(
                [_issue(row=0)],
                path,
                pd.DataFrame({"amount": ["1020"]}),
                None,
                allow_llm=False,
                model="gemini-2.0-flash",
                allow_pii=False,
                confirm_pii=False,
                confirm_escalations=False,
                interactive=False,
            )

        assert accepted == []
        assert attempts[0][0].status == "escalated"
        assert "needs confirmation" in attempts[0][0].reason

    def test_resolve_escalation_confirms_pii_and_rechecks(self) -> None:
        candidate = _proposed_fix(
            row=0, column="phone_number", old_value="bad", new_value="2175550101"
        )
        mock_filter = Mock()
        mock_filter.evaluate.return_value = SafetyResult(
            verdict=SafetyVerdict.ALLOW,
            reason="confirmed",
        )

        with patch("dataforge.cli.repair.typer.confirm", return_value=True):
            updated_context, updated_result = _resolve_escalation(
                candidate,
                None,
                SafetyContext(allow_pii=True),
                mock_filter,
                SafetyResult(
                    verdict=SafetyVerdict.ESCALATE,
                    reason="requires confirmation",
                    rule_ids=("NO_PII_OVERWRITE",),
                ),
            )

        assert updated_context.confirm_pii is True
        assert updated_result.verdict == SafetyVerdict.ALLOW

    def test_resolve_escalation_keeps_result_when_aggregate_not_confirmed(self) -> None:
        candidate = _proposed_fix(row=0)
        mock_filter = Mock()
        original_result = SafetyResult(
            verdict=SafetyVerdict.ESCALATE,
            reason="aggregate sensitive",
            rule_ids=("NO_AGGREGATE_BREAK",),
        )

        with patch("dataforge.cli.repair.typer.confirm", return_value=False):
            updated_context, updated_result = _resolve_escalation(
                candidate,
                None,
                SafetyContext(),
                mock_filter,
                original_result,
            )

        assert updated_context == SafetyContext()
        assert updated_result == original_result
        mock_filter.evaluate.assert_not_called()

    def test_render_attempt_summary_includes_domain_prefix(self) -> None:
        console = Console(record=True, width=120)
        failed_attempt = RepairAttempt(
            issue=_issue(row=0),
            attempt_number=3,
            status="attempted_not_fixed",
            reason="Issue was attempted but not fixed after 3 attempt(s).",
            unsat_core=("domain::amount::min::row::0",),
        )

        count = _render_attempt_summary([[failed_attempt]], console)

        assert count == 1
        assert "domain bound rejection" in console.export_text().lower()
