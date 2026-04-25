"""CLI tests for Week 2 repair and revert commands."""

from __future__ import annotations

import hashlib
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
    apply_fixes_to_csv,
)
from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.repairers.base import ProposedFix, RepairAttempt
from dataforge.safety import SafetyContext, SafetyResult, SafetyVerdict
from dataforge.transactions.log import append_created_transaction
from dataforge.transactions.txn import CellFix, RepairTransaction
from dataforge.verifier import VerificationResult, VerificationVerdict

runner = CliRunner()


def _write_repairable_csv(path: Path) -> None:
    """Write a small CSV with a deterministic decimal-shift issue."""
    path.write_text(
        "id,amount\n1,100\n2,105\n3,98\n4,1020\n5,103\n",
        encoding="utf-8",
    )


def _issue(*, issue_type: str = "decimal_shift", row: int = 3, column: str = "amount") -> Issue:
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
    detector_id: str = "decimal_shift",
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
            apply_fixes_to_csv(
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
            apply_fixes_to_csv(
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
            apply_fixes_to_csv(
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
            apply_fixes_to_csv(
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
        _write_repairable_csv(csv_path)

        result = runner.invoke(app, ["repair", str(csv_path), "--dry-run"])

        assert result.exit_code == 0
        assert "Proposed Repairs" in result.output
        assert "decimal_shift" in result.output
        assert not (tmp_path / ".dataforge").exists()

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
        _write_repairable_csv(csv_path)
        original_bytes = csv_path.read_bytes()

        def fake_apply(path: Path, fixes: list[object]) -> str:
            log_files = list((tmp_path / ".dataforge" / "transactions").glob("*.jsonl"))
            assert len(log_files) == 1
            assert path.read_bytes() == original_bytes
            mutated = b"id,amount\n1,100\n2,105\n3,98\n4,102\n5,103\n"
            path.write_bytes(mutated)
            return hashlib.sha256(mutated).hexdigest()

        with patch("dataforge.cli.repair.apply_fixes_to_csv", side_effect=fake_apply):
            result = runner.invoke(app, ["repair", str(csv_path), "--apply"])

        assert result.exit_code == 0
        assert "Transaction ID" in result.output

    def test_apply_then_revert_round_trip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        original_bytes = csv_path.read_bytes()

        apply_result = runner.invoke(app, ["repair", str(csv_path), "--apply"])
        txn_match = re.search(r"txn-\d{4}-\d{2}-\d{2}-[0-9a-f]{6}", apply_result.output)

        assert apply_result.exit_code == 0
        assert txn_match is not None
        assert csv_path.read_bytes() != original_bytes

        revert_result = runner.invoke(app, ["revert", txn_match.group(0)])

        assert revert_result.exit_code == 0
        assert csv_path.read_bytes() == original_bytes
        assert "restored" in revert_result.output.lower()

    def test_transaction_log_write_failure_leaves_source_untouched(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_repairable_csv(csv_path)
        original_bytes = csv_path.read_bytes()

        with patch(
            "dataforge.cli.repair.append_created_transaction", side_effect=OSError("disk full")
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
            "dataforge.cli.repair.SafetyFilter.evaluate",
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
                "dataforge.cli.repair.SafetyFilter.evaluate",
                return_value=SafetyResult(
                    verdict=SafetyVerdict.ALLOW,
                    reason="ok",
                ),
            ),
            patch(
                "dataforge.cli.repair.SMTVerifier.verify",
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

        with patch("dataforge.cli.repair.append_applied_event", side_effect=fake_append_applied):
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
                "dataforge.cli.repair._propose_repairs",
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
                "dataforge.cli.repair.SafetyFilter.evaluate_batch",
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
                "dataforge.cli.repair._propose_repairs",
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
                "dataforge.cli.repair.SafetyFilter.evaluate_batch",
                return_value=SafetyResult(verdict=SafetyVerdict.ALLOW, reason="ok"),
            ),
            patch("dataforge.cli.repair._apply_transaction", return_value="txn-2026-04-21-abcdef"),
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

        with patch("dataforge.cli.repair.build_repairers", return_value={}):
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
                "dataforge.cli.repair.build_repairers",
                return_value={"decimal_shift": _StaticRepairer(candidate)},
            ),
            patch(
                "dataforge.cli.repair.SafetyFilter.evaluate",
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
