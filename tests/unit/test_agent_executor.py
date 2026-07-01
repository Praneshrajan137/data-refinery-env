"""Unit tests for the verified action executor gate."""

from __future__ import annotations

from pathlib import Path

from dataforge.agent.executor import VerifiedActionExecutor
from dataforge.agent.tool_actions import parse_action
from dataforge.cli.common import load_schema
from dataforge.safety import SafetyContext
from dataforge.table import cell_value, read_csv


def _table(tmp_path: Path, content: str) -> object:
    path = tmp_path / "data.csv"
    path.write_text(content, encoding="utf-8")
    return read_csv(path)


def _fix(row: int, column: str, value: str, fix_type: str = "correct_value"):
    return parse_action(
        {
            "action_type": "FIX",
            "row": row,
            "column": column,
            "new_value": value,
            "justification": "test",
            "fix_type": fix_type,
        }
    )


class TestVerifiedFixGate:
    def test_accepts_safe_update_without_schema(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,20\n3,abc\n")
        ex = VerifiedActionExecutor(df, None, provenance="deterministic")
        outcome = ex.execute(_fix(2, "score", "30"))
        assert outcome.accepted is True
        assert ex.staged_fixes and ex.staged_fixes[0].fix.new_value == "30"
        assert cell_value(df, 2, "score") == "30"

    def test_denies_row_delete(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,20\n")
        ex = VerifiedActionExecutor(df, None, provenance="deterministic")
        outcome = ex.execute(_fix(1, "score", "x", fix_type="delete_row"))
        assert outcome.accepted is False
        assert "NO_ROW_DELETE" in (outcome.rejection_reason or "")
        assert ex.staged_fixes == []

    def test_rejects_out_of_bounds_with_unsat_core(self, tmp_path: Path) -> None:
        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text(
            "columns:\n  id: str\n  score: float\ndomain_bounds:\n  score:\n    min: 0\n    max: 100\n",
            encoding="utf-8",
        )
        schema = load_schema(schema_path)
        df = _table(tmp_path, "id,score\n1,10\n2,20\n3,30\n")
        ex = VerifiedActionExecutor(df, schema, provenance="deterministic")
        outcome = ex.execute(_fix(2, "score", "9999"))
        assert outcome.accepted is False
        # Out-of-range value must not be written.
        assert cell_value(df, 2, "score") == "30"
        assert ex.staged_fixes == []

    def test_llm_write_escalates_without_confirmation(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,20\n3,abc\n")
        ex = VerifiedActionExecutor(df, None, provenance="llm_live")
        outcome = ex.execute(_fix(2, "score", "30"))
        assert outcome.accepted is False
        assert ex.staged_fixes == []

    def test_llm_write_accepts_with_confirmation(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,20\n3,abc\n")
        ex = VerifiedActionExecutor(
            df, None, provenance="llm_live", safety_context=SafetyContext(confirm_escalations=True)
        )
        outcome = ex.execute(_fix(2, "score", "30"))
        assert outcome.accepted is True

    def test_rejects_unknown_column_and_oob_row(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n")
        ex = VerifiedActionExecutor(df, None, provenance="deterministic")
        assert ex.execute(_fix(0, "missing", "x")).accepted is False
        assert ex.execute(_fix(99, "score", "x")).accepted is False

    def test_resolved_cell_cannot_be_refixed(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,20\n")
        ex = VerifiedActionExecutor(df, None, provenance="deterministic")
        ex.mark_resolved(1, "score")
        outcome = ex.execute(_fix(1, "score", "30"))
        assert outcome.accepted is False
        assert "already" in (outcome.rejection_reason or "").lower()


class TestReadOnlyTools:
    def test_inspect_rows(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,20\n3,30\n")
        ex = VerifiedActionExecutor(df, None)
        outcome = ex.execute(parse_action({"action_type": "INSPECT_ROWS", "row_indices": [0, 2]}))
        assert outcome.action_type == "INSPECT_ROWS"
        assert "score" in outcome.feedback

    def test_pattern_match_finds_non_numeric(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,abc\n3,30\n")
        ex = VerifiedActionExecutor(df, None)
        outcome = ex.execute(
            parse_action(
                {
                    "action_type": "PATTERN_MATCH",
                    "pattern": r"^\d+$",
                    "column": "score",
                    "expect_match": False,
                }
            )
        )
        assert "1" in outcome.feedback  # row index 1 is non-numeric

    def test_stat_test_flags_outlier(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n2,11\n3,12\n4,9\n5,9999\n")
        ex = VerifiedActionExecutor(df, None)
        outcome = ex.execute(
            parse_action({"action_type": "STAT_TEST", "test_type": "zscore", "column": "score"})
        )
        assert outcome.action_type == "STAT_TEST"

    def test_hypothesis_and_diagnose_update_scratchpad(self, tmp_path: Path) -> None:
        df = _table(tmp_path, "id,score\n1,10\n")
        ex = VerifiedActionExecutor(df, None)
        ex.execute(
            parse_action(
                {
                    "action_type": "HYPOTHESIS",
                    "claim": "decimal shift",
                    "affected_rows": [0],
                    "affected_columns": ["score"],
                    "root_cause_type": "decimal_shift",
                }
            )
        )
        ex.execute(
            parse_action(
                {
                    "action_type": "DIAGNOSE",
                    "row": 0,
                    "column": "score",
                    "issue_type": "type_mismatch",
                }
            )
        )
        assert ex.scratchpad.hypotheses
        assert ex.scratchpad.confirmed_issues
