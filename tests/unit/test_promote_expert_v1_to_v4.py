"""Unit tests for the expert_v1 -> expert_v4 teacher-trajectory adapter."""

from __future__ import annotations

from typing import Any

import pytest

import scripts.data.promote_expert_v1_to_v4 as adapter
from dataforge.repair_contract import CONTRACT_VERSION_V2


def _record(*, fix: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "trajectory_id": "t1",
        "task_id": "hospital:easy",
        "schema_version": "expert_v1",
        "dataset": "hospital",
        "difficulty": "easy",
        "seed": 0,
        "chunk_index": 0,
        "teacher": {"provider": "azure", "model": "gpt-5.6-sol"},
        "diagnosis": [item.get("reason", "") for item in fix] or ["finish"],
        "tool_calls": [],
        "messages": [{"role": "user", "content": "{}"}],
        "metrics": {"episode_f1": 1.0},
        "state": {
            "schema_summary": {"columns": ["ProviderNumber", "HospitalName"]},
            "target_rows": [{"_row": "3", "ProviderNumber": "10018", "HospitalName": "rgnl"}],
            "context_rows": [{"_row": "4", "ProviderNumber": "10018", "HospitalName": "good"}],
        },
        "fix": fix,
        "provenance": {"collection_method": "llm_react_chunk", "citation": "raha"},
    }


class TestPureHelpers:
    def test_row_indices_reads_absolute_row_ids(self) -> None:
        rows = [{"_row": "3", "c": "v"}, {"_row": "4", "c": "v"}]
        assert adapter._row_indices(rows) == (3, 4)

    def test_repairs_from_fix_defaults_reason(self) -> None:
        repairs = adapter._repairs_from_fix([{"row": 3, "column": "C", "new_value": "v"}])
        assert repairs[0].reason == "repair proposal"


class TestPromoteRecord:
    def test_empty_fix_becomes_finish_abstention(self) -> None:
        # No dataset load happens on the finish path.
        promoted = adapter.promote_record(_record(fix=[]), datasets=None)  # type: ignore[arg-type]
        assert promoted["schema_version"] == "expert_v4"
        assert promoted["prompt_contract_version"] == CONTRACT_VERSION_V2
        assert promoted["inferability"] == "not_inferable_from_prompt"
        assert promoted["fix"] == []
        assert promoted["provenance"]["label_source"] == "teacher_react_verified"
        assert promoted["provenance"]["base_collection_method"] == "llm_react_chunk"
        assert promoted["provenance"]["teacher_verified"] is True

    def test_external_reference_repair_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            adapter, "inferability_for_record", lambda **_: "external_reference_required"
        )
        monkeypatch.setattr(adapter._DatasetCache, "get", lambda self, name: object())
        record = _record(
            fix=[{"row": 3, "column": "HospitalName", "new_value": "good", "reason": "guess"}]
        )
        promoted = adapter.promote_record(
            record, datasets=adapter._DatasetCache(verify_hashes=False)
        )
        assert promoted["inferability"] == "external_reference_required"
        assert promoted["fix"] == []  # coupling: abstention slices carry no repair
        assert "abstain" in promoted["diagnosis"][0]

    def test_deterministic_repair_keeps_teacher_rationale(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            adapter, "inferability_for_record", lambda **_: "deterministic_normalization"
        )
        monkeypatch.setattr(adapter._DatasetCache, "get", lambda self, name: object())
        record = _record(
            fix=[
                {
                    "row": 3,
                    "column": "HospitalName",
                    "new_value": "good",
                    "reason": "matches same provider",
                }
            ]
        )
        promoted = adapter.promote_record(
            record, datasets=adapter._DatasetCache(verify_hashes=False)
        )
        assert promoted["inferability"] == "deterministic_normalization"
        assert len(promoted["fix"]) == 1
        assert promoted["diagnosis"] == ["matches same provider"]


class TestPromoteRecords:
    def test_batch_report_counts_labels(self) -> None:
        records = [_record(fix=[]), _record(fix=[])]
        promoted, report = promote_records_finish_only(records)
        assert report["promoted_records"] == 2
        assert report["abstention_records"] == 2
        assert report["inferability_distribution"] == {"not_inferable_from_prompt": 2}
        assert report["failures"] == []
        assert len(promoted) == 2


def promote_records_finish_only(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Any]:
    """Promote finish-only records (no dataset load needed)."""
    return adapter.promote_records(records, verify_hashes=False)
