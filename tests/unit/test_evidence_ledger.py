"""Tests for the canonical evidence ledger."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.evidence.evidence_ledger import summarize_ledger, validate_ledger


def test_evidence_ledger_is_valid() -> None:
    assert validate_ledger() == []


def test_evidence_ledger_records_sft_v8_as_failed_non_public_evidence() -> None:
    summary = summarize_ledger()

    assert summary["schema_version"] == "dataforge_evidence_ledger_v1"
    assert summary["status_counts"]["failed_diagnostic"] >= 2
    assert "model_05b_sft_v8_failed_smoke" in summary["non_release_entries"]
    assert "model_05b_grpo_v4_blocked" in summary["non_release_entries"]


def test_evidence_ledger_rejects_public_claim_for_failed_candidate(tmp_path: Path) -> None:
    ledger = {
        "schema_version": "dataforge_evidence_ledger_v1",
        "north_star": "test",
        "entries": [
            {
                "id": "bad_model",
                "surface": "models",
                "status": "failed_diagnostic",
                "claim": "bad",
                "claim_policy": "bad",
                "public_claim_allowed": True,
                "evidence_paths": ["README.md"],
                "blockers": [],
            }
        ],
    }
    path = tmp_path / "ledger.json"
    path.write_text(json.dumps(ledger), encoding="utf-8")

    errors = validate_ledger(path)

    assert any("cannot allow public claims" in error for error in errors)


def test_evidence_ledger_rejects_missing_evidence_path(tmp_path: Path) -> None:
    ledger = {
        "schema_version": "dataforge_evidence_ledger_v1",
        "north_star": "test",
        "entries": [
            {
                "id": "missing_path",
                "surface": "release",
                "status": "shipped",
                "claim": "bad",
                "claim_policy": "bad",
                "public_claim_allowed": True,
                "evidence_paths": ["does/not/exist.json"],
                "blockers": [],
            }
        ],
    }
    path = tmp_path / "ledger.json"
    path.write_text(json.dumps(ledger), encoding="utf-8")

    errors = validate_ledger(path)

    assert any("is missing" in error for error in errors)
