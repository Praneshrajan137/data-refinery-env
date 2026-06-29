"""Tests for the frozen SFT-v8 smoke evidence."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT = (
    ROOT / "eval" / "results" / "kaggle_sft_v8_diagnostic_v1" / "sft_v8_candidate_eval_report.json"
)
POSTMORTEM = ROOT / "eval" / "results" / "sft_v8_smoke_v1_postmortem.json"


def _load(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_sft_v8_smoke_is_failed_diagnostic_not_grpo_predecessor() -> None:
    report = _load(REPORT)
    metrics = report["metrics"]
    training_metrics = report["training_metrics"]
    assert isinstance(metrics, dict)
    assert isinstance(training_metrics, dict)

    assert report["status"] == "quality_gate_failed_no_upload"
    assert report["ok"] is False
    assert report["promote_to_grpo"] is False
    assert report["model_uploaded"] is False
    assert metrics["strict_macro_f1"] == 0.0
    assert metrics["parse_success_rate"] == 0.03
    assert metrics["schema_case_error_count"] == 26
    assert training_metrics["training_stage"] == "smoke"
    assert training_metrics["label_mask_audit"]["ok"] is True  # type: ignore[index]


def test_sft_v8_evidence_keeps_raw_and_constrained_tracks_separate() -> None:
    report = _load(REPORT)
    training_metrics = report["training_metrics"]
    assert isinstance(training_metrics, dict)
    tracks = training_metrics["evaluation_tracks"]
    assert isinstance(tracks, dict)

    raw = tracks["raw_research"]
    product = tracks["product_constrained"]
    assert isinstance(raw, dict)
    assert isinstance(product, dict)

    assert raw["enabled"] is True
    assert raw["decoding"] == "unconstrained_greedy"
    assert raw["claim_policy"] == "research evidence only"
    assert product["enabled"] is True
    assert product["decoding"] == "json_schema_or_grammar_constrained"
    assert product["status"] == "schema_metadata_recorded_decoder_not_run_in_sft_candidate"


def test_sft_v8_postmortem_blocks_continuation() -> None:
    postmortem = _load(POSTMORTEM)

    assert postmortem["status"] == "failed_diagnostic_evidence"
    assert postmortem["decision"] == "block_diagnostic_candidate_and_grpo_v4"
    assert "Do not run GRPO-v4" in " ".join(postmortem["next_action"])  # type: ignore[arg-type]
