"""Tests for the corrector auto-apply promotion release gate.

The gate is wired to the REAL verdict source (corrector_promotion_verdict over
committed benchmark records), not a hand-authored field.
"""

from __future__ import annotations

import json
from pathlib import Path

from dataforge.release.corrector_gate import check_corrector_release_gate

_REAL_RESULTS = Path(__file__).resolve().parents[2] / "eval" / "results"
_REAL_RECORD = _REAL_RESULTS / "corrector_gpt5mini_hospital.json"


def _policy(path: Path, thresholds: dict[str, float]) -> Path:
    path.write_text(json.dumps({"policy": {"auto_apply_thresholds": thresholds}}), encoding="utf-8")
    return path


def _corrector_record(path: Path, *, precision: float, ece: float, count: int) -> None:
    """Write a schema-valid corrector benchmark record with overridden metrics."""
    payload = json.loads(_REAL_RECORD.read_text(encoding="utf-8"))
    entry = payload["records"][0]
    entry["precision_at_auto_apply"] = precision
    entry["ece"] = ece
    entry["auto_apply_count"] = count
    path.write_text(json.dumps({"records": [entry]}), encoding="utf-8")


def test_committed_artifact_promotes_nothing() -> None:
    """The real committed artifact must auto-apply zero LLM classes."""
    result = check_corrector_release_gate()
    assert result.passed, result.reason
    assert result.enabled_classes == []


def test_disabled_sentinels_pass(tmp_path: Path) -> None:
    artifact = _policy(tmp_path / "a.json", {"fd_violation": 1.01, "format_violation": 1.01})
    result = check_corrector_release_gate(artifact, results_dir=tmp_path)
    assert result.passed
    assert result.enabled_classes == []


def test_enabled_class_without_passing_measurement_fails(tmp_path: Path) -> None:
    artifact = _policy(tmp_path / "a.json", {"fd_violation": 0.9})
    # results_dir has no records at all -> nothing clears the bar.
    result = check_corrector_release_gate(artifact, results_dir=tmp_path)
    assert not result.passed
    assert "fd_violation" in result.reason


def test_real_committed_records_cannot_unlock_auto_apply(tmp_path: Path) -> None:
    """Enabling a class while only the real (rejecting) records exist must FAIL."""
    artifact = _policy(tmp_path / "a.json", {"fd_violation": 0.9})
    result = check_corrector_release_gate(artifact, results_dir=_REAL_RESULTS)
    assert not result.passed, "gemini 0.16 / gpt5mini 0.077 must not unlock auto-apply"


def test_enabled_class_with_passing_measurement_passes(tmp_path: Path) -> None:
    artifact = _policy(tmp_path / "a.json", {"fd_violation": 0.9})
    _corrector_record(tmp_path / "corrector_synthetic.json", precision=0.97, ece=0.05, count=10)
    result = check_corrector_release_gate(artifact, results_dir=tmp_path)
    assert result.passed, result.reason
    assert result.enabled_classes == ["fd_violation"]
    assert result.passing_measurements


def test_weak_measurement_does_not_pass(tmp_path: Path) -> None:
    artifact = _policy(tmp_path / "a.json", {"fd_violation": 0.9})
    _corrector_record(tmp_path / "corrector_synthetic.json", precision=0.5, ece=0.4, count=10)
    result = check_corrector_release_gate(artifact, results_dir=tmp_path)
    assert not result.passed


def test_missing_artifact_passes(tmp_path: Path) -> None:
    result = check_corrector_release_gate(tmp_path / "nope.json", results_dir=tmp_path)
    assert result.passed


def test_malformed_artifact_fails_closed(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    result = check_corrector_release_gate(bad, results_dir=tmp_path)
    assert not result.passed
