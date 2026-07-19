"""Tests for the corrector auto-apply promotion release gate."""

from __future__ import annotations

import json
from pathlib import Path

from dataforge.release.corrector_gate import check_corrector_release_gate


def _write_artifact(path: Path, thresholds: dict[str, float], **extra: object) -> Path:
    payload: dict[str, object] = {"policy": {"auto_apply_thresholds": thresholds}}
    payload.update(extra)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_committed_artifact_promotes_nothing() -> None:
    """The real committed artifact must auto-apply zero LLM classes."""
    result = check_corrector_release_gate()
    assert result.passed, result.reason
    assert result.enabled_classes == []


def test_disabled_sentinels_pass(tmp_path: Path) -> None:
    artifact = _write_artifact(
        tmp_path / "a.json", {"fd_violation": 1.01, "format_violation": 1.01}
    )
    result = check_corrector_release_gate(artifact)
    assert result.passed
    assert result.enabled_classes == []


def test_enabled_class_without_evidence_fails(tmp_path: Path) -> None:
    artifact = _write_artifact(tmp_path / "a.json", {"fd_violation": 0.9})
    result = check_corrector_release_gate(artifact)
    assert not result.passed
    assert "fd_violation" in result.reason


def test_enabled_class_with_weak_evidence_fails(tmp_path: Path) -> None:
    artifact = _write_artifact(
        tmp_path / "a.json",
        {"fd_violation": 0.9},
        promotion_evidence={"fd_violation": {"precision_at_auto_apply": 0.5, "ece": 0.4}},
    )
    result = check_corrector_release_gate(artifact)
    assert not result.passed


def test_enabled_class_with_sufficient_evidence_passes(tmp_path: Path) -> None:
    artifact = _write_artifact(
        tmp_path / "a.json",
        {"fd_violation": 0.9},
        promotion_evidence={"fd_violation": {"precision_at_auto_apply": 0.97, "ece": 0.05}},
    )
    result = check_corrector_release_gate(artifact)
    assert result.passed, result.reason
    assert result.enabled_classes == ["fd_violation"]


def test_missing_artifact_passes(tmp_path: Path) -> None:
    result = check_corrector_release_gate(tmp_path / "does_not_exist.json")
    assert result.passed


def test_malformed_artifact_fails_closed(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    result = check_corrector_release_gate(bad)
    assert not result.passed
