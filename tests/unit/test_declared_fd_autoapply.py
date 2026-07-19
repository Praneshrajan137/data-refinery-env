"""Tests for the declared-FD-only auto-apply opt-in (constraint circularity, option B).

By default, an accepted (reviewed) inferred FD is authoritative and its correction
auto-applies. Under ``require_declared_fds_for_autoapply=True``, a correction
justified only by an inferred FD is held for review, because an approximate
inferred FD can be coincidental. A HAND-DECLARED FD still auto-applies.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from dataforge.detectors.base import FunctionalDependency, Schema
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.schema_inference import (
    build_constraint_review_artifact,
    infer_schema,
    update_constraint_review_artifact,
)
from dataforge.table import read_csv

# 10 rows: zip -> city holds at 0.9 (one violation: the SF cell should be NY).
_CSV = (
    "zip,city\n"
    "10001,NY\n10001,NY\n10001,NY\n10001,NY\n10001,SF\n"
    "90210,LA\n90210,LA\n90210,LA\n90210,LA\n90210,LA\n"
)


def _accepted_fd_artifact(source: Path):
    df = read_csv(source)
    inference = infer_schema(df)
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    artifact = build_constraint_review_artifact(inference, source_path=source, source_sha256=sha)
    fd_ids = [
        r.candidate_id
        for r in artifact.candidates
        if r.candidate.kind == "functional_dependency" and r.candidate.dependent == "city"
    ]
    assert fd_ids, "expected an inferred zip->city FD candidate to survive mining"
    return update_constraint_review_artifact(artifact, accept_ids=fd_ids)


def _write(tmp_path: Path) -> Path:
    source = tmp_path / "z.csv"
    source.write_text(_CSV, encoding="utf-8")
    return source


def test_accepted_inferred_fd_auto_applies_by_default(tmp_path: Path) -> None:
    source = _write(tmp_path)
    artifact = _accepted_fd_artifact(source)
    result = run_repair_pipeline(
        RepairPipelineRequest(source_path=source, mode="apply", constraints=artifact)
    )
    assert result.receipt.applied is True
    assert "SF" not in source.read_text(encoding="utf-8"), "the inferred-FD fix should have applied"


def test_strict_mode_holds_inferred_fd_correction(tmp_path: Path) -> None:
    source = _write(tmp_path)
    artifact = _accepted_fd_artifact(source)
    before = source.read_text(encoding="utf-8")
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="apply",
            constraints=artifact,
            require_declared_fds_for_autoapply=True,
        )
    )
    # The inferred-FD correction is held, not applied.
    assert source.read_text(encoding="utf-8") == before, "strict mode must not apply an inferred FD"
    reasons = {s.review_reason for s in result.receipt.suggested_fixes}
    assert "inferred_fd_not_declared" in reasons, reasons


def test_strict_mode_still_applies_a_declared_fd(tmp_path: Path) -> None:
    source = _write(tmp_path)
    declared = Schema(
        columns={"zip": "str", "city": "str"},
        functional_dependencies=(FunctionalDependency(determinant=("zip",), dependent="city"),),
    )
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="apply",
            schema=declared,
            require_declared_fds_for_autoapply=True,
        )
    )
    assert result.receipt.applied is True
    assert "SF" not in source.read_text(encoding="utf-8"), "a declared FD must still auto-apply"
