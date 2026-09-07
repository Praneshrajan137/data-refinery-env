"""Tests for declared-vs-mined FD write authority (C4).

**This file previously specified the opposite default, and the inversion is deliberate.**
Until 2026-09-07 an accepted (reviewed) MINED FD was authoritative and its correction
auto-applied; holding it required the ``require_declared_fds_for_autoapply`` opt-in. The
default is now reversed: a mined FD never confers write authority, and restoring the old
behaviour requires ``mined_constraints_grant_write_authority=True``.

The evidence is in ``docs/trust/premise-acquisition-result.md``. Across ten externally
annotated tables, the best of four in-table measures discards 16 of 143 hand-annotated true
dependencies when its threshold is carried to a table it was not fitted on, so no confidence
floor can rescue a mined premise. On hospital the mined premise produced 451 repairs with 116
clean-cell corruptions; the declared premise produced 393 with none.

A HAND-DECLARED FD still auto-applies, unchanged. That is K2 in
``eval/preregistration/premise_acquisition.md``: if the declared arm moves, C4 is withdrawn.
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


def test_an_accepted_mined_fd_does_not_auto_apply_by_default(tmp_path: Path) -> None:
    """The inversion. Accepting a mined candidate is not declaring a constraint."""
    source = _write(tmp_path)
    artifact = _accepted_fd_artifact(source)
    before = source.read_text(encoding="utf-8")

    result = run_repair_pipeline(
        RepairPipelineRequest(source_path=source, mode="apply", constraints=artifact)
    )

    assert source.read_text(encoding="utf-8") == before, (
        "a mined FD accepted in review must not authorise a write on its own"
    )
    reasons = {s.review_reason for s in result.receipt.suggested_fixes}
    assert "mined_constraint_not_declared" in reasons, reasons


def test_restoring_mined_authority_restores_the_old_behaviour(tmp_path: Path) -> None:
    """The opt-out must work, or the default is untestable and the change unfalsifiable."""
    source = _write(tmp_path)
    artifact = _accepted_fd_artifact(source)

    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="apply",
            constraints=artifact,
            mined_constraints_grant_write_authority=True,
        )
    )

    assert result.receipt.applied is True
    assert "SF" not in source.read_text(encoding="utf-8"), (
        "with mined authority restored, the pre-2026-09-07 write must reappear"
    )


def test_strict_mode_holds_inferred_fd_correction(tmp_path: Path) -> None:
    """The older opt-in still works, and still names its own reason.

    Retained rather than deleted: it is a different control from C4 (it fires even when
    mined authority is granted), and removing a guard because another change made it
    redundant is how guards get lost.
    """
    source = _write(tmp_path)
    artifact = _accepted_fd_artifact(source)
    before = source.read_text(encoding="utf-8")
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="apply",
            constraints=artifact,
            require_declared_fds_for_autoapply=True,
            mined_constraints_grant_write_authority=True,
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
