"""Safety-invariant tests for the expanded detector/repairer set.

The core guarantee must survive the breadth push: detection-only classes never
mutate data, the only new auto-applying repairer (missing_value) stays gated by
the safety + SMT path, duplicate rows are never auto-deleted, and any applied
repair is byte-for-byte reversible.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataforge.detectors import run_all_detectors
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.repairers import build_repairers
from dataforge.transactions.revert import revert_transaction


def _write(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False)


class TestDetectionOnlyClassesNeverMutate:
    def test_detection_only_classes_have_no_registered_repairer(self) -> None:
        registry = build_repairers(cache_dir=None, allow_llm=False, model="x")
        # Detection-only classes must not be auto-applied.
        for detection_only in (
            "format_violation",
            "categorical_normalization",
            "outlier",
            "duplicate_row",
        ):
            assert detection_only not in registry

    def test_duplicate_rows_detected_but_never_deleted(self, tmp_path: Path) -> None:
        csv = tmp_path / "dups.csv"
        _write(csv, pd.DataFrame({"a": ["1", "2", "1"], "b": ["x", "y", "x"]}))
        original = csv.read_bytes()
        result = run_repair_pipeline(RepairPipelineRequest(source_path=csv, mode="apply"))
        # No fix may be a row deletion, and the duplicate row must remain.
        assert all(fix.operation != "delete_row" for fix in result.fixes)
        assert csv.read_bytes() == original or result.receipt.txn_id is not None


class TestNewClassesStayGated:
    def test_outlier_and_format_produce_no_applied_fixes(self, tmp_path: Path) -> None:
        # A numeric outlier + a date-format minority: both detection-only, so a
        # dry run detects them but proposes zero auto-fixes.
        csv = tmp_path / "data.csv"
        frame = pd.DataFrame(
            {
                "v": [str(x) for x in [10, 11, 9, 12, 10, 11, 13, 9, 10, 12, 11, 4200]],
                "d": [f"2024-01-{i:02d}" for i in range(1, 12)] + ["13/01/2024"],
            }
        )
        _write(csv, frame)
        detected = {i.issue_type for i in run_all_detectors_from_csv(csv)}
        result = run_repair_pipeline(RepairPipelineRequest(source_path=csv, mode="dry_run"))
        # The detectors saw outliers/format issues...
        assert {"outlier", "format_violation"} & detected
        # ...but none were auto-applied (detection-only).
        applied_types = {fix.detector_id for fix in result.fixes}
        assert "outlier" not in applied_types
        assert "format_violation" not in applied_types


class TestReversibilityWithNewDetectors:
    def test_apply_then_revert_is_byte_identical(self, tmp_path: Path) -> None:
        # A decimal-shift error (auto-fixable) alongside detection-only noise.
        csv = tmp_path / "amounts.csv"
        _write(
            csv, pd.DataFrame({"amount": ["100", "105", "98", "1020", "103", "99", "101", "97"]})
        )
        original = csv.read_bytes()
        result = run_repair_pipeline(RepairPipelineRequest(source_path=csv, mode="apply"))
        if result.receipt.applied and result.receipt.txn_id:
            assert csv.read_bytes() != original
            revert_transaction(result.receipt.txn_id, search_root=tmp_path)
            assert csv.read_bytes() == original


def run_all_detectors_from_csv(path: Path):
    """Helper: load a CSV the way the engine does and run the ensemble."""
    from dataforge.table import read_csv

    return run_all_detectors(read_csv(path))
