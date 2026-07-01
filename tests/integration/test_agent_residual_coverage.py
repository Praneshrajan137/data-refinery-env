"""Verify the verified agent and residual curriculum see the expanded residual.

The expanded detectors surface detection-only classes (format_violation,
outlier, etc.). Those have no auto-applying repairer, so they flow through as
the *residual* the verified agent is meant to work - and they appear in the
residual curriculum. No new wiring is needed; this test pins that behavior.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from dataforge.agent import AgentRepairRequest, run_agent_repair

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.data.build_residual_curriculum import build_records  # noqa: E402


def _write(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False)


class TestAgentSeesExpandedResidual:
    def test_detection_only_issues_become_agent_residual(self, tmp_path: Path) -> None:
        # A robust numeric outlier is detection-only -> unrepaired by the floor ->
        # it must show up as residual the agent is handed.
        csv = tmp_path / "data.csv"
        _write(
            csv,
            pd.DataFrame(
                {"v": [str(x) for x in [10, 11, 9, 12, 10, 11, 13, 9, 10, 12, 11, 4200]]}
            ),
        )
        result = run_agent_repair(
            AgentRepairRequest(source_path=csv, mode="dry_run", policy="deterministic")
        )
        # Deterministic policy adds nothing, but the residual must include the
        # detection-only outlier (the agent's opportunity surface).
        assert result.residual_count >= 1


class TestResidualCurriculumCoversNewClasses:
    def test_curriculum_emits_new_class_residual(self, tmp_path: Path) -> None:
        # Missing value the floor cannot fill (no FD) -> residual -> curriculum
        # trajectory teaching the oracle value.
        dirty = tmp_path / "dirty.csv"
        clean = tmp_path / "clean.csv"
        city = ["NY", "LA", "", "SF", "BOS", "DC", "LA", "NY"]
        clean_city = ["NY", "LA", "SEA", "SF", "BOS", "DC", "LA", "NY"]
        _write(dirty, pd.DataFrame({"city": city}))
        _write(clean, pd.DataFrame({"city": clean_city}))
        records = build_records(dirty, clean, "synthetic", None)
        # The missing cell (row 2) has a clean oracle value -> a teaching record.
        assert any(r["row"] == 2 and r["column"] == "city" for r in records)
