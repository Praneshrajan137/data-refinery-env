"""Unit test for the residual curriculum generator."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.data.build_residual_curriculum import build_records  # noqa: E402


class TestResidualCurriculum:
    def test_emits_fix_trajectory_for_residual_cell(self, tmp_path: Path) -> None:
        # 'abc' in a numeric column is detected but not deterministically repaired
        # -> residual. The clean oracle supplies the correct value 30.
        dirty = tmp_path / "dirty.csv"
        clean = tmp_path / "clean.csv"
        dirty.write_text("id,score\n1,10\n2,20\n3,abc\n", encoding="utf-8")
        clean.write_text("id,score\n1,10\n2,20\n3,30\n", encoding="utf-8")

        records = build_records(dirty, clean, "synthetic", None)
        assert records, "expected at least one residual trajectory"

        record = records[0]
        assert record["dataset"] == "synthetic"
        assistant = json.loads(record["messages"][-1]["content"])
        assert assistant["action_type"] == "FIX"
        assert assistant["new_value"] == "30"
        # System prompt is the verified-agent prompt; user carries the observation.
        assert record["messages"][0]["role"] == "system"
        assert "residual" in record["messages"][1]["content"].lower()
