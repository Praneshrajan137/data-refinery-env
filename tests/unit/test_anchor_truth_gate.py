"""Pin the anchor-truth gate's refusals, not its happy path.

This gate exists because the hospital anchor drifted 0.7926 -> 0.8178 -> 0.8352 while three
other gates stayed green: they compared the artifact to the report, to prose, and to a constant
in a test, and none of them compared it to the code. A replacement guard that could itself pass
vacuously would reproduce the original failure, so what is pinned here is every way this gate
must refuse.

The gate's ability to detect real drift is not asserted here by simulation -- it was observed:
run against the pre-regeneration artifact it failed and named both
`hospital: fp 178 -> 120, f1 0.7926 -> 0.8352` and `flights: fp 92 -> 9`.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

_GATE = Path(__file__).resolve().parents[2] / "scripts" / "ci" / "anchor_truth.py"


@pytest.fixture(scope="module")
def gate() -> Any:
    spec = importlib.util.spec_from_file_location("_anchor_truth", _GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_an_artifact_with_no_heuristic_records_is_refused(
    gate: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Zero records to check must FAIL, never pass. This is the vacuity trap itself."""
    artifact = tmp_path / "a.json"
    artifact.write_text(
        json.dumps({"records": [{"method": "random", "dataset": "hospital", "f1": 0.0}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr("sys.argv", ["anchor_truth.py", "--artifact", str(artifact)])

    assert gate.main() == 1
    assert "vacuously" in capsys.readouterr().err


def test_a_missing_artifact_is_refused(
    gate: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "sys.argv", ["anchor_truth.py", "--artifact", str(tmp_path / "absent.json")]
    )
    assert gate.main() == 1


def test_a_record_missing_a_metric_is_an_error_not_a_default(gate: Any) -> None:
    """Defaulting a missing field would let a truncated record compare equal to a measurement."""
    with pytest.raises(SystemExit) as caught:
        gate._number({"method": "heuristic", "dataset": "hospital"}, "fp", "hospital")
    assert "malformed" in str(caught.value)


def test_a_boolean_is_not_accepted_as_a_metric(gate: Any) -> None:
    """`True` is an int in Python. Accepting it would let `fp: true` compare against 1."""
    with pytest.raises(SystemExit):
        gate._number({"fp": True}, "fp", "hospital")


def test_a_none_measurement_is_an_error_not_a_vacuous_match(gate: Any) -> None:
    """SeedBenchmarkResult types metrics as optional; None == None would pass for free."""
    with pytest.raises(SystemExit) as caught:
        gate._measured(None, "tp", "hospital")
    assert "nothing to compare" in str(caught.value)


def test_a_real_measurement_passes_through_unchanged(gate: Any) -> None:
    assert gate._measured(451, "tp", "hospital") == 451.0
    assert gate._number({"tp": 451}, "tp", "hospital") == 451.0


def test_seeds_are_collapsed_to_one_record_per_dataset(gate: Any, tmp_path: Path) -> None:
    """The method is deterministic, so re-measuring three identical seeds proves nothing thrice."""
    artifact = tmp_path / "a.json"
    artifact.write_text(
        json.dumps(
            {
                "records": [
                    {"method": "heuristic", "dataset": "hospital", "seed": s, "f1": 0.8352}
                    for s in (0, 1, 2)
                ]
            }
        ),
        encoding="utf-8",
    )
    committed = gate._committed(artifact)
    assert list(committed) == ["hospital"]


def test_the_gate_is_wired_into_the_backend_gate() -> None:
    """An unwired gate is a dead gate, which is the pattern this repository keeps rediscovering."""
    backend = (
        Path(__file__).resolve().parents[2] / "scripts" / "ci" / "backend_gate.py"
    ).read_text(encoding="utf-8")
    assert "anchor_truth.py" in backend
    assert '"anchor truth"' in backend
