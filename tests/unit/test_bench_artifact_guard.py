"""The benchmark writer must not silently narrow a committed artifact.

**This is a regression test for something that actually happened.** On 2026-09-07, while
investigating the hospital anchor, a diagnostic
`dataforge bench --methods heuristic --datasets hospital --seeds 1` overwrote
`eval/results/agent_comparison.json` -- a committed artifact that `docs_truth`,
`benchmark_truth` and `tests/unit/test_corpus_tiering.py` all read -- replacing twelve records
with one. Nothing warned. The evidence survived only because the file happens to be tracked by
git, which is luck rather than a guarantee.

Frozen evidence that any diagnostic command can overwrite in place is not frozen, so the pin
here is the **refusal**, not the write.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataforge.bench.core import BenchmarkCoverageLossError, write_run_output


def _artifact(pairs: list[tuple[str, str]]) -> dict[str, object]:
    return {"records": [{"method": method, "dataset": dataset} for method, dataset in pairs]}


class _FakeOutput:
    """Minimal stand-in: write_run_output needs `.records` and `.model_dump_json()`."""

    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        self.records = [{"method": method, "dataset": dataset} for method, dataset in pairs]

    def model_dump_json(self, indent: int = 2) -> str:
        return json.dumps({"records": self.records}, indent=indent)


def test_narrowing_a_committed_artifact_is_refused(tmp_path: Path) -> None:
    """The exact command that destroyed the twelve-record artifact must now fail."""
    path = tmp_path / "agent_comparison.json"
    path.write_text(
        json.dumps(
            _artifact(
                [
                    ("random", "hospital"),
                    ("heuristic", "hospital"),
                    ("random", "flights"),
                    ("heuristic", "flights"),
                ]
            )
        ),
        encoding="utf-8",
    )
    before = path.read_bytes()

    with pytest.raises(BenchmarkCoverageLossError) as caught:
        write_run_output(_FakeOutput([("heuristic", "hospital")]), path)

    message = str(caught.value)
    assert "random/hospital" in message, "the refusal must name what would have been destroyed"
    assert "--allow-coverage-loss" in message, "the refusal must name the deliberate override"
    assert path.read_bytes() == before, "the artifact must be byte-identical after a refusal"


def test_rerunning_the_same_coverage_is_allowed(tmp_path: Path) -> None:
    """A full re-run is the normal way to refresh an anchor and must not be blocked."""
    path = tmp_path / "a.json"
    pairs = [("random", "hospital"), ("heuristic", "hospital")]
    path.write_text(json.dumps(_artifact(pairs)), encoding="utf-8")

    write_run_output(_FakeOutput(pairs), path)

    assert len(json.loads(path.read_text(encoding="utf-8"))["records"]) == 2


def test_extending_coverage_is_allowed(tmp_path: Path) -> None:
    """Adding a method or dataset loses nothing, so it must not require the override."""
    path = tmp_path / "a.json"
    path.write_text(json.dumps(_artifact([("heuristic", "hospital")])), encoding="utf-8")

    write_run_output(_FakeOutput([("heuristic", "hospital"), ("heuristic", "flights")]), path)

    assert len(json.loads(path.read_text(encoding="utf-8"))["records"]) == 2


def test_the_override_still_permits_a_deliberate_narrowing(tmp_path: Path) -> None:
    """The guard is a tripwire, not a lock: an explicit narrowing must remain possible."""
    path = tmp_path / "a.json"
    path.write_text(
        json.dumps(_artifact([("random", "hospital"), ("heuristic", "hospital")])),
        encoding="utf-8",
    )

    write_run_output(_FakeOutput([("heuristic", "hospital")]), path, allow_coverage_loss=True)

    assert len(json.loads(path.read_text(encoding="utf-8"))["records"]) == 1


def test_writing_a_fresh_path_is_unaffected(tmp_path: Path) -> None:
    """There is nothing to protect when the file does not exist yet."""
    path = tmp_path / "nested" / "a.json"
    write_run_output(_FakeOutput([("heuristic", "hospital")]), path)
    assert path.exists()


def test_an_unreadable_existing_artifact_does_not_block_the_write(tmp_path: Path) -> None:
    """Corrupt bytes are not evidence, so they must not become an unfixable blocker.

    Without this the guard could wedge the repo: a truncated artifact would refuse every
    subsequent write, including the full re-run that would repair it.
    """
    path = tmp_path / "a.json"
    path.write_text("{not json", encoding="utf-8")

    write_run_output(_FakeOutput([("heuristic", "hospital")]), path)

    assert json.loads(path.read_text(encoding="utf-8"))["records"]


def test_the_cli_exposes_the_override_flag() -> None:
    """A guard with no documented escape hatch gets worked around by deleting the file."""
    from dataforge.cli.bench import bench

    names = bench.__annotations__
    assert "allow_coverage_loss" in names
