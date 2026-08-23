"""Unit test for the entity_consensus benchmark method (deterministic, offline)."""

from __future__ import annotations

import pandas as pd

from dataforge.bench.methods import run_entity_consensus_episode, run_heuristic_episode
from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from tests.support.corpora import build_fixture_metadata

_SHA = "0" * 64


def _multi_source_dataset() -> RealWorldDataset:
    """6 entities x 4 rows; each has its OWN consensus value (high diversity).

    Entity A's row 0 holds a wrong value the cross-row consensus repairs. Values
    are distinct uppercase tokens so only the entity-consensus detector fires
    (no categorical-normalization or format-violation cluster match).
    """
    keys: list[str] = []
    vals: list[str] = []
    clean_vals: list[str] = []
    for entity in "ABCDEF":
        keys.extend([entity] * 4)
        vals.extend([f"VAL{entity}"] * 4)
        clean_vals.extend([f"VAL{entity}"] * 4)
    vals[0] = "WRONGVAL"  # entity A, row 0: disagrees with consensus VALA
    dirty_df = pd.DataFrame({"entity": keys, "val": vals})
    clean_df = pd.DataFrame({"entity": keys, "val": clean_vals})
    metadata = build_fixture_metadata(
        name="flights",
        domain="aviation",
        n_rows=24,
        n_columns=2,
        error_types=("formatting",),
        source_urls=("dirty", "clean"),
        source_revision="fixture",
        dirty_sha256=_SHA,
        clean_sha256=_SHA,
        citation="fixture",
    )
    return RealWorldDataset(
        metadata=metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=("entity", "val"),
        ground_truth=(
            GroundTruthCell(row=0, column="val", dirty_value="WRONGVAL", clean_value="VALA"),
        ),
        dirty_sha256=_SHA,
        clean_sha256=_SHA,
    )


class TestEntityConsensusEpisode:
    def test_recovers_the_error_and_is_never_worse_than_heuristic(self) -> None:
        # Note: on this clean synthetic table an entity->val FD is inferable, so the
        # heuristic FD repairer also fixes it (entity consensus OVERLAPS FD). The
        # honest, robust contract is: the consensus episode recovers the error and
        # is never worse than heuristic. Its DISTINCT value (where FD-inference
        # fails on noisy multi-source data) is proven by the committed flights
        # artifact: correction F1 0.0000 -> 0.4467.
        dataset = _multi_source_dataset()
        heuristic = run_heuristic_episode(dataset, seed=0)
        consensus = run_entity_consensus_episode(dataset, seed=0)

        assert consensus.tp >= 1
        assert consensus.f1 > 0.0
        assert consensus.tp >= heuristic.tp
        assert consensus.f1 >= heuristic.f1

    def test_is_deterministic_and_llm_free(self) -> None:
        dataset = _multi_source_dataset()
        result = run_entity_consensus_episode(dataset, seed=0)
        assert result.method == "entity_consensus"
        assert result.provider == "local"
        assert result.model == "deterministic"
        assert result.llm_calls == 0
        assert result.quota_units == 0.0


class TestEntityConsensusCoverageFloor:
    def test_committed_flights_artifact_meets_floors(self) -> None:
        # Regression guard for the real flights capability: the committed artifact's
        # per-class correction recalls must stay above the coverage floors. No live
        # run or network - it reads the committed reproducible artifact.
        import json
        from pathlib import Path

        from dataforge.bench.core import BenchmarkRunOutput
        from dataforge.bench.error_classes import check_coverage_regression

        root = Path(__file__).resolve().parents[2]
        artifact = root / "eval" / "results" / "entity_consensus_flights.json"
        floors = json.loads(
            (root / "eval" / "thresholds" / "coverage_floors.json").read_text(encoding="utf-8")
        )["floors"]
        key = "entity_consensus/flights"
        assert key in floors, "entity_consensus/flights floor must be committed"
        output = BenchmarkRunOutput.model_validate_json(artifact.read_text(encoding="utf-8"))
        passed, failures = check_coverage_regression(output.records, {key: floors[key]})
        assert passed, failures
