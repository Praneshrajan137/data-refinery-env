"""Unit tests for the entity-consensus detector (cross-row multi-source repair)."""

from __future__ import annotations

import pandas as pd

from dataforge.detectors.entity_consensus import EntityConsensusDetector


def _detect(df: pd.DataFrame):
    return EntityConsensusDetector().detect(df)


def _multi_entity(consensus_by_entity: dict[str, str], rows_per_entity: int = 4) -> pd.DataFrame:
    """Build a (key, val) table: each entity repeats its consensus value."""
    keys: list[str] = []
    vals: list[str] = []
    for entity, value in consensus_by_entity.items():
        keys.extend([entity] * rows_per_entity)
        vals.extend([value] * rows_per_entity)
    return pd.DataFrame({"key": keys, "val": vals})


class TestEntityConsensusDetector:
    def test_flags_disagreeing_and_blank_cells_with_consensus_value(self) -> None:
        # 6 entities, each with its OWN distinct consensus value (high diversity ->
        # a genuine key->attribute determination). Inject one wrong + one blank.
        df = _multi_entity({c: f"v_{c}" for c in "ABCDEF"})
        df.loc[0, "val"] = "WRONG"  # entity A, row 0: disagrees with consensus v_A
        df.loc[4, "val"] = ""  # entity B, row 4: blank while siblings agree v_B
        issues = {(i.row, i.column): i for i in _detect(df)}
        assert issues[(0, "val")].expected == "v_A"
        assert issues[(4, "val")].expected == "v_B"
        assert all(i.issue_type == "entity_consensus" for i in issues.values())

    def test_rejects_categorical_correlation_low_diversity(self) -> None:
        # 6 entities but the consensus is a tiny SHARED vocabulary ("EN" for all):
        # the key correlates with, but does not determine, the target. The minority
        # differing values are correct, not errors -> the diversity guard rejects.
        df = _multi_entity(dict.fromkeys("ABCDEF", "EN"))
        df.loc[0, "val"] = "FR"  # a legitimately different value, not an error
        df.loc[4, "val"] = "DE"
        assert _detect(df) == []

    def test_abstains_when_key_does_not_govern_target(self) -> None:
        # zip does NOT determine salary: every group disagrees internally.
        keys = [str(k) for k in range(6) for _ in range(4)]
        salary = [str(v) for v in range(24)]
        df = pd.DataFrame({"zip": keys, "salary": salary})
        assert _detect(df) == []

    def test_skips_near_unique_key(self) -> None:
        # An id column with a distinct value per row is not an entity key.
        df = pd.DataFrame({"id": [str(i) for i in range(12)], "val": ["x"] * 12})
        assert _detect(df) == []

    def test_requires_minimum_governed_entities(self) -> None:
        # Only 3 entities agree distinctly -> below the min-governed-groups floor,
        # so a handful of coincidentally-consistent groups cannot "govern".
        df = _multi_entity({c: f"v_{c}" for c in "ABC"})
        df.loc[0, "val"] = "WRONG"
        assert _detect(df) == []

    def test_skips_tiny_tables(self) -> None:
        df = pd.DataFrame({"flight": ["A", "A", "B"], "arr": ["9:30", "9:30", "1:00"]})
        assert _detect(df) == []

    def test_no_flags_when_every_group_agrees(self) -> None:
        assert _detect(_multi_entity({c: f"v_{c}" for c in "ABCDEF"})) == []

    def test_confidence_reflects_support(self) -> None:
        # 6 entities of 5 rows; entity A has 4/5 agree -> support 0.8 on the wrong cell.
        df = _multi_entity({c: f"v_{c}" for c in "ABCDEF"}, rows_per_entity=5)
        df.loc[0, "val"] = "WRONG"  # 1 of A's 5 rows wrong -> consensus support 0.8
        issues = {(i.row, i.column): i for i in _detect(df)}
        assert issues[(0, "val")].confidence == 0.8
