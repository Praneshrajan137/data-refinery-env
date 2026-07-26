"""Unit tests for the review-queue ranker (offline; no network)."""

from __future__ import annotations

import pandas as pd

from dataforge.review import ReviewRanker


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ProviderNumber": ["10018", "10018", "99999"],
            "HospitalName": ["rgnl", "cullman regional", "other"],
            "City": ["cullman", "cullman", "elsewhere"],
        }
    )


class TestReviewRanker:
    def test_never_mutates_and_returns_scores(self) -> None:
        df = _df()
        before = df.copy(deep=True)

        # Say "yes" for HospitalName cells, "no" otherwise.
        def fake(prompt: list[dict[str, str]]) -> str:
            return "yes" if '"flagged_column":"HospitalName"' in prompt[1]["content"] else "no"

        ranker = ReviewRanker(model="test", completion_fn=fake)
        ranked = ranker.rank([(0, "HospitalName"), (0, "City")], df)
        assert [cs.column for cs in ranked] == ["HospitalName", "City"]  # yes first
        assert ranked[0].score == 1.0 and ranked[1].score == 0.0
        assert ranked[0].provenance == "llm_live"
        pd.testing.assert_frame_equal(df, before)  # ranker mutated nothing

    def test_self_consistency_fraction(self) -> None:
        df = _df()
        calls = {"n": 0}

        def alternating(prompt: list[dict[str, str]]) -> str:
            calls["n"] += 1
            return "yes" if calls["n"] % 3 != 0 else "no"  # 2 of every 3 say yes

        ranker = ReviewRanker(model="test", samples=3, completion_fn=alternating)
        ranked = ranker.rank([(0, "HospitalName")], df)
        assert ranked[0].score == round(2 / 3, 4)

    def test_cache_hit_avoids_second_call(self, tmp_path) -> None:
        df = _df()
        calls = {"n": 0}

        def counting(prompt: list[dict[str, str]]) -> str:
            calls["n"] += 1
            return "yes"

        ranker = ReviewRanker(model="test", cache_dir=tmp_path, completion_fn=counting)
        first = ranker.rank([(0, "HospitalName")], df)
        assert first[0].provenance == "llm_live"
        second = ranker.rank([(0, "HospitalName")], df)
        assert second[0].provenance == "llm_cache"
        assert calls["n"] == 1  # second call served from cache

    def test_invalid_cell_scores_zero(self) -> None:
        df = _df()
        ranker = ReviewRanker(model="test", completion_fn=lambda _p: "yes")
        ranked = ranker.rank([(999, "HospitalName"), (0, "NoSuchColumn")], df)
        assert all(cs.score == 0.0 and cs.provenance == "invalid" for cs in ranked)
