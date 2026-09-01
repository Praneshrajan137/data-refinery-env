"""The review triager must be reachable, and must never be able to write.

Two independent properties are locked here.

**Reachability.** The triager was measured as a decisive win on flooded review
queues (hospital review-queue precision 5.0% -> 40.7%, ROC-AUC 0.95 vs the free
detector-confidence baseline's 0.49; rayyan ~50x queue-precision lift) and yet
was reachable only through ``dataforge bench``: not exported, no CLI flag, no
tool, no HTTP field. Measured value that no user can obtain is not a capability,
so these tests assert the surfaces exist.

**Safety.** A triage score is presentation-only. It orders a human's queue; it can
never become a mutation. The ranker is passed as a separate argument to
``run_repair_pipeline`` rather than as a field on ``RepairPipelineRequest``,
precisely so there is no path from a score into the verified apply gate.

All tests are offline: the ranker's provider call is replaced by an injected
``completion_fn``.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import dataforge
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline
from dataforge.review import CellScore, ReviewRanker

_RESULTS = Path(__file__).resolve().parents[2] / "eval" / "results"


class TestReachability:
    """Every surface a user could plausibly reach the triager through."""

    def test_exported_from_the_package_root(self) -> None:
        assert "ReviewRanker" in dataforge.__all__
        assert "CellScore" in dataforge.__all__
        assert dataforge.ReviewRanker is ReviewRanker
        assert dataforge.CellScore is CellScore

    def test_cli_repair_exposes_a_review_rank_flag(self) -> None:
        from dataforge.cli.repair import repair

        # Assert registration STRUCTURALLY. Typer's Rich colorizer injects ANSI
        # codes into --help and wraps by terminal width, so substring-matching
        # rendered help passes locally and fails on CI.
        assert "review_rank" in inspect.signature(repair).parameters

    def test_pipeline_accepts_the_ranker_as_a_separate_argument(self) -> None:
        params = inspect.signature(run_repair_pipeline).parameters
        assert "review_ranker" in params
        # Keyword-only, so it can never be passed positionally by accident.
        assert params["review_ranker"].kind is inspect.Parameter.KEYWORD_ONLY

    def test_ranker_is_not_a_request_field(self) -> None:
        # The load-bearing safety design: if it were a request field it would sit
        # on the same object the apply path consumes.
        assert "review_ranker" not in RepairPipelineRequest.model_fields


class TestPresentationOnly:
    """A triage score can never become a write."""

    def _csv(self, tmp_path: Path) -> Path:
        path = tmp_path / "cities.csv"
        rows = ["provider,city"]
        rows += [f"P{i:03d},ATLANTA" for i in range(8)]
        rows += [f"Q{i:03d},BIRMINGHAM" for i in range(6)]
        rows.append("Z999,ATLNTA")
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        return path

    def test_ranking_populates_the_receipt_without_adding_fixes(self, tmp_path: Path) -> None:
        path = self._csv(tmp_path)
        baseline = run_repair_pipeline(RepairPipelineRequest(source_path=path))
        ranked = run_repair_pipeline(
            RepairPipelineRequest(source_path=path),
            review_ranker=ReviewRanker(
                cache_dir=None, model="test", completion_fn=lambda _m: "yes"
            ),
        )

        # The ranking is additive information only: the verified fix set is
        # identical with and without it.
        def cells(result: object) -> list[tuple[int, str, str]]:
            fixes = result.receipt.applied_fixes  # type: ignore[attr-defined]
            return [(f.row, f.column, f.new_value) for f in fixes]

        assert cells(ranked) == cells(baseline)
        assert ranked.receipt.fixes_count == baseline.receipt.fixes_count
        assert baseline.receipt.review_ranking == []

    def test_source_bytes_are_untouched_by_ranking(self, tmp_path: Path) -> None:
        path = self._csv(tmp_path)
        before = path.read_bytes()
        run_repair_pipeline(
            RepairPipelineRequest(source_path=path),
            review_ranker=ReviewRanker(
                cache_dir=None, model="test", completion_fn=lambda _m: "yes"
            ),
        )
        assert path.read_bytes() == before

    def test_ranker_never_returns_a_value_to_write(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"city": ["ATLANTA"] * 6 + ["ATLNTA"]})
        ranker = ReviewRanker(cache_dir=None, model="test", completion_fn=lambda _m: "yes")
        scores = ranker.rank([(6, "city")], df)
        assert all(isinstance(s, CellScore) for s in scores)
        # A CellScore carries a score and provenance -- deliberately no
        # `new_value` field, so there is nothing a caller could apply.
        assert not hasattr(scores[0], "new_value")
        assert 0.0 <= scores[0].score <= 1.0


class TestNoAutoFireGate:
    """The auto-fire gate was measured and refuted; it must stay unshipped."""

    def test_gate_probe_records_a_no_go(self) -> None:
        artifact = _RESULTS / "review_gate_probe.json"
        if not artifact.exists():
            pytest.fail(
                "review_gate_probe.json not committed -- it is tracked in git and required by this test"
            )
        payload = json.loads(artifact.read_text(encoding="utf-8"))
        # Shipping an entropy-based auto-fire gate would abstain on rayyan, where
        # the triager delivers a ~50x queue-precision lift. The probe is the
        # standing evidence for why firing is an explicit user decision.
        assert payload["verdict"] == "NO_GO"
        assert "rayyan" in payload["mispredicted_datasets"]

    def test_rayyan_is_the_counterexample(self) -> None:
        artifact = _RESULTS / "review_gate_probe.json"
        if not artifact.exists():
            pytest.fail(
                "review_gate_probe.json not committed -- it is tracked in git and required by this test"
            )
        rayyan = json.loads(artifact.read_text(encoding="utf-8"))["findings"]["rayyan"]
        # Well-spread confidences (so a dispersion gate abstains) yet the LLM
        # helps decisively -- dispersion does not imply baseline informativeness.
        assert rayyan["gate_would_fire"] is False
        assert rayyan["llm_actually_helps"] is True


class TestEvidenceAwarePrompt:
    """The ranker may be shown detector findings, without disturbing the old path.

    The original prompt carried only the flagged cell and its row, so the model was asked
    to re-derive from strictly less information than the pipeline already held: `rank()`
    received `(row, column)` tuples and discarded `issue_type`, `confidence`, `reason` and
    `expected` entirely. Passing evidence is opt-in precisely so the two paths stay
    comparable -- the evidence-free prompt must remain byte-identical or every cached
    result and every previously published number silently changes meaning.
    """

    @staticmethod
    def _frame() -> Any:
        import pandas as pd

        return pd.DataFrame({"a": ["x", "y"], "b": ["1", "2"]})

    def _ranker(self) -> Any:
        from dataforge.review import ReviewRanker

        return ReviewRanker(cache_dir=None, model="m", completion_fn=lambda _m: "yes")

    def test_evidence_free_prompt_is_unchanged(self) -> None:
        """Back-compat guard: changing this invalidates every cache and prior measurement."""
        prompt = self._ranker()._build_messages(0, "a", self._frame(), None)
        assert prompt[0]["content"] == (
            "You are a data-quality auditor. A specific cell in a table row has been "
            "flagged as possibly erroneous. Using the whole row as context, decide "
            "whether the flagged cell's value is actually erroneous. Respond with ONLY "
            "'yes' (erroneous) or 'no' (fine). No prose."
        )
        assert "detector_findings" not in prompt[1]["content"]

    def test_evidence_reaches_the_model(self) -> None:
        prompt = self._ranker()._build_messages(
            0, "a", self._frame(), {"issue_type": "format_violation", "confidence": 0.95}
        )
        assert "detector_findings" in prompt[1]["content"]
        assert "format_violation" in prompt[1]["content"]

    def test_evidence_prompt_states_detectors_are_low_precision(self) -> None:
        """Detector precision is 0.34-0.95 by dataset, so findings are evidence, not truth."""
        prompt = self._ranker()._build_messages(0, "a", self._frame(), {"issue_type": "x"})
        assert "low precision" in prompt[0]["content"]
        assert "not as ground truth" in prompt[0]["content"]

    def test_evidence_changes_the_cache_key(self) -> None:
        """Otherwise an evidence-free cache hit would silently answer an evidence query."""
        import tempfile

        ranker_module = __import__("dataforge.review.ranker", fromlist=["ReviewRanker"])
        with tempfile.TemporaryDirectory() as raw:
            ranker = ranker_module.ReviewRanker(
                cache_dir=Path(raw), model="m", completion_fn=lambda _m: "yes"
            )
            frame = self._frame()
            bare = ranker._cache_path(0, "a", frame, ranker._build_messages(0, "a", frame, None))
            rich = ranker._cache_path(
                0, "a", frame, ranker._build_messages(0, "a", frame, {"issue_type": "x"})
            )
            assert bare != rich

    def test_rank_accepts_evidence_and_stays_presentation_only(self) -> None:
        """The evidence path must still return scores only -- never a value to write."""
        from dataforge.review import ReviewRanker

        ranker = ReviewRanker(cache_dir=None, model="m", completion_fn=lambda _m: "yes")
        scores = ranker.rank([(0, "a")], self._frame(), {(0, "a"): {"issue_type": "x"}})
        assert len(scores) == 1
        assert not hasattr(scores[0], "new_value")
        assert scores[0].score == pytest.approx(1.0)
