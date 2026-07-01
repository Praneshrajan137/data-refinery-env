"""Offline tests for the llm_corrector benchmark method (C4).

No API key or network: the provider is a fixed stub client. These assert the
method scores corrections, tracks quota/latency, and reports calibration
(ECE) and the fixed-threshold precision_at_auto_apply, plus the promotion gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from dataforge.bench.core import SeedBenchmarkResult
from dataforge.bench.groq_client import GroqCompletion
from dataforge.bench.methods import corrector_promotion_verdict, run_llm_corrector_episode
from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from dataforge.datasets.registry import DatasetMetadata

FIXTURE_SHA = "a" * 64


def _dataset() -> RealWorldDataset:
    # A single blank cell in an otherwise-populated free-text column reliably
    # triggers a missing_value detection the corrector must fill. The column
    # needs >= 8 rows for the populated-rate estimate to be meaningful.
    populated = ["Boston", "Denver", "Austin", "Reno", "Miami", "Chicago", "Dallas"]
    dirty_df = pd.DataFrame({"city": [*populated, ""]})
    clean_df = pd.DataFrame({"city": [*populated, "Seattle"]})
    metadata = DatasetMetadata(
        name="hospital",
        domain="healthcare",
        n_rows=8,
        n_columns=1,
        error_types=("missing_value",),
        source_urls=("dirty", "clean"),
        source_revision="fixture",
        dirty_sha256=FIXTURE_SHA,
        clean_sha256=FIXTURE_SHA,
        citation="fixture",
    )
    return RealWorldDataset(
        metadata=metadata,
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=("city",),
        ground_truth=(
            GroundTruthCell(row=7, column="city", dirty_value="", clean_value="Seattle"),
        ),
        dirty_sha256=FIXTURE_SHA,
        clean_sha256=FIXTURE_SHA,
    )


@dataclass
class _FixedClient:
    """Stub provider that returns a fixed completion for every call."""

    text: str
    model: str = "fake-model"
    calls: int = field(default=0)

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        self.calls += 1
        return GroqCompletion(
            text=self.text,
            prompt_tokens=3,
            completion_tokens=2,
            warnings=(),
        )


class TestRunLLMCorrectorEpisode:
    def test_correct_fill_is_scored_and_metrics_populated(self) -> None:
        dataset = _dataset()
        client = _FixedClient(text="Seattle")

        result = run_llm_corrector_episode(dataset, seed=0, client=client, samples=3)

        assert isinstance(result, SeedBenchmarkResult)
        assert result.method == "llm_corrector"
        assert result.status == "ok"
        # The blank was detected and filled with the ground-truth value.
        assert result.tp == 1
        assert result.f1 == 1.0
        # Quota/latency tracked: 3 self-consistency samples for the one issue.
        assert client.calls == 3
        assert result.llm_calls == 3
        assert result.prompt_tokens == 9
        assert result.completion_tokens == 6
        assert result.quota_units > 0.0
        # Calibration diagnostics present.
        assert result.ece is not None
        assert result.precision_at_auto_apply == 1.0
        assert result.auto_apply_count == 1
        assert result.provider == "groq"
        assert result.model == "fake-model"

    def test_wrong_fill_lowers_precision_at_auto_apply(self) -> None:
        dataset = _dataset()
        client = _FixedClient(text="Portland")  # wrong but contract-valid fill

        result = run_llm_corrector_episode(dataset, seed=0, client=client, samples=3)

        # A confident-but-wrong correction is auto-apply-eligible and wrong, so
        # precision_at_auto_apply must reflect the miss rather than hide it.
        assert result.auto_apply_count == 1
        assert result.precision_at_auto_apply == 0.0
        assert result.tp == 0


class TestPromotionVerdict:
    def test_promotes_on_precise_calibrated_record(self) -> None:
        record = _minimal_record(precision_at_auto_apply=1.0, ece=0.02, auto_apply_count=5)

        promote, reasons = corrector_promotion_verdict(record)

        assert promote is True
        assert reasons == []

    def test_rejects_low_precision(self) -> None:
        record = _minimal_record(precision_at_auto_apply=0.80, ece=0.02, auto_apply_count=5)

        promote, reasons = corrector_promotion_verdict(record)

        assert promote is False
        assert any("precision_at_auto_apply" in reason for reason in reasons)

    def test_rejects_poor_calibration(self) -> None:
        record = _minimal_record(precision_at_auto_apply=0.99, ece=0.5, auto_apply_count=5)

        promote, reasons = corrector_promotion_verdict(record)

        assert promote is False
        assert any("ECE" in reason for reason in reasons)

    def test_rejects_insufficient_evidence(self) -> None:
        record = _minimal_record(precision_at_auto_apply=1.0, ece=0.0, auto_apply_count=0)

        promote, reasons = corrector_promotion_verdict(record)

        assert promote is False


def _minimal_record(
    *,
    precision_at_auto_apply: float,
    ece: float,
    auto_apply_count: int,
) -> SeedBenchmarkResult:
    return SeedBenchmarkResult(
        method="llm_corrector",
        dataset="hospital",
        seed=0,
        status="ok",
        precision=0.9,
        recall=0.9,
        f1=0.9,
        reproduction_command="dataforge bench --methods llm_corrector",
        ece=ece,
        precision_at_auto_apply=precision_at_auto_apply,
        auto_apply_count=auto_apply_count,
    )
