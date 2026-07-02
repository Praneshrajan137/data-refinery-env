"""Unit tests for benchmark orchestration."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dataforge.bench.core import SeedBenchmarkResult
from dataforge.bench.runner import run_agent_comparison

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "bench"


def _populate_cache(cache_root: Path) -> None:
    dataset_dir = cache_root / "real_world" / "hospital"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "dirty.csv").write_text(
        (_FIXTURES / "hospital_dirty.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (dataset_dir / "clean.csv").write_text(
        (_FIXTURES / "hospital_clean.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )


class TestRunAgentComparison:
    """Benchmark orchestration should write JSON and skip unavailable LLM methods."""

    def test_runner_rejects_unknown_inputs(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown benchmark methods"):
            run_agent_comparison(
                methods=["unknown"],
                datasets=["hospital"],
                seeds=1,
                output_json=tmp_path / "out.json",
                really_run_big_bench=False,
                cache_root=tmp_path / "cache",
            )

    def test_runner_writes_json_and_skips_unconfigured_llm(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_root = tmp_path / "cache"
        output_json = tmp_path / "eval" / "results" / "agent_comparison.json"
        _populate_cache(cache_root)

        # Force a provider whose credential is absent so LLM methods skip.
        # load_dotenv() in the runner will not override existing env vars by default.
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "gemini")
        monkeypatch.setenv("GROQ_API_KEY", "")
        monkeypatch.setenv("GEMINI_API_KEY", "")

        result = run_agent_comparison(
            methods=["heuristic", "llm_zeroshot"],
            datasets=["hospital"],
            seeds=1,
            output_json=output_json,
            really_run_big_bench=False,
            cache_root=cache_root,
            verify_dataset_hashes=False,
        )

        assert output_json.exists()
        assert result.aggregates[0].dataset == "hospital"
        assert any(row.method == "heuristic" and row.status == "ok" for row in result.aggregates)
        assert any(
            row.method == "llm_zeroshot" and row.status == "skipped" for row in result.aggregates
        )

        payload = json.loads(output_json.read_text(encoding="utf-8"))
        assert payload["metadata"]["datasets"] == ["hospital"]
        assert payload["records"][0]["dataset"] == "hospital"

    def test_heuristic_runner_produces_nonzero_true_positives(self, tmp_path: Path) -> None:
        cache_root = tmp_path / "cache"
        output_json = tmp_path / "agent_comparison.json"
        _populate_cache(cache_root)

        result = run_agent_comparison(
            methods=["heuristic"],
            datasets=["hospital"],
            seeds=1,
            output_json=output_json,
            really_run_big_bench=False,
            cache_root=cache_root,
            verify_dataset_hashes=False,
        )

        aggregate = result.aggregates[0]
        assert aggregate.status == "ok"
        assert aggregate.f1_mean is not None and aggregate.f1_mean > 0.0

    def test_runner_blocks_large_llm_bench_without_override(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "groq")
        monkeypatch.setenv("GROQ_API_KEY", "test-key")

        with pytest.raises(ValueError, match="really-run-big-bench"):
            run_agent_comparison(
                methods=["llm_zeroshot", "llm_react"],
                datasets=["hospital", "flights", "beers"],
                seeds=3,
                output_json=tmp_path / "out.json",
                really_run_big_bench=False,
                cache_root=tmp_path / "cache",
            )

    def test_runner_rejects_unknown_dataset_and_nonpositive_seeds(self, tmp_path: Path) -> None:
        """Input validation covers dataset names and seed counts independently."""
        with pytest.raises(ValueError, match="Unknown benchmark datasets"):
            run_agent_comparison(
                methods=["heuristic"],
                datasets=["unknown"],
                seeds=1,
                output_json=tmp_path / "out.json",
                really_run_big_bench=False,
                cache_root=tmp_path / "cache",
            )

        with pytest.raises(ValueError, match="must be >= 1"):
            run_agent_comparison(
                methods=["heuristic"],
                datasets=["hospital"],
                seeds=0,
                output_json=tmp_path / "out.json",
                really_run_big_bench=False,
                cache_root=tmp_path / "cache",
            )

    def test_runner_executes_configured_llm_methods_with_env_fallbacks(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Configured LLM paths instantiate the client and normalize reproduction metadata."""
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root)
        output_json = tmp_path / "agent_comparison.json"
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "groq")
        monkeypatch.setenv("GROQ_API_KEY", "test-key")
        monkeypatch.setenv("DATAFORGE_GROQ_MIN_INTERVAL_S", "not-a-float")
        monkeypatch.setenv("DATAFORGE_GROQ_TIMEOUT_S", "not-a-float")
        monkeypatch.setenv("DATAFORGE_GROQ_MAX_TOKENS", "not-an-int")
        monkeypatch.setenv("DATAFORGE_GROQ_MAX_RETRIES", "not-an-int")

        stale_command = "old reproduction command"
        zero_result = SeedBenchmarkResult(
            method="llm_zeroshot",
            dataset="hospital",
            seed=0,
            status="ok",
            tp=1,
            fp=0,
            fn=0,
            precision=1.0,
            recall=1.0,
            f1=1.0,
            avg_steps=1,
            runtime_s=0.1,
            llm_calls=1,
            prompt_tokens=10,
            completion_tokens=2,
            quota_units=0.001,
            provider="groq",
            model="mock",
            reproduction_command=stale_command,
        )
        react_result = zero_result.model_copy(update={"method": "llm_react"})

        with (
            patch("dataforge.bench.runner.GroqBenchClient", return_value=MagicMock()) as client,
            patch(
                "dataforge.bench.runner.run_llm_zeroshot_episode",
                return_value=zero_result,
            ) as zeroshot,
            patch(
                "dataforge.bench.runner.run_llm_react_episode", return_value=react_result
            ) as react,
        ):
            output = run_agent_comparison(
                methods=["llm_zeroshot", "llm_react"],
                datasets=["hospital"],
                seeds=1,
                output_json=output_json,
                really_run_big_bench=True,
                cache_root=cache_root,
                verify_dataset_hashes=False,
            )

        client.assert_called_once()
        assert client.call_args.kwargs["min_interval_s"] == 1.0
        assert client.call_args.kwargs["timeout_s"] == 30.0
        assert client.call_args.kwargs["max_tokens"] == 256
        assert client.call_args.kwargs["max_retries"] == 3
        zeroshot.assert_called_once()
        react.assert_called_once()
        assert all(
            record.reproduction_command
            == "dataforge bench --methods llm_zeroshot,llm_react --datasets hospital --seeds 1"
            for record in output.records
        )

    def test_runner_uses_bedrock_client_when_provider_is_bedrock(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Provider bedrock constructs a BedrockBenchClient from env config."""
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root)
        output_json = tmp_path / "agent_comparison.json"
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "bedrock")
        monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", "test-key")
        monkeypatch.setenv("DATAFORGE_BEDROCK_MODEL", "us.anthropic.claude-sonnet-5-test-v1:0")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.delenv("GROQ_API_KEY", raising=False)

        zero_result = SeedBenchmarkResult(
            method="llm_zeroshot",
            dataset="hospital",
            seed=0,
            status="ok",
            tp=1,
            fp=0,
            fn=0,
            precision=1.0,
            recall=1.0,
            f1=1.0,
            avg_steps=1,
            runtime_s=0.1,
            llm_calls=1,
            prompt_tokens=10,
            completion_tokens=2,
            quota_units=0.001,
            provider="bedrock",
            model="us.anthropic.claude-sonnet-5-test-v1:0",
            reproduction_command="cmd",
        )

        with (
            patch(
                "dataforge.bench.runner.BedrockBenchClient", return_value=MagicMock()
            ) as bedrock_client,
            patch("dataforge.bench.runner.GroqBenchClient") as groq_client,
            patch(
                "dataforge.bench.runner.run_llm_zeroshot_episode",
                return_value=zero_result,
            ) as zeroshot,
        ):
            run_agent_comparison(
                methods=["llm_zeroshot"],
                datasets=["hospital"],
                seeds=1,
                output_json=output_json,
                really_run_big_bench=True,
                cache_root=cache_root,
                verify_dataset_hashes=False,
            )

        bedrock_client.assert_called_once()
        assert bedrock_client.call_args.kwargs["model"] == (
            "us.anthropic.claude-sonnet-5-test-v1:0"
        )
        assert bedrock_client.call_args.kwargs["region"] == "us-east-1"
        groq_client.assert_not_called()
        zeroshot.assert_called_once()

    def test_runner_skips_bedrock_without_key(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Bedrock provider without the bearer token yields a skipped result."""
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root)
        output_json = tmp_path / "agent_comparison.json"
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "bedrock")
        monkeypatch.delenv("AWS_BEARER_TOKEN_BEDROCK", raising=False)

        output = run_agent_comparison(
            methods=["llm_zeroshot"],
            datasets=["hospital"],
            seeds=1,
            output_json=output_json,
            really_run_big_bench=True,
            cache_root=cache_root,
            verify_dataset_hashes=False,
        )

        assert all(record.status == "skipped" for record in output.records)
        assert all(
            record.skip_reason == "AWS_BEARER_TOKEN_BEDROCK is not set."
            for record in output.records
        )

    def test_runner_uses_gemini_client_when_provider_is_gemini(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Provider gemini constructs a GeminiBenchClient from env config."""
        cache_root = tmp_path / "cache"
        _populate_cache(cache_root)
        output_json = tmp_path / "agent_comparison.json"
        monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "gemini")
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        monkeypatch.setenv("DATAFORGE_GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
        monkeypatch.delenv("GROQ_API_KEY", raising=False)

        zero_result = SeedBenchmarkResult(
            method="llm_zeroshot",
            dataset="hospital",
            seed=0,
            status="ok",
            tp=1,
            fp=0,
            fn=0,
            precision=1.0,
            recall=1.0,
            f1=1.0,
            avg_steps=1,
            runtime_s=0.1,
            llm_calls=1,
            prompt_tokens=10,
            completion_tokens=2,
            quota_units=0.001,
            provider="gemini",
            model="gemini-3.1-flash-lite-preview",
            reproduction_command="cmd",
        )

        with (
            patch(
                "dataforge.bench.runner.GeminiBenchClient", return_value=MagicMock()
            ) as gemini_client,
            patch("dataforge.bench.runner.GroqBenchClient") as groq_client,
            patch(
                "dataforge.bench.runner.run_llm_zeroshot_episode",
                return_value=zero_result,
            ) as zeroshot,
        ):
            run_agent_comparison(
                methods=["llm_zeroshot"],
                datasets=["hospital"],
                seeds=1,
                output_json=output_json,
                really_run_big_bench=True,
                cache_root=cache_root,
                verify_dataset_hashes=False,
            )

        gemini_client.assert_called_once()
        assert gemini_client.call_args.kwargs["model"] == "gemini-3.1-flash-lite-preview"
        groq_client.assert_not_called()
        zeroshot.assert_called_once()

    def test_runner_records_explicit_seed_list_and_dataset_evidence(
        self,
        tmp_path: Path,
    ) -> None:
        """Benchmark metadata should be concrete enough to reproduce and audit."""
        cache_root = tmp_path / "cache"
        output_json = tmp_path / "agent_comparison.json"
        _populate_cache(cache_root)

        output = run_agent_comparison(
            methods=["random"],
            datasets=["hospital"],
            seeds=99,
            seed_list=[2, 4],
            output_json=output_json,
            really_run_big_bench=False,
            cache_root=cache_root,
            verify_dataset_hashes=False,
        )

        payload = json.loads(output_json.read_text(encoding="utf-8"))
        metadata = payload["metadata"]
        assert metadata["schema_version"] == "dataforge_benchmark_run_v2"
        assert metadata["seeds"] == 2
        assert metadata["seed_list"] == [2, 4]
        assert {record.seed for record in output.records} == {2, 4}
        assert "--seed-list 2,4" in metadata["reproduction_command"]
        evidence = metadata["dataset_evidence"][0]
        assert evidence["name"] == "hospital"
        assert evidence["source_revision"]
        assert len(evidence["dirty_sha256"]) == 64
        assert len(evidence["clean_sha256"]) == 64
        assert (
            metadata["artifact_sha256s"]["dataset:hospital:dirty.csv"] == evidence["dirty_sha256"]
        )
