"""Unit tests for Week 9 SFT trajectory collection."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from dataforge.bench.core import BenchmarkRepair
from dataforge.bench.groq_client import GroqCompletion
from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from scripts.data.collect_sft_trajectories import (
    BudgetGuard,
    RuntimeDeadline,
    TrajectoryKey,
    _build_parser,
    _build_provider_client,
    _context_row_indices,
    _normalization_candidates,
    _repairs_for_rows,
    _resolve_collection_settings,
    _teacher_api_key_env,
    collect_episode_trajectories,
    ensure_ready_for_push,
    existing_trajectory_keys,
    main,
    push_trajectory_dataset,
    validate_trajectory_record,
    write_jsonl_records,
)
from scripts.data.validate_sft_readiness import SftReadinessError
from tests.support.corpora import build_fixture_metadata

FIXTURE_REVISION = "fixture"
FIXTURE_DIRTY_SHA256 = "d" * 64
FIXTURE_CLEAN_SHA256 = "c" * 64


def _dataset() -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "Phone": ["2175550101", "not available"],
            "Score": ["5", "45"],
        }
    )
    clean_df = pd.DataFrame(
        {
            "Phone": ["2175550101", "2175550202"],
            "Score": ["5", "4.5"],
        }
    )
    return RealWorldDataset(
        metadata=build_fixture_metadata(
            name="hospital",
            domain="healthcare",
            n_rows=2,
            n_columns=2,
            error_types=("missing_value", "formatting"),
            source_urls=("dirty", "clean"),
            source_revision=FIXTURE_REVISION,
            dirty_sha256=FIXTURE_DIRTY_SHA256,
            clean_sha256=FIXTURE_CLEAN_SHA256,
            citation="fixture citation",
        ),
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=("Phone", "Score"),
        ground_truth=(
            GroundTruthCell(
                row=1,
                column="Phone",
                dirty_value="not available",
                clean_value="2175550202",
            ),
            GroundTruthCell(row=1, column="Score", dirty_value="45", clean_value="4.5"),
        ),
        dirty_sha256=FIXTURE_DIRTY_SHA256,
        clean_sha256=FIXTURE_CLEAN_SHA256,
    )


def _named_dataset(
    name: str,
    dirty_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    canonical_columns: tuple[str, ...],
    ground_truth: tuple[GroundTruthCell, ...],
) -> RealWorldDataset:
    return RealWorldDataset(
        metadata=build_fixture_metadata(
            name=name,
            domain="test",
            n_rows=len(dirty_df.index),
            n_columns=len(canonical_columns),
            error_types=("formatting",),
            source_urls=("dirty", "clean"),
            source_revision=FIXTURE_REVISION,
            dirty_sha256=FIXTURE_DIRTY_SHA256,
            clean_sha256=FIXTURE_CLEAN_SHA256,
            citation="fixture citation",
        ),
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=canonical_columns,
        ground_truth=ground_truth,
        dirty_sha256=FIXTURE_DIRTY_SHA256,
        clean_sha256=FIXTURE_CLEAN_SHA256,
    )


def _beers_dataset() -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "ounces": ["12.0 oz.", "12.0 oz", "12.0 oz. Alumi-Tek"],
            "abv": ["0.065%", "0.05", "N/A"],
            "ibu": ["N/A", "42", "NA"],
        }
    )
    clean_df = pd.DataFrame(
        {
            "ounces": ["12", "12", "12"],
            "abv": ["0.065", "0.05", ""],
            "ibu": ["", "42", ""],
        }
    )
    return _named_dataset(
        "beers",
        dirty_df,
        clean_df,
        ("ounces", "abv", "ibu"),
        (
            GroundTruthCell(row=0, column="ounces", dirty_value="12.0 oz.", clean_value="12"),
            GroundTruthCell(row=0, column="abv", dirty_value="0.065%", clean_value="0.065"),
            GroundTruthCell(row=0, column="ibu", dirty_value="N/A", clean_value=""),
        ),
    )


def _flights_dataset() -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "src": ["aa", "aa"],
            "flight": ["1", "1"],
            "sched_dep_time": ["Fri Dec 2 5:11 a.m.", ""],
            "act_dep_time": ["11:08 p.m.            On Time", "11:08 p.m."],
            "sched_arr_time": ["", "7:45 p.m."],
            "act_arr_time": ["Delayed", "7:45 p.m."],
        }
    )
    clean_df = pd.DataFrame(
        {
            "src": ["aa", "aa"],
            "flight": ["1", "1"],
            "sched_dep_time": ["5:11 a.m.", "5:11 a.m."],
            "act_dep_time": ["11:08 p.m.", "11:08 p.m."],
            "sched_arr_time": ["7:45 p.m.", "7:45 p.m."],
            "act_arr_time": ["Delayed", "7:45 p.m."],
        }
    )
    return _named_dataset(
        "flights",
        dirty_df,
        clean_df,
        (
            "src",
            "flight",
            "sched_dep_time",
            "act_dep_time",
            "sched_arr_time",
            "act_arr_time",
        ),
        (
            GroundTruthCell(
                row=0,
                column="sched_dep_time",
                dirty_value="Fri Dec 2 5:11 a.m.",
                clean_value="5:11 a.m.",
            ),
            GroundTruthCell(
                row=0,
                column="act_dep_time",
                dirty_value="11:08 p.m.            On Time",
                clean_value="11:08 p.m.",
            ),
        ),
    )


def _flights_blank_schedule_dataset() -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "src": ["flylouisville"],
            "flight": ["CO-1586-IAH-MCO"],
            "sched_dep_time": [""],
            "act_dep_time": ["7:14 p.m."],
            "sched_arr_time": [""],
            "act_arr_time": ["10:29 p.m."],
        }
    )
    clean_df = pd.DataFrame(
        {
            "src": ["flylouisville"],
            "flight": ["CO-1586-IAH-MCO"],
            "sched_dep_time": ["7:00 p.m."],
            "act_dep_time": ["7:14 p.m."],
            "sched_arr_time": ["10:15 p.m."],
            "act_arr_time": ["10:29 p.m."],
        }
    )
    return _named_dataset(
        "flights",
        dirty_df,
        clean_df,
        (
            "src",
            "flight",
            "sched_dep_time",
            "act_dep_time",
            "sched_arr_time",
            "act_arr_time",
        ),
        (
            GroundTruthCell(
                row=0,
                column="sched_dep_time",
                dirty_value="",
                clean_value="7:00 p.m.",
            ),
            GroundTruthCell(
                row=0,
                column="sched_arr_time",
                dirty_value="",
                clean_value="10:15 p.m.",
            ),
        ),
    )


@dataclass
class _StubClient:
    responses: list[GroqCompletion]
    model: str = "llama-3.3-70b-versatile"
    provider: str = "groq"

    def __post_init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
        self.calls.append(messages)
        return self.responses.pop(0)


def test_validate_trajectory_record_requires_expert_v1_schema() -> None:
    record = {
        "schema_version": "expert_v1",
        "trajectory_id": "hospital:easy:0:1",
        "task_id": "hospital:easy",
        "dataset": "hospital",
        "difficulty": "easy",
        "seed": 0,
        "chunk_index": 1,
        "state": {"rows": []},
        "tool_calls": [],
        "diagnosis": [],
        "fix": [],
        "messages": [{"role": "user", "content": "repair"}],
        "teacher": {"provider": "groq", "model": "llama-3.3-70b-versatile"},
        "metrics": {"episode_f1": 1.0, "chunk_f1": 0.0},
        "provenance": {"citation": "fixture citation"},
    }

    validated = validate_trajectory_record(record)

    assert validated["schema_version"] == "expert_v1"
    assert validated["trajectory_id"] == "hospital:easy:0:1"


def test_existing_keys_are_chunk_level(tmp_path: Path) -> None:
    output = tmp_path / "expert_v1.jsonl"
    write_jsonl_records(
        output,
        [
            validate_trajectory_record(
                {
                    "schema_version": "expert_v1",
                    "trajectory_id": "hospital:easy:7:0",
                    "task_id": "hospital:easy",
                    "dataset": "hospital",
                    "difficulty": "easy",
                    "seed": 7,
                    "chunk_index": 0,
                    "state": {"rows": []},
                    "tool_calls": [],
                    "diagnosis": [],
                    "fix": [],
                    "messages": [{"role": "user", "content": "repair"}],
                    "teacher": {"provider": "groq", "model": "llama-3.3-70b-versatile"},
                    "metrics": {"episode_f1": 1.0, "chunk_f1": 0.0},
                    "provenance": {"citation": "fixture citation"},
                }
            )
        ],
    )

    assert existing_trajectory_keys(output) == {TrajectoryKey("hospital:easy", 7, 0)}


def test_budget_guard_blocks_over_limit() -> None:
    guard = BudgetGuard(max_total_requests=2)

    guard.consume(2)

    with pytest.raises(RuntimeError, match="request budget"):
        guard.consume(1)


def test_budget_guard_blocks_daily_limit_before_total_limit() -> None:
    guard = BudgetGuard(max_total_requests=10, daily_request_budget=2)

    guard.consume(2)

    with pytest.raises(RuntimeError, match="daily request budget"):
        guard.consume(1)


def test_context_row_indices_can_limit_to_local_window() -> None:
    assert _context_row_indices(10, (4, 5), None) == tuple(range(10))
    assert _context_row_indices(10, (4, 5), 2) == (2, 3, 4, 5, 6, 7)
    assert _context_row_indices(10, (0, 1), 3) == (0, 1, 2, 3, 4)


def test_normalization_candidates_cover_beers_patterns() -> None:
    candidates = _normalization_candidates(
        _beers_dataset(),
        row_indices=(0,),
        context_indices=(0, 1, 2),
    )

    assert {
        (candidate["row"], candidate["column"], candidate["suggested_value"])
        for candidate in candidates
    } == {
        (0, "ounces", "12"),
        (0, "abv", "0.065"),
        (0, "ibu", ""),
    }


def test_normalization_candidates_cover_flights_patterns() -> None:
    candidates = _normalization_candidates(
        _flights_dataset(),
        row_indices=(0,),
        context_indices=(0, 1),
    )

    assert {
        (candidate["row"], candidate["column"], candidate["suggested_value"])
        for candidate in candidates
    } == {
        (0, "sched_dep_time", "5:11 a.m."),
        (0, "act_dep_time", "11:08 p.m."),
        (0, "sched_arr_time", "7:45 p.m."),
    }


def test_normalization_candidates_cover_flights_date_suffix_and_notes() -> None:
    dataset = _named_dataset(
        "flights",
        pd.DataFrame(
            {
                "src": ["aa", "aa"],
                "flight": ["1", "1"],
                "sched_dep_time": ["6:55 a.m. Fri 02-Dec-2011", ""],
                "act_dep_time": ["", "7:00 a.m."],
                "sched_arr_time": ["", "8:00 a.m."],
                "act_arr_time": ["3:20 p.m. (Runway)", ""],
            }
        ),
        pd.DataFrame(
            {
                "src": ["aa", "aa"],
                "flight": ["1", "1"],
                "sched_dep_time": ["6:55 a.m.", "6:55 a.m."],
                "act_dep_time": ["", "7:00 a.m."],
                "sched_arr_time": ["8:00 a.m.", "8:00 a.m."],
                "act_arr_time": ["3:30 p.m.", ""],
            }
        ),
        (
            "src",
            "flight",
            "sched_dep_time",
            "act_dep_time",
            "sched_arr_time",
            "act_arr_time",
        ),
        (),
    )

    candidates = _normalization_candidates(dataset, row_indices=(0,), context_indices=(0,))

    assert {
        (candidate["row"], candidate["column"], candidate["suggested_value"])
        for candidate in candidates
    } == {
        (0, "sched_dep_time", "6:55 a.m."),
        (0, "act_dep_time", "7:00 a.m."),
        (0, "sched_arr_time", "8:00 a.m."),
    }


def test_normalization_candidates_skip_ambiguous_blank_flight_reference() -> None:
    dataset = _named_dataset(
        "flights",
        pd.DataFrame(
            {
                "src": ["aa", "aa", "aa"],
                "flight": ["1", "1", "1"],
                "sched_dep_time": ["", "6:55 a.m.", "7:05 a.m."],
                "act_dep_time": ["", "", ""],
                "sched_arr_time": ["", "", ""],
                "act_arr_time": ["", "", ""],
            }
        ),
        pd.DataFrame(
            {
                "src": ["aa", "aa", "aa"],
                "flight": ["1", "1", "1"],
                "sched_dep_time": ["", "6:55 a.m.", "7:05 a.m."],
                "act_dep_time": ["", "", ""],
                "sched_arr_time": ["", "", ""],
                "act_arr_time": ["", "", ""],
            }
        ),
        (
            "src",
            "flight",
            "sched_dep_time",
            "act_dep_time",
            "sched_arr_time",
            "act_arr_time",
        ),
        (),
    )

    candidates = _normalization_candidates(dataset, row_indices=(0,), context_indices=(0,))

    assert candidates == []


def test_normalization_candidates_cover_flights_actual_time_context() -> None:
    dataset = _named_dataset(
        "flights",
        pd.DataFrame(
            {
                "src": ["aa"],
                "flight": ["1"],
                "sched_dep_time": ["4:00 p.m."],
                "act_dep_time": ["3:58 p.m."],
                "sched_arr_time": ["6:46 p.m."],
                "act_arr_time": [""],
            }
        ),
        pd.DataFrame(
            {
                "src": ["aa"],
                "flight": ["1"],
                "sched_dep_time": ["4:00 p.m."],
                "act_dep_time": ["4:00 p.m."],
                "sched_arr_time": ["6:46 p.m."],
                "act_arr_time": ["6:46 p.m."],
            }
        ),
        (
            "src",
            "flight",
            "sched_dep_time",
            "act_dep_time",
            "sched_arr_time",
            "act_arr_time",
        ),
        (),
    )

    candidates = _normalization_candidates(dataset, row_indices=(0,), context_indices=(0,))

    assert {
        (candidate["row"], candidate["column"], candidate["suggested_value"])
        for candidate in candidates
    } == {
        (0, "act_dep_time", "4:00 p.m."),
        (0, "act_arr_time", "6:46 p.m."),
    }


def test_smoke_preset_resolves_laptop_safe_defaults() -> None:
    args = _build_parser().parse_args(["--preset", "smoke"])

    settings = _resolve_collection_settings(args)

    assert settings.preset == "smoke"
    assert settings.datasets == ["hospital"]
    assert settings.difficulties == ["easy"]
    assert settings.max_trajectories == 32
    assert settings.max_total_requests == 256
    assert settings.max_runtime_min == 30.0
    assert settings.ready_min_records == 32


def test_explicit_timeout_and_retry_flags_override_preset_defaults() -> None:
    args = _build_parser().parse_args(
        [
            "--preset",
            "smoke",
            "--datasets",
            "beers",
            "--difficulties",
            "medium",
            "--seeds",
            "3",
            "--groq-timeout-s",
            "7",
            "--groq-max-retries",
            "4",
            "--progress-every-chunks",
            "0",
        ]
    )

    settings = _resolve_collection_settings(args)

    assert settings.datasets == ["beers"]
    assert settings.difficulties == ["medium"]
    assert settings.seeds == 3
    assert settings.teacher_timeout_s == 7.0
    assert settings.teacher_max_retries == 4
    assert settings.progress_every_chunks == 0


def test_cerebras_provider_resolves_default_model_and_env_key() -> None:
    args = _build_parser().parse_args(["--preset", "smoke", "--teacher-provider", "cerebras"])

    settings = _resolve_collection_settings(args)

    assert settings.teacher_provider == "cerebras"
    assert settings.teacher_model == "llama3.1-8b"
    assert settings.min_interval_s == 2.1


def test_gemini_provider_resolves_default_model() -> None:
    args = _build_parser().parse_args(["--preset", "smoke", "--teacher-provider", "gemini"])

    settings = _resolve_collection_settings(args)

    assert settings.teacher_provider == "gemini"
    assert settings.teacher_model == "gemini-3.1-pro-preview"


def test_azure_teacher_resolves_deployment_from_env_and_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Azure teacher: deployment comes from DATAFORGE_AZURE_MODEL; key is AZURE_API_KEY."""
    monkeypatch.setenv("DATAFORGE_AZURE_MODEL", "gpt-5.5")
    args = _build_parser().parse_args(["--preset", "smoke", "--teacher-provider", "azure"])

    settings = _resolve_collection_settings(args)

    assert settings.teacher_provider == "azure"
    assert settings.teacher_model == "gpt-5.5"
    assert _teacher_api_key_env("azure") == "AZURE_API_KEY"


def test_azure_teacher_client_uses_endpoint_and_deployment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_build_provider_client builds an AzureBenchClient from env endpoint."""
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://my-res.openai.azure.com")
    monkeypatch.setenv("AZURE_OPENAI_API_VERSION", "2025-04-01-preview")

    client = _build_provider_client(
        provider="azure",
        model="gpt-5.5",
        api_key="test-key",
        min_interval_s=1.0,
        max_tokens=256,
        max_retries=3,
        max_retry_after_s=60.0,
        timeout_s=30.0,
    )

    assert client.provider == "azure"
    assert client.model == "gpt-5.5"


def test_azure_teacher_client_requires_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing AZURE_OPENAI_ENDPOINT fails fast with an actionable message."""
    monkeypatch.delenv("AZURE_OPENAI_ENDPOINT", raising=False)

    with pytest.raises(ValueError, match="AZURE_OPENAI_ENDPOINT"):
        _build_provider_client(
            provider="azure",
            model="gpt-5.5",
            api_key="test-key",
            min_interval_s=1.0,
            max_tokens=256,
            max_retries=3,
            max_retry_after_s=60.0,
            timeout_s=30.0,
        )


def test_flights_verified_mode_resolves_verifier_model() -> None:
    args = _build_parser().parse_args(
        [
            "--preset",
            "smoke",
            "--flights-repair-mode",
            "verified",
            "--flights-verifier-model",
            "llama-3.3-70b-versatile",
        ]
    )

    settings = _resolve_collection_settings(args)

    assert settings.flights_repair_mode == "verified"
    assert settings.flights_verifier_model == "llama-3.3-70b-versatile"


def test_deadline_stops_before_stub_client_is_called() -> None:
    client = _StubClient(
        [
            GroqCompletion(
                text='{"action":"finish"}', prompt_tokens=1, completion_tokens=1, warnings=()
            )
        ]
    )

    with pytest.raises(RuntimeError, match="runtime deadline"):
        collect_episode_trajectories(
            _dataset(),
            difficulty="easy",
            seed=0,
            client=client,
            existing_keys=set(),
            budget=BudgetGuard(max_total_requests=4),
            min_episode_f1=0.6,
            deadline=RuntimeDeadline(started_at=time.monotonic() - 10, max_runtime_s=1),
        )

    assert client.calls == []


def test_push_readiness_gate_refuses_missing_jsonl(tmp_path: Path) -> None:
    with pytest.raises(SftReadinessError, match="Missing trajectory JSONL"):
        ensure_ready_for_push(output=tmp_path / "missing.jsonl", ready_min_records=32)


def test_push_trajectory_dataset_uploads_dataset_readme(tmp_path: Path) -> None:
    class _FakeApi:
        def __init__(self) -> None:
            self.uploads: list[str] = []

        def create_repo(
            self,
            *,
            repo_id: str,
            repo_type: str,
            exist_ok: bool,
            token: str | None = None,
        ) -> object:
            return object()

        def upload_file(
            self,
            *,
            path_or_fileobj: str,
            path_in_repo: str,
            repo_id: str,
            repo_type: str,
            token: str | None = None,
            commit_message: str,
        ) -> object:
            self.uploads.append(path_in_repo)
            return object()

    output = tmp_path / "expert_v1.jsonl"
    output.write_text("{}\n", encoding="utf-8")
    api = _FakeApi()

    push_trajectory_dataset(output=output, repo_id="tester/data", token=None, api=api)

    assert api.uploads == [
        "expert_v1.jsonl",
        "README.md",
        "sft_05b.yaml",
        "MODEL_CARD_TEMPLATE.md",
        "split_manifest.json",
    ]


def test_dataset_readme_documents_oracle_labels_and_smoke_lineage() -> None:
    readme = Path("training/DATASET_README.md").read_text(encoding="utf-8")

    assert "expert_v1.jsonl" in readme
    assert "oracle_from_clean_diff" in readme
    assert "v0-smoke" in readme
    assert "not a performance-improvement claim" in readme
    assert "Groq, Cerebras, or Gemini" in readme
    assert "discoveries" in readme
    assert "Raha" in readme


def test_repairs_for_rows_filters_context_row_repairs() -> None:
    repairs = [
        BenchmarkRepair(row=0, column="Score", new_value="5", reason="context"),
        BenchmarkRepair(row=1, column="Score", new_value="4.5", reason="target"),
    ]

    filtered = _repairs_for_rows(repairs, (1,))

    assert filtered == [repairs[1]]


def test_collect_episode_filters_low_f1() -> None:
    client = _StubClient(
        [
            GroqCompletion(
                text='{"action":"finish"}', prompt_tokens=1, completion_tokens=1, warnings=()
            ),
            GroqCompletion(
                text='{"action":"finish"}', prompt_tokens=1, completion_tokens=1, warnings=()
            ),
        ]
    )

    records = collect_episode_trajectories(
        _dataset(),
        difficulty="easy",
        seed=0,
        client=client,
        existing_keys={
            TrajectoryKey("beers:easy", 0, 1),
            TrajectoryKey("beers:easy", 0, 2),
        },
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
    )

    assert records == []
    assert len(client.calls) == 2


def test_finish_with_candidates_triggers_validation_retry() -> None:
    client = _StubClient(
        [
            GroqCompletion(
                text='{"action":"finish"}',
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"ounces","new_value":"12","reason":"strip unit"},'
                    '{"row":0,"column":"abv","new_value":"0.065","reason":"strip percent"},'
                    '{"row":0,"column":"ibu","new_value":"","reason":"placeholder"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]
    )

    records = collect_episode_trajectories(
        _beers_dataset(),
        difficulty="easy",
        seed=0,
        client=client,
        existing_keys={
            TrajectoryKey("beers:easy", 0, 1),
            TrajectoryKey("beers:easy", 0, 2),
        },
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
        context_window_rows=4,
    )

    assert len(client.calls) == 2
    assert records[0]["metrics"]["warnings"] == ["validation_retry"]
    assert records[0]["metrics"]["episode_f1"] == 1.0


def test_invalid_json_triggers_validation_retry() -> None:
    client = _StubClient(
        [
            GroqCompletion(text="not json", prompt_tokens=1, completion_tokens=1, warnings=()),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"ounces","new_value":"12","reason":"strip unit"},'
                    '{"row":0,"column":"abv","new_value":"0.065","reason":"strip percent"},'
                    '{"row":0,"column":"ibu","new_value":"","reason":"placeholder"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]
    )

    records = collect_episode_trajectories(
        _beers_dataset(),
        difficulty="easy",
        seed=0,
        client=client,
        existing_keys={
            TrajectoryKey("beers:easy", 0, 1),
            TrajectoryKey("beers:easy", 0, 2),
        },
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
        context_window_rows=4,
    )

    assert len(client.calls) == 2
    assert records[0]["metrics"]["episode_f1"] == 1.0


def test_invalid_repairs_trigger_retry_and_filtering() -> None:
    client = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":99,"column":"ounces","new_value":"12","reason":"bad row"},'
                    '{"row":0,"column":"bad","new_value":"12","reason":"bad column"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"ounces","new_value":"12","reason":"strip unit"},'
                    '{"row":0,"column":"abv","new_value":"0.065","reason":"strip percent"},'
                    '{"row":0,"column":"ibu","new_value":"","reason":"placeholder"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]
    )

    records = collect_episode_trajectories(
        _beers_dataset(),
        difficulty="easy",
        seed=0,
        client=client,
        existing_keys={
            TrajectoryKey("beers:easy", 0, 1),
            TrajectoryKey("beers:easy", 0, 2),
        },
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
        context_window_rows=4,
    )

    assert len(client.calls) == 2
    assert {repair["column"] for repair in records[0]["fix"]} == {"ounces", "abv", "ibu"}


def test_flights_requires_candidate_backed_repairs_after_retry() -> None:
    client = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"5:11 a.m.",'
                    '"reason":"strip date"},'
                    '{"row":0,"column":"act_arr_time","new_value":"3:30 p.m.",'
                    '"reason":"guess runway correction"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"5:11 a.m.",'
                    '"reason":"strip date"},'
                    '{"row":0,"column":"act_arr_time","new_value":"3:30 p.m.",'
                    '"reason":"guess runway correction"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]
    )

    records = collect_episode_trajectories(
        _flights_dataset(),
        difficulty="easy",
        seed=0,
        client=client,
        existing_keys={TrajectoryKey("flights:easy", 0, 1)},
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
        context_window_rows=4,
    )

    assert len(client.calls) == 2
    assert records[0]["fix"] == [
        {
            "row": 0,
            "column": "sched_dep_time",
            "new_value": "5:11 a.m.",
            "reason": "strip date",
        }
    ]
    assert records[0]["metrics"]["warnings"] == [
        "validation_retry",
        "unsupported_flights_repairs_dropped",
    ]
    assert records[0]["metrics"]["unsupported_flights_repairs_dropped"] == 1


def test_flights_verified_mode_requires_verifier_approval() -> None:
    teacher = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"7:00 p.m.",'
                    '"reason":"infer scheduled departure","evidence":"actual departure is nearby",'
                    '"confidence":0.82},'
                    '{"row":0,"column":"sched_arr_time","new_value":"10:15 p.m.",'
                    '"reason":"infer scheduled arrival","evidence":"actual arrival is nearby",'
                    '"confidence":0.82}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            )
        ]
    )
    verifier = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"verify_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"7:00 p.m.",'
                    '"approved":true,"reason":"near actual departure"},'
                    '{"row":0,"column":"sched_arr_time","new_value":"10:15 p.m.",'
                    '"approved":true,"reason":"near actual arrival"}]}'
                ),
                prompt_tokens=2,
                completion_tokens=3,
                warnings=(),
            )
        ],
        model="verifier-model",
    )

    records = collect_episode_trajectories(
        _flights_blank_schedule_dataset(),
        difficulty="easy",
        seed=0,
        client=teacher,
        verifier_client=verifier,
        flights_repair_mode="verified",
        existing_keys=set(),
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
        context_window_rows=4,
    )

    assert len(teacher.calls) == 1
    assert len(verifier.calls) == 1
    assert {repair["column"] for repair in records[0]["fix"]} == {
        "sched_dep_time",
        "sched_arr_time",
    }
    assert records[0]["metrics"]["episode_f1"] == 1.0
    assert records[0]["metrics"]["flights_repair_mode"] == "verified"
    assert records[0]["metrics"]["flights_teacher_proposed_repairs"] == 2
    assert records[0]["metrics"]["flights_verified_repairs"] == 2
    assert records[0]["metrics"]["flights_dropped_repairs"] == 0
    assert records[0]["metrics"]["flights_verifier_llm_calls"] == 1


def test_flights_verified_mode_drops_unverified_teacher_repairs() -> None:
    teacher = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"7:00 p.m.",'
                    '"reason":"infer scheduled departure","evidence":"actual departure is nearby",'
                    '"confidence":0.82}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            )
        ]
    )

    records = collect_episode_trajectories(
        _flights_blank_schedule_dataset(),
        difficulty="easy",
        seed=0,
        client=teacher,
        verifier_client=None,
        flights_repair_mode="verified",
        existing_keys=set(),
        budget=BudgetGuard(max_total_requests=2),
        min_episode_f1=0.0,
        context_window_rows=4,
    )

    assert records[0]["fix"] == []
    assert records[0]["metrics"]["flights_teacher_proposed_repairs"] == 1
    assert records[0]["metrics"]["flights_verified_repairs"] == 0
    assert records[0]["metrics"]["flights_dropped_repairs"] == 1


def test_flights_verified_mode_retries_malformed_teacher_proposal() -> None:
    teacher = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"7:00 p.m.",'
                    '"reason":"infer scheduled departure"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"7:00 p.m.",'
                    '"reason":"infer scheduled departure","evidence":"actual departure is nearby",'
                    '"confidence":0.82}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]
    )
    verifier = _StubClient(
        [
            GroqCompletion(
                text=(
                    '{"action":"verify_repairs","repairs":['
                    '{"row":0,"column":"sched_dep_time","new_value":"7:00 p.m.",'
                    '"approved":true,"reason":"near actual departure"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            )
        ]
    )

    records = collect_episode_trajectories(
        _flights_blank_schedule_dataset(),
        difficulty="easy",
        seed=0,
        client=teacher,
        verifier_client=verifier,
        flights_repair_mode="verified",
        existing_keys=set(),
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.0,
        context_window_rows=4,
    )

    assert len(teacher.calls) == 2
    assert records[0]["metrics"]["warnings"] == ["validation_retry"]
    assert records[0]["fix"] == [
        {
            "row": 0,
            "column": "sched_dep_time",
            "new_value": "7:00 p.m.",
            "reason": "infer scheduled departure",
        }
    ]


def test_collect_episode_emits_auditable_chunk_records_when_episode_passes() -> None:
    client = _StubClient(
        [
            GroqCompletion(
                text='{"action":"finish"}', prompt_tokens=1, completion_tokens=1, warnings=()
            ),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":1,"column":"Phone","new_value":"2175550202","reason":"phone"},'
                    '{"row":1,"column":"Score","new_value":"4.5","reason":"score"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]
    )

    records = collect_episode_trajectories(
        _dataset(),
        difficulty="medium",
        seed=3,
        client=client,
        existing_keys=set(),
        budget=BudgetGuard(max_total_requests=4),
        min_episode_f1=0.6,
    )

    assert [record["chunk_index"] for record in records] == [0, 1]
    assert records[1]["fix"][0]["new_value"] == "2175550202"
    assert records[1]["metrics"]["episode_f1"] == 1.0
    assert records[1]["messages"][-1]["role"] == "assistant"


def test_main_slices_accepted_episode_to_remaining_trajectory_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FakeGroqClient:
        responses = [
            GroqCompletion(
                text='{"action":"finish"}', prompt_tokens=1, completion_tokens=1, warnings=()
            ),
            GroqCompletion(
                text=(
                    '{"action":"submit_repairs","repairs":['
                    '{"row":1,"column":"Phone","new_value":"2175550202","reason":"phone"},'
                    '{"row":1,"column":"Score","new_value":"4.5","reason":"score"}]}'
                ),
                prompt_tokens=1,
                completion_tokens=1,
                warnings=(),
            ),
        ]

        def __init__(self, *, model: str, **_: object) -> None:
            self.model = model
            self.provider = "groq"

        def complete(self, messages: list[dict[str, str]]) -> GroqCompletion:
            return self.responses.pop(0)

    output = tmp_path / "expert_v1.jsonl"
    monkeypatch.setenv("DATAFORGE_LLM_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr("scripts.data.collect_sft_trajectories.GroqBenchClient", _FakeGroqClient)
    monkeypatch.setattr(
        "scripts.data.collect_sft_trajectories.load_real_world_dataset",
        lambda *_args, **_kwargs: _dataset(),
    )

    result = main(
        [
            "--preset",
            "smoke",
            "--output",
            str(output),
            "--max-trajectories",
            "1",
            "--seeds",
            "1",
            "--max-total-requests",
            "4",
            "--daily-request-budget",
            "4",
            "--progress-every-chunks",
            "0",
        ]
    )

    assert result == 0
    assert len(output.read_text(encoding="utf-8").splitlines()) == 1
