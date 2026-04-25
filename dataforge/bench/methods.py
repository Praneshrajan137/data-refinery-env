"""Benchmark method implementations for DataForge."""

from __future__ import annotations

import json
import random
import time
from math import ceil
from statistics import median

from dataforge.bench.core import BenchmarkRepair, SeedBenchmarkResult, quota_units, score_repairs
from dataforge.bench.groq_client import GroqBenchClient
from dataforge.datasets.real_world import RealWorldDataset
from dataforge.detectors import run_all_detectors
from dataforge.repairers import propose_fixes


def _reproduction_command(method: str, dataset: str, seeds: int) -> str:
    """Build the canonical reproduction command for one method/dataset selection."""
    return f"dataforge bench --methods {method} --datasets {dataset} --seeds {seeds}"


def _repairs_from_proposed_fixes(dataset: RealWorldDataset) -> list[BenchmarkRepair]:
    """Run the shipped deterministic detector/repair stack on one dataset."""
    issues = run_all_detectors(dataset.dirty_df.copy(deep=True), schema=None)
    proposals = propose_fixes(
        issues,
        dataset.dirty_df.copy(deep=True),
        None,
        cache_dir=None,
        allow_llm=False,
    )
    return [
        BenchmarkRepair(
            row=proposal.fix.row,
            column=proposal.fix.column,
            new_value=proposal.fix.new_value,
            reason=proposal.reason,
        )
        for proposal in proposals
    ]


def run_heuristic_episode(dataset: RealWorldDataset, *, seed: int) -> SeedBenchmarkResult:
    """Run the current deterministic DataForge stack as the heuristic baseline."""
    start = time.perf_counter()
    repairs = _repairs_from_proposed_fixes(dataset)
    metrics = score_repairs(dataset.ground_truth, repairs)
    runtime_s = round(time.perf_counter() - start, 4)
    return SeedBenchmarkResult(
        method="heuristic",
        dataset=dataset.metadata.name,
        seed=seed,
        status="ok",
        precision=metrics.precision,
        recall=metrics.recall,
        f1=metrics.f1,
        tp=metrics.tp,
        fp=metrics.fp,
        fn=metrics.fn,
        avg_steps=float(1 + len(repairs)),
        llm_calls=0,
        prompt_tokens=0,
        completion_tokens=0,
        quota_units=0.0,
        runtime_s=runtime_s,
        provider="local",
        model="deterministic",
        reproduction_command=_reproduction_command("heuristic", dataset.metadata.name, 1),
    )


def run_random_episode(dataset: RealWorldDataset, *, seed: int) -> SeedBenchmarkResult:
    """Run the bounded random baseline on one dataset."""
    rng = random.Random(seed)
    start = time.perf_counter()
    budget = min(200, max(25, ceil(len(dataset.ground_truth) / 10)))
    column_values = {
        column: [str(value) for value in dataset.dirty_df[column].tolist()]
        for column in dataset.canonical_columns
    }
    repairs: list[BenchmarkRepair] = []
    for _ in range(budget):
        row_index = rng.randrange(len(dataset.dirty_df.index))
        column = rng.choice(dataset.canonical_columns)
        new_value = rng.choice(column_values[column])
        repairs.append(
            BenchmarkRepair(
                row=row_index,
                column=column,
                new_value=new_value,
                reason="random baseline",
            )
        )
    metrics = score_repairs(dataset.ground_truth, repairs)
    runtime_s = round(time.perf_counter() - start, 4)
    return SeedBenchmarkResult(
        method="random",
        dataset=dataset.metadata.name,
        seed=seed,
        status="ok",
        precision=metrics.precision,
        recall=metrics.recall,
        f1=metrics.f1,
        tp=metrics.tp,
        fp=metrics.fp,
        fn=metrics.fn,
        avg_steps=float(budget),
        llm_calls=0,
        prompt_tokens=0,
        completion_tokens=0,
        quota_units=0.0,
        runtime_s=runtime_s,
        provider="local",
        model="random",
        reproduction_command=_reproduction_command("random", dataset.metadata.name, 1),
    )


def _chunk_records(dataset: RealWorldDataset, row_indices: tuple[int, ...]) -> list[dict[str, str]]:
    """Serialize one row chunk for prompting."""
    records: list[dict[str, str]] = []
    for row_index in row_indices:
        row_payload: dict[str, str] = {"_row": str(row_index)}
        for column in dataset.canonical_columns:
            row_payload[column] = str(dataset.dirty_df.iloc[row_index][column])
        records.append(row_payload)
    return records


def _column_stats(
    dataset: RealWorldDataset, columns: list[str]
) -> dict[str, dict[str, str | float | int]]:
    """Return simple benchmark-local column statistics for ReAct prompting."""
    stats: dict[str, dict[str, str | float | int]] = {}
    for column in columns:
        series = dataset.dirty_df[column].astype(str)
        non_empty = [value for value in series.tolist() if value != ""]
        numeric_values: list[float] = []
        for value in non_empty:
            try:
                numeric_values.append(float(value))
            except ValueError:
                continue
        stats[column] = {
            "non_empty_count": len(non_empty),
            "unique_count": len(set(non_empty)),
        }
        if numeric_values:
            stats[column]["median"] = round(float(median(numeric_values)), 4)
    return stats


def _extract_json_object(text: str) -> dict[str, object] | None:
    """Parse the first JSON object found in an LLM response string."""
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:].strip()
    decoder = json.JSONDecoder()
    for offset, char in enumerate(stripped):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(stripped[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _repairs_from_payload(payload: dict[str, object]) -> list[BenchmarkRepair]:
    """Convert a parsed JSON payload into benchmark repairs."""
    raw_repairs = payload.get("repairs", [])
    if not isinstance(raw_repairs, list):
        return []
    repairs: list[BenchmarkRepair] = []
    for raw_repair in raw_repairs:
        if not isinstance(raw_repair, dict):
            continue
        row = raw_repair.get("row")
        column = raw_repair.get("column")
        new_value = raw_repair.get("new_value")
        reason = raw_repair.get("reason", "LLM repair")
        if (
            not isinstance(row, int)
            or not isinstance(column, str)
            or not isinstance(new_value, str)
        ):
            continue
        repairs.append(
            BenchmarkRepair(
                row=row,
                column=column,
                new_value=new_value,
                reason=str(reason),
            )
        )
    return repairs


def run_llm_zeroshot_episode(
    dataset: RealWorldDataset,
    *,
    seed: int,
    client: GroqBenchClient,
) -> SeedBenchmarkResult:
    """Run the zero-shot Groq baseline across fixed contiguous row chunks."""
    start = time.perf_counter()
    llm_calls = 0
    prompt_tokens = 0
    completion_tokens = 0
    warnings: list[str] = []
    repairs: list[BenchmarkRepair] = []

    for row_indices in chunk_row_indices(len(dataset.dirty_df.index)):
        chunk_payload = _chunk_records(dataset, row_indices)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are benchmarking tabular data cleaning. Reply with strict JSON: "
                    '{"repairs":[{"row":0,"column":"Column","new_value":"value","reason":"why"}]}.'
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "dataset": dataset.metadata.name,
                        "columns": list(dataset.canonical_columns),
                        "rows": chunk_payload,
                    },
                    sort_keys=True,
                ),
            },
        ]
        completion = client.complete(messages)
        llm_calls += 1
        prompt_tokens += completion.prompt_tokens
        completion_tokens += completion.completion_tokens
        warnings.extend(list(completion.warnings))
        parsed = _extract_json_object(completion.text)
        if parsed is not None:
            repairs.extend(_repairs_from_payload(parsed))

    metrics = score_repairs(dataset.ground_truth, repairs)
    runtime_s = round(time.perf_counter() - start, 4)
    return SeedBenchmarkResult(
        method="llm_zeroshot",
        dataset=dataset.metadata.name,
        seed=seed,
        status="ok",
        precision=metrics.precision,
        recall=metrics.recall,
        f1=metrics.f1,
        tp=metrics.tp,
        fp=metrics.fp,
        fn=metrics.fn,
        avg_steps=float(llm_calls),
        llm_calls=llm_calls,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        quota_units=quota_units(
            llm_calls=llm_calls,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        ),
        runtime_s=runtime_s,
        provider="groq",
        model=client.model,
        warnings=warnings,
        reproduction_command=_reproduction_command("llm_zeroshot", dataset.metadata.name, 1),
    )


def run_llm_react_episode(
    dataset: RealWorldDataset,
    *,
    seed: int,
    client: GroqBenchClient,
) -> SeedBenchmarkResult:
    """Run the constrained ReAct-style Groq baseline with one optional tool step."""
    start = time.perf_counter()
    llm_calls = 0
    tool_calls = 0
    prompt_tokens = 0
    completion_tokens = 0
    warnings: list[str] = []
    repairs: list[BenchmarkRepair] = []

    for row_indices in chunk_row_indices(len(dataset.dirty_df.index)):
        chunk_payload = _chunk_records(dataset, row_indices)
        schema_summary = {
            "dataset": dataset.metadata.name,
            "columns": list(dataset.canonical_columns),
            "chunk_rows": len(row_indices),
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are benchmarking tabular data cleaning with a constrained tool loop. "
                    "Respond with one JSON action object. Allowed actions: "
                    "inspect_rows, column_stats, submit_repairs, finish."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "schema_summary": schema_summary,
                        "rows": chunk_payload,
                    },
                    sort_keys=True,
                ),
            },
        ]
        first = client.complete(messages)
        llm_calls += 1
        prompt_tokens += first.prompt_tokens
        completion_tokens += first.completion_tokens
        warnings.extend(list(first.warnings))
        first_payload = _extract_json_object(first.text)
        if first_payload is None:
            continue

        action = first_payload.get("action")
        if action == "submit_repairs":
            repairs.extend(_repairs_from_payload(first_payload))
            continue
        if action == "finish":
            continue

        tool_result: dict[str, object]
        if action == "inspect_rows":
            requested_rows = first_payload.get("row_indices", [])
            if not isinstance(requested_rows, list):
                requested_rows = []
            safe_rows = [
                row for row in requested_rows if isinstance(row, int) and row in row_indices
            ]
            tool_result = {"rows": _chunk_records(dataset, tuple(safe_rows))}
        elif action == "column_stats":
            requested_columns = first_payload.get("columns", [])
            if not isinstance(requested_columns, list):
                requested_columns = []
            safe_columns = [
                column
                for column in requested_columns
                if isinstance(column, str) and column in dataset.canonical_columns
            ]
            tool_result = {"column_stats": _column_stats(dataset, safe_columns)}
        else:
            continue
        tool_calls += 1
        messages.append({"role": "assistant", "content": first.text})
        messages.append({"role": "user", "content": json.dumps(tool_result, sort_keys=True)})
        second = client.complete(messages)
        llm_calls += 1
        prompt_tokens += second.prompt_tokens
        completion_tokens += second.completion_tokens
        warnings.extend(list(second.warnings))
        second_payload = _extract_json_object(second.text)
        if second_payload is not None and second_payload.get("action") == "submit_repairs":
            repairs.extend(_repairs_from_payload(second_payload))

    metrics = score_repairs(dataset.ground_truth, repairs)
    runtime_s = round(time.perf_counter() - start, 4)
    return SeedBenchmarkResult(
        method="llm_react",
        dataset=dataset.metadata.name,
        seed=seed,
        status="ok",
        precision=metrics.precision,
        recall=metrics.recall,
        f1=metrics.f1,
        tp=metrics.tp,
        fp=metrics.fp,
        fn=metrics.fn,
        avg_steps=float(llm_calls + tool_calls),
        llm_calls=llm_calls,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        quota_units=quota_units(
            llm_calls=llm_calls,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        ),
        runtime_s=runtime_s,
        provider="groq",
        model=client.model,
        warnings=warnings,
        reproduction_command=_reproduction_command("llm_react", dataset.metadata.name, 1),
    )


def chunk_row_indices(n_rows: int) -> tuple[tuple[int, ...], ...]:
    """Local import wrapper that avoids circular imports in the LLM helpers."""
    from dataforge.bench.core import chunk_row_indices as _chunk_row_indices

    return _chunk_row_indices(n_rows)
