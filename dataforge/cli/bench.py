"""CLI subcommand: ``dataforge bench``."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

_console = Console(stderr=True)
run_agent_comparison: Callable[..., Any] | None = None


def _parse_csv_list(raw_value: str) -> list[str]:
    """Parse a comma-separated CLI option into a list of strings."""
    values = [item.strip() for item in raw_value.split(",")]
    return [value for value in values if value]


def _parse_seed_list(raw_value: str | None) -> list[int] | None:
    """Parse an optional comma-separated seed list."""
    if raw_value is None:
        return None
    seeds = [item.strip() for item in raw_value.split(",") if item.strip()]
    return [int(seed) for seed in seeds]


def _runner() -> Callable[..., Any]:
    """Load the benchmark runner lazily so core CLI imports stay lightweight."""
    global run_agent_comparison
    if run_agent_comparison is None:
        from dataforge.bench.runner import run_agent_comparison as loaded_runner

        run_agent_comparison = loaded_runner
    return run_agent_comparison


def _render_summary(output: Any) -> None:
    """Render the aggregate F1/quota summary table."""
    table = Table(title="DataForge Benchmark Summary")
    table.add_column("Method")
    table.add_column("Dataset")
    table.add_column("Status")
    table.add_column("F1")
    table.add_column("Avg Steps")
    table.add_column("Quota")
    for aggregate in output.aggregates:
        table.add_row(
            aggregate.method,
            aggregate.dataset,
            aggregate.status,
            "Skipped" if aggregate.f1_mean is None else f"{aggregate.f1_mean:.4f}",
            "Skipped" if aggregate.avg_steps_mean is None else f"{aggregate.avg_steps_mean:.2f}",
            "Skipped"
            if aggregate.quota_units_mean is None
            else f"{aggregate.quota_units_mean:.4f}",
        )
    Console().print(table)
    if any(aggregate.status == "skipped" for aggregate in output.aggregates):
        Console().print(
            Panel(
                "Some LLM baselines were skipped. Set DATAFORGE_LLM_PROVIDER=groq and "
                "GROQ_API_KEY to enable them.",
                title="Benchmark Warning",
                style="yellow",
            )
        )


def _render_coverage_matrix(output: Any) -> None:
    """Render the per-error-class recall matrix (honest coverage view)."""
    from dataforge.bench.error_classes import BENCH_ERROR_CLASSES, class_coverage_matrix

    matrix = class_coverage_matrix(list(output.records))
    if not matrix:
        return
    table = Table(title="Per-Error-Class Detection Recall (coverage)")
    table.add_column("Method/Dataset")
    for error_class in BENCH_ERROR_CLASSES:
        table.add_column(error_class)
    for (method, dataset), scores in sorted(matrix.items()):
        row = [f"{method}/{dataset}"]
        for error_class in BENCH_ERROR_CLASSES:
            score = scores.get(error_class)
            if score is None or score.support == 0:
                row.append("-")
            else:
                row.append(f"{score.detection_recall:.2f}/{score.recall:.2f} (n={score.support})")
        table.add_row(*row)
    Console().print(table)
    Console().print(
        Panel(
            "Each cell shows detection_recall/correction_recall per error class on "
            "the full RAHA datasets ('-' = no ground-truth cells of that class). "
            "Detection credits flagging the error; correction credits producing the "
            "exact right value. The honest split: a class can be well-detected yet "
            "not auto-correctable (no derivable value), which is reported, not hidden.",
            title="Coverage",
            style="cyan",
        )
    )


def bench(
    methods: Annotated[
        str,
        typer.Option(
            "--methods",
            help="Comma-separated benchmark methods.",
        ),
    ] = "heuristic,llm_zeroshot",
    datasets: Annotated[
        str,
        typer.Option(
            "--datasets",
            help="Comma-separated benchmark datasets.",
        ),
    ] = "hospital",
    seeds: Annotated[
        int,
        typer.Option("--seeds", help="Number of seeds per method/dataset pair."),
    ] = 3,
    seed_list: Annotated[
        str | None,
        typer.Option(
            "--seed-list",
            help="Explicit comma-separated seed list. Overrides --seeds for reproducibility.",
        ),
    ] = None,
    really_run_big_bench: Annotated[
        bool,
        typer.Option(
            "--really-run-big-bench",
            help="Override the free-tier benchmark quota guard when estimated calls exceed 500.",
        ),
    ] = False,
    output_json: Annotated[
        Path,
        typer.Option(
            "--output-json",
            help="Where to write eval/results/agent_comparison.json.",
        ),
    ] = Path("eval/results/agent_comparison.json"),
    cache_root: Annotated[
        Path | None,
        typer.Option(
            "--cache-root",
            help="Benchmark dataset cache root. Defaults to the user DataForge cache.",
        ),
    ] = None,
    verify_dataset_hashes: Annotated[
        bool,
        typer.Option(
            "--verify-dataset-hashes/--no-verify-dataset-hashes",
            help="Verify cached benchmark bytes against pinned upstream hashes.",
        ),
    ] = True,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print benchmark results as JSON."),
    ] = False,
    quick: Annotated[
        bool,
        typer.Option(
            "--quick",
            help="Offline coverage check: run random,heuristic on all datasets (1 seed) "
            "and print the per-error-class coverage matrix. No API keys required.",
        ),
    ] = False,
    allow_coverage_loss: Annotated[
        bool,
        typer.Option(
            "--allow-coverage-loss",
            help="Permit overwriting the output artifact even when this run produces "
            "fewer method/dataset pairs than it already holds. Off by default, because "
            "a narrow diagnostic run would otherwise destroy committed evidence in place.",
        ),
    ] = False,
) -> None:
    """Run real-world benchmark methods across cached benchmark datasets."""
    if quick:
        methods = "random,heuristic"
        datasets = datasets if datasets != "hospital" else "hospital,flights"
        seeds = 1
        seed_list = None
    try:
        # Load .env at the CLI boundary (not in the library runner) so real runs
        # pick up provider credentials while the library stays hermetic for tests.
        from dotenv import load_dotenv

        load_dotenv()
        output = _runner()(
            methods=_parse_csv_list(methods),
            datasets=_parse_csv_list(datasets),
            seeds=seeds,
            seed_list=_parse_seed_list(seed_list),
            output_json=output_json,
            really_run_big_bench=really_run_big_bench,
            cache_root=cache_root,
            verify_dataset_hashes=verify_dataset_hashes,
            allow_coverage_loss=allow_coverage_loss,
        )
    except Exception as exc:
        _console.print(
            Panel(
                f"[bold red]{exc}[/bold red]",
                title="Benchmark Error",
                style="red",
            )
        )
        raise typer.Exit(code=2) from exc

    # Record spend BEFORE rendering, so a paid run is audited even if presentation fails.
    _write_spend_receipt(output)

    if json_output:
        typer.echo(json.dumps(output.model_dump(mode="json"), indent=2, sort_keys=True))
        return

    _render_summary(output)
    if quick:
        _render_coverage_matrix(output)


def _write_spend_receipt(output: Any) -> None:
    """Append an auditable spend receipt for a bench run that actually spent money.

    WHY THIS EXISTS. `docs/trust/spend-accountability.md` presents the ledger at
    `eval/results/spend_ledger.json` as the after-the-fact record of spend, but `append_receipt` was
    called from exactly one place -- the Azure capability probe. The main bench path, which is where
    API-phase money is actually spent, wrote nothing. A 270-call gpt-5.6-sol corrector run costing
    about $0.98 left no trace in the ledger at all, so the documented guarantee was false for the
    path that spends the most.

    Written at the CLI boundary, next to `load_dotenv`, for the reason recorded there: this is the
    real-run edge. The library runner stays hermetic so tests do not write to a shared ledger.

    One receipt per (provider, model) pair, because a single invocation can span methods that use
    different deployments and summing them would attribute spend to whichever happened to be first.
    Prices come from `prices_from_env`, so the receipt agrees with the cap the run enforced rather
    than with the provider-level table.
    """
    from dataforge.spend import SpendReceipt, append_receipt, cap_from_env, prices_from_env

    records = getattr(output, "records", None) or []
    totals: dict[tuple[str, str], dict[str, int]] = {}
    for record in records:
        provider = getattr(record, "provider", None)
        model = getattr(record, "model", None)
        calls = int(getattr(record, "llm_calls", 0) or 0)
        if not provider or not model or calls <= 0:
            continue
        bucket = totals.setdefault((provider, model), {"calls": 0, "prompt": 0, "completion": 0})
        bucket["calls"] += calls
        bucket["prompt"] += int(getattr(record, "prompt_tokens", 0) or 0)
        bucket["completion"] += int(getattr(record, "completion_tokens", 0) or 0)

    if not totals:
        return

    ledger = Path("eval") / "results" / "spend_ledger.json"
    for (provider, model), bucket in sorted(totals.items()):
        # Pass the model: omitting it meters every Azure deployment at one provider rate,
        # and the measured spread is 46x, so a receipt would misstate spend by that factor.
        price = prices_from_env(provider, model)
        if price is None:
            # Unpriced provider: the USD guard is disabled by design, so inventing a figure here
            # would be worse than recording none.
            continue
        estimated = price.usd_for(bucket["prompt"], bucket["completion"])
        try:
            append_receipt(
                ledger,
                SpendReceipt(
                    run_id=uuid.uuid4().hex[:12],
                    utc=datetime.now(tz=UTC).isoformat(),
                    provider=provider,
                    model=model,
                    calls=bucket["calls"],
                    prompt_tokens=bucket["prompt"],
                    completion_tokens=bucket["completion"],
                    # Bench records do not break reasoning tokens out of completion tokens, so
                    # this is 0 rather than a guess. They are billed at the output rate and are
                    # already inside completion_tokens, so the USD figure is unaffected.
                    reasoning_tokens=0,
                    estimated_usd=estimated,
                    cap_usd=cap_from_env(provider),
                    method="bench",
                    dataset=None,
                    git_sha=None,
                    notes=("reasoning_tokens not itemised by bench records",),
                ),
            )
        except OSError as exc:
            # A ledger write failure must not discard a completed paid run.
            _console.print(f"[yellow]Could not append spend receipt: {exc}[/yellow]")
