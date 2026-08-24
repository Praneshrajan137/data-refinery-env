"""Can an LLM filter improve the rayyan review queue without damaging it?

**Why this experiment exists.** rayyan is the worst case for the heuristic ensemble and the
sharpest test of an LLM triage filter, because its queue is two detectors pulling in opposite
directions. Measured offline before any spend: `missing_value` contributes 1,155 flagged cells at
**0.0649** precision (1,080 false positives, half the queue), and `date_transposition`
contributes 637 at **1.0000** (a quarter of the queue, nothing to gain, 637 correct detections to
lose).

**Why it is stratified.** A pooled before/after precision number adds a possible large gain on
the first stratum to a possible large loss on the second and reports the sum. A filter that
destroys a perfect detector would look like an improvement. The strata are disjoint --
`run_all_detectors` keeps one issue per cell by precedence, and the per-detector counts sum
exactly to the ensemble total -- so attribution is unambiguous.

**Why the sample is enriched within each stratum.** A uniform sample would spend half its calls
on `missing_value` and recover ~6% true cells there, estimating the safety term on a handful.
Population composition is known exactly from ground truth, so only the two conditional keep-rates
are sampled and they are projected onto known counts. Enrichment changes which cells are drawn,
not the population projected onto.

**The safety condition outranks the gain.** Q2 (recall retained on `date_transposition` >= 0.95)
and Q3 (whole-queue recall retained >= 0.90) are pre-registered *before* any precision gain was
visible, precisely so a large gain cannot be used to argue a loss was acceptable.

Pre-registration: `eval/preregistration/queue_filter_rayyan.md`.
Baselines: `eval/results/cell_detection_rayyan.json`.
Estimator: `dataforge.bench.stratified`.

Run foreground and bounded::

    python scripts/bench/probe_queue_filter.py --calibrate --max-usd 1
    python scripts/bench/probe_queue_filter.py --max-usd 10
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import subprocess
import sys
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from dataforge.bench.groq_client import AzureBenchClient  # noqa: E402
from dataforge.bench.stratified import (  # noqa: E402
    StratumSample,
    project_queue_filter,
)
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors import run_all_detectors  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402
from dataforge.spend import (  # noqa: E402
    CostCapExceededError,
    ModelPrice,
    append_receipt,
    price_for,
)
from dataforge.table import cell_value  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "queue_filter_rayyan.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_queue_filter_v1"

SEED = 0
_REASONING_EFFORT = "none"
_DATASET = "rayyan"

#: Pre-registered sample sizes, per stratum. `type_mismatch` is deliberately absent: 64 cells
#: with 2 true errors cannot support a rate estimate, so it is carried unfiltered and named as
#: uncovered rather than pooled into a neighbour.
_PLAN: dict[str, tuple[int, int]] = {
    # stratum: (true cells to sample, false cells to sample)
    "missing_value": (75, 125),
    "date_transposition": (100, 0),
    "format_violation": (60, 60),
}
#: The zero-precision tail, pooled because each is too small alone. All false by construction,
#: which is what makes Q4 a real sanity check: the correct answer is known in advance.
_TAIL = ("outlier", "categorical_normalization", "decimal_shift", "entity_consensus")
_TAIL_SAMPLE = 60


#: Two system prompts. `guarded` carries a hint about optional empty fields, added to help the
#: `missing_value` stratum where rayyan's false positives are legitimately absent optional
#: bibliographic fields. `neutral` omits it.
#:
#: The first run used `guarded` and produced a near-constant "no" (478 of 480 cells rejected,
#: zero parse failures). That hint is a plausible cause, so the two are compared on identical
#: cells rather than the result being published from one prompt. A finding that turns on a
#: sentence I wrote is a finding about my prompt.
_PROMPTS: dict[str, str] = {
    "guarded": (
        "You are a data-quality auditor for a table of bibliographic records from a "
        "systematic-review screening tool. A specific cell has been flagged as "
        "possibly erroneous. Using the whole row as context, decide whether the "
        "flagged cell's value is actually erroneous. An empty or absent value in an "
        "optional field is NOT an error. Respond with ONLY 'yes' (erroneous) or "
        "'no' (fine). No prose."
    ),
    "neutral": (
        "You are a data-quality auditor for a table of bibliographic records from a "
        "systematic-review screening tool. A specific cell has been flagged as "
        "possibly erroneous. Using the whole row as context, decide whether the "
        "flagged cell's value is actually erroneous. Respond with ONLY 'yes' "
        "(erroneous) or 'no' (fine). No prose."
    ),
}


def _git_commit() -> tuple[str | None, bool]:
    """Return the current commit and whether the worktree is dirty."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, cwd=ROOT
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True, check=True, cwd=ROOT
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None, False
    return commit, bool(status)


def _ask_is_erroneous(
    client: AzureBenchClient,
    *,
    column: str,
    row_values: dict[str, str],
    failures: list[int],
    prompt_variant: str = "guarded",
) -> bool | None:
    """Ask whether one flagged cell is genuinely erroneous.

    Returns None on a failed or unparseable call, which is counted and treated as **keep** by
    the caller. Treating a failure as a rejection would silently discard queue items and read
    as filter precision.
    """
    messages = [
        {"role": "system", "content": _PROMPTS[prompt_variant]},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "flagged_column": column,
                    "flagged_value": row_values.get(column, ""),
                    "row": row_values,
                },
                separators=(",", ":"),
            ),
        },
    ]
    try:
        text = client.complete(messages).text
    except CostCapExceededError:
        raise
    except Exception as exc:  # noqa: BLE001 - one bad response must not kill a paid run
        failures[0] += 1
        print(f"      call failed: {type(exc).__name__}: {str(exc)[:70]}")
        return None
    answer = text.strip().lower()
    if answer.startswith("y"):
        return True
    if answer.startswith("n"):
        return False
    failures[0] += 1
    return None


def _queue_state() -> dict[str, Any]:
    """Build the rayyan queue, attributed per detector."""
    dataset = load_real_world_dataset(_DATASET, verify_hashes=True)
    frame = dataset.dirty_df.copy(deep=True)
    schema = infer_schema(frame).to_schema(include_inferred_constraints=True)
    issues = run_all_detectors(dataset.dirty_df.copy(deep=True), schema=schema)
    truth = {(cell.row, cell.column) for cell in dataset.ground_truth}
    by_detector: dict[str, list[tuple[int, str]]] = {}
    for issue in issues:
        by_detector.setdefault(issue.issue_type, []).append((issue.row, issue.column))
    return {
        "dataset": dataset,
        "frame": frame,
        "truth": truth,
        "by_detector": {k: sorted(set(v)) for k, v in by_detector.items()},
        "columns": list(dataset.canonical_columns),
    }


def _judge(
    client: AzureBenchClient,
    cells: list[tuple[int, str]],
    state: dict[str, Any],
    failures: list[int],
    *,
    max_usd: float,
    label: str,
    prompt_variant: str = "guarded",
) -> tuple[int, int]:
    """Judge a list of cells. Returns (kept, judged)."""
    kept = judged = 0
    for index, (row, column) in enumerate(cells, start=1):
        if client.cumulative_usd >= max_usd:
            print(f"      local budget reached; stopping {label} at {judged}/{len(cells)}")
            break
        values = {col: str(cell_value(state["frame"], row, col)) for col in state["columns"]}
        verdict = _ask_is_erroneous(
            client,
            column=column,
            row_values=values,
            failures=failures,
            prompt_variant=prompt_variant,
        )
        judged += 1
        # A failed call is KEEP: the filter had no opinion, so the cell stays in the queue.
        if verdict is not False:
            kept += 1
        if index % 25 == 0:
            print(f"      {label} {index}/{len(cells)}, ${client.cumulative_usd:.3f}")
    return kept, judged


def main(argv: list[str] | None = None) -> int:
    """Run the rayyan queue-filter measurement."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-usd", type=float, default=10.0)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument(
        "--calibrate", action="store_true", help="Judge 10 cells, project cost, exit."
    )
    parser.add_argument(
        "--prompt-sensitivity",
        action="store_true",
        help=(
            "Diagnostic: judge identical cells under both the guarded and neutral system "
            "prompts and report the keep-rates side by side. Resolves whether a near-constant "
            "answer is a property of the model or of the prompt."
        ),
    )
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)

    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
    api_key = os.environ.get("AZURE_API_KEY")
    deployment = os.environ.get("DATAFORGE_AZURE_MODEL", "gpt-5.6-sol")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview")
    if not endpoint or not api_key:
        print("AZURE_OPENAI_ENDPOINT and AZURE_API_KEY are required; see .env.example")
        return 2

    state = _queue_state()
    truth = state["truth"]
    by_detector = state["by_detector"]
    flagged_total = sum(len(v) for v in by_detector.values())
    print(f"{_DATASET} queue: {flagged_total} flagged cells across {len(by_detector)} detectors")

    price = price_for("azure") or ModelPrice(usd_per_1k_input=0.005, usd_per_1k_output=0.015)
    client = AzureBenchClient(
        api_key=api_key,
        model=deployment,
        endpoint=endpoint,
        api_version=api_version,
        max_usd=args.max_usd,
        timeout_s=args.timeout_s,
        reasoning_effort=_REASONING_EFFORT,
        usd_per_1k_input=price.usd_per_1k_input,
        usd_per_1k_output=price.usd_per_1k_output,
        max_tokens=64,
    )
    failures = [0]
    rng = random.Random(SEED)

    if args.calibrate:
        sample = (by_detector.get("missing_value") or [])[:10]
        _judge(client, sample, state, failures, max_usd=args.max_usd, label="calibrate")
        calls = max(1, client.meter.calls)
        per_call = client.cumulative_usd / calls
        planned = sum(t + f for t, f in _PLAN.values()) + _TAIL_SAMPLE
        print(
            f"\ncalibration: {calls} calls, ${client.cumulative_usd:.4f} "
            f"(${per_call:.5f}/call), {failures[0]} failed"
        )
        print(f"projection: {planned} calls -> ${per_call * planned:.2f} for the full run")
        return 0

    run_id = args.run_id or f"queue-filter-{uuid.uuid4().hex[:8]}"

    if args.prompt_sensitivity:
        # Paired by construction: identical cells, identical order, only the system prompt
        # differs. The safety stratum (all-true) and the headroom stratum's false cells are the
        # two that decide whether the collapse is real.
        truth_cells = (by_detector.get("date_transposition") or [])[:]
        false_cells = [c for c in (by_detector.get("missing_value") or []) if c not in truth]
        probe_true = rng.sample(truth_cells, min(80, len(truth_cells)))
        probe_false = rng.sample(false_cells, min(60, len(false_cells)))
        arms: dict[str, Any] = {}
        for variant in ("guarded", "neutral"):
            tk, tn = _judge(
                client,
                probe_true,
                state,
                failures,
                max_usd=args.max_usd,
                label=f"{variant}/true",
                prompt_variant=variant,
            )
            fk, fn = _judge(
                client,
                probe_false,
                state,
                failures,
                max_usd=args.max_usd,
                label=f"{variant}/false",
                prompt_variant=variant,
            )
            arms[variant] = {
                "keep_true": round(tk / tn, 4) if tn else None,
                "keep_true_n": tn,
                "keep_false": round(fk / fn, 4) if fn else None,
                "keep_false_n": fn,
                # Discrimination is the quantity that matters: a filter must keep true cells
                # more often than false ones. Q4 as pre-registered checks only one side and is
                # satisfied by a constant-"no" answerer, which is a defect in that condition.
                "discrimination": (round(tk / tn - fk / fn, 4) if (tn and fn) else None),
            }
            print(f"  {variant}: {arms[variant]}")
        payload = {
            "schema_version": "dataforge_queue_filter_prompt_sensitivity_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "dataset": _DATASET,
            "model": deployment,
            "seed": SEED,
            "question": (
                "Is the near-constant 'no' in queue_filter_rayyan.json a property of the "
                "model or of the guarded system prompt's hint about empty optional fields?"
            ),
            "paired": "identical cells in identical order; only the system prompt differs",
            "arms": arms,
            "prompts": _PROMPTS,
            "failed_calls": failures[0],
            "calls": client.meter.calls,
            "estimated_usd": round(client.cumulative_usd, 6),
            "run_id": run_id,
        }
        out = ROOT / "eval" / "results" / "queue_filter_prompt_sensitivity.json"
        out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        try:
            if client.meter.calls:
                append_receipt(
                    _LEDGER,
                    client.meter.receipt(
                        run_id=run_id,
                        method="queue_filter_prompt_sensitivity",
                        dataset=_DATASET,
                        notes=("diagnostic", "paired prompt variants"),
                    ),
                )
        except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
            print(f"WARNING: receipt not written: {type(exc).__name__}: {exc}")
        print(f"\ncalls={client.meter.calls} spend=${client.cumulative_usd:.4f}")
        print(f"artifact: {out.relative_to(ROOT)}")
        return 0

    strata: list[StratumSample] = []

    for name, (n_true, n_false) in _PLAN.items():
        cells = by_detector.get(name) or []
        if not cells:
            continue
        true_cells = [c for c in cells if c in truth]
        false_cells = [c for c in cells if c not in truth]
        true_sample = rng.sample(true_cells, min(n_true, len(true_cells)))
        false_sample = rng.sample(false_cells, min(n_false, len(false_cells)))
        print(
            f"\n{name}: population {len(true_cells)} true / {len(false_cells)} false; "
            f"sampling {len(true_sample)} / {len(false_sample)}"
        )
        true_kept, true_judged = _judge(
            client, true_sample, state, failures, max_usd=args.max_usd, label=f"{name}/true"
        )
        false_kept, false_judged = _judge(
            client, false_sample, state, failures, max_usd=args.max_usd, label=f"{name}/false"
        )
        strata.append(
            StratumSample(
                name=name,
                true_population=len(true_cells),
                false_population=len(false_cells),
                true_kept=true_kept,
                true_sampled=true_judged,
                false_kept=false_kept,
                false_sampled=false_judged,
            )
        )

    # The zero-precision tail, pooled. Q4's correct answer is known: reject nearly all.
    tail_cells = [c for name in _TAIL for c in (by_detector.get(name) or [])]
    tail_true = [c for c in tail_cells if c in truth]
    tail_false = [c for c in tail_cells if c not in truth]
    if tail_false:
        tail_sample = rng.sample(tail_false, min(_TAIL_SAMPLE, len(tail_false)))
        print(
            f"\nzero_precision_tail: population {len(tail_true)} true / {len(tail_false)} "
            f"false; sampling 0 / {len(tail_sample)}"
        )
        tail_kept, tail_judged = _judge(
            client, tail_sample, state, failures, max_usd=args.max_usd, label="tail/false"
        )
        strata.append(
            StratumSample(
                name="zero_precision_tail",
                true_population=len(tail_true),
                false_population=len(tail_false),
                true_kept=0,
                true_sampled=0,
                false_kept=tail_kept,
                false_sampled=tail_judged,
            )
        )

    # Everything not planned or tailed is carried unfiltered and named.
    covered = set(_PLAN) | set(_TAIL)
    for name, cells in sorted(by_detector.items()):
        if name in covered:
            continue
        strata.append(
            StratumSample(
                name=name,
                true_population=sum(1 for c in cells if c in truth),
                false_population=sum(1 for c in cells if c not in truth),
                true_kept=0,
                true_sampled=0,
                false_kept=0,
                false_sampled=0,
            )
        )

    projection = project_queue_filter(strata, total_true_errors_in_table=len(truth))

    by_name = {s.name: s for s in projection.per_stratum}
    q1 = by_name.get("missing_value")
    q2 = by_name.get("date_transposition")
    tail = by_name.get("zero_precision_tail")
    verdicts = {
        "Q1_missing_value_precision_after_filter": q1.projected_precision if q1 else None,
        "Q1_threshold": 0.20,
        "Q1_supported": bool(q1 and (q1.projected_precision or 0) >= 0.20),
        "Q2_date_transposition_recall_retained": q2.keep_true_rate if q2 else None,
        "Q2_threshold": 0.95,
        "Q2_supported": bool(q2 and (q2.keep_true_rate or 0) >= 0.95),
        "Q3_queue_recall_retained": projection.recall_retained,
        "Q3_threshold": 0.90,
        "Q3_supported": bool((projection.recall_retained or 0) >= 0.90),
        "Q4_tail_rejection_rate": (
            round(1 - (tail.keep_false_rate or 0), 4)
            if tail and tail.keep_false_rate is not None
            else None
        ),
        "Q4_threshold": 0.50,
        "Q4_supported": bool(
            tail and tail.keep_false_rate is not None and (1 - tail.keep_false_rate) >= 0.50
        ),
    }

    commit, dirty = _git_commit()
    payload = {
        "schema_version": _SCHEMA,
        "generated_at": datetime.now(UTC).isoformat(),
        "dataset": _DATASET,
        "scoring_unit": "cell",
        "debatable_class_available": False,
        "provider": "azure",
        "model": deployment,
        "api_version": api_version,
        "seed": SEED,
        "reasoning_effort": _REASONING_EFFORT,
        "baseline": {
            "flagged": projection.baseline_flagged,
            "true": projection.baseline_true,
            "precision": projection.baseline_precision,
            "recall_of_table": round(projection.baseline_true / len(truth), 4) if truth else None,
        },
        "projected": {
            "precision": projection.projected_precision,
            "recall_retained": projection.recall_retained,
            "true_errors_lost": projection.true_errors_lost,
            "true_kept": projection.projected_true_kept,
            "false_kept": projection.projected_false_kept,
        },
        "per_stratum": [asdict(s) for s in projection.per_stratum],
        "uncovered_strata": list(projection.uncovered_strata),
        "verdicts": verdicts,
        "failed_calls": failures[0],
        "calls": client.meter.calls,
        "estimated_usd": round(client.cumulative_usd, 6),
        "thresholds": {
            "pre_registration": "eval/preregistration/queue_filter_rayyan.md",
            "Q1_min_missing_value_precision": 0.20,
            "Q2_min_date_transposition_recall": 0.95,
            "Q3_min_queue_recall_retained": 0.90,
            "Q4_min_tail_rejection": 0.50,
        },
        "limitations": [
            "L1: ONE corpus. rayyan is the worst case for the heuristic ensemble and the same "
            "detector scores 1.0000 on flights against 0.0649 here. Nothing transfers to "
            "another table; that swing is the reason.",
            "L2: two-way labels. RAHA ships no ground_truth_debatable class, so a cell the "
            "model plausibly regards as arguable is scored as a hard error or a hard "
            "non-error.",
            "L3: the filter sees ONE ROW. A detector whose evidence is the column's "
            "distribution is judged by an instrument that cannot see that evidence.",
            "L4: the projection assumes sampled keep-rates transfer to the unsampled "
            "remainder of each stratum. With 1,080 false missing_value cells represented by "
            "125 draws, that assumption is doing real work.",
            "L5: uncovered strata are carried UNFILTERED, neither dropped nor assumed "
            "rejected. They are named in uncovered_strata.",
            "Detection triage only. Nothing here proposes a value and nothing auto-applies.",
        ],
        "provenance": {
            "git_commit": commit,
            "git_worktree_dirty": dirty,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "dirty_sha256": state["dataset"].dirty_sha256,
        },
        "run_id": run_id,
    }

    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    try:
        if client.meter.calls:
            append_receipt(
                _LEDGER,
                client.meter.receipt(
                    run_id=run_id,
                    method="queue_filter_probe",
                    dataset=_DATASET,
                    notes=(
                        f"strata={len(strata)}",
                        f"Q2_recall_retained={verdicts['Q2_date_transposition_recall_retained']}",
                        "detection triage only",
                    ),
                ),
            )
    except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
        print(f"WARNING: receipt not written: {type(exc).__name__}: {exc}")

    print(
        f"\n{'stratum':<24} {'pop':>6} {'base':>7} {'keepT':>7} {'keepF':>7} {'after':>7} {'lost':>7}"
    )
    for stratum in projection.per_stratum:
        print(
            f"{stratum.name:<24} {stratum.population:>6} "
            f"{str(stratum.baseline_precision):>7} {str(stratum.keep_true_rate):>7} "
            f"{str(stratum.keep_false_rate):>7} {str(stratum.projected_precision):>7} "
            f"{stratum.true_errors_lost:>7}"
        )
    print(
        f"\nQUEUE precision {projection.baseline_precision} -> {projection.projected_precision}"
        f"   recall retained {projection.recall_retained}"
        f"   true errors lost {projection.true_errors_lost}"
    )
    for key in ("Q1", "Q2", "Q3", "Q4"):
        supported = verdicts[f"{key}_supported"]
        print(f"  {key}: {'SUPPORTED' if supported else 'NOT SUPPORTED'}")
    print(f"calls={client.meter.calls} failed={failures[0]} spend=${client.cumulative_usd:.4f}")
    print(f"artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
