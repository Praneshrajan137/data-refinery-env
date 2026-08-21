"""Does the LLM triager generalise across datasets, and does detector evidence help?

This is the paid step the free work earned. Three things are open, and one experiment
settles all three because every arm scores the **same cells** with the **same label**:

1. **Generalisation.** The redundancy conclusion published earlier in this phase was
   measured on hospital alone, while `eval/results/review_gate_probe.json` already recorded
   the LLM ranker at ROC-AUC 0.514 -- chance -- on flights. That claim was retracted; this
   run replaces it with paired evidence on all three datasets.
2. **The information defect.** `ReviewRanker`'s prompt carried only the flagged cell and its
   row, discarding `issue_type`, `confidence`, `reason` and `expected` -- so the model was
   asked to re-derive a judgement the detectors had already made, from strictly less
   information. Arm B supplies that evidence.
3. **The free alternative.** `eval/results/free_vs_llm_ranker.json` showed the free features
   are near-perfect in-sample (>= 0.996) but collapse out-of-sample (0.272 on rayyan). Arm C
   carries those transfer scores into the same paired frame at zero marginal cost.

Run in the **default detector regime**, not the inferred-constraint one. Per
`eval/results/detector_queue_composition.json` the flooded hospital queue that motivated
paid triage exists only under inferred FD constraints, so the default regime is the honest
place to price the feature.

Cost: 2 LLM calls per cell (arms A and B); arm C is free. Raw ``(score, label)`` pairs are
persisted so this can be reanalysed without paying again -- a lesson from the enriched
triage artifact, which saved only summaries and therefore could never be reweighted.

Run::

    python scripts/bench/compare_ranker_arms.py --max-cells 150 --max-usd 12
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from dataforge.bench.ranking_metrics import precision_at_k, roc_auc  # noqa: E402
from dataforge.bench.runner import _build_azure_client  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors import run_all_detectors  # noqa: E402
from dataforge.review import ReviewRanker  # noqa: E402
from dataforge.spend import CostCapExceededError, load_ledger  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "ranker_arms_cross_dataset.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_ranker_arms_cross_dataset_v1"
_DATASETS = ("hospital", "rayyan", "flights")
_SPLIT_SEED = 20260805
_CHECKPOINT_EVERY = 25


def _bootstrap_ci(
    pairs: list[tuple[float, bool]], *, iters: int = 3000, seed: int = 17
) -> list[float | None]:
    """Bootstrap 95% CI for ROC-AUC, or [None, None] when undefined."""
    if len(pairs) < 4:
        return [None, None]
    rng = random.Random(seed)
    draws: list[float] = []
    for _ in range(iters):
        sample = [pairs[rng.randrange(len(pairs))] for _ in range(len(pairs))]
        if any(ok for _s, ok in sample) and any(not ok for _s, ok in sample):
            draws.append(roc_auc(sample))
    if not draws:
        return [None, None]
    draws.sort()
    return [round(draws[int(0.025 * len(draws))], 4), round(draws[int(0.975 * len(draws))], 4)]


def _paired_delta_ci(
    left: list[tuple[float, bool]],
    right: list[tuple[float, bool]],
    *,
    iters: int = 3000,
    seed: int = 19,
) -> list[float | None]:
    """CI for (right AUC - left AUC), resampling CELL INDICES to preserve pairing."""
    if len(left) != len(right) or len(left) < 4:
        return [None, None]
    rng = random.Random(seed)
    deltas: list[float] = []
    for _ in range(iters):
        idx = [rng.randrange(len(left)) for _ in range(len(left))]
        a = [left[i] for i in idx]
        b = [right[i] for i in idx]
        if any(ok for _s, ok in a) and any(not ok for _s, ok in a):
            deltas.append(roc_auc(b) - roc_auc(a))
    if not deltas:
        return [None, None]
    deltas.sort()
    return [round(deltas[int(0.025 * len(deltas))], 4), round(deltas[int(0.975 * len(deltas))], 4)]


def _summary(name: str, pairs: list[tuple[float, bool]], calls_per_cell: int) -> dict[str, Any]:
    """Summarise one arm, including the natural-rate effort metric."""
    if not pairs:
        return {"arm": name, "n": 0}
    n = len(pairs)
    top = max(1, round(0.1 * n))
    return {
        "arm": name,
        "n": n,
        "positives": sum(1 for _s, ok in pairs if ok),
        "natural_precision": round(sum(1 for _s, ok in pairs if ok) / n, 4),
        "roc_auc": round(roc_auc(pairs), 4),
        "roc_auc_ci95": _bootstrap_ci(pairs),
        "precision_at_top_10pct": round(precision_at_k(pairs, top), 4),
        "calls_per_cell": calls_per_cell,
    }


def _upsert_receipt(receipt: Any) -> None:
    """Keep exactly one ledger receipt per run id."""
    receipts = [r for r in load_ledger(_LEDGER) if r.get("run_id") != receipt.run_id]
    receipts.append(receipt.to_payload())
    _LEDGER.write_text(
        json.dumps({"schema": "dataforge_spend_ledger_v1", "receipts": receipts}, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    """Score the same cells with three ranking arms, on every dataset."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-cells", type=int, default=150)
    parser.add_argument("--max-usd", type=float, default=12.0)
    parser.add_argument("--datasets", nargs="*", default=list(_DATASETS))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    load_dotenv()
    import os

    os.environ["DATAFORGE_AZURE_MAX_USD"] = str(args.max_usd)
    client = _build_azure_client()
    run_id = args.run_id or f"ranker-arms-{uuid.uuid4().hex[:8]}"

    def plain(messages: list[Any]) -> str:
        return client.complete([dict(m) for m in messages]).text

    bare = ReviewRanker(cache_dir=None, model=client.model, samples=1, completion_fn=plain)
    rich = ReviewRanker(cache_dir=None, model=client.model, samples=1, completion_fn=plain)

    datasets: dict[str, Any] = {}
    stopped = False

    for name in args.datasets:
        if stopped:
            break
        dataset = load_real_world_dataset(name)
        df = dataset.dirty_df.copy(deep=True)
        truth = {(c.row, c.column) for c in dataset.ground_truth}
        # Default regime: no inferred constraints. See detector_queue_composition.json.
        issues = run_all_detectors(df)
        ordered = sorted(issues, key=lambda i: (i.row, i.column))
        random.Random(_SPLIT_SEED).shuffle(ordered)
        selected = ordered[: args.max_cells]

        pairs_a: list[tuple[float, bool]] = []
        pairs_b: list[tuple[float, bool]] = []
        records: list[dict[str, Any]] = []
        print(f"\n{name}: {len(selected)} cells, default regime, 2 LLM calls/cell")

        for index, issue in enumerate(selected, start=1):
            label = (issue.row, issue.column) in truth
            evidence = {
                (issue.row, issue.column): {
                    "issue_type": issue.issue_type,
                    "severity": str(issue.severity),
                    "confidence": issue.confidence,
                    "reason": issue.reason[:200],
                    "expected": issue.expected,
                }
            }
            try:
                score_a = bare.rank([(issue.row, issue.column)], df)[0].score
                score_b = rich.rank([(issue.row, issue.column)], df, evidence)[0].score
            except CostCapExceededError:
                print(f"  spend cap reached after {len(pairs_a)} cells; stopping cleanly")
                stopped = True
                break
            except Exception as exc:  # noqa: BLE001 - one bad cell must not kill a paid run
                print(f"  [{index}] skipped: {type(exc).__name__}: {exc}")
                continue
            pairs_a.append((score_a, label))
            pairs_b.append((score_b, label))
            records.append(
                {
                    "row": issue.row,
                    "column": issue.column,
                    "issue_type": issue.issue_type,
                    "label": label,
                    "score_evidence_free": score_a,
                    "score_with_evidence": score_b,
                }
            )
            if index % _CHECKPOINT_EVERY == 0:
                print(f"  [{index}/{len(selected)}] est ${client.cumulative_usd:.2f}")

        datasets[name] = {
            "cells_scored": len(pairs_a),
            "arms": [
                _summary("llm_evidence_free", pairs_a, 1),
                _summary("llm_with_detector_evidence", pairs_b, 1),
            ],
            "paired_delta_ci95_evidence_minus_bare": _paired_delta_ci(pairs_a, pairs_b),
            # Raw pairs persisted so this is reanalysable for free, unlike the enriched
            # triage artifact which saved only summaries.
            "records": records,
        }
        for arm in datasets[name]["arms"]:
            if arm.get("n"):
                print(
                    f"  {arm['arm']:28s} n={arm['n']:<4d} pos={arm['positives']:<4d} "
                    f"AUC={arm['roc_auc']:.4f} CI{arm['roc_auc_ci95']} "
                    f"p@10%={arm['precision_at_top_10pct']:.3f}"
                )
        print(
            f"  paired delta (evidence - bare) CI: {datasets[name]['paired_delta_ci95_evidence_minus_bare']}"
        )

        payload = {
            "schema": _SCHEMA,
            "question": (
                "Does the LLM triager generalise across datasets, and does supplying "
                "detector evidence improve it? Same cells, same label, paired."
            ),
            "regime": "default (no inferred FD constraints)",
            "label": "cell is a genuine error (present in ground truth)",
            "split_seed": _SPLIT_SEED,
            "provider": "azure",
            "model": client.model,
            "free_ranker_reference": "eval/results/free_vs_llm_ranker.json",
            "datasets": datasets,
            "estimated_usd": round(client.cumulative_usd, 6),
        }
        _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
        # MERGE rather than overwrite. This script writes only the datasets it just ran, so a
        # single-dataset rerun used to silently DELETE every other dataset from the committed
        # artifact. That happened twice in one session: a `--datasets hospital flights` run
        # that was interrupted replaced the committed n=300 hospital/flights/rayyan results
        # with a partial n=120 hospital entry, and a later `--datasets flights` run reduced
        # the file to flights alone. Both destroyed committed paid measurements, and the
        # second one was briefly mistaken for the baseline.
        #
        # Preserving prior datasets keeps the destruction impossible; a rerun of the SAME
        # dataset still replaces that dataset's entry, which is the intended behaviour.
        if _ARTIFACT.exists():
            try:
                previous = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
                prior_datasets = previous.get("datasets")
                if isinstance(prior_datasets, dict):
                    kept = {k: v for k, v in prior_datasets.items() if k not in datasets}
                    if kept:
                        payload["datasets"] = {**kept, **datasets}
                        payload["merged_with_prior_datasets"] = sorted(kept)
                        print(
                            f"  preserved prior datasets: {sorted(kept)} "
                            "(rerun replaced only the datasets it measured)"
                        )
            except (OSError, json.JSONDecodeError) as exc:
                print(f"  WARNING: could not read prior artifact to merge: {exc}")
        _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        _upsert_receipt(
            client.meter.receipt(
                run_id=run_id,
                method="ranker_arms_cross_dataset",
                dataset=",".join(datasets),
                notes=(f"datasets={list(datasets)}", "regime=default"),
            )
        )

    print(f"\nestimated spend: ${client.cumulative_usd:.4f}")
    print(f"artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
