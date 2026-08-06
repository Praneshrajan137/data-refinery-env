"""Are the review ranker and the structured corrector redundant as triagers?

The API phase shipped two LLM features that both produce a per-cell score:

* ``ReviewRanker`` -- one grounded yes/no call per flagged cell, returns a score only.
* the structured corrector -- ``k`` calls per cell, returns a candidate value **and** a
  confidence.

Their reported ranking powers looked interchangeable (ROC-AUC 0.946 vs 0.948), but that
comparison was invalid on two counts, and this script exists to fix both:

1. **Different populations.** The corrector's AUC was measured only over cells where it
   chose to propose (~18% of attempts); the ranker scores every flagged cell. Comparing
   a survivor subpopulation against a full population measures nothing.
2. **Different labels.** The corrector's AUC used "was the proposed value correct"; the
   ranker's used "is this flagged cell really an error". Those are different questions.

Here both scorers see the **same cells** and predict the **same label** -- *is this
flagged cell a genuine error?* -- which is the question a review queue actually asks.
Corrector abstention is scored as 0.0 rather than dropped, because in product use an
abstention is itself a "do not review this first" signal; dropping it would restore the
survivor bias this script exists to remove.

Outcome is a decision, not a number: if the corrector matches the ranker, one call path
can be deleted and users get candidate values for free. If it does not, both stay.

Run foreground and bounded::

    python scripts/bench/compare_triage_scorers.py --max-cells 150 --max-usd 5
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from dataforge.agent.providers import Message  # noqa: E402
from dataforge.bench.ranking_metrics import precision_at_k, roc_auc  # noqa: E402
from dataforge.bench.runner import _build_azure_client  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.repairers.contract import build_correction_contract  # noqa: E402
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer  # noqa: E402
from dataforge.review import ReviewRanker  # noqa: E402
from dataforge.spend import CostCapExceededError, load_ledger  # noqa: E402
from scripts.bench.sweep_corrector_arms import sweep_and_flagship_issues  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "triage_scorer_comparison.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_triage_scorer_comparison_v1"

_K_CORRECTOR = 3
_K_RANKER = 1
# Lesson from the failed flagship run: a coarse checkpoint interval loses the whole
# run when one request stalls. Checkpoint often.
_CHECKPOINT_EVERY = 10


def _bootstrap_ci(
    pairs: list[tuple[float, bool]], *, iters: int = 4000, seed: int = 11
) -> tuple[float | None, float | None]:
    """Return a bootstrap 95% CI for ROC-AUC, or (None, None) if undefined."""
    if len(pairs) < 4:
        return (None, None)
    rng = random.Random(seed)
    draws: list[float] = []
    for _ in range(iters):
        sample = [pairs[rng.randrange(len(pairs))] for _ in range(len(pairs))]
        if any(ok for _, ok in sample) and any(not ok for _, ok in sample):
            draws.append(roc_auc(sample))
    if not draws:
        return (None, None)
    draws.sort()
    return (
        round(draws[int(0.025 * len(draws))], 4),
        round(draws[int(0.975 * len(draws))], 4),
    )


def _paired_auc_delta_ci(
    ranker: list[tuple[float, bool]],
    corrector: list[tuple[float, bool]],
    *,
    iters: int = 4000,
    seed: int = 13,
) -> tuple[float | None, float | None]:
    """Bootstrap CI for (corrector AUC - ranker AUC) on PAIRED cells.

    Resampling cell indices (not each arm independently) preserves the pairing, which
    is what makes the difference estimate meaningful at small n.
    """
    if len(ranker) != len(corrector) or len(ranker) < 4:
        return (None, None)
    rng = random.Random(seed)
    deltas: list[float] = []
    n = len(ranker)
    for _ in range(iters):
        idx = [rng.randrange(n) for _ in range(n)]
        r = [ranker[i] for i in idx]
        c = [corrector[i] for i in idx]
        if any(ok for _, ok in r) and any(not ok for _, ok in r):
            deltas.append(roc_auc(c) - roc_auc(r))
    if not deltas:
        return (None, None)
    deltas.sort()
    return (
        round(deltas[int(0.025 * len(deltas))], 4),
        round(deltas[int(0.975 * len(deltas))], 4),
    )


def _summary(
    name: str,
    pairs: list[tuple[float, bool]],
    calls_per_cell: int,
    *,
    enriched: bool,
) -> dict[str, Any]:
    """Summarise one scorer on the shared population.

    ``precision_at_*`` is omitted when the sample was enriched: unlike ROC-AUC it is
    not invariant to class balance, so reporting it from a stratified draw would
    overstate real-world triage precision.
    """
    if not pairs:
        return {"scorer": name, "n": 0}
    lo, hi = _bootstrap_ci(pairs)
    n = len(pairs)
    out: dict[str, Any] = {
        "scorer": name,
        "n": n,
        "positives": sum(1 for _, ok in pairs if ok),
        "base_rate": round(sum(1 for _, ok in pairs if ok) / n, 4),
        "roc_auc": round(roc_auc(pairs), 4),
        "roc_auc_ci95": [lo, hi],
        "calls_per_cell": calls_per_cell,
    }
    if enriched:
        out["precision_at_k_suppressed"] = (
            "sample was enriched with positives; precision@k is not base-rate invariant"
        )
    else:
        out["precision_at_top_10pct"] = round(precision_at_k(pairs, max(1, round(0.1 * n))), 4)
        out["precision_at_top_20pct"] = round(precision_at_k(pairs, max(1, round(0.2 * n))), 4)
    return out


def _upsert_receipt(receipt: Any) -> None:
    """Replace this run's ledger receipt in place (one entry per run)."""
    receipts = [r for r in load_ledger(_LEDGER) if r.get("run_id") != receipt.run_id]
    receipts.append(receipt.to_payload())
    _LEDGER.write_text(
        json.dumps({"schema": "dataforge_spend_ledger_v1", "receipts": receipts}, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    """Run the paired comparison and commit the artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-cells", type=int, default=150)
    parser.add_argument("--max-usd", type=float, default=5.0)
    parser.add_argument("--dataset", default="hospital")
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--enrich-positives",
        type=int,
        default=0,
        help="Oversample this many genuine errors into the sample. LEGITIMATE because "
        "ROC-AUC is invariant to class balance (it is P(random positive outranks random "
        "negative)). At the natural ~4.5%% base rate a 150-cell sample yields ~3 "
        "positives, which cannot support any comparison. NOTE: precision@k is NOT "
        "base-rate invariant and is suppressed when this is used.",
    )
    parser.add_argument("--enrich-seed", type=int, default=20260805)
    args = parser.parse_args()

    load_dotenv()
    import os

    os.environ["DATAFORGE_AZURE_MAX_USD"] = str(args.max_usd)
    client = _build_azure_client()
    run_id = args.run_id or f"triage-compare-{uuid.uuid4().hex[:8]}"

    dataset = load_real_world_dataset(args.dataset)
    df = dataset.dirty_df.copy(deep=True)
    # Use the flagship set so this stays disjoint from the arm-selection slice.
    _sweep, flagship = sweep_and_flagship_issues(dataset)
    truth = {(c.row, c.column) for c in dataset.ground_truth}

    enriched = args.enrich_positives > 0
    if enriched:
        # Stratified draw. Valid for AUC (base-rate invariant), invalid for precision@k.
        pos = [i for i in flagship if (i.row, i.column) in truth]
        neg = [i for i in flagship if (i.row, i.column) not in truth]
        rng = random.Random(args.enrich_seed)
        rng.shuffle(pos)
        rng.shuffle(neg)
        n_pos = min(args.enrich_positives, len(pos))
        issues = pos[:n_pos] + neg[: max(0, args.max_cells - n_pos)]
        rng.shuffle(issues)
        print(
            f"{args.dataset}: ENRICHED sample of {len(issues)} cells "
            f"({n_pos} positives oversampled from {len(pos)} available). "
            "AUC is base-rate invariant so this is valid; precision@k is suppressed."
        )
    else:
        issues = flagship[: args.max_cells]
        print(
            f"{args.dataset}: {len(issues)} flagged cells at the NATURAL base rate; "
            "both scorers on the SAME cells, label = 'is a genuine error'."
        )

    def plain(messages: list[Message]) -> str:
        return client.complete([dict(m) for m in messages]).text  # type: ignore[arg-type]

    def structured(messages: list[Message], response_format: dict[str, object] | None) -> str:
        return client.complete(
            [dict(m) for m in messages],  # type: ignore[arg-type]
            response_format,
        ).text

    ranker = ReviewRanker(
        cache_dir=None, model=client.model, samples=_K_RANKER, completion_fn=plain
    )
    corrector = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model=client.model,
        samples=_K_CORRECTOR,
        structured_completion_fn=structured,
        structured=True,
    )
    constraints = corrector._constraints_for(df, None)

    ranker_pairs: list[tuple[float, bool]] = []
    corrector_pairs: list[tuple[float, bool]] = []
    abstentions = 0
    done = 0

    def checkpoint() -> dict[str, Any]:
        payload = {
            "schema": _SCHEMA,
            "question": (
                "Do ReviewRanker and the structured corrector rank a review queue "
                "equivalently, on the same cells and the same label?"
            ),
            "dataset": args.dataset,
            "provider": "azure",
            "model": client.model,
            "population": (
                "ENRICHED stratified draw from the flagship set"
                if enriched
                else "flagged cells from the flagship set at the natural base rate"
            ),
            "enriched": enriched,
            "enrich_seed": args.enrich_seed if enriched else None,
            "natural_base_rate_note": (
                "The detector queue's natural precision on this dataset is ~4.5% "
                "(371 genuine errors among 8299 flagged cells), so an unenriched "
                "150-cell sample yields ~3 positives -- too few for any comparison."
            ),
            "label": "cell is a genuine error (present in ground truth)",
            "abstention_handling": (
                "corrector abstention scored 0.0, not dropped -- dropping would restore "
                "the survivor bias this comparison exists to remove"
            ),
            "cells_scored": done,
            "cells_planned": len(issues),
            "corrector_abstentions": abstentions,
            "corrector_abstention_rate": (round(abstentions / done, 4) if done else None),
            "scorers": [
                _summary("review_ranker", ranker_pairs, _K_RANKER, enriched=enriched),
                _summary("structured_corrector", corrector_pairs, _K_CORRECTOR, enriched=enriched),
            ],
            "paired_auc_delta_ci95": list(_paired_auc_delta_ci(ranker_pairs, corrector_pairs)),
            "estimated_usd": round(client.cumulative_usd, 6),
        }
        _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
        _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        _upsert_receipt(
            replace(
                client.meter.receipt(
                    run_id=run_id,
                    method="triage_scorer_comparison",
                    dataset=args.dataset,
                    notes=(f"cells_scored={done}", f"abstentions={abstentions}"),
                )
            )
        )
        return payload

    for index, issue in enumerate(issues, start=1):
        contract = build_correction_contract(issue, constraints)
        if not contract.is_cell_correction:
            continue
        label = (issue.row, issue.column) in truth
        try:
            scores = ranker.rank([(issue.row, issue.column)], df)
            fix = corrector.propose(issue, df, None)
        except CostCapExceededError:
            print(f"  spend cap reached after {done} cells; stopping cleanly")
            break
        except Exception as exc:  # noqa: BLE001 - one bad cell must not kill a paid run
            print(f"  [{index}] skipped: {type(exc).__name__}: {exc}")
            continue
        if not scores:
            continue
        ranker_pairs.append((scores[0].score, label))
        if fix is None:
            abstentions += 1
            corrector_pairs.append((0.0, label))
        else:
            corrector_pairs.append((fix.confidence, label))
        done += 1
        if done % _CHECKPOINT_EVERY == 0:
            checkpoint()
            print(f"  [{done}/{len(issues)}] est ${client.cumulative_usd:.2f} (checkpointed)")

    payload = checkpoint()
    print("\n=== paired triage comparison ===")
    for s in payload["scorers"]:
        if s["n"]:
            print(
                f"{s['scorer']:22s} n={s['n']:3d} pos={s['positives']:3d} "
                f"AUC={s['roc_auc']:.3f} CI{s['roc_auc_ci95']} "
                f"calls/cell={s['calls_per_cell']}"
            )
    lo, hi = payload["paired_auc_delta_ci95"]
    print(f"\npaired AUC delta (corrector - ranker) 95% CI: [{lo}, {hi}]")
    if lo is not None:
        if lo > 0:
            print("  -> corrector ranks BETTER; it also returns candidate values.")
        elif hi < 0:
            print("  -> ranker ranks BETTER; keep it as the dedicated triager.")
        else:
            print("  -> INDISTINGUISHABLE at this n; cost per cell decides (1 vs 3 calls).")
    print(f"corrector abstention rate: {payload['corrector_abstention_rate']}")
    print(f"estimated spend: ${client.cumulative_usd:.4f}")
    print(f"artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
