"""Stage 2: choose the corrector arm on a held-aside slice, per pre-registration.

Runs the four pre-registered arms on the SWEEP slice only (the first 20% of the
deterministically shuffled hospital issue list) and reports, per arm, the
projected certified threshold from the real ``conformal.certify_threshold``.

Two design choices matter for honesty and cost:

* **The slice is disjoint from the flagship set.** Choosing an arm and then
  certifying on the same data would be post-hoc selection dressed up as a
  guarantee. The flagship uses the remaining 80%.
* **Draws are paired.** Nine structured samples are drawn once per issue and
  ``k = 3, 5, 9`` are evaluated on nested prefixes. Prefixes of iid draws are iid,
  so this is valid; it halves the call count and removes between-arm sampling
  noise from the ``k`` comparison.

Read ``eval/preregistration/api_phase_certification.md`` before changing anything
here. Run foreground and bounded.

Usage::

    python scripts/bench/sweep_corrector_arms.py --max-issues 60 --max-usd 8
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
from dataforge.bench.error_classes import expected_calibration_error  # noqa: E402
from dataforge.bench.ranking_metrics import precision_at_k, roc_auc  # noqa: E402
from dataforge.bench.runner import _build_azure_client  # noqa: E402
from dataforge.conformal import certification_reason, certify_threshold  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors import run_all_detectors  # noqa: E402
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402
from dataforge.spend import CostCapExceededError  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "corrector_arm_sweep.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_corrector_arm_sweep_v1"

# Fixed by the pre-registration. Do not tune.
_SPLIT_SEED = 20260804
_SWEEP_FRACTION = 0.20
_ALPHA = 0.05
_DELTA = 0.05
_MIN_SUPPORT = 30
_STRUCTURED_DRAWS = 9
_FREETEXT_DRAWS = 3
_K_VALUES = (3, 5, 9)


def sweep_and_flagship_issues(dataset: Any) -> tuple[list[Any], list[Any]]:
    """Return (sweep_slice, flagship_set) using the pre-registered split.

    Shared with the flagship script so both agree on the partition by
    construction rather than by comment.
    """
    inferred = infer_schema(dataset.dirty_df.copy(deep=True)).to_schema(
        include_inferred_constraints=True
    )
    issues = run_all_detectors(dataset.dirty_df.copy(deep=True), schema=inferred)
    ordered = sorted(issues, key=lambda issue: (issue.row, issue.column))
    random.Random(_SPLIT_SEED).shuffle(ordered)
    cut = int(len(ordered) * _SWEEP_FRACTION)
    return ordered[:cut], ordered[cut:]


def _draw(
    corrector: LLMCorrectorRepairer,
    issue: Any,
    df: Any,
    draws: int,
) -> list[str]:
    """Draw ``draws`` raw completions for one issue using the corrector's prompt."""
    from dataforge.repairers.contract import build_correction_contract

    constraints = corrector._constraints_for(df, None)
    contract = build_correction_contract(issue, constraints)
    if not contract.is_cell_correction:
        return []
    prompt = corrector._build_messages(issue, df, contract)
    response_format = corrector._response_format(df, issue)
    return [corrector._one_sample(prompt, response_format) for _ in range(draws)]


def _evaluate(
    raw: list[str],
    issue: Any,
    df: Any,
    *,
    k: int,
    structured: bool,
) -> tuple[str | None, float]:
    """Replay the first ``k`` draws through the REAL vote logic.

    Deliberately reuses ``LLMCorrectorRepairer`` rather than reimplementing the
    vote: any divergence between the sweep's scoring and the product's scoring
    would invalidate the whole comparison.
    """
    prefix = raw[:k]
    if not prefix:
        return None, 0.0
    cursor = {"i": 0}

    def replay(_messages: list[Message]) -> str:
        index = min(cursor["i"], len(prefix) - 1)
        cursor["i"] += 1
        return prefix[index]

    corrector = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model="replay",
        samples=k,
        completion_fn=replay,
        structured=structured,
    )
    fix = corrector.propose(issue, df, None)
    if fix is None:
        return None, 0.0
    return fix.fix.new_value, fix.confidence


def _bootstrap_auc_ci(
    pairs: list[tuple[float, bool]], *, iters: int = 4000, seed: int = 7
) -> tuple[float | None, float | None]:
    """Return a bootstrap 95% CI for ROC-AUC, or (None, None) if undefined.

    The CI is what makes the discrimination claim checkable: at n~37 a point
    estimate alone cannot distinguish "real ordering" from "lucky draws".
    """
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


def _arm_report(
    samples_by_type: dict[str, list[tuple[float, bool]]],
    *,
    proposals: int,
    attempted: int,
) -> dict[str, Any]:
    """Summarise one arm, including its projected certified threshold."""
    flat = [pair for pairs in samples_by_type.values() for pair in pairs]
    correct = sum(1 for _, ok in flat if ok)
    grid = sorted({round(conf, 6) for conf, _ in flat})
    per_type: dict[str, Any] = {}
    best_coverage = 0.0
    for issue_type, pairs in sorted(samples_by_type.items()):
        threshold = certify_threshold(pairs, alpha=_ALPHA, delta=_DELTA, min_support=_MIN_SUPPORT)
        coverage = (
            0.0
            if threshold is None
            else sum(1 for conf, _ in pairs if conf >= threshold) / len(pairs)
        )
        best_coverage = max(best_coverage, coverage)
        per_type[issue_type] = {
            "n": len(pairs),
            "precision": round(correct / len(flat), 4) if flat else 0.0,
            "certified_threshold": threshold,
            "projected_coverage": round(coverage, 4),
            "uncertified_reason": certification_reason(
                pairs, alpha=_ALPHA, delta=_DELTA, min_support=_MIN_SUPPORT
            ),
        }

    return {
        "attempted_issues": attempted,
        "proposals": proposals,
        "abstentions": attempted - proposals,
        "abstention_rate": round((attempted - proposals) / attempted, 4) if attempted else 0.0,
        "precision": round(correct / len(flat), 4) if flat else 0.0,
        "correct": correct,
        "confidence_grid_size": len(grid),
        "confidence_grid": grid[:25],
        # DISCRIMINATION is the metric that matters for certification: only the
        # precision of the accepted set affects certify_threshold. ROC-AUC is
        # threshold-free, so unlike a hand-picked slice it cannot be gamed.
        "roc_auc": round(roc_auc(flat), 4) if flat else None,
        "roc_auc_ci95": list(_bootstrap_auc_ci(flat)),
        "precision_at_top_20pct": (
            round(precision_at_k(flat, max(1, round(0.2 * len(flat)))), 4) if flat else None
        ),
        # ECE is reported as a SECONDARY observation only, never as evidence of
        # value. It is a weighted mean of |mean_confidence - accuracy|, so when
        # accuracy is low any uniformly-lower score improves it with zero gain in
        # ordering -- it conflates calibration with discrimination.
        "ece_secondary_not_evidence": (
            round(expected_calibration_error(flat), 4) if flat else None
        ),
        "ece": round(expected_calibration_error(flat), 4) if flat else None,
        "by_issue_type": per_type,
        # Persisting the raw pairs is what makes every downstream question
        # ('is there a clean high-confidence slice?') answerable offline instead
        # of requiring another paid run.
        "samples_by_type": {
            issue_type: [[conf, ok] for conf, ok in pairs]
            for issue_type, pairs in sorted(samples_by_type.items())
        },
        "projected_certified_coverage": round(best_coverage, 4),
    }


def _build_payload(
    *,
    args: argparse.Namespace,
    client: Any,
    sweep_slice: list[Any],
    flagship_set: list[Any],
    issues: list[Any],
    arms: dict[str, dict[str, Any]],
    completed: int,
    prior_usd: float = 0.0,
) -> dict[str, Any]:
    """Assemble the artifact payload from whatever has been measured so far.

    Factored out so the run can checkpoint: a long paid run that is interrupted
    must still leave usable evidence on disk rather than discarding money.
    """
    report = {
        name: _arm_report(
            arm["samples_by_type"], proposals=arm["proposals"], attempted=arm["attempted"]
        )
        for name, arm in arms.items()
    }

    # Pre-registered selection rule: highest projected certified coverage, ties
    # (including all-zero) break to the lowest k for cost.
    def rank(item: tuple[str, dict[str, Any]]) -> tuple[float, int]:
        name, data = item
        k = int(name.rsplit("k", 1)[1])
        return (-data["projected_certified_coverage"], k)

    selected = sorted(report.items(), key=rank)[0][0]
    all_zero = all(r["projected_certified_coverage"] == 0.0 for r in report.values())
    return {
        "schema": _SCHEMA,
        "preregistration": "eval/preregistration/api_phase_certification.md",
        "dataset": args.dataset,
        "provider": "azure",
        "model": client.model,
        "complete": completed >= len(issues),
        "issues_completed": completed,
        "issues_planned": len(issues),
        "split": {
            "seed": _SPLIT_SEED,
            "sweep_fraction": _SWEEP_FRACTION,
            "sweep_size": len(sweep_slice),
            "flagship_size": len(flagship_set),
            "probed": len(issues),
        },
        "conformal": {"alpha": _ALPHA, "delta": _DELTA, "min_support": _MIN_SUPPORT},
        "arms": report,
        "selected_arm": selected,
        "all_arms_project_zero_coverage": all_zero,
        "selection_rule": (
            "highest projected certified coverage; ties and all-zero break to lowest k"
        ),
        "estimated_usd": round(client.cumulative_usd + prior_usd, 6),
        "estimated_usd_this_process": round(client.cumulative_usd, 6),
    }


def _load_checkpoint() -> tuple[dict[str, dict[str, Any]], float] | None:
    """Return (arm state, prior spend) so a run can resume without re-paying.

    Resuming matters because these are billable draws: re-running the first N
    issues to extend a sweep would pay twice for the same evidence. The prior
    spend is carried forward because ``cumulative_usd`` is per-process -- without
    it, a resumed run would report only its own cost and the ledger would lose
    the earlier segment.
    """
    if not _ARTIFACT.exists():
        return None
    try:
        payload = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    arms = payload.get("arms")
    if not isinstance(arms, dict):
        return None
    restored: dict[str, dict[str, Any]] = {}
    for name, data in arms.items():
        raw = data.get("samples_by_type")
        if not isinstance(raw, dict):
            return None
        restored[name] = {
            "samples_by_type": {
                str(t): [(float(c), bool(ok)) for c, ok in pairs] for t, pairs in raw.items()
            },
            "proposals": int(data.get("proposals", 0)),
            "attempted": int(data.get("attempted_issues", 0)),
        }
    prior = float(payload.get("estimated_usd", 0.0) or 0.0)
    return (restored, prior) if restored else None


def _checkpoint(
    *,
    args: argparse.Namespace,
    client: Any,
    sweep_slice: list[Any],
    flagship_set: list[Any],
    issues: list[Any],
    arms: dict[str, dict[str, Any]],
    completed: int,
    run_id: str,
    prior_usd: float = 0.0,
) -> dict[str, Any]:
    """Write the artifact AND a spend receipt for work completed so far.

    The receipt is written on every checkpoint, not only at the end: an
    interrupted paid run previously left real spend with no ledger entry, which
    is precisely the accounting hole this phase exists to close.
    """
    payload = _build_payload(
        args=args,
        client=client,
        sweep_slice=sweep_slice,
        flagship_set=flagship_set,
        issues=issues,
        arms=arms,
        completed=completed,
        prior_usd=prior_usd,
    )
    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    receipt = client.meter.receipt(
        run_id=run_id,
        method="corrector_arm_sweep",
        dataset=args.dataset,
        notes=(
            f"selected={payload['selected_arm']}",
            f"issues_completed={completed}",
            f"complete={payload['complete']}",
        ),
    )
    if prior_usd:
        # The receipt must report the whole run's spend, not just this process's
        # segment, or resuming would silently erase the earlier cost.
        receipt = replace(
            receipt,
            estimated_usd=round(receipt.estimated_usd + prior_usd, 6),
            notes=(*receipt.notes, f"includes_prior_segments_usd={prior_usd:.6f}"),
        )
    _upsert_receipt(receipt)
    return payload


def _upsert_receipt(receipt: Any) -> None:
    """Replace this run's ledger receipt in place, keeping the ledger one-per-run.

    Append-only is right across runs, but a *single* run that checkpoints ten
    times must not look like ten separate runs, so its own entry is updated.
    """
    from dataforge.spend import load_ledger

    receipts = [r for r in load_ledger(_LEDGER) if r.get("run_id") != receipt.run_id]
    receipts.append(receipt.to_payload())
    _LEDGER.parent.mkdir(parents=True, exist_ok=True)
    _LEDGER.write_text(
        json.dumps({"schema": "dataforge_spend_ledger_v1", "receipts": receipts}, indent=2) + "\n",
        encoding="utf-8",
    )


def _reanalyse(args: argparse.Namespace) -> int:
    """Recompute the artifact's metrics offline from its persisted samples.

    Free and network-free. Exists because the first analysis of this sweep leaned on
    ECE, which is confounded for this question; the persisted pairs let the corrected
    discrimination metrics be produced without paying again.
    """
    if not _ARTIFACT.exists():
        print(f"No artifact at {_ARTIFACT}; nothing to reanalyse.", file=sys.stderr)
        return 2
    payload = json.loads(_ARTIFACT.read_text(encoding="utf-8"))
    arms = payload.get("arms")
    if not isinstance(arms, dict):
        print("Artifact has no arms block.", file=sys.stderr)
        return 2

    rebuilt: dict[str, Any] = {}
    for name, data in arms.items():
        raw = data.get("samples_by_type") or {}
        samples = {str(t): [(float(c), bool(ok)) for c, ok in pairs] for t, pairs in raw.items()}
        rebuilt[name] = _arm_report(
            samples,
            proposals=int(data.get("proposals", 0)),
            attempted=int(data.get("attempted_issues", 0)),
        )
    payload["arms"] = rebuilt
    payload["reanalysed"] = True
    payload["reanalysis_note"] = (
        "Metrics recomputed offline from persisted (confidence, correct) pairs. No "
        "provider calls, no spend. Adds roc_auc + bootstrap CI + top-20% precision, "
        "which are the confound-free discrimination metrics; ECE is retained only as a "
        "secondary observation because it conflates calibration with discrimination."
    )
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print("arm                  n   prec   ROC-AUC  CI95            top20%  ECE(2nd)")
    for name, r in rebuilt.items():
        lo, hi = r["roc_auc_ci95"]
        ci = f"[{lo}, {hi}]" if lo is not None else "n/a"
        print(
            f"{name:18s} {r['proposals']:3d}  {r['precision']:.3f}  "
            f"{r['roc_auc']!s:>6}   {ci:16s} {r['precision_at_top_20pct']!s:>6}  "
            f"{r['ece_secondary_not_evidence']}"
        )
    print(f"\nArtifact rewritten (no spend): {_ARTIFACT.relative_to(ROOT)}")
    return 0


def main() -> int:
    """Run the arm sweep and commit the selection artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-issues", type=int, default=60)
    parser.add_argument("--max-usd", type=float, default=8.0)
    parser.add_argument("--dataset", default="hospital")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse the committed checkpoint and only measure issues not yet paid for.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Stable ledger run id; reuse it when resuming so the run has ONE receipt.",
    )
    parser.add_argument(
        "--reanalyse",
        action="store_true",
        help="Recompute metrics from the committed checkpoint's persisted samples and "
        "rewrite the artifact. Makes NO provider calls and spends nothing -- this is "
        "why raw (confidence, correct) pairs are persisted.",
    )
    args = parser.parse_args()

    load_dotenv()
    import os

    if args.reanalyse:
        return _reanalyse(args)

    os.environ["DATAFORGE_AZURE_MAX_USD"] = str(args.max_usd)
    client = _build_azure_client()

    dataset = load_real_world_dataset(args.dataset)
    df = dataset.dirty_df.copy(deep=True)
    sweep_slice, flagship_set = sweep_and_flagship_issues(dataset)
    issues = sweep_slice[: args.max_issues]
    print(
        f"{args.dataset}: {len(sweep_slice)} sweep / {len(flagship_set)} flagship; "
        f"probing {len(issues)} issues x {_FREETEXT_DRAWS + _STRUCTURED_DRAWS} calls"
    )

    ground_truth = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}

    def structured_call(messages: list[Message], response_format: dict[str, object] | None) -> str:
        # Forwarding the schema is the whole point: without it the "structured"
        # arm would silently be free text wearing a structured prompt.
        completion = client.complete(
            [dict(m) for m in messages],  # type: ignore[arg-type]
            response_format,
        )
        return completion.text

    def plain_call(messages: list[Message]) -> str:
        completion = client.complete([dict(m) for m in messages])  # type: ignore[arg-type]
        return completion.text

    freetext_drawer = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model=client.model,
        samples=_FREETEXT_DRAWS,
        completion_fn=plain_call,
        pool_constrained=True,
    )
    structured_drawer = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model=client.model,
        samples=_STRUCTURED_DRAWS,
        structured_completion_fn=structured_call,
        structured=True,
    )

    arms: dict[str, dict[str, Any]] = {
        "A_freetext_k3": {"samples_by_type": {}, "proposals": 0, "attempted": 0},
        **{
            f"B_structured_k{k}": {"samples_by_type": {}, "proposals": 0, "attempted": 0}
            for k in _K_VALUES
        },
    }

    start_index = 0
    prior_usd = 0.0
    if args.resume:
        checkpoint = _load_checkpoint()
        if checkpoint is None:
            print("--resume requested but no reusable checkpoint found; starting fresh.")
        elif set(checkpoint[0]) != set(arms):
            print("--resume found a checkpoint with different arms; starting fresh.")
        else:
            arms, prior_usd = checkpoint
            start_index = max(a["attempted"] for a in arms.values())
            print(
                f"Resuming after {start_index} already-paid issues "
                f"({sum(a['proposals'] for a in arms.values())} proposals restored, "
                f"${prior_usd:.4f} prior spend carried forward)."
            )

    run_id = args.run_id or f"corrector-arm-sweep-{uuid.uuid4().hex[:8]}"
    pending = issues[start_index:]
    if not pending:
        print("Nothing left to measure for this --max-issues; increase it to extend the sweep.")
    completed = start_index
    for offset, issue in enumerate(pending, start=1):
        index = start_index + offset
        key = (issue.row, issue.column)
        clean = ground_truth.get(key)
        try:
            free_raw = _draw(freetext_drawer, issue, df, _FREETEXT_DRAWS)
            structured_raw = _draw(structured_drawer, issue, df, _STRUCTURED_DRAWS)
        except CostCapExceededError:
            print(f"  spend cap reached after {completed} issues; stopping cleanly")
            break
        except Exception as exc:  # noqa: BLE001 - one bad issue must not kill the run
            print(f"  [{index}/{len(issues)}] {key} skipped: {type(exc).__name__}: {exc}")
            continue

        plan = [("A_freetext_k3", free_raw, 3, False)] + [
            (f"B_structured_k{k}", structured_raw, k, True) for k in _K_VALUES
        ]
        for arm_name, raw, k, structured in plan:
            arm = arms[arm_name]
            arm["attempted"] += 1
            value, confidence = _evaluate(raw, issue, df, k=k, structured=structured)
            if value is None:
                continue
            arm["proposals"] += 1
            was_correct = clean is not None and value == clean
            arm["samples_by_type"].setdefault(issue.issue_type, []).append(
                (confidence, was_correct)
            )
        completed = index
        if index % 10 == 0:
            # Checkpoint artifact AND receipt: an interrupted paid run must leave
            # both its evidence and its accounting on disk.
            _checkpoint(
                args=args,
                client=client,
                sweep_slice=sweep_slice,
                flagship_set=flagship_set,
                issues=issues,
                arms=arms,
                completed=completed,
                run_id=run_id,
                prior_usd=prior_usd,
            )
            print(f"  [{index}/{len(issues)}] est ${client.cumulative_usd:.3f} (checkpointed)")

    payload = _checkpoint(
        args=args,
        client=client,
        sweep_slice=sweep_slice,
        flagship_set=flagship_set,
        issues=issues,
        arms=arms,
        completed=completed,
        run_id=run_id,
        prior_usd=prior_usd,
    )
    report = payload["arms"]
    selected = payload["selected_arm"]
    all_zero = payload["all_arms_project_zero_coverage"]

    print("\nArm                  prop  prec   grid  ECE     proj.coverage")
    for name, data in report.items():
        print(
            f"{name:20s} {data['proposals']:4d}  {data['precision']:.3f}  "
            f"{data['confidence_grid_size']:4d}  "
            f"{data['ece'] if data['ece'] is not None else float('nan'):.4f}  "
            f"{data['projected_certified_coverage']:.4f}"
        )
    print(f"\nSelected arm: {selected}")
    if all_zero:
        print("All arms project ZERO certified coverage (a pre-registered valid outcome).")
    print(f"Estimated spend this run: ${client.cumulative_usd:.4f}")
    print(f"Artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
