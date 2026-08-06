"""Stage 3: the flagship run on the disjoint flagship set, per pre-registration.

Runs the selected arm (`B_structured_k9`, see AMENDMENT 2) on the 80% of hospital
issues that played no part in arm selection, then discharges the pre-registered
analysis end to end:

    split_by_class(seed)            -> disjoint calibration / test halves
    fit_calibration_map_by_class    -> isotonic maps, fit on CALIBRATION only
    calibrate_samples_by_class      -> apply maps to the TEST half
    certify_thresholds_by_class     -> Clopper-Pearson + fixed sequential testing
    calibrated_conformal_corrector_policy -> (policy, maps)

Two honesty properties are structural rather than advisory:

* **The policy is keyed by `issue_type`**, which is the key
  ``_partition_auto_apply`` actually uses at inference. Keying by ground-truth
  error class would certify a vocabulary the engine never consults.
* **ECE is reported on the disjoint TEST half only.** Fitting a calibration map and
  then reporting its ECE on the same samples measures nothing.

Per AMENDMENT 2 the primary endpoint is expected to be a NULL: certification needs
>= 59 all-correct accepted samples and the budget buys roughly 24. This run
therefore reports a measurement and an honest uncertified reason, never a
certificate. The artifact is written to a mode-keyed path so it can never be
mistaken for -- or overwrite -- the free-text `corrector_calibration.json`, whose
maps were fit on a different distribution.

Run foreground and bounded::

    python scripts/bench/run_flagship_certification.py --max-issues 700 --max-usd 20
"""

from __future__ import annotations

import argparse
import json
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
from dataforge.bench.runner import _build_azure_client  # noqa: E402
from dataforge.calibration import calibrated_conformal_corrector_policy  # noqa: E402
from dataforge.calibration_map import (  # noqa: E402
    calibrate_samples_by_class,
    fit_calibration_map_by_class,
)
from dataforge.conformal import (  # noqa: E402
    certification_reason,
    certify_thresholds_by_class,
    min_samples_for_certification,
    split_by_class,
)
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.repairers.contract import build_correction_contract  # noqa: E402
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer  # noqa: E402
from dataforge.spend import CostCapExceededError, load_ledger  # noqa: E402
from scripts.bench.sweep_corrector_arms import sweep_and_flagship_issues  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "corrector_calibration_structured.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_corrector_calibration_structured_v1"

# Fixed by the pre-registration.
_SPLIT_SEED = 20260804
_ALPHA = 0.05
_DELTA = 0.05
_MIN_SUPPORT = 30
_CALIB_FRACTION = 0.5
_K = 9
_ARM = "B_structured_k9"


def _upsert_receipt(receipt: Any) -> None:
    """Replace this run's ledger receipt in place (one entry per run)."""
    receipts = [r for r in load_ledger(_LEDGER) if r.get("run_id") != receipt.run_id]
    receipts.append(receipt.to_payload())
    _LEDGER.parent.mkdir(parents=True, exist_ok=True)
    _LEDGER.write_text(
        json.dumps({"schema": "dataforge_spend_ledger_v1", "receipts": receipts}, indent=2) + "\n",
        encoding="utf-8",
    )


def _analyse(samples_by_type: dict[str, list[tuple[float, bool]]]) -> dict[str, Any]:
    """Run the full pre-registered conformal analysis on collected samples."""
    if not samples_by_type:
        return {"status": "no_samples"}

    calibration, test = split_by_class(
        samples_by_type, seed=_SPLIT_SEED, calib_fraction=_CALIB_FRACTION
    )
    maps = fit_calibration_map_by_class(calibration, method="isotonic", min_support=1)
    calibrated_test = calibrate_samples_by_class(maps, test)
    thresholds = certify_thresholds_by_class(
        calibration, alpha=_ALPHA, delta=_DELTA, min_support=_MIN_SUPPORT
    )
    policy, policy_maps = calibrated_conformal_corrector_policy(
        calibration, method="isotonic", alpha=_ALPHA, delta=_DELTA, min_support=_MIN_SUPPORT
    )

    flat_test = [p for pairs in test.values() for p in pairs]
    flat_cal_test = [p for pairs in calibrated_test.values() for p in pairs]
    per_type: dict[str, Any] = {}
    total_accepted = 0
    for issue_type, pairs in sorted(test.items()):
        threshold = thresholds.get(issue_type)
        reachable = threshold is not None and threshold <= 1.0
        accepted = (
            [ok for conf, ok in pairs if conf >= threshold] if reachable and threshold else []
        )
        total_accepted += len(accepted)
        per_type[issue_type] = {
            "n_calibration": len(calibration.get(issue_type, [])),
            "n_test": len(pairs),
            "threshold": threshold,
            "threshold_is_reachable": reachable,
            "test_accepted": len(accepted),
            "test_coverage": round(len(accepted) / len(pairs), 4) if pairs else 0.0,
            "test_errors_in_accepted": sum(1 for ok in accepted if not ok),
            "uncertified_reason": certification_reason(
                calibration.get(issue_type, []),
                alpha=_ALPHA,
                delta=_DELTA,
                min_support=_MIN_SUPPORT,
            ),
        }

    # The clean-slice measurement: the propose-only tier this run exists to quantify.
    all_pairs = sorted(
        (p for pairs in samples_by_type.values() for p in pairs), key=lambda x: -x[0]
    )
    clean_slice = {"n": 0, "threshold": None}
    for cut in sorted({c for c, _ in all_pairs}, reverse=True):
        accepted = [ok for c, ok in all_pairs if c >= cut]
        if all(accepted):
            clean_slice = {"n": len(accepted), "threshold": cut}
        else:
            break

    return {
        "status": "analysed",
        "n_total": len(all_pairs),
        "precision_overall": round(sum(1 for _, ok in all_pairs if ok) / len(all_pairs), 4),
        "ece_test_before": round(expected_calibration_error(flat_test), 4) if flat_test else None,
        "ece_test_after": (
            round(expected_calibration_error(flat_cal_test), 4) if flat_cal_test else None
        ),
        "overall_test_coverage": (round(total_accepted / len(flat_test), 4) if flat_test else 0.0),
        "by_issue_type": per_type,
        "largest_all_correct_slice": clean_slice,
        "samples_needed_to_certify": min_samples_for_certification(_ALPHA, _DELTA),
        "policy": policy.model_dump(),
        "maps": {k: v.model_dump() for k, v in policy_maps.items()},
        "samples_by_type": {
            t: [[c, ok] for c, ok in pairs] for t, pairs in sorted(samples_by_type.items())
        },
    }


def _payload(
    *,
    client: Any,
    dataset: str,
    completed: int,
    planned: int,
    samples_by_type: dict[str, list[tuple[float, bool]]],
    attempted: int,
    proposals: int,
) -> dict[str, Any]:
    """Assemble the committed artifact."""
    analysis = _analyse(samples_by_type)
    return {
        "schema": _SCHEMA,
        "preregistration": "eval/preregistration/api_phase_certification.md",
        "deviation": (
            "AMENDMENT 2: arm selection overridden from A_freetext_k3 to "
            "B_structured_k9 (free-text confidence has measured zero "
            "discrimination). Primary endpoint expected NULL and declared so in "
            "advance; certification is arithmetically out of reach within budget."
        ),
        "arm": _ARM,
        "k": _K,
        "dataset": dataset,
        "provider": "azure",
        "model": client.model,
        "complete": completed >= planned,
        "issues_completed": completed,
        "issues_planned": planned,
        "attempted": attempted,
        "proposals": proposals,
        "conformal": {
            "alpha": _ALPHA,
            "delta": _DELTA,
            "min_support": _MIN_SUPPORT,
            "calib_fraction": _CALIB_FRACTION,
            "split_seed": _SPLIT_SEED,
            "pooled_across_seeds": False,
        },
        "analysis": analysis,
        "certified": bool(
            analysis.get("status") == "analysed"
            and any(v["threshold_is_reachable"] for v in analysis.get("by_issue_type", {}).values())
        ),
        "estimated_usd": round(client.cumulative_usd, 6),
    }


def main() -> int:
    """Run the flagship measurement and commit the artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-issues", type=int, default=700)
    parser.add_argument("--max-usd", type=float, default=20.0)
    parser.add_argument("--dataset", default="hospital")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    load_dotenv()
    import os

    os.environ["DATAFORGE_AZURE_MAX_USD"] = str(args.max_usd)
    client = _build_azure_client()
    run_id = args.run_id or f"flagship-{uuid.uuid4().hex[:8]}"

    dataset = load_real_world_dataset(args.dataset)
    df = dataset.dirty_df.copy(deep=True)
    _sweep, flagship_set = sweep_and_flagship_issues(dataset)
    issues = flagship_set[: args.max_issues]
    ground_truth = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}
    print(
        f"{args.dataset}: flagship set {len(flagship_set)}; running {_ARM} on "
        f"{len(issues)} issues x {_K} calls (disjoint from the sweep slice)"
    )

    def structured_call(messages: list[Message], response_format: dict[str, object] | None) -> str:
        completion = client.complete(
            [dict(m) for m in messages],  # type: ignore[arg-type]
            response_format,
        )
        return completion.text

    corrector = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model=client.model,
        samples=_K,
        structured_completion_fn=structured_call,
        structured=True,
    )
    constraints = corrector._constraints_for(df, None)

    samples_by_type: dict[str, list[tuple[float, bool]]] = {}
    attempted = 0
    proposals = 0
    completed = 0

    def checkpoint() -> dict[str, Any]:
        payload = _payload(
            client=client,
            dataset=args.dataset,
            completed=completed,
            planned=len(issues),
            samples_by_type=samples_by_type,
            attempted=attempted,
            proposals=proposals,
        )
        _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
        _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        receipt = replace(
            client.meter.receipt(
                run_id=run_id,
                method="flagship_structured_k9",
                dataset=args.dataset,
                notes=(
                    f"arm={_ARM}",
                    f"issues_completed={completed}",
                    f"proposals={proposals}",
                    f"certified={payload['certified']}",
                ),
            )
        )
        _upsert_receipt(receipt)
        return payload

    for index, issue in enumerate(issues, start=1):
        contract = build_correction_contract(issue, constraints)
        if not contract.is_cell_correction:
            continue
        attempted += 1
        try:
            fix = corrector.propose(issue, df, None)
        except CostCapExceededError:
            print(f"  spend cap reached after {completed} issues; stopping cleanly")
            break
        except Exception as exc:  # noqa: BLE001 - one bad issue must not kill a paid run
            print(f"  [{index}] skipped: {type(exc).__name__}: {exc}")
            continue
        completed = index
        if fix is not None:
            proposals += 1
            clean = ground_truth.get((issue.row, issue.column))
            was_correct = clean is not None and fix.fix.new_value == clean
            samples_by_type.setdefault(issue.issue_type, []).append((fix.confidence, was_correct))
        if index % 25 == 0:
            checkpoint()
            print(
                f"  [{index}/{len(issues)}] proposals={proposals} "
                f"est ${client.cumulative_usd:.2f} (checkpointed)"
            )

    payload = checkpoint()
    analysis = payload["analysis"]
    print("\n=== flagship result ===")
    print(f"attempted={attempted} proposals={proposals}")
    if analysis.get("status") == "analysed":
        print(f"overall precision      : {analysis['precision_overall']}")
        print(
            f"ECE test before/after  : {analysis['ece_test_before']} / {analysis['ece_test_after']}"
        )
        print(
            f"largest all-correct    : n={analysis['largest_all_correct_slice']['n']} "
            f"at conf>={analysis['largest_all_correct_slice']['threshold']}"
        )
        print(f"needed to certify      : {analysis['samples_needed_to_certify']} all-correct")
        print(f"certified              : {payload['certified']}")
        for t, v in analysis["by_issue_type"].items():
            print(f"  {t}: {str(v['uncertified_reason'])[:100]}")
    print(f"estimated spend        : ${client.cumulative_usd:.4f}")
    print(f"artifact               : {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
