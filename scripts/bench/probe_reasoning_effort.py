"""Does `reasoning_effort` raise corrector precision? The one lever unique to gpt-5.6-sol.

**Why this experiment exists.** The flagship certification target is arithmetically out of
reach, and the binding term is *precision*, not sample size. Measured from the 25-issue
flagship throughput probe: $0.0525/issue, 44.4s/issue, 12% proposal rate. The full remaining
budget buys ~157 proposals, while certification at `alpha = 0.05` needs **59 accepted samples
with zero errors** (1 error in 60 gives a Clopper-Pearson upper bound of 0.077 > 0.05). At the
best precision ever measured for this corrector (0.2973, `corrector_arm_sweep.json`), 157
proposals yield ~47 correct in total. Precision must exceed **0.50** for 59 correct proposals
to be merely *available*.

So the only question worth paying for is whether precision can be moved at all. Every prior
arm varied the *confidence signal* (free-text vs structured, k = 3/5/9); none varied the
model's *reasoning*. `probe_azure_capabilities.py` established that gpt-5.6-sol supports
`reasoning_effort` in `{none, low, medium, high, xhigh}` and **rejects `minimal`** -- the value
every committed gpt-5-mini reproduction command uses. gpt-5-mini had no comparable lever, so
this is genuinely new capability that the frontier deployment buys.

**Scope discipline, deliberately.** This runs on the pre-registered SWEEP slice only
(`eval/preregistration/api_phase_certification.md`), never the flagship set. The flagship arm
is already fixed at `B_structured_k9`; choosing a new arm on flagship data and then certifying
on it would be post-hoc selection wearing a conformal hat, which that pre-registration exists
to prevent. This is therefore **exploratory** and cannot produce a certificate. It writes its
own artifact and does not touch `corrector_arm_sweep.json`.

**Paired by construction.** Both arms see the identical issue list in the identical order, so
the comparison carries no between-arm sampling noise. k is held at 3: precision is the
question, and the confidence-grid question was already answered by the k sweep.

Run foreground and bounded::

    python scripts/bench/probe_reasoning_effort.py --max-issues 30 --max-usd 8
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from dataforge.bench.runner import _build_azure_client  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.repairers.contract import build_correction_contract  # noqa: E402
from dataforge.repairers.llm_corrector import LLMCorrectorRepairer  # noqa: E402
from dataforge.spend import CostCapExceededError, append_receipt  # noqa: E402
from scripts.bench.sweep_corrector_arms import sweep_and_flagship_issues  # noqa: E402

_ARTIFACT = ROOT / "eval" / "results" / "corrector_reasoning_effort_probe.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_K = 3
#: `minimal` is absent on purpose: gpt-5.6-sol rejects it outright.
_EFFORTS = ("none", "xhigh")


def _run_arm(
    *,
    effort: str,
    issues: list[Any],
    df: Any,
    ground_truth: dict[tuple[int, str], Any],
    max_usd: float,
    run_id: str,
    dataset_name: str,
) -> dict[str, Any]:
    """Measure one reasoning-effort arm on the shared issue list."""
    os.environ["DATAFORGE_AZURE_REASONING_EFFORT"] = effort
    client = _build_azure_client()

    def structured_call(messages: list[Any], response_format: dict[str, object] | None) -> str:
        return client.complete([dict(m) for m in messages], response_format).text

    corrector = LLMCorrectorRepairer(
        cache_dir=None,
        allow_llm=True,
        model=client.model,
        samples=_K,
        structured_completion_fn=structured_call,
        structured=True,
    )
    constraints = corrector._constraints_for(df, None)

    samples: list[dict[str, Any]] = []
    attempted = 0
    started = time.monotonic()
    for index, issue in enumerate(issues, start=1):
        contract = build_correction_contract(issue, constraints)
        if not contract.is_cell_correction:
            continue
        attempted += 1
        try:
            fix = corrector.propose(issue, df, None)
        except CostCapExceededError:
            print(f"    spend cap reached after {attempted} issues; stopping cleanly")
            break
        except Exception as exc:  # noqa: BLE001 - one bad issue must not kill a paid run
            print(f"    [{index}] skipped: {type(exc).__name__}: {str(exc)[:90]}")
            continue
        if fix is None:
            continue
        clean = ground_truth.get((issue.row, issue.column))
        samples.append(
            {
                "row": issue.row,
                "column": issue.column,
                "issue_type": issue.issue_type,
                "confidence": fix.confidence,
                "correct": bool(clean is not None and fix.fix.new_value == clean),
            }
        )
        if attempted % 10 == 0:
            print(f"    [{attempted}] proposals={len(samples)} est ${client.cumulative_usd:.2f}")
        if client.cumulative_usd >= max_usd:
            print(f"    local budget {max_usd} reached; stopping arm")
            break

    elapsed = time.monotonic() - started
    correct = sum(1 for s in samples if s["correct"])
    receipt = client.meter.receipt(
        run_id=f"{run_id}-{effort}",
        method="reasoning_effort_probe",
        dataset=dataset_name,
        notes=(
            f"effort={effort}",
            f"k={_K}",
            f"proposals={len(samples)}",
            "exploratory: sweep slice, no certificate",
        ),
    )
    return {
        "reasoning_effort": effort,
        "k": _K,
        "attempted": attempted,
        "proposals": len(samples),
        "correct": correct,
        "precision": round(correct / len(samples), 4) if samples else None,
        "proposal_rate": round(len(samples) / attempted, 4) if attempted else None,
        "mean_confidence": round(sum(s["confidence"] for s in samples) / len(samples), 4)
        if samples
        else None,
        "calls": receipt.calls,
        "prompt_tokens": receipt.prompt_tokens,
        "completion_tokens": receipt.completion_tokens,
        "reasoning_tokens": receipt.reasoning_tokens,
        "estimated_usd": round(client.cumulative_usd, 6),
        "seconds": round(elapsed, 1),
        "seconds_per_issue": round(elapsed / attempted, 2) if attempted else None,
        "receipt": receipt,
        "samples": samples,
    }


def main(argv: list[str] | None = None) -> int:
    """Run both reasoning-effort arms on the shared sweep slice and commit the artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-issues", type=int, default=30)
    parser.add_argument("--max-usd", type=float, default=8.0)
    parser.add_argument("--dataset", default="hospital")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)

    run_id = args.run_id or f"reasoning-effort-{uuid.uuid4().hex[:8]}"
    dataset = load_real_world_dataset(args.dataset)
    df = dataset.dirty_df.copy(deep=True)
    sweep_slice, _flagship = sweep_and_flagship_issues(dataset)
    issues = sweep_slice[: args.max_issues]
    ground_truth = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}

    print(
        f"{args.dataset}: SWEEP slice {len(sweep_slice)} issues; "
        f"running {len(issues)} issues x k={_K} for each of {_EFFORTS}"
    )
    print("(exploratory: sweep slice only, cannot produce a certificate)")

    per_arm_usd = args.max_usd / len(_EFFORTS)
    arms: list[dict[str, Any]] = []
    for effort in _EFFORTS:
        print(f"\n  arm reasoning_effort={effort} (budget ${per_arm_usd:.2f})")
        arms.append(
            _run_arm(
                effort=effort,
                issues=issues,
                df=df,
                ground_truth=ground_truth,
                max_usd=per_arm_usd,
                run_id=run_id,
                dataset_name=args.dataset,
            )
        )

    total_usd = sum(a["estimated_usd"] for a in arms)
    payload = {
        "schema": "dataforge_reasoning_effort_probe_v1",
        "provider": "azure",
        "model": os.environ.get("DATAFORGE_AZURE_MODEL", ""),
        "dataset": args.dataset,
        "slice": "preregistered_sweep_20pct",
        "exploratory": True,
        "why_exploratory": (
            "Runs on the sweep slice only. The flagship arm is fixed at B_structured_k9 by "
            "eval/preregistration/api_phase_certification.md; selecting an arm on flagship "
            "data and certifying on it would be post-hoc selection. No certificate can come "
            "from this artifact."
        ),
        "question": (
            "Can reasoning_effort move corrector PRECISION? Precision must exceed 0.50 for "
            "59 correct proposals to be available within budget, which is the binding term "
            "in the alpha=0.05 certification floor."
        ),
        "k": _K,
        "efforts": list(_EFFORTS),
        "issues_offered": len(issues),
        "arms": [{k: v for k, v in a.items() if k != "receipt"} for a in arms],
        "estimated_usd_total": round(total_usd, 6),
        "run_id": run_id,
    }
    _ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print("\n=== reasoning effort comparison ===")
    print(
        f"{'effort':<10}{'attempted':>10}{'props':>7}{'prec':>8}{'meanconf':>10}{'s/issue':>9}{'usd':>8}"
    )
    for a in arms:
        print(
            f"{a['reasoning_effort']:<10}{a['attempted']:>10}{a['proposals']:>7}"
            f"{str(a['precision']):>8}{str(a['mean_confidence']):>10}"
            f"{str(a['seconds_per_issue']):>9}{a['estimated_usd']:>8.3f}"
        )
    print(f"\ntotal estimated spend: ${total_usd:.4f}")
    print(f"artifact: {_ARTIFACT.relative_to(ROOT)}")

    try:
        for a in arms:
            if a["calls"]:
                append_receipt(_LEDGER, a["receipt"])
    except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
        print(f"WARNING: receipt not written: {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
