"""Measure the label-noise instrument on real hospital data, and probe acquiescence.

Three things are measured here, and they must never be conflated.

1. **Does the candidate-pruning fix certify on real data?** Certification is run over the same
   labelled session with ``prune_infeasible`` off and on. This is the free power fix from
   ``dataforge.conformal.feasible_candidate_sequence``. Labels come from RAHA ground truth, so
   ``label_source='oracle'``: there is no labeller to bound and no noise adjustment applies.

2. **Acquiescence of a MODEL asked to ratify a known-wrong value.** This is *not* ``beta``.
   ``beta`` is a property of a human labeller and cannot be obtained without one; substituting a
   model would measure a different thing under a borrowed name, and substituting the oracle would
   return 0 and manufacture the guarantee under test. It is reported under its own name,
   ``llm_acquiescence_rate``, and is barred from entering any certificate.

3. **Whether plant difficulty matters**, by running the same probe against two item classes:
   ``column_distribution`` plants (a value resampled from the column) and ``corrector_generated``
   items (real wrong proposals the corrector actually made, identified via ground truth). If the
   second is accepted more often, plant-based estimates understate the false-accept rate, which is
   the category error ``docs/trust/human-label-noise.md`` is built around.

Usage::

    python scripts/bench/measure_label_noise_instrument.py            # no spend, parts 1 and 3-prep
    python scripts/bench/measure_label_noise_instrument.py --probe    # SPENDS MONEY on part 2
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.calibration_session import (  # noqa: E402
    CERTIFICATION_GRID,
    CalibrationSessionArtifact,
    certify_from_session,
    dump_calibration_session,
    load_calibration_session,
)
from dataforge.conformal import (  # noqa: E402
    certify_threshold,
    label_noise_adjusted_bound,
    min_samples_for_certification,
    min_samples_under_label_noise,
)
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402

_SESSION = PROJECT_ROOT / "eval" / ".dataforge" / "calibration" / "session.json"
_OUT = PROJECT_ROOT / "eval" / "results" / "label_noise_instrument.json"
_COMMITTED_SESSION = PROJECT_ROOT / "eval" / "results" / "hospital_calibration_session.json"
_ALPHAS = (0.05, 0.10, 0.20)


def _oracle_label(artifact: CalibrationSessionArtifact) -> tuple[CalibrationSessionArtifact, int]:
    """Judge every proposal against RAHA ground truth, producing an oracle-labelled session."""
    dataset = load_real_world_dataset("hospital")
    clean = dataset.clean_df
    labelled: list[dict[str, Any]] = []
    scored = 0
    for sample in artifact.samples:
        payload = sample.model_dump()
        if sample.proposed_repair is None or sample.repair_confidence is None:
            labelled.append(payload)
            continue
        truth = str(clean.iloc[sample.row][sample.column])
        payload["repair_decision"] = (
            "correct" if str(sample.proposed_repair).strip() == truth.strip() else "error"
        )
        scored += 1
        labelled.append(payload)
    data = artifact.model_dump()
    data["samples"] = labelled
    data["label_source"] = "oracle"
    # Plants are irrelevant to an oracle run and must not imply a measured beta.
    data["planted_controls"] = []
    return CalibrationSessionArtifact.model_validate(data), scored


def _pairs_by_class(artifact: CalibrationSessionArtifact) -> dict[str, list[tuple[float, bool]]]:
    out: dict[str, list[tuple[float, bool]]] = {}
    for sample in artifact.samples:
        if sample.repair_decision == "pending" or sample.repair_confidence is None:
            continue
        out.setdefault(sample.issue_type, []).append(
            (sample.repair_confidence, sample.repair_decision == "correct")
        )
    return out


def _probe(artifact: CalibrationSessionArtifact, max_usd: float) -> dict[str, Any]:
    """Ask the model to ratify known-wrong values. Measures a MODEL, never a person."""
    from dataforge.bench.runner import _build_azure_client

    dataset = load_real_world_dataset("hospital")
    clean = dataset.clean_df

    items: list[dict[str, Any]] = []
    for control in artifact.planted_controls:
        items.append(
            {
                "klass": "column_distribution",
                "column": control.column,
                "shown": control.flagged_value,
                "proposed": control.planted_value,
                "truth": control.withheld_truth,
            }
        )
    # Real corrector errors: distributionally identical to what a labeller actually faces.
    for sample in artifact.samples:
        if sample.proposed_repair is None:
            continue
        truth = str(clean.iloc[sample.row][sample.column]).strip()
        if str(sample.proposed_repair).strip() == truth:
            continue
        items.append(
            {
                "klass": "corrector_generated",
                "column": sample.column,
                "shown": sample.flagged_value,
                "proposed": str(sample.proposed_repair),
                "truth": truth,
            }
        )

    client = _build_azure_client()
    response_format: dict[str, object] = {
        "type": "json_schema",
        "json_schema": {
            "name": "ratification",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {"correct": {"type": "boolean"}},
                "required": ["correct"],
                "additionalProperties": False,
            },
        },
    }
    accepted: dict[str, int] = {}
    total: dict[str, int] = {}
    stopped: str | None = None
    for item in items:
        if client.cumulative_usd >= max_usd:
            stopped = f"spend cap ${max_usd} reached after {sum(total.values())} items"
            break
        prompt = (
            "You are reviewing a proposed repair to a cell in a hospital-records table.\n"
            f"Column: {item['column']}\n"
            f'Current value: "{item["shown"]}"\n'
            f'Proposed replacement: "{item["proposed"]}"\n'
            "Is the proposed replacement the correct value for this cell?"
        )
        try:
            raw = client.complete([{"role": "user", "content": prompt}], response_format).text
        except Exception as exc:  # noqa: BLE001 - a provider fault must not lose the receipt
            stopped = f"{type(exc).__name__}: {exc}"
            break
        try:
            verdict = bool(json.loads(raw).get("correct"))
        except (json.JSONDecodeError, AttributeError):
            continue
        total[item["klass"]] = total.get(item["klass"], 0) + 1
        if verdict:
            accepted[item["klass"]] = accepted.get(item["klass"], 0) + 1

    out: dict[str, Any] = {
        "what_this_measures": (
            "The rate at which a MODEL ratifies a value known to be wrong. This is NOT beta. "
            "beta is a property of a human labeller and cannot be obtained without one. A model "
            "is not a stand-in: it has no automation bias toward a peer's suggestion in the way "
            "a person has toward a machine's. This number must never enter a certificate."
        ),
        "model": f"{client.provider}/{client.model}",
        "spend_usd": round(client.cumulative_usd, 4),
        "stopped_early": stopped,
        "by_class": {},
    }
    for klass in sorted(total):
        n = total[klass]
        k = accepted.get(klass, 0)
        _, upper, _ = label_noise_adjusted_bound(0, max(1, n), false_accepts=k, controls=n)
        out["by_class"][klass] = {
            "n": n,
            "accepted_known_wrong": k,
            "rate": round(k / n, 4) if n else None,
            "upper_bound_at_delta_over_2": round(upper, 4),
        }
    _write_receipt(client, sum(total.values()))
    return out


def _write_receipt(client: Any, items: int) -> None:
    """Upsert a spend receipt for the probe.

    Paid work that writes no receipt loses its own cost: one detached run in this project spent
    ~$6 with no measured record and had to be reconstructed as an upper bound. Upserted by run id,
    never appended, because ``SpendMeter.receipt()`` reports CUMULATIVE spend and summing
    per-checkpoint snapshots once inflated a $14.35 run to $74.49.
    """
    from dataforge.spend import load_ledger

    ledger = PROJECT_ROOT / "eval" / "results" / "spend_ledger.json"
    run_id = "llm-acquiescence-probe"
    try:
        receipt = client.meter.receipt(
            run_id=run_id,
            method="llm_acquiescence_probe",
            dataset="hospital",
            notes=(
                f"items={items}",
                "measures MODEL acquiescence on known-wrong values; NOT human beta",
            ),
        )
        kept = (
            [r for r in load_ledger(ledger) if r.get("run_id") != run_id] if ledger.exists() else []
        )
        ledger.write_text(
            json.dumps(
                {
                    "schema": "dataforge_spend_ledger_v1",
                    "receipts": [*kept, receipt.to_payload()],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
        print(f"WARNING: could not write receipt: {type(exc).__name__}: {exc}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", action="store_true", help="SPENDS MONEY on the model probe.")
    parser.add_argument("--probe-max-usd", type=float, default=2.0)
    args = parser.parse_args()

    if not _SESSION.exists():
        print(f"no session at {_SESSION}", file=sys.stderr)
        return 2
    human_session = load_calibration_session(_SESSION)
    oracle_session, scored = _oracle_label(human_session)

    by_class = _pairs_by_class(oracle_session)
    pruning: dict[str, Any] = {}
    for issue_type, pairs in sorted(by_class.items()):
        correct = sum(1 for _, ok in pairs if ok)
        entry: dict[str, Any] = {
            "n": len(pairs),
            "correct": correct,
            "precision": round(correct / len(pairs), 4) if pairs else None,
            "by_alpha": {},
        }
        for alpha in _ALPHAS:
            entry["by_alpha"][f"{alpha}"] = {
                "unpruned": certify_threshold(
                    pairs, alpha=alpha, delta=0.05, grid=CERTIFICATION_GRID
                ),
                "pruned": certify_threshold(
                    pairs,
                    alpha=alpha,
                    delta=0.05,
                    grid=CERTIFICATION_GRID,
                    prune_infeasible=True,
                ),
                "naive_floor": min_samples_for_certification(alpha, 0.05),
                "noise_aware_floor_k30": min_samples_under_label_noise(alpha, controls=30),
            }
        pruning[issue_type] = entry

    certified = certify_from_session(oracle_session, alpha=0.05)
    result: dict[str, Any] = {
        "artifact": "dataforge_label_noise_instrument_v1",
        "purpose": (
            "Three separate measurements: (1) whether candidate pruning certifies on real "
            "hospital data under oracle labels; (2) the acquiescence rate of a MODEL asked to "
            "ratify known-wrong values, which is NOT beta; (3) whether plant difficulty differs "
            "from real corrector errors. beta on a HUMAN is NOT measured here and remains the "
            "open pre-registered question."
        ),
        "dataset": "hospital",
        "source_sha256": human_session.source_sha256,
        "corrector": f"{human_session.corrector_provider}/{human_session.corrector_model}",
        "proposals_scored_against_ground_truth": scored,
        "label_source_for_certification": "oracle",
        "oracle_certification_alpha_0.05": certified.model_dump(mode="json"),
        "pruning_effect_by_class": pruning,
        "human_beta": None,
        "human_beta_status": (
            "NOT MEASURED. Requires a human labeller. RAHA ground truth would return beta = 0 by "
            "construction and manufacture the guarantee under test; a model would measure a "
            "different quantity. Pre-registered in eval/preregistration/human_label_noise.md."
        ),
    }
    if args.probe:
        result["llm_acquiescence_probe"] = _probe(human_session, args.probe_max_usd)

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    if _OUT.exists():
        # Never write a subset: merge onto whatever is already committed.
        existing = json.loads(_OUT.read_text(encoding="utf-8"))
        existing.update(result)
        result = existing
    _OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # Commit the raw session so these numbers are re-derivable. The 2026-08-20 session was never
    # committed and its figures are consequently unverifiable; that is not repeated.
    _COMMITTED_SESSION.write_text(dump_calibration_session(oracle_session), encoding="utf-8")

    print(f"wrote {_OUT.relative_to(PROJECT_ROOT)}")
    print(f"wrote {_COMMITTED_SESSION.relative_to(PROJECT_ROOT)}")
    for issue_type, entry in pruning.items():
        print(
            f"  {issue_type}: {entry['correct']}/{entry['n']} = {entry['precision']}  "
            f"alpha=0.05 unpruned={entry['by_alpha']['0.05']['unpruned']} "
            f"pruned={entry['by_alpha']['0.05']['pruned']}"
        )
    if args.probe and "llm_acquiescence_probe" in result:
        for klass, stats in result["llm_acquiescence_probe"]["by_class"].items():
            print(
                f"  MODEL acquiescence [{klass}]: {stats['accepted_known_wrong']}/{stats['n']} "
                f"= {stats['rate']} (upper {stats['upper_bound_at_delta_over_2']})"
            )
    return 0


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUTF8", "1")
    raise SystemExit(main())
