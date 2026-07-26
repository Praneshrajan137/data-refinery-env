"""Step 2 probe (credit-gated): evaluate gpt-5.6-sol as an error DETECTOR.

Two value propositions, measured against the deterministic ensemble's two failure
modes (see step 1):

  A) flights recall-booster: detectors there are high-precision but miss ~49% of
     errors. Does the LLM flag residual errors (the ones detectors missed), and at
     what precision?
  B) hospital precision-filter: detectors flag ~10.4k cells to surface ~455 real
     errors (~4% precision). Can the LLM triage a flagged cell as truly-wrong vs
     not, lifting the review-queue precision?

The LLM only FLAGS (proposition A) or JUDGES a flagged cell (proposition B); it
never proposes a value and nothing is auto-applied. Uses the USD-guarded
AzureBenchClient. Writes a committed artifact for the GO/NO-GO gate.
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from dataforge.bench.runner import _build_azure_client  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors import run_all_detectors  # noqa: E402
from dataforge.repair_contract import extract_json_payload  # noqa: E402
from dataforge.schema_inference import infer_schema  # noqa: E402
from dataforge.table import cell_value  # noqa: E402

FLIGHTS_ROWS = 60
HOSP_TRUE = 60
HOSP_FALSE = 90
SEED = 0


def _detector_state(name: str) -> dict[str, Any]:
    ds = load_real_world_dataset(name, verify_hashes=True)
    df = ds.dirty_df.copy(deep=True)
    schema = infer_schema(df).to_schema(include_inferred_constraints=True)
    issues = run_all_detectors(ds.dirty_df.copy(deep=True), schema=schema)
    detected = {(i.row, i.column) for i in issues}
    gt = {(c.row, c.column): c.clean_value for c in ds.ground_truth}
    columns = list(ds.canonical_columns)
    return {"ds": ds, "df": df, "detected": detected, "gt": gt, "columns": columns}


def _row_values(df: Any, row: int, columns: list[str]) -> dict[str, str]:
    return {col: str(cell_value(df, row, col)) for col in columns}


def _safe_complete(client: Any, messages: list[dict[str, str]], failures: list[int]) -> str:
    """Call the model; on a transient provider error return "" and count it.

    A single bad server response (e.g. a transient HTTP 500, which the client does
    not retry) must not abort the whole probe. Failed calls are counted and treated
    as "no signal" so the measurement degrades gracefully and stays honest.
    """
    try:
        return client.complete(messages).text
    except Exception:  # noqa: BLE001 - probe resilience; provider errors are expected
        failures[0] += 1
        return ""


def _flagged_columns(text: str, columns: set[str]) -> set[str]:
    try:
        payload = extract_json_payload(text)
    except ValueError:
        return set()
    if not isinstance(payload, list):
        return set()
    return {str(item) for item in payload if str(item) in columns}


def probe_a_flights(client: Any) -> dict[str, Any]:
    """Recall-booster: does the LLM flag the errors the detectors missed?"""
    st = _detector_state("flights")
    df, detected, gt, columns = st["df"], st["detected"], st["gt"], st["columns"]
    col_set = set(columns)
    residual = set(gt) - detected
    rows_with_residual = sorted({row for row, _ in residual})
    rng = random.Random(SEED)
    sample_rows = sorted(rng.sample(rows_with_residual, min(FLIGHTS_ROWS, len(rows_with_residual))))

    residual_in_sample = {(r, c) for (r, c) in residual if r in sample_rows}
    gt_in_sample = {(r, c) for (r, c) in gt if r in sample_rows}
    llm_flags: set[tuple[int, str]] = set()
    failures = [0]
    for row in sample_rows:
        values = _row_values(df, row, columns)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data-quality auditor for a flights table. Given one row, "
                    "identify columns whose value looks erroneous (wrong, malformed, or "
                    "inconsistent with the rest of the row). Respond with ONLY a compact "
                    "JSON array of column names that look wrong, or [] if none. Do not "
                    "correct values; do not add prose."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({"columns": columns, "row": values}, separators=(",", ":")),
            },
        ]
        text = _safe_complete(client, messages, failures)
        for col in _flagged_columns(text, col_set):
            llm_flags.add((row, col))

    flagged_true = llm_flags & set(gt_in_sample)
    residual_hits = llm_flags & residual_in_sample
    det_residual_baseline = 0  # detectors miss the residual by definition
    return {
        "proposition": "A_flights_recall_booster",
        "sample_rows": len(sample_rows),
        "failed_calls": failures[0],
        "residual_in_sample": len(residual_in_sample),
        "gt_errors_in_sample": len(gt_in_sample),
        "llm_flags": len(llm_flags),
        "llm_flags_that_are_true_errors": len(flagged_true),
        "llm_detection_precision": round(len(flagged_true) / len(llm_flags), 4)
        if llm_flags
        else 0.0,
        "llm_residual_recall": round(len(residual_hits) / len(residual_in_sample), 4)
        if residual_in_sample
        else 0.0,
        "detector_residual_recall_baseline": det_residual_baseline,
    }


def probe_b_hospital(client: Any) -> dict[str, Any]:
    """Precision-filter: can the LLM separate true-flagged from false-flagged cells?"""
    st = _detector_state("hospital")
    df, detected, gt, columns = st["df"], st["detected"], st["gt"], st["columns"]
    true_flagged = sorted(detected & set(gt))
    false_flagged = sorted(detected - set(gt))
    rng = random.Random(SEED)
    true_sample = rng.sample(true_flagged, min(HOSP_TRUE, len(true_flagged)))
    false_sample = rng.sample(false_flagged, min(HOSP_FALSE, len(false_flagged)))
    sample = [(cell, True) for cell in true_sample] + [(cell, False) for cell in false_sample]
    rng.shuffle(sample)

    tp = fp = fn = tn = 0
    failures = [0]
    for (row, col), is_true in sample:
        values = _row_values(df, row, columns)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data-quality auditor for a hospital table. A specific cell "
                    "has been flagged as possibly erroneous. Using the whole row as context, "
                    "decide whether the flagged cell's value is actually erroneous. Respond "
                    "with ONLY 'yes' (erroneous) or 'no' (fine). No prose."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {"flagged_column": col, "flagged_value": values.get(col, ""), "row": values},
                    separators=(",", ":"),
                ),
            },
        ]
        says_wrong = _safe_complete(client, messages, failures).strip().lower().startswith("y")
        if says_wrong and is_true:
            tp += 1
        elif says_wrong and not is_true:
            fp += 1
        elif not says_wrong and is_true:
            fn += 1
        else:
            tn += 1

    sample_precision = round(tp / (tp + fp), 4) if (tp + fp) else 0.0
    sample_recall = round(tp / (tp + fn), 4) if (tp + fn) else 0.0
    # Project the review-queue precision lift onto the natural base rate.
    n_true = len(true_flagged)
    n_false = len(false_flagged)
    tp_keep = tp / len(true_sample) if true_sample else 0.0
    fp_keep = fp / len(false_sample) if false_sample else 0.0
    kept_true = n_true * tp_keep
    kept_false = n_false * fp_keep
    projected = round(kept_true / (kept_true + kept_false), 4) if (kept_true + kept_false) else 0.0
    return {
        "proposition": "B_hospital_precision_filter",
        "flagged_total": len(detected),
        "failed_calls": failures[0],
        "flagged_true_errors": n_true,
        "baseline_queue_precision": round(n_true / len(detected), 4) if detected else 0.0,
        "sample": {"true": len(true_sample), "false": len(false_sample)},
        "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
        "llm_sample_precision": sample_precision,
        "llm_sample_recall": sample_recall,
        "projected_queue_precision_after_filter": projected,
        "projected_true_retained_fraction": round(tp_keep, 4),
        "projected_false_retained_fraction": round(fp_keep, 4),
    }


def _wilson(k: int, n: int) -> list[float]:
    """95% Wilson score interval for k successes in n trials."""
    if n == 0:
        return [0.0, 0.0]
    z = 1.96
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)) / denom
    return [round(max(0.0, center - half), 4), round(min(1.0, center + half), 4)]


def confirm_b_hospital(client: Any, n: int) -> dict[str, Any]:
    """Confirm B on the NATURAL distribution: a uniform random sample of flagged
    cells (no reweighting), directly measuring the real post-filter queue
    precision and recall with 95% Wilson intervals."""
    st = _detector_state("hospital")
    df, detected, gt, columns = st["df"], st["detected"], st["gt"], st["columns"]
    flagged = sorted(detected)
    rng = random.Random(SEED)
    sample = rng.sample(flagged, min(n, len(flagged)))

    failures = [0]
    tp = fp = fn = tn = 0
    for row, col in sample:
        is_true = (row, col) in gt
        values = _row_values(df, row, columns)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data-quality auditor for a hospital table. A specific cell "
                    "has been flagged as possibly erroneous. Using the whole row as context, "
                    "decide whether the flagged cell's value is actually erroneous. Respond "
                    "with ONLY 'yes' (erroneous) or 'no' (fine). No prose."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {"flagged_column": col, "flagged_value": values.get(col, ""), "row": values},
                    separators=(",", ":"),
                ),
            },
        ]
        says_wrong = _safe_complete(client, messages, failures).strip().lower().startswith("y")
        if says_wrong and is_true:
            tp += 1
        elif says_wrong and not is_true:
            fp += 1
        elif not says_wrong and is_true:
            fn += 1
        else:
            tn += 1

    natural_true = tp + fn
    says_wrong = tp + fp
    return {
        "proposition": "B_confirm_natural_distribution",
        "flagged_total": len(detected),
        "sample_size": len(sample),
        "natural_true_in_sample": natural_true,
        "baseline_precision": round(natural_true / len(sample), 4) if sample else 0.0,
        "failed_calls": failures[0],
        "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
        "post_filter_queue_precision": round(tp / says_wrong, 4) if says_wrong else 0.0,
        "post_filter_queue_precision_ci95": _wilson(tp, says_wrong),
        "recall_retained": round(tp / natural_true, 4) if natural_true else 0.0,
        "recall_retained_ci95": _wilson(tp, natural_true),
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm-b",
        type=int,
        default=0,
        help="If >0, run ONLY the natural-distribution confirmation of B on N uniform flagged cells.",
    )
    args = parser.parse_args()

    load_dotenv()  # repo-root .env (Azure creds + USD guard)
    client = _build_azure_client()

    if args.confirm_b > 0:
        result: dict[str, Any] = {
            "artifact": "dataforge_llm_detector_confirm_v1",
            "model": client.model,
            "seed": SEED,
            "B_confirm": confirm_b_hospital(client, args.confirm_b),
        }
        out = ROOT / "eval" / "results" / "llm_detector_confirm.json"
    else:
        result = {
            "artifact": "dataforge_llm_detector_probe_v1",
            "model": client.model,
            "seed": SEED,
            "A": probe_a_flights(client),
            "B": probe_b_hospital(client),
        }
        out = ROOT / "eval" / "results" / "llm_detector_probe.json"

    print(json.dumps(result, indent=2, sort_keys=True))
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
