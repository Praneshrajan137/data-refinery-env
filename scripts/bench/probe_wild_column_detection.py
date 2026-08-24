"""Can an LLM detect errors in unconstrained wild columns, where heuristics get ~0.02 precision?

**Why this experiment exists.** On 2,397 real columns drawn from tables in the wild, the
heuristic ensemble reaches precision 0.0285 (RT-bench) and 0.0215 (ST-bench) -- 97 to 98 of every
100 flags are wrong. That is the hard case for this project, and whether a language model does
better on it is unanswered. `docs/trust/contamination-audit-result.md` authorises the question:
two paired probes and a passing negative control found no memorisation of these corpora.

**Why per column rather than per value.** 166,387 distinct values against 2,397 columns. One call
per column is ~70x cheaper, and it is also the *better* experiment: a value's plausibility is only
assessable against its column, so showing the model one value in isolation would handicap it in a
way no real deployment would.

**Why stratified, and why recall needs no projection.** Every ground-truth error lives in a
labelled column, and there are only 175 of those. Score them as a **census** and `tp`, `fn` and
recall are exact. Only false positives on the ~2,200 unlabelled columns need estimating, so a
single term is projected and the precision interval derives from it alone. See
`dataforge.bench.stratified.stratified_precision`.

**The baseline is recomputed on the same columns.** Comparing an LLM measured on a sample against
the published full-corpus heuristic figure would be sloppy, so the heuristic ensemble is scored on
exactly the sampled columns, restricted to `EVALUABLE_ON_DISTINCT_VALUES`. Including a
frequency-dependent detector would corrupt the baseline in an unknown direction, which is the
error `docs/trust/frequency-dependence-correction.md` records.

**Honest limit on the absolute level.** Unlabelled values are scored as clean, but the corpus
labels only errors annotators found, so an unlabelled-but-erroneous value counts against both
arms. The absolute precision is a lower bound. The *comparison* is fair because both arms are
scored against the same labels.

Run foreground and bounded::

    python scripts/bench/probe_wild_column_detection.py --calibrate --max-usd 1
    python scripts/bench/probe_wild_column_detection.py --max-usd 20
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

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from dataforge.bench.abstention import (  # noqa: E402
    ThreeWayScore,
    aggregate_three_way,
    score_detection_three_way,
)
from dataforge.bench.detection import (  # noqa: E402
    DECLARED_APPLICABILITY,
    EVALUABLE_ON_DISTINCT_VALUES,
    NotEvaluableError,
)
from dataforge.bench.groq_client import AzureBenchClient  # noqa: E402
from dataforge.bench.stratified import stratified_precision  # noqa: E402
from dataforge.datasets.column_corpus import BenchmarkColumn, load_column_benchmark  # noqa: E402
from dataforge.detectors import default_detectors  # noqa: E402
from dataforge.spend import (  # noqa: E402
    CostCapExceededError,
    ModelPrice,
    append_receipt,
    price_for,
)

_ARTIFACT = ROOT / "eval" / "results" / "wild_column_detection.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_wild_column_detection_v1"

SEED = 0
_REASONING_EFFORT = "none"
#: Unlabelled columns to sample per corpus. Fixed before the run.
_UNLABELLED_SAMPLE = 120
#: Values shown per column. Long columns are truncated to bound prompt cost; the truncation is
#: recorded per column so a reader can see which columns were not fully shown.
_MAX_VALUES_SHOWN = 60

_PROMPT = (
    "You are auditing one column of a real spreadsheet or database table. You are given the "
    "column header and its distinct values.\n\n"
    "Identify which values are ERRONEOUS: malformed, corrupted, wrongly typed, or inconsistent "
    "with the column's evident convention. Do NOT flag values that are merely unusual, rare, or "
    "that you personally cannot verify. Most columns contain NO errors at all -- returning an "
    "empty list is the correct answer for most columns.\n\n"
    "Reply with ONLY a JSON array of the erroneous values, copied exactly as given. Return [] if "
    "none are erroneous. No prose."
)


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


def _shown_values(column: BenchmarkColumn) -> tuple[list[str], bool]:
    """Return the values shown to the model, and whether truncation occurred.

    Labelled values are always retained, so truncation can never remove a ground-truth error and
    silently depress recall. The remainder is filled in corpus order.
    """
    values = list(column.distinct_values)
    if len(values) <= _MAX_VALUES_SHOWN:
        return values, False
    labelled = set(column.ground_truth) | set(column.debatable)
    keep = [value for value in values if value in labelled]
    for value in values:
        if len(keep) >= _MAX_VALUES_SHOWN:
            break
        if value not in labelled:
            keep.append(value)
    # Restore corpus order so the model never sees labelled values grouped first.
    order = {value: index for index, value in enumerate(values)}
    return sorted(keep, key=lambda v: order[v]), True


def _ask(
    client: AzureBenchClient,
    column: BenchmarkColumn,
    shown: list[str],
    failures: list[int],
) -> set[str]:
    """Ask which values in a column are erroneous. Returns the flagged subset of `shown`."""
    messages = [
        {"role": "system", "content": _PROMPT},
        {
            "role": "user",
            "content": json.dumps(
                {"header": column.header, "values": shown},
                ensure_ascii=False,
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
        return set()
    start, end = text.find("["), text.rfind("]")
    if start < 0 or end <= start:
        failures[0] += 1
        return set()
    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        failures[0] += 1
        return set()
    if not isinstance(parsed, list):
        failures[0] += 1
        return set()
    # Only values actually shown can be flagged. A hallucinated value is not a prediction about
    # this column and must not become a false positive.
    shown_set = set(shown)
    return {str(item) for item in parsed if str(item) in shown_set}


def _heuristic_flags(column: BenchmarkColumn, shown: list[str]) -> set[str]:
    """Flags from the evaluable heuristic detectors on the same values the model saw.

    Restricted to `EVALUABLE_ON_DISTINCT_VALUES`. A frequency-dependent detector scored on a
    deduplicated column describes a distribution that does not exist, and including one would
    corrupt this baseline in an unknown direction.
    """
    frame = pd.DataFrame({column.header or "column": shown}, dtype=str)
    flagged: set[str] = set()
    for detector in default_detectors():
        name = type(detector).__name__
        applicability = DECLARED_APPLICABILITY.get(name)
        if applicability is None:
            raise NotEvaluableError(f"{name} is not classified in DECLARED_APPLICABILITY")
        if applicability not in EVALUABLE_ON_DISTINCT_VALUES:
            continue
        for issue in detector.detect(frame, None):
            if 0 <= issue.row < len(shown):
                flagged.add(shown[issue.row])
    return flagged


def _score(column: BenchmarkColumn, shown: list[str], flagged: set[str]) -> ThreeWayScore:
    """Score one column under the three-way rule."""
    shown_set = set(shown)
    return score_detection_three_way(
        distinct_values=shown,
        # Only labelled values actually shown can be recalled; the truncation keeps all of
        # them, so in practice these are equal.
        ground_truth=[v for v in column.ground_truth if v in shown_set],
        debatable=[v for v in column.debatable if v in shown_set],
        predicted=flagged,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the wild-column LLM detection measurement."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-usd", type=float, default=20.0)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument(
        "--calibrate", action="store_true", help="Score 8 columns, project cost, exit."
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

    audit = ROOT / "eval" / "results" / "contamination_audit.json"
    if not audit.exists():
        print("refusing to run: eval/results/contamination_audit.json is absent.")
        print("The contamination audit gates this measurement; run probe_contamination.py first.")
        return 2
    verdict = json.loads(audit.read_text(encoding="utf-8"))
    if verdict.get("cancels_wild_column_measurement"):
        print(f"refusing to run: contamination audit status is {verdict.get('status')!r}.")
        print("The pre-registered kill criterion cancels this measurement.")
        return 2
    print(
        f"contamination audit: {verdict.get('status')} "
        f"(suspected={verdict.get('contamination_suspected')})"
    )

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
        max_tokens=1024,
    )
    failures = [0]
    rng = random.Random(SEED)
    run_id = args.run_id or f"wild-column-{uuid.uuid4().hex[:8]}"

    per_corpus: dict[str, Any] = {}
    for corpus in ("rt_bench", "st_bench"):
        benchmark = load_column_benchmark(corpus)
        # The census is every column carrying ANY label. Columns holding only debatable values
        # contribute no tp and no fn, but they can still attract false positives on their
        # unlabelled values -- excluding them would drop 89 columns of false-positive surface
        # from both arms. Census plus unlabelled must partition the corpus exactly.
        labelled = [c for c in benchmark.columns if c.ground_truth or c.debatable]
        unlabelled = [c for c in benchmark.columns if not c.ground_truth and not c.debatable]
        if len(labelled) + len(unlabelled) != len(benchmark.columns):
            raise NotEvaluableError(
                f"{corpus}: census {len(labelled)} + unlabelled {len(unlabelled)} does not "
                f"partition {len(benchmark.columns)} columns"
            )
        sample = rng.sample(unlabelled, min(_UNLABELLED_SAMPLE, len(unlabelled)))

        targets = labelled[:4] + sample[:4] if args.calibrate else labelled + sample

        print(
            f"\n{corpus}: census {len(labelled)} labelled columns, "
            f"sampling {len(sample)} of {len(unlabelled)} unlabelled"
        )

        llm_census: list[ThreeWayScore] = []
        heur_census: list[ThreeWayScore] = []
        llm_unlabelled_fp: list[int] = []
        heur_unlabelled_fp: list[int] = []
        truncated = 0
        for index, column in enumerate(targets, start=1):
            if client.cumulative_usd >= args.max_usd:
                print(f"      budget reached; stopping {corpus} at {index - 1}/{len(targets)}")
                break
            shown, was_truncated = _shown_values(column)
            truncated += int(was_truncated)
            llm_flags = _ask(client, column, shown, failures)
            heur_flags = _heuristic_flags(column, shown)
            if column.ground_truth or column.debatable:
                llm_census.append(_score(column, shown, llm_flags))
                heur_census.append(_score(column, shown, heur_flags))
            else:
                # No labels: every flag is a false positive under this corpus's labels.
                llm_unlabelled_fp.append(len(llm_flags))
                heur_unlabelled_fp.append(len(heur_flags))
            if index % 25 == 0:
                print(f"      {index}/{len(targets)} columns, ${client.cumulative_usd:.3f}")

        if args.calibrate:
            continue

        result: dict[str, Any] = {
            "census_columns": len(llm_census),
            "unlabelled_sampled": len(llm_unlabelled_fp),
            "unlabelled_population": len(unlabelled),
            "columns_truncated": truncated,
            "max_values_shown": _MAX_VALUES_SHOWN,
        }
        for arm, census, fps in (
            ("llm", llm_census, llm_unlabelled_fp),
            ("heuristic_evaluable", heur_census, heur_unlabelled_fp),
        ):
            if not census:
                result[arm] = {"available": False, "detail": "no labelled column was scored"}
                continue
            aggregate = aggregate_three_way(census)
            estimate = stratified_precision(
                census_score=aggregate,
                per_column_fp=fps,
                population_columns=len(unlabelled),
            )
            result[arm] = {
                "available": True,
                "census": aggregate.model_dump(),
                "projected": asdict(estimate),
                "projected_precision": estimate.precision,
                "projected_precision_ci95": list(estimate.precision_ci),
                "recall_exact": estimate.recall,
                "projected_fp_total": estimate.total_fp,
                "scale": estimate.scale,
            }
        per_corpus[corpus] = result

    if args.calibrate:
        calls = max(1, client.meter.calls)
        per_call = client.cumulative_usd / calls
        planned = 2 * (_UNLABELLED_SAMPLE + 110)
        print(
            f"\ncalibration: {calls} calls, ${client.cumulative_usd:.4f} "
            f"(${per_call:.5f}/call), {failures[0]} failed"
        )
        print(f"projection: ~{planned} calls -> ${per_call * planned:.2f} for the full run")
        return 0

    commit, dirty = _git_commit()
    payload = {
        "schema_version": _SCHEMA,
        "generated_at": datetime.now(UTC).isoformat(),
        "scoring_unit": "distinct_value",
        "scoring_spec": "specs/SPEC_abstention_scoring.md",
        "axis": "detection",
        "provider": "azure",
        "model": deployment,
        "api_version": api_version,
        "seed": SEED,
        "reasoning_effort": _REASONING_EFFORT,
        "contamination_audit": {
            "status": verdict.get("status"),
            "contamination_suspected": verdict.get("contamination_suspected"),
            "exchangeability_available": verdict.get("exchangeability_available"),
        },
        "baseline_detectors": sorted(
            name
            for name, applicability in DECLARED_APPLICABILITY.items()
            if applicability in EVALUABLE_ON_DISTINCT_VALUES
        ),
        "excluded_detectors": sorted(
            name
            for name, applicability in DECLARED_APPLICABILITY.items()
            if applicability not in EVALUABLE_ON_DISTINCT_VALUES
        ),
        "per_corpus": per_corpus,
        "failed_calls": failures[0],
        "calls": client.meter.calls,
        "estimated_usd": round(client.cumulative_usd, 6),
        "limitations": [
            "L1: dist_val holds DISTINCT values. These are not cell-level metrics and are NOT "
            "comparable to the cell-level figures in cell_detection_*.json; the measured gap "
            "between units runs up to total (docs/trust/scoring-unit-reconciliation.md).",
            "L2: ground_truth contains only unambiguous errors, so recall is an UPPER bound on "
            "recall over all real errors.",
            "L3: no clean values ship with these corpora. Detection only. Any correction number "
            "sourced from here would be fabricated.",
            "L4: unlabelled values are scored as clean, but the corpus labels only errors "
            "annotators found. An unlabelled-but-erroneous value counts against BOTH arms, so "
            "the absolute precision is a LOWER bound while the comparison stays fair.",
            "L5: the false-positive projection assumes unlabelled columns are exchangeable "
            "draws. Zero-flag columns are retained in the sample so the per-column rate is not "
            "inflated.",
            "L6: long columns are truncated to 60 shown values. All labelled values are always "
            "retained, so truncation cannot depress recall; it can only reduce the "
            "false-positive surface, which flatters both arms equally.",
            "L7: the heuristic baseline excludes frequency-dependent detectors, which cannot be "
            "evaluated on a deduplicated corpus. It is therefore not the full ensemble.",
        ],
        "provenance": {
            "git_commit": commit,
            "git_worktree_dirty": dirty,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
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
                    method="wild_column_detection",
                    dataset="rt_bench+st_bench",
                    notes=(
                        f"unlabelled_sample={_UNLABELLED_SAMPLE}/corpus",
                        f"max_values_shown={_MAX_VALUES_SHOWN}",
                        "detection only",
                    ),
                ),
            )
    except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
        print(f"WARNING: receipt not written: {type(exc).__name__}: {exc}")

    print(
        f"\n{'corpus':<12} {'arm':<22} {'precision':>10} {'ci95':>20} {'recall':>8} {'fp_tot':>9}"
    )
    for corpus, result in per_corpus.items():
        for arm in ("llm", "heuristic_evaluable"):
            entry = result.get(arm) or {}
            if not entry.get("available"):
                continue
            ci = entry["projected_precision_ci95"]
            print(
                f"{corpus:<12} {arm:<22} {str(entry['projected_precision']):>10} "
                f"{f'[{ci[0]}, {ci[1]}]':>20} {str(entry['recall_exact']):>8} "
                f"{entry['projected_fp_total']:>9}"
            )
    print(f"\ncalls={client.meter.calls} failed={failures[0]} spend=${client.cumulative_usd:.4f}")
    print(f"artifact: {_ARTIFACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
