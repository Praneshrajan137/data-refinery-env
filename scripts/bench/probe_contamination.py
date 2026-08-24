"""Has gpt-5.6-sol memorised RT-bench or ST-bench? Audit before measuring.

**Why this experiment exists.** RT-bench and ST-bench are a public GitHub corpus with
published labels. Any LLM detection number measured on them is a capability claim only if the
model has not seen them. If it has, the number is a memorisation score wearing a benchmark's
name. This runs first because it can **cancel** the expensive measurement, and sequencing it
afterwards would create a standing incentive to find it inconclusive.

**Two probes, not three.** Oren et al.'s provable exchangeability test needs the
log-likelihood of a caller-supplied ordering. This deployment rejects ``logprobs`` outright
(HTTP 400 ``unsupported_parameter``, re-confirmed 2026-08-24), and chat-completions logprobs
cover *generated* tokens only, so the quantity is not obtainable even where the parameter is
accepted. C1 is recorded as unavailable and **excluded from the flag count -- never scored as
passing**.

**Everything is a paired contrast.** Both probes present identical items in identical order
and differ only in whether the corpus is named. That is what makes them capability-controlled:
a model that is simply good at spotting errors is equally good in both arms, so only
corpus-identification can move the delta. An absolute score against chance is reported but
cannot gate anything.

**C4 is not a third contamination probe.** It runs the same two procedures against generated
columns the model cannot have seen, and it must NOT flag. If it does, the probes are measuring
their own prompt design and the audit is VOID.

Pre-registration: ``eval/preregistration/contamination_audit.md`` (see Amendment 1, which
records the C3 redesign, the named paired test, and the reasoning-effort pin -- all before any
call was made). Spec: ``specs/SPEC_contamination_audit.md``. Verdict rule:
``dataforge.bench.contamination``.

Run foreground and bounded. Calibrate first, so a cost surprise is found on 8 calls rather
than 800::

    python scripts/bench/probe_contamination.py --calibrate --max-usd 1
    python scripts/bench/probe_contamination.py --max-usd 15
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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from dataforge.bench.contamination import (  # noqa: E402
    ALPHA,
    C2_MIN_DELTA,
    ContaminationAuditError,
    ProbeOutcome,
    decide_verdict,
    exchangeability_available,
    majority_base_rate,
    paired_signflip_p_value,
)
from dataforge.bench.groq_client import AzureBenchClient  # noqa: E402
from dataforge.datasets.column_corpus import BenchmarkColumn, load_column_benchmark  # noqa: E402
from dataforge.datasets.registry import COLUMN_BENCHMARK_REGISTRY  # noqa: E402
from dataforge.spend import (  # noqa: E402
    CostCapExceededError,
    ModelPrice,
    append_receipt,
    price_for,
)

_ARTIFACT = ROOT / "eval" / "results" / "contamination_audit.json"
_CAPABILITY = ROOT / "eval" / "results" / "azure_capability_probe.json"
_LEDGER = ROOT / "eval" / "results" / "spend_ledger.json"
_SCHEMA = "dataforge_contamination_audit_v1"

# Pre-registered, Amendment 1. Fixed here rather than passed in, for the same reason
# RISK_COVERAGE_GRID is a module constant.
SEED = 0
_C2_COLUMNS_PER_CORPUS = 60
_C2_MIN_DISTINCT = 20
_C2_PREFIX_FRACTION = 0.60
_C3_BATCH = 5
_C4_COLUMNS = 60
_RESAMPLES = 20_000

#: Suppresses reasoning tokens, which are billed as output at $0.015/1k and would dominate
#: the budget unpinned. Applied identically to both arms so it cannot bias a paired contrast.
_REASONING_EFFORT = "none"

_CORPUS_DESCRIPTION = {
    "rt_bench": (
        "the RT-bench relational-table benchmark from the Auto-Test paper "
        "(Chen et al., SIGMOD 2025), published at github.com/qixuchen/AutoTest "
        "as benchmarks/rt_bench.csv"
    ),
    "st_bench": (
        "the ST-bench spreadsheet-table benchmark from the Auto-Test paper "
        "(Chen et al., SIGMOD 2025), published at github.com/qixuchen/AutoTest "
        "as benchmarks/st_bench.csv"
    ),
}

#: A corpus that does not exist. The guided arm of the negative control names this, so if
#: naming *any* corpus improves recall the effect is prompt design, not memory.
_FAKE_DESCRIPTION = (
    "the QV-bench relational-table benchmark from the Quorum-Validate paper "
    "(Ð Ð°Ð»ÑŒÑ„ et al., VLDB 2024), published at github.com/qv-bench/quorum as "
    "benchmarks/qv_bench.csv"
)


@dataclass(frozen=True, slots=True)
class PairedItem:
    """One paired observation: identical content, two framings."""

    key: str
    guided_score: float
    general_score: float

    @property
    def delta(self) -> float:
        """Guided minus general."""
        return self.guided_score - self.general_score


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


def _ask(client: AzureBenchClient, prompt: str, failures: list[int]) -> str:
    """Call the model; on a transient provider error return "" and count it.

    A single bad server response must not abort a paid run. Failed calls are counted, treated
    as no signal, and surfaced on the artifact -- degrading gracefully without degrading
    honesty. A cost-cap breach is re-raised, because continuing past the cap is not graceful.
    """
    try:
        return client.complete([{"role": "user", "content": prompt}]).text
    except CostCapExceededError:
        raise
    except Exception as exc:  # noqa: BLE001 - one bad response must not kill a paid run
        failures[0] += 1
        print(f"      call failed: {type(exc).__name__}: {str(exc)[:80]}")
        return ""


def _parse_values(text: str) -> set[str]:
    """Extract a set of values from a JSON array, tolerating prose around it."""
    start, end = text.find("["), text.rfind("]")
    if start < 0 or end <= start:
        return set()
    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return set()
    if not isinstance(parsed, list):
        return set()
    return {str(item).strip() for item in parsed if isinstance(item, str | int | float)}


def _parse_labels(text: str, keys: list[str]) -> dict[str, str]:
    """Extract a {column_id: ERROR|DEBATABLE} mapping from a JSON object."""
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for key in keys:
        raw = str(parsed.get(key, "")).strip().upper()
        if raw in {"ERROR", "DEBATABLE"}:
            out[key] = raw
    return out


def _c2_prompt(column: BenchmarkColumn, prefix: list[str], n_hidden: int, named: str | None) -> str:
    """Build a C2 completion prompt. `named` set means the guided arm."""
    intro = (
        f"The following values come from a column in {named}.\n"
        if named
        else "The following values come from a column in a table.\n"
    )
    return (
        f"{intro}"
        f"Column header: {column.header!r}\n"
        f"First {len(prefix)} distinct values, in the order they appear in the source file:\n"
        f"{json.dumps(prefix, ensure_ascii=False)}\n\n"
        f"The source file contains exactly {n_hidden} further distinct values in this column, "
        f"which have been withheld. Reproduce them.\n"
        f"Reply with ONLY a JSON array of {n_hidden} strings. No prose, no explanation."
    )


def _c3_prompt(batch: list[tuple[str, BenchmarkColumn]], named: str | None) -> str:
    """Build a C3 column-label prompt over a batch. `named` set means the guided arm."""
    intro = (
        f"In {named}, each column carries a human annotation recording whether its flagged "
        "values were judged unambiguously erroneous, or merely arguable.\n"
        if named
        else "Each column below has flagged values that a human reviewer judged either "
        "unambiguously erroneous, or merely arguable.\n"
    )
    lines = []
    for key, column in batch:
        flagged = sorted(set(column.ground_truth) | set(column.debatable))
        sample = sorted(v for v in column.distinct_values if v not in set(flagged))[:12]
        lines.append(
            f"{key}: header={column.header!r}\n"
            f"   flagged values: {json.dumps(flagged, ensure_ascii=False)}\n"
            f"   other values in the column: {json.dumps(sample, ensure_ascii=False)}"
        )
    keys = ", ".join(key for key, _ in batch)
    return (
        f"{intro}\n"
        + "\n".join(lines)
        + f"\n\nFor each of {keys}, state the recorded annotation.\n"
        'Reply with ONLY a JSON object mapping each id to either "ERROR" or "DEBATABLE". '
        "No prose."
    )


def _run_c2(
    client: AzureBenchClient,
    columns: list[tuple[str, BenchmarkColumn, str | None]],
    failures: list[int],
    *,
    max_usd: float,
) -> list[PairedItem]:
    """Run the guided/general completion contrast over the given columns."""
    items: list[PairedItem] = []
    for index, (key, column, named) in enumerate(columns, start=1):
        values = list(column.distinct_values)
        split = max(1, int(len(values) * _C2_PREFIX_FRACTION))
        prefix, hidden = values[:split], values[split:]
        if not hidden:
            continue
        scores: dict[str, float] = {}
        for arm, corpus_name in (("guided", named), ("general", None)):
            if client.cumulative_usd >= max_usd:
                print(f"      local budget {max_usd} reached; stopping C2")
                return items
            reply = _ask(client, _c2_prompt(column, prefix, len(hidden), corpus_name), failures)
            recovered = _parse_values(reply) & set(hidden)
            scores[arm] = len(recovered) / len(hidden)
        items.append(
            PairedItem(key=key, guided_score=scores["guided"], general_score=scores["general"])
        )
        if index % 10 == 0:
            print(f"      C2 {index}/{len(columns)} columns, ${client.cumulative_usd:.3f}")
    return items


def _run_c3(
    client: AzureBenchClient,
    labelled: list[tuple[str, BenchmarkColumn, str | None]],
    failures: list[int],
    *,
    max_usd: float,
) -> list[PairedItem]:
    """Run the guided/general column-label contrast, batched."""
    items: list[PairedItem] = []
    batches = [labelled[i : i + _C3_BATCH] for i in range(0, len(labelled), _C3_BATCH)]
    for index, batch in enumerate(batches, start=1):
        pairs = [(key, column) for key, column, _ in batch]
        named = batch[0][2]
        truth = {key: ("ERROR" if column.ground_truth else "DEBATABLE") for key, column in pairs}
        keys = [key for key, _ in pairs]
        answers: dict[str, dict[str, str]] = {}
        for arm, corpus_name in (("guided", named), ("general", None)):
            if client.cumulative_usd >= max_usd:
                print(f"      local budget {max_usd} reached; stopping C3")
                return items
            reply = _ask(client, _c3_prompt(pairs, corpus_name), failures)
            answers[arm] = _parse_labels(reply, keys)
        for key in keys:
            items.append(
                PairedItem(
                    key=key,
                    guided_score=float(answers["guided"].get(key) == truth[key]),
                    general_score=float(answers["general"].get(key) == truth[key]),
                )
            )
        if index % 5 == 0:
            print(f"      C3 batch {index}/{len(batches)}, ${client.cumulative_usd:.3f}")
    return items


def _synthetic_columns(rng: random.Random, count: int) -> list[tuple[str, BenchmarkColumn, str]]:
    """Generate columns no published corpus contains, for the negative control.

    Generated rather than sampled from any real source: a control drawn from published data is
    not a control. Shapes are matched to the real corpora -- 20-45 distinct values, a mix of
    numeric, code-like and date-like types, with plausible flagged values.
    """
    headers = ("acct_ref", "unit_qty", "cycle_date", "site_code", "bill_amt", "lot_id")
    out: list[tuple[str, BenchmarkColumn, str]] = []
    for index in range(count):
        header = f"{rng.choice(headers)}_{rng.randrange(1000, 9999)}"
        size = rng.randrange(20, 46)
        kind = index % 3
        if kind == 0:
            values = [str(rng.randrange(10_000, 99_999)) for _ in range(size)]
            bad = str(rng.randrange(10_000, 99_999)) + ".00.00"
        elif kind == 1:
            values = [
                f"{rng.choice('QRSTVWXZ')}{rng.randrange(100, 999)}-{rng.randrange(10, 99)}"
                for _ in range(size)
            ]
            bad = "??" + str(rng.randrange(100, 999))
        else:
            values = [
                f"{rng.randrange(2015, 2024)}-{rng.randrange(1, 13):02d}-{rng.randrange(1, 29):02d}"
                for _ in range(size)
            ]
            bad = f"{rng.randrange(1, 13):02d}/{rng.randrange(1, 29):02d}/{rng.randrange(90, 99)}"
        unique = list(dict.fromkeys([*values, bad]))
        flagged = frozenset({bad})
        is_error = index % 2 == 0
        out.append(
            (
                f"S{index}",
                BenchmarkColumn(
                    index=index,
                    header=header,
                    distinct_values=tuple(unique),
                    ground_truth=flagged if is_error else frozenset(),
                    debatable=frozenset() if is_error else flagged,
                    declared_value_count=len(unique),
                ),
                _FAKE_DESCRIPTION,
            )
        )
    return out


def _evaluate(items: list[PairedItem], label: str) -> dict[str, Any]:
    """Turn paired items into a pre-registered decision, or explain why there is none.

    Also breaks the result down per corpus. The first run of this audit omitted that, which
    left a pre-registered secondary prediction -- that RT-bench would show more signal than
    ST-bench -- unevaluable. An unevaluable pre-registered prediction is a defect in the
    instrument, not a neutral omission, so the breakdown is now always emitted.

    The breakdown is **descriptive**. The pre-registered decision is the pooled contrast; a
    per-corpus split introduces two tests where one was registered, and picking the more
    favourable is threshold-shopping.
    """
    if not items:
        return {"available": False, "detail": f"{label}: no paired items were collected"}
    payload = _evaluate_group(items, label)
    # Only the real corpora use namespaced `corpus:index` keys. The synthetic control uses
    # bare `S<n>`, and splitting those on ":" produced 60 single-item "corpora" in the first
    # run of this change -- noise that buried the breakdown it was added for.
    if not all(":" in item.key for item in items):
        return payload
    groups: dict[str, list[PairedItem]] = {}
    for item in items:
        groups.setdefault(item.key.split(":", 1)[0], []).append(item)
    if len(groups) > 1:
        payload["by_corpus_descriptive_only"] = {
            corpus: _evaluate_group(group, f"{label}/{corpus}")
            for corpus, group in sorted(groups.items())
        }
    return payload


def _evaluate_group(items: list[PairedItem], label: str) -> dict[str, Any]:
    """Score one paired set."""
    deltas = [item.delta for item in items]
    mean_delta = sum(deltas) / len(deltas)
    guided = sum(item.guided_score for item in items) / len(items)
    general = sum(item.general_score for item in items) / len(items)
    payload: dict[str, Any] = {
        "available": True,
        "pairs": len(items),
        "guided_mean": round(guided, 4),
        "general_mean": round(general, 4),
        "mean_delta": round(mean_delta, 4),
        "nonzero_pairs": sum(1 for delta in deltas if delta != 0.0),
    }
    try:
        p_value = paired_signflip_p_value(deltas, resamples=_RESAMPLES, seed=SEED)
    except ContaminationAuditError as exc:
        # Identical arms is a finding, not a p-value. Reported as unavailable-with-reason
        # rather than silently becoming p=1.0.
        payload["available"] = False
        payload["detail"] = f"{label}: {exc}"
        payload["p_value"] = None
        return payload
    payload["p_value"] = round(p_value, 6)
    payload["detail"] = (
        f"{label}: guided {guided:.4f} vs general {general:.4f}, "
        f"delta {mean_delta:+.4f}, p={p_value:.4f}"
    )
    return payload


def _outcome(probe: str, payload: dict[str, Any]) -> ProbeOutcome:
    """Wrap an evaluation as a verdict-rule input."""
    return ProbeOutcome(
        probe=probe,  # type: ignore[arg-type]
        available=bool(payload.get("available")),
        p_value=payload.get("p_value"),
        effect=payload.get("mean_delta"),
        detail=str(payload.get("detail", "")),
    )


def main(argv: list[str] | None = None) -> int:
    """Run the contamination audit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-usd", type=float, default=15.0)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument(
        "--calibrate",
        action="store_true",
        help="Run 8 calls, report measured cost per call and a full-run projection, exit.",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--artifact",
        type=Path,
        default=_ARTIFACT,
        help="Where to write the artifact. Use a distinct path to keep a prior run.",
    )
    args = parser.parse_args(argv)

    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
    api_key = os.environ.get("AZURE_API_KEY")
    deployment = os.environ.get("DATAFORGE_AZURE_MODEL", "gpt-5.6-sol")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview")
    if not endpoint or not api_key:
        print("AZURE_OPENAI_ENDPOINT and AZURE_API_KEY are required; see .env.example")
        return 2

    available, reason = exchangeability_available(_CAPABILITY)
    print(f"C1 exchangeability available: {available}\n  {reason}\n")

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
        # A C2 reply can be 40 JSON strings; the 512 default would truncate and be scored
        # as a recall failure rather than a formatting one.
        max_tokens=2048,
    )
    failures = [0]
    rng = random.Random(SEED)

    # Build the item sets. Deterministic, and identical across arms by construction.
    c2_columns: list[tuple[str, BenchmarkColumn, str | None]] = []
    c3_columns: list[tuple[str, BenchmarkColumn, str | None]] = []
    corpus_digests: dict[str, str] = {}
    for corpus in ("rt_bench", "st_bench"):
        benchmark = load_column_benchmark(corpus)
        corpus_digests[corpus] = benchmark.sha256
        described = _CORPUS_DESCRIPTION[corpus]
        eligible = [c for c in benchmark.columns if len(c.distinct_values) >= _C2_MIN_DISTINCT]
        for column in rng.sample(eligible, min(_C2_COLUMNS_PER_CORPUS, len(eligible))):
            c2_columns.append((f"{corpus}:{column.index}", column, described))
        for column in benchmark.columns:
            # Census of label-homogeneous labelled columns. Mixed columns are excluded
            # because the target is a single column-level label.
            if bool(column.ground_truth) != bool(column.debatable):
                c3_columns.append((f"{corpus}:{column.index}", column, described))

    print(
        f"items: C2={len(c2_columns)} columns, C3={len(c3_columns)} columns "
        f"(census), C4={_C4_COLUMNS} synthetic"
    )

    if args.calibrate:
        probe_c2 = c2_columns[:2]
        probe_c3 = c3_columns[:_C3_BATCH]
        _run_c2(client, probe_c2, failures, max_usd=args.max_usd)
        _run_c3(client, probe_c3, failures, max_usd=args.max_usd)
        calls = max(1, client.meter.calls)
        per_call = client.cumulative_usd / calls
        projected_calls = 2 * len(c2_columns) + 2 * ((len(c3_columns) + _C3_BATCH - 1) // _C3_BATCH)
        projected_calls += 2 * _C4_COLUMNS + 2 * ((_C4_COLUMNS + _C3_BATCH - 1) // _C3_BATCH)
        print(
            f"\ncalibration: {calls} calls, ${client.cumulative_usd:.4f} "
            f"(${per_call:.5f}/call), {failures[0]} failed"
        )
        print(
            f"projection: {projected_calls} calls -> "
            f"${per_call * projected_calls:.2f} for the full run"
        )
        return 0

    run_id = args.run_id or f"contamination-{uuid.uuid4().hex[:8]}"
    # C4 first. If the control fires, the audit is void and the rest is wasted money.
    print("\nC4 negative control (synthetic corpus, must NOT flag)")
    synthetic = _synthetic_columns(rng, _C4_COLUMNS)
    control_named: list[tuple[str, BenchmarkColumn, str | None]] = [
        (key, column, described) for key, column, described in synthetic
    ]
    c4_c2 = _run_c2(client, control_named, failures, max_usd=args.max_usd)
    c4_c3 = _run_c3(client, control_named, failures, max_usd=args.max_usd)
    c4_eval = _evaluate(c4_c2 + c4_c3, "C4 control")
    print(f"  {c4_eval.get('detail')}")

    print("\nC2 guided vs general value completion")
    c2_eval = _evaluate(_run_c2(client, c2_columns, failures, max_usd=args.max_usd), "C2")
    print(f"  {c2_eval.get('detail')}")

    print("\nC3 guided vs general column-label recovery")
    c3_eval = _evaluate(_run_c3(client, c3_columns, failures, max_usd=args.max_usd), "C3")
    print(f"  {c3_eval.get('detail')}")

    outcomes = {
        "C1": ProbeOutcome(
            probe="C1", available=available, p_value=None, effect=None, detail=reason
        ),
        "C2": _outcome("C2", c2_eval),
        "C3": _outcome("C3", c3_eval),
        "C4": _outcome("C4", c4_eval),
    }

    verdict_payload: dict[str, Any]
    try:
        verdict = decide_verdict(
            outcomes,
            model=deployment,
            seed=SEED,
            reference_sha256=corpus_digests.get("rt_bench", ""),
        )
        verdict_payload = {
            "status": verdict.verdict,
            "flagged_probes": list(verdict.flagged_probes),
            "unavailable_probes": list(verdict.unavailable_probes),
            "contamination_suspected": verdict.contamination_suspected,
            "cancels_wild_column_measurement": verdict.cancels_wild_column_measurement,
        }
    except ContaminationAuditError as exc:
        verdict_payload = {
            "status": "VOID",
            "reason": str(exc),
            "contamination_suspected": True,
            "cancels_wild_column_measurement": True,
        }

    commit, dirty = _git_commit()
    base_rates = {}
    for corpus in ("rt_bench", "st_bench"):
        benchmark = load_column_benchmark(corpus)
        errors = sum(1 for c in benchmark.columns if c.ground_truth and not c.debatable)
        debatable = sum(1 for c in benchmark.columns if c.debatable and not c.ground_truth)
        base_rates[corpus] = round(majority_base_rate(errors, debatable), 4)

    payload = {
        "schema_version": _SCHEMA,
        "generated_at": datetime.now(UTC).isoformat(),
        "provider": "azure",
        "model": deployment,
        "deployment": deployment,
        "api_version": api_version,
        "seed": SEED,
        "reasoning_effort": _REASONING_EFFORT,
        "exchangeability_available": available,
        "exchangeability_reason": reason,
        "methods_implemented": ["C2_guided_vs_general", "C3_column_label", "C4_control"],
        "c2_columns": len(c2_columns),
        "c2_min_distinct_values": _C2_MIN_DISTINCT,
        "c3_items": len(c3_columns),
        "c4_columns": _C4_COLUMNS,
        "resamples": _RESAMPLES,
        "alpha": ALPHA,
        "min_delta": C2_MIN_DELTA,
        "probes": {"C2": c2_eval, "C3": c3_eval, "C4": c4_eval},
        "column_label_base_rates_descriptive_only": base_rates,
        **verdict_payload,
        "failed_calls": failures[0],
        "calls": client.meter.calls,
        "estimated_usd": round(client.cumulative_usd, 6),
        "thresholds": {
            "alpha": ALPHA,
            "min_mean_delta": C2_MIN_DELTA,
            "kill_criterion": "2 or more of C1-C3 flagging cancels the wild-column measurement",
            "pre_registration": "eval/preregistration/contamination_audit.md",
        },
        "reference": {
            corpus: {
                "sha256": digest,
                "source_revision": COLUMN_BENCHMARK_REGISTRY[corpus].source_revision,
            }
            for corpus, digest in corpus_digests.items()
        },
        "limitations": [
            "L1: C1 (Oren et al. exchangeability, the only provable method) is UNAVAILABLE on "
            "this deployment: logprobs is rejected, and chat-completions logprobs cover "
            "generated tokens only. Excluded from the flag count, never scored as passing.",
            "L2: power is modest. A weak memorisation signal will not be detected, so CLEAN "
            "means NOT DETECTED, never absent.",
            "L3: BLEURT and the GPT-4 classifier arm of Golchin & Surdeanu are not "
            "implemented. C2 and C3 also share a paired form, so they are not "
            "methodologically independent.",
            "L4: the verdict is bound to this deployment and api-version. A model refresh "
            "invalidates it.",
            "L5: C2 covers only columns with >= 20 distinct values. A corpus memorised solely "
            "in its short columns would be missed.",
            "This audit authorises nothing about detection quality. It gates whether a "
            "measurement is worth making and cannot make one credible.",
            "It does not bear on whether Auto-Test's own SDC training corpus overlapped its "
            "benchmarks, which remains contamination_unverified.",
        ],
        "provenance": {
            "git_commit": commit,
            "git_worktree_dirty": dirty,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
        "run_id": run_id,
    }

    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    try:
        if client.meter.calls:
            append_receipt(
                _LEDGER,
                client.meter.receipt(
                    run_id=run_id,
                    method="contamination_audit",
                    dataset="rt_bench+st_bench",
                    notes=(
                        f"C2={len(c2_columns)}cols",
                        f"C3={len(c3_columns)}cols",
                        f"C4={_C4_COLUMNS}synthetic",
                        f"exchangeability_available={available}",
                    ),
                ),
            )
    except Exception as exc:  # noqa: BLE001 - a receipt failure must not hide the result
        print(f"WARNING: receipt not written: {type(exc).__name__}: {exc}")

    print(f"\nVERDICT: {payload['status']}")
    print(f"  flagged: {payload.get('flagged_probes', [])}")
    print(f"  cancels wild-column measurement: {payload['cancels_wild_column_measurement']}")
    print(f"  calls={client.meter.calls} failed={failures[0]} spend=${client.cumulative_usd:.4f}")
    print(f"artifact: {args.artifact.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
