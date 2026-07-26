"""CLI: build the Selective-Repair Calibration Benchmark artifact from runs.

Reads one or more LLM-corrector benchmark JSON outputs (each carrying
``calibration_samples_by_class``), pools samples *within* each named condition,
and writes a committed artifact JSON plus a human-readable methods note.

Example:
    python scripts/bench/certified_coverage_report.py \
        --run minimal=eval/results/corrector_gpt5mini_hospital_minimal.json \
        --run medium=eval/results/corrector_gpt5mini_hospital_medium.json \
        --out-json eval/results/selective_repair_calibration.json \
        --out-md docs/selective-repair-calibration.md

The expensive step (the live runs) is done separately; this is pure,
reproducible post-processing.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dataforge.bench.calibration_artifact import (
    build_calibration_artifact,
    render_methods_note,
)


def _load_record(path: Path) -> dict[str, object]:
    """Load and pool all seed records from a corrector bench output JSON.

    A ``--seed-list 0,1,2`` run writes one record per seed. Pooling their
    ``calibration_samples_by_class`` across seeds increases certification power
    instead of discarding seeds 1..n. Aggregate scalars are echoed as
    sample-count-weighted summaries (informational; the load-bearing
    certification is recomputed from the pooled samples).

    CAVEAT (honest): pooling assumes the per-seed samples are approximately
    exchangeable. Different seeds subsample different issues, but overlap is
    possible, so pooled samples are not strictly i.i.d. This is acceptable here
    only because the result is a null (certified coverage 0.0): added dependence
    can inflate certified coverage, never deflate it, so a null under pooling is
    conservative. Do NOT rely on pooled certification to CLAIM non-zero coverage.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") if isinstance(payload, dict) else None
    if not records:
        raise ValueError(f"No benchmark records found in {path}")

    usable = [r for r in records if isinstance(r, dict) and r.get("calibration_samples_by_class")]
    if not usable:
        raise ValueError(
            f"{path} has no calibration_samples_by_class; re-run the corrector "
            "benchmark with the per-class sample persistence enabled."
        )

    pooled_by_class: dict[str, list[list[object]]] = {}
    for record in usable:
        raw = record.get("calibration_samples_by_class") or {}
        if not isinstance(raw, dict):
            continue
        for error_class, pairs in raw.items():
            pooled_by_class.setdefault(str(error_class), []).extend(pairs)

    def _mean(field: str) -> float | None:
        values = [float(v) for r in usable if isinstance((v := r.get(field)), (int, float))]
        return sum(values) / len(values) if values else None

    def _sum_int(field: str) -> int:
        return sum(int(v) for r in usable if isinstance((v := r.get(field)), (int, float)))

    first = usable[0]
    return {
        "provider": first.get("provider"),
        "model": first.get("model"),
        "dataset": first.get("dataset"),
        "seeds_pooled": [r.get("seed") for r in usable],
        "precision": _mean("precision"),
        "recall": _mean("recall"),
        "f1": _mean("f1"),
        "ece": _mean("ece"),
        "precision_at_auto_apply": _mean("precision_at_auto_apply"),
        "auto_apply_count": _sum_int("auto_apply_count"),
        "calibration_samples_by_class": pooled_by_class,
    }


def _parse_run(raw: str) -> tuple[str, Path]:
    """Parse a ``label=path`` CLI value."""
    label, _, path = raw.partition("=")
    if not label or not path:
        raise argparse.ArgumentTypeError(f"--run must be label=path; got {raw!r}")
    return label, Path(path)


def main(argv: list[str] | None = None) -> int:
    """Build and write the calibration artifact from the supplied runs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        action="append",
        required=True,
        type=_parse_run,
        metavar="LABEL=PATH",
        help="A named condition and its corrector benchmark JSON (repeatable).",
    )
    parser.add_argument("--out-json", type=Path, required=True)
    parser.add_argument("--out-md", type=Path, required=True)
    parser.add_argument("--primary-alpha", type=float, default=0.05)
    parser.add_argument("--delta", type=float, default=0.05)
    parser.add_argument("--min-support", type=int, default=30)
    parser.add_argument("--splits", type=int, default=200)
    args = parser.parse_args(argv)

    records_by_condition = {label: _load_record(path) for label, path in args.run}
    artifact = build_calibration_artifact(
        records_by_condition,
        delta=args.delta,
        min_support=args.min_support,
        splits=args.splits,
        primary_alpha=args.primary_alpha,
    )
    args.out_json.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    args.out_md.write_text(render_methods_note(artifact), encoding="utf-8")
    print(f"Wrote {args.out_json} and {args.out_md}")
    print(f"Conclusion: {artifact['conclusion']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
