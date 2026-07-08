"""CLI: build the Selective-Repair Calibration Benchmark artifact from runs.

Reads one or more LLM-corrector benchmark JSON outputs (each carrying
``calibration_samples_by_class``), pools samples *within* each named condition,
and writes a committed artifact JSON plus a human-readable methods note.

Example:
    python scripts/bench/certified_coverage_report.py \
        --run minimal=eval/results/corrector_gpt5mini_hospital_min.json \
        --run medium=eval/results/corrector_gpt5mini_hospital_med.json \
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
    """Load the first (single-seed) corrector record from a bench output JSON."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") if isinstance(payload, dict) else None
    if not records:
        raise ValueError(f"No benchmark records found in {path}")
    record = records[0]
    if not isinstance(record, dict):
        raise ValueError(f"Unexpected record shape in {path}")
    if not record.get("calibration_samples_by_class"):
        raise ValueError(
            f"{path} has no calibration_samples_by_class; re-run the corrector "
            "benchmark with the per-class sample persistence enabled."
        )
    return record


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
