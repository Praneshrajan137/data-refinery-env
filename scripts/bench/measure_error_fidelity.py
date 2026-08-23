"""Assess an error generator's fidelity against a real-error reference.

Emits ``eval/results/error_fidelity_<generator>_<reference>.json`` under
``specs/SPEC_error_fidelity.md``. A ``REFUSED`` verdict is a publishable finding, not a
failure of this script, so the exit code is 0 either way unless ``--fail-on-refusal`` is
given.

Usage::

    python scripts/bench/measure_error_fidelity.py --reference rt_bench
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.column_corpus import load_column_benchmark  # noqa: E402
from dataforge.datasets.inject import (  # noqa: E402
    FIDELITY_MAX_COVERAGE_MISMATCH,
    FIDELITY_MAX_PRECISION_GAP,
    FIDELITY_MIN_FIRING_DETECTORS,
    FIDELITY_MIN_RANK_CORRELATION,
    assess_fidelity,
    generate_character_noise_corpus,
)
from dataforge.datasets.registry import COLUMN_BENCHMARK_REGISTRY  # noqa: E402

SCHEMA_VERSION = "dataforge_error_fidelity_v1"

GENERATORS = {"character_noise_v1": generate_character_noise_corpus}


def _git_commit() -> tuple[str | None, bool]:
    """Return the current commit and whether the worktree is dirty."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=PROJECT_ROOT,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
            cwd=PROJECT_ROOT,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None, False
    return commit, bool(status)


def main(argv: list[str] | None = None) -> int:
    """Run the fidelity assessment CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reference", default="rt_bench", choices=sorted(COLUMN_BENCHMARK_REGISTRY)
    )
    parser.add_argument("--generator", default="character_noise_v1", choices=sorted(GENERATORS))
    parser.add_argument("--seed", type=int, default=20260823)
    parser.add_argument("--rate", type=float, default=0.02)
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "eval" / "results")
    parser.add_argument("--fail-on-refusal", action="store_true")
    args = parser.parse_args(argv)

    reference = load_column_benchmark(args.reference)
    generated = GENERATORS[args.generator](reference, seed=args.seed, rate=args.rate)
    verdict = assess_fidelity(generated, reference)

    commit, dirty = _git_commit()
    payload: dict[str, Any] = asdict(verdict) | {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "injected_values": generated.injected_values,
        "generated_columns": generated.benchmark.n_columns,
        "injection_rate": args.rate,
        "thresholds": {
            "F1_min_rank_correlation": FIDELITY_MIN_RANK_CORRELATION,
            "F2_max_precision_gap": FIDELITY_MAX_PRECISION_GAP,
            "F3_max_coverage_mismatch": FIDELITY_MAX_COVERAGE_MISMATCH,
            "F4_min_firing_detectors": FIDELITY_MIN_FIRING_DETECTORS,
            "pre_registration": "eval/preregistration/error_fidelity.md",
        },
        "provenance": {
            "git_commit": commit,
            "git_worktree_dirty": dirty,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / f"error_fidelity_{args.generator}_{args.reference}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"generator={args.generator} seed={args.seed} reference={args.reference}")
    print(
        f"injected {generated.injected_values} values across {generated.benchmark.n_columns} columns"
    )
    print(f"\nSTATUS: {verdict.status}")
    print(
        f"  F1 rank correlation  : {verdict.rank_correlation}  (need >= {FIDELITY_MIN_RANK_CORRELATION})"
    )
    print(
        f"  F2 max precision gap : {verdict.max_precision_gap}  (need <= {FIDELITY_MAX_PRECISION_GAP})"
    )
    print(
        f"  F3 coverage mismatch : {verdict.coverage_mismatch}  (need <= {FIDELITY_MAX_COVERAGE_MISMATCH})"
    )
    print(
        f"  F4 firing detectors  : {verdict.firing_detectors}  (need >= {FIDELITY_MIN_FIRING_DETECTORS})"
    )
    for condition in verdict.failed_conditions:
        print(f"  FAILED: {condition}")
    print(f"\n{'detector':<34} {'generated':>10} {'reference':>10}")
    for entry in verdict.per_detector:
        if entry["generated_precision"] is None and entry["reference_precision"] is None:
            continue
        print(
            f"{entry['detector']:<34} {str(entry['generated_precision']):>10} "
            f"{str(entry['reference_precision']):>10}"
        )
    print(f"\nwrote {path.relative_to(PROJECT_ROOT)}")

    if args.fail_on_refusal and not verdict.admissible:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
