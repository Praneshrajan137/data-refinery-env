"""Measure cell-level detection on the frequency-preserving corpora.

Emits ``eval/results/cell_detection_<dataset>.json``. The companion to
``measure_detection.py``, which scores distinct values on RT/ST-bench.

Both exist because the two units are not convertible -- measured, with gaps up to total, in
``docs/trust/scoring-unit-reconciliation.md``. Cell level is the unit a review queue is
counted in; distinct-value level is the unit the only real-error corpus ships.

Usage::

    python scripts/bench/measure_cell_detection.py --datasets rayyan,hospital,flights
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

from dataforge.bench.cell_detection import (  # noqa: E402
    CellDetectionRunResult,
    measure_cell_detection,
)
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.datasets.registry import DATASET_REGISTRY  # noqa: E402

SCHEMA_VERSION = "dataforge_cell_detection_v1"

LIMITATIONS = (
    "Scoring unit is the CELL. These numbers are NOT comparable to the distinct-value "
    "figures in detection_rt_bench.json / detection_st_bench.json: measured on rayyan, the "
    "same detector shows gaps up to total between the two units, in both directions "
    "(docs/trust/scoring-unit-reconciliation.md).",
    "RAHA pairs ship NO ground_truth_debatable class, so this scoring is TWO-WAY. Ambiguous "
    "cells were resolved by the corpus author and the resolution is unrecorded, so these "
    "numbers carry the identification problem the three-way rule removes. Cell level buys "
    "the right unit at the cost of the neutral zone; neither harness dominates.",
    "Detection only. A detector reaching precision 1.0 here is finding error cells, not "
    "producing correct replacement values, and most of these have no repairer at all.",
    "Corpus provenance matters and is recorded per run: hospital is injected (its errors are "
    "one substituted character), tax is synthetic, flights has contested labels.",
    "Precision varies by up to 15x for the SAME detector across corpora. No number here "
    "generalises to an unseen table; that is the argument for per-table certification, not "
    "against it.",
)


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


def _serialise(result: CellDetectionRunResult) -> dict[str, Any]:
    """Render one run as a JSON-ready payload."""
    per_detector = []
    for measurement in result.per_detector:
        payload = asdict(measurement)
        payload["score"] = asdict(measurement.score) if measurement.score is not None else None
        if measurement.score is not None:
            payload["score"]["flag_rate"] = measurement.score.flag_rate
        payload["fired"] = measurement.fired
        per_detector.append(payload)
    return {
        "dataset": result.dataset,
        "scoring_unit": result.scoring_unit,
        "debatable_class_available": result.debatable_class_available,
        "error_provenance": result.error_provenance,
        "tier": result.tier,
        "rows": result.rows,
        "columns": result.columns,
        "ground_truth_cells": result.ground_truth_cells,
        "total_cells": result.total_cells,
        "best_precision_detector": result.best_precision_detector,
        "per_detector": per_detector,
        "dataset_evidence": {
            "source_urls": list(DATASET_REGISTRY[result.dataset].source_urls),
            "source_revision": DATASET_REGISTRY[result.dataset].source_revision,
            "dirty_sha256": DATASET_REGISTRY[result.dataset].dirty_sha256,
            "clean_sha256": DATASET_REGISTRY[result.dataset].clean_sha256,
        },
    }


def _parse_datasets(value: str) -> tuple[str, ...]:
    """Parse and validate a comma-separated dataset list."""
    names = tuple(item.strip() for item in value.split(",") if item.strip())
    if not names:
        raise argparse.ArgumentTypeError("at least one dataset is required")
    unknown = sorted(set(names) - set(DATASET_REGISTRY))
    if unknown:
        raise argparse.ArgumentTypeError("unknown dataset(s): " + ", ".join(unknown))
    return names


def main(argv: list[str] | None = None) -> int:
    """Run the cell-level detection CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets", type=_parse_datasets, default=("rayyan", "hospital", "flights")
    )
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "eval" / "results")
    args = parser.parse_args(argv)

    commit, dirty = _git_commit()
    for name in args.datasets:
        dataset = load_real_world_dataset(name)
        result = measure_cell_detection(dataset)
        payload = _serialise(result) | {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now(UTC).isoformat(),
            "limitations": list(LIMITATIONS),
            "provenance": {
                "git_commit": commit,
                "git_worktree_dirty": dirty,
                "python": sys.version.split()[0],
                "platform": platform.platform(),
            },
        }
        args.output_dir.mkdir(parents=True, exist_ok=True)
        path = args.output_dir / f"cell_detection_{name}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        print(f"\n=== {name} ({result.error_provenance}/{result.tier}) ===")
        print(
            f"rows={result.rows} cols={result.columns} "
            f"gt_cells={result.ground_truth_cells} total_cells={result.total_cells}"
        )
        print(
            f"{'detector':<34} {'applicability':<20} {'prec':>7} {'rec':>7} "
            f"{'tp':>6} {'fp':>6} {'flag_rate':>10}"
        )
        for measurement in result.per_detector:
            if measurement.score is None:
                continue
            score = measurement.score
            print(
                f"{measurement.detector:<34} {measurement.applicability:<20} "
                f"{str(score.precision):>7} {str(score.recall):>7} "
                f"{score.tp:>6} {score.fp:>6} {score.flag_rate:>10}"
            )
        print(f"best precision: {result.best_precision_detector}")
        print(f"wrote {path.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
