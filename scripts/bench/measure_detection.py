"""Measure DataForge detection on the real-error column benchmarks.

Emits ``eval/results/detection_<benchmark>.json`` under
``specs/SPEC_abstention_scoring.md``, with the full provenance block so the artifact
records what was measured rather than what was expected.

Usage::

    python scripts/bench/measure_detection.py --benchmarks rt_bench,st_bench
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

from dataforge.bench.detection import (  # noqa: E402
    DetectionRunResult,
    measure_column_benchmark,
)
from dataforge.datasets.column_corpus import load_column_benchmark  # noqa: E402
from dataforge.datasets.registry import COLUMN_BENCHMARK_REGISTRY  # noqa: E402
from dataforge.detectors import default_detectors  # noqa: E402
from dataforge.detectors.base import Detector  # noqa: E402
from dataforge.detectors.semantic_domain import (  # noqa: E402
    SemanticDomainDetector,
    load_pattern_sdcs,
)

SCHEMA_VERSION = "dataforge_detection_run_v1"

# Carried in every artifact. These are properties of the corpus, not of the run, and
# they are the difference between a number that means something and one that does not.
# Enumerated as L1-L4 in specs/SPEC_abstention_scoring.md.
LIMITATIONS = (
    "L1: dist_val holds DISTINCT values, so a value occurring 900 times counts once. "
    "These are not cell-level metrics and are not comparable to BENCHMARK_REPORT.md.",
    "L2: ground_truth contains only unambiguous errors, so recall here is an UPPER "
    "bound on recall over all real errors.",
    "L3: no clean values ship with these corpora. Detection only. Any correction or "
    "repair number sourced from here would be fabricated.",
    "L4: each row is one column, so detectors needing row or cross-column context are "
    "not_applicable, which is NOT recall 0.",
    "Label density is ~0.0005: precision rests on six figures of real values, recall on "
    "double-digit support. Recall must be reported with its bound, never bare.",
    "This is not a state-of-the-art claim. It authorises exactly one comparison: against "
    "Auto-Test's published curves on these same bytes under this same rule.",
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


def _serialise(result: DetectionRunResult) -> dict[str, Any]:
    """Render one run result as a JSON-ready payload."""
    per_detector = []
    for measurement in result.per_detector:
        payload = asdict(measurement)
        payload["score"] = measurement.score.model_dump() if measurement.score is not None else None
        payload["frontier"] = [dict(point) for point in measurement.frontier]
        per_detector.append(payload)
    return {
        "benchmark": result.benchmark,
        "axis": "detection",
        "scoring_spec": "specs/SPEC_abstention_scoring.md",
        "frequencies_available": result.frequencies_available,
        "columns_scored": result.columns_scored,
        "columns_quarantined": result.columns_quarantined,
        "ground_truth_values": result.ground_truth_values,
        "debatable_values": result.debatable_values,
        "distinct_values": result.distinct_values,
        "label_density": result.label_density,
        # Named `evaluable_ensemble`, never `ensemble`. The previous key unioned every
        # detector including ones this corpus cannot score, and the resulting number was
        # published. A reader who greps for `ensemble` should not find a bare total.
        "evaluable_ensemble": result.evaluable_ensemble.model_dump(),
        "evaluable_detectors": list(result.evaluable_detectors),
        "not_evaluable_detectors": list(result.not_evaluable_detectors),
        "excluded_flags_upper_bound": result.excluded_false_positive_flags,
        "per_detector": per_detector,
        "dataset_evidence": {
            "source_url": COLUMN_BENCHMARK_REGISTRY[result.benchmark].source_url,
            "source_revision": result.source_revision,
            "sha256": result.sha256,
            "license_spdx": COLUMN_BENCHMARK_REGISTRY[result.benchmark].license_spdx,
            "citation": COLUMN_BENCHMARK_REGISTRY[result.benchmark].citation,
        },
    }


def measure(
    benchmarks: tuple[str, ...],
    *,
    output_dir: Path,
    with_semantic_domain: bool = False,
) -> dict[str, Any]:
    """Measure each benchmark and write one artifact per benchmark."""
    commit, dirty = _git_commit()
    written: list[str] = []
    summaries: list[dict[str, Any]] = []

    detectors: list[Detector] = list(default_detectors())
    sdc_provenance: dict[str, Any] | None = None
    if with_semantic_domain:
        # Opt-in because it needs a fetched, hash-verified artifact. The default ensemble
        # stays offline; see dataforge/detectors/semantic_domain.py.
        loaded = load_pattern_sdcs()
        detectors.append(SemanticDomainDetector(loaded.sdcs))
        sdc_provenance = {
            "pattern_sdcs_loaded": len(loaded.sdcs),
            "total_in_artifact": loaded.total_in_artifact,
            "declined_by_family": loaded.declined_by_family,
            "sha256": loaded.sha256,
        }

    for name in benchmarks:
        loaded_benchmark = load_column_benchmark(name)
        result = measure_column_benchmark(loaded_benchmark, detectors=detectors)
        payload = _serialise(result)
        payload |= {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now(UTC).isoformat(),
            "limitations": list(LIMITATIONS),
            "semantic_domain_sdcs": sdc_provenance,
            "provenance": {
                "git_commit": commit,
                "git_worktree_dirty": dirty,
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "distinct_values_only": True,
                "ground_truth_scope": "unambiguous_only",
            },
        }
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"detection_{name}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        written.append(str(path.relative_to(PROJECT_ROOT)))
        summaries.append(payload)

    return {"written": written, "runs": summaries}


def _print_summary(payload: dict[str, Any]) -> None:
    """Print a compact human summary of one run."""
    ensemble = payload["evaluable_ensemble"]
    print(f"\n=== {payload['benchmark']} ===")
    print(
        f"columns={payload['columns_scored']} quarantined={payload['columns_quarantined']} "
        f"G={payload['ground_truth_values']} D={payload['debatable_values']} "
        f"distinct={payload['distinct_values']} density={payload['label_density']}"
    )
    print(f"frequencies_available={payload['frequencies_available']}")
    print(
        f"EVALUABLE ENSEMBLE  tp={ensemble['tp']} fp={ensemble['fp']} fn={ensemble['fn']} "
        f"precision={ensemble['precision']} recall={ensemble['recall']} f1={ensemble['f1']} "
        f"coverage={ensemble['coverage']}"
    )
    print(
        f"  (excludes {len(payload['not_evaluable_detectors'])} not-evaluable detector(s); "
        f"they emitted {payload['excluded_flags_upper_bound']} flags this corpus cannot score)"
    )
    print(
        f"{'detector':<34} {'applicability':<20} {'tp':>4} {'fp':>6} {'fn':>4} {'prec':>7} {'rec':>7}"
    )
    for entry in payload["per_detector"]:
        score = entry["score"]
        if not entry["evaluable"]:
            note = f"NOT EVALUABLE ({entry['values_flagged']} flags, unscored)"
            print(f"{entry['detector']:<34} {entry['applicability']:<20} {note}")
            continue
        if score is None:
            print(f"{entry['detector']:<34} {entry['applicability']:<20} never fired")
            continue
        print(
            f"{entry['detector']:<34} {entry['applicability']:<20} "
            f"{score['tp']:>4} {score['fp']:>6} {score['fn']:>4} "
            f"{str(score['precision']):>7} {str(score['recall']):>7}"
        )


def _parse_benchmarks(value: str) -> tuple[str, ...]:
    """Parse and validate a comma-separated benchmark list."""
    names = tuple(item.strip() for item in value.split(",") if item.strip())
    if not names:
        raise argparse.ArgumentTypeError("at least one benchmark is required")
    unknown = sorted(set(names) - set(COLUMN_BENCHMARK_REGISTRY))
    if unknown:
        raise argparse.ArgumentTypeError("unknown benchmark(s): " + ", ".join(unknown))
    return names


def main(argv: list[str] | None = None) -> int:
    """Run the detection measurement CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmarks",
        type=_parse_benchmarks,
        default=tuple(sorted(COLUMN_BENCHMARK_REGISTRY)),
    )
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "eval" / "results")
    parser.add_argument(
        "--with-semantic-domain",
        action="store_true",
        help=(
            "Include SemanticDomainDetector, which fetches and hash-verifies the pinned "
            "Auto-Test SDC artifact. Detection-only: it has no repairer and is not in "
            "CONSTRAINT_CHECKABLE_DETECTORS, so it has no write path."
        ),
    )
    args = parser.parse_args(argv)

    report = measure(
        args.benchmarks,
        output_dir=args.output_dir,
        with_semantic_domain=args.with_semantic_domain,
    )
    for payload in report["runs"]:
        _print_summary(payload)
    print("\nwrote:", ", ".join(report["written"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
