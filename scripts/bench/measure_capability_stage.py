"""H2: at which stage of the pipeline is DataForge's headline correction F1 measured?

Executes `eval/preregistration/capability_measurement_stage.md`. Read that first.

## The question

`dataforge/bench/methods.py::run_heuristic_episode` produces the widely quoted **0.7926** by
calling `_repairs_from_proposed_fixes` -- `run_all_detectors`, then `propose_fixes`, then
`score_repairs`. It never builds a `RepairPipelineRequest`, so **no verifier, no safety filter and
no auto-apply gate** stands between the proposal and the score. This harness measures the same
table with the same ground truth and the same scorer at the stage a user actually reaches.

## Why the arms are built the way they are

Three arms, one scorer:

- **proposal_stage** -- reproduces the published path. Its only job is to hit 0.7926. Under **K2**,
  if it misses, nothing else here may be reported: an instrument that cannot reproduce the number
  it criticises has no standing.
- **pipeline_legacy** -- `run_repair_pipeline` with `mined_constraints_grant_write_authority=True`.
- **pipeline_c4** -- the shipped default.

Every arm is scored by `dataforge.bench.core.score_repairs`, imported rather than reimplemented,
so no arm can differ from another by its scoring. That is the whole design: if the numbers differ,
the stage is the only thing that can have caused it.

Read-only apart from one artifact under `eval/results/`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from dataforge.bench.core import BenchmarkRepair, score_repairs  # noqa: E402
from dataforge.bench.methods import _repairs_from_proposed_fixes  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline  # noqa: E402
from dataforge.schema_inference import (  # noqa: E402
    build_constraint_review_artifact,
    infer_schema,
    update_constraint_review_artifact,
)

K1_NO_GAP_F1 = 0.70
K2_TOLERANCE = 0.0001
P5_MIN_TP_RATIO = 100.0
ARTIFACT = REPO / "eval" / "results" / "agent_comparison.json"
OUTPUT = REPO / "eval" / "results" / "capability_measurement_stage.json"


def _published_f1() -> float:
    """Read the published anchor from the artifact, never from a constant in this file.

    AMENDMENT 1 exists because K2 originally compared against the literal 0.7926, which had
    rotted: the code moved to 0.8352 across two commits and nothing re-ran the benchmark, so a
    documented constant is exactly the wrong referent. `scripts/ci/anchor_truth.py` gates this
    artifact against the code, so reading it here inherits that guarantee instead of duplicating
    a number that can go stale independently.
    """
    payload = json.loads(ARTIFACT.read_text(encoding="utf-8"))
    for record in payload.get("records", []):
        if record.get("method") == "heuristic" and record.get("dataset") == "hospital":
            return round(float(record["f1"]), 4)
    raise SystemExit("FAIL: no committed heuristic/hospital record to take the anchor from.")


def _score(repairs: list[BenchmarkRepair], ground_truth: Any) -> dict[str, Any]:
    """Score one arm with the published scorer, imported not reimplemented."""
    metrics = score_repairs(ground_truth, repairs)
    return {
        "writes": len(repairs),
        "tp": metrics.tp,
        "fp": metrics.fp,
        "fn": metrics.fn,
        "precision": round(metrics.precision, 4),
        "recall": round(metrics.recall, 4),
        "f1": round(metrics.f1, 4),
    }


def _pipeline_repairs(source: Path, artifact: Any, *, trust_mined: bool) -> list[BenchmarkRepair]:
    """Return what the shipped pipeline would write, as scoreable repairs.

    `dry_run` writes nothing; `result.fixes` is the would-apply set, which is exactly the
    quantity the proposal-stage arm is missing a gate for.
    """
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="dry_run",
            constraints=artifact,
            mined_constraints_grant_write_authority=trust_mined,
        )
    )
    return [
        BenchmarkRepair(
            row=fix.row,
            column=fix.column,
            new_value=str(fix.new_value),
            reason=getattr(fix, "reason", "pipeline_auto_apply"),
        )
        for fix in result.fixes
    ]


def main() -> int:
    """Measure the same table at proposal stage and at pipeline stage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    published_f1 = _published_f1()
    dataset = load_real_world_dataset("hospital")
    ground_truth = dataset.ground_truth
    if not ground_truth:
        print("FAIL K3: hospital yielded 0 ground-truth cells; dataset misloaded.", file=sys.stderr)
        return 2

    # --- proposal stage: the published path. K2 hangs on this arm. ---
    proposals, _detected = _repairs_from_proposed_fixes(dataset)
    proposal = _score(proposals, ground_truth)

    k2_delta = abs(proposal["f1"] - published_f1)
    k2_reproduced = k2_delta <= K2_TOLERANCE
    print(
        f"  proposal stage : F1 {proposal['f1']} (published {published_f1}, delta {k2_delta:.6f})"
    )
    if not k2_reproduced:
        print(
            f"FAIL K2: this harness scores the published path at {proposal['f1']}, not "
            f"{published_f1}. The instrument does not reproduce the number it examines, so "
            "NOTHING may be reported from this run. Fix the harness first.",
            file=sys.stderr,
        )
        return 3

    # --- pipeline stage: what a user reaches, same premise, same scorer. ---
    dirty = dataset.dirty_df
    with tempfile.TemporaryDirectory(prefix="dataforge-stage-") as raw_tmp:
        source = Path(raw_tmp) / "hospital.csv"
        dirty.to_csv(source, index=False)
        artifact = build_constraint_review_artifact(
            infer_schema(dirty),
            source_path=source,
            source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        )
        fd_ids = [
            reviewed.candidate_id
            for reviewed in artifact.candidates
            if reviewed.candidate.kind == "functional_dependency"
        ]
        artifact = update_constraint_review_artifact(artifact, accept_ids=fd_ids)

        legacy = _score(_pipeline_repairs(source, artifact, trust_mined=True), ground_truth)
        c4 = _score(_pipeline_repairs(source, artifact, trust_mined=False), ground_truth)

    print(f"  pipeline legacy: F1 {legacy['f1']} ({legacy['writes']} writes)")
    print(f"  pipeline c4    : F1 {c4['f1']} ({c4['writes']} writes)")

    best_pipeline_f1 = max(legacy["f1"], c4["f1"])
    best_pipeline_tp = max(legacy["tp"], c4["tp"])
    tp_ratio = proposal["tp"] / best_pipeline_tp if best_pipeline_tp else None

    verdict = {
        "k1_no_material_gap": best_pipeline_f1 >= K1_NO_GAP_F1,
        "k2_instrument_reproduces_published_f1": k2_reproduced,
        "h2_headline_not_attainable_through_write_path": best_pipeline_f1 < K1_NO_GAP_F1,
        "p1_c4_f1_is_zero": c4["f1"] == 0.0,
        "p2_legacy_f1_at_most_one_percent": legacy["f1"] <= 0.01,
        "p3_proposal_stage_reproduced": k2_reproduced,
        "p4_pipeline_precision_at_least_proposal": (
            max(legacy["precision"], c4["precision"]) >= proposal["precision"]
        ),
        "p5_tp_ratio_exceeds_100x": bool(tp_ratio is not None and tp_ratio > P5_MIN_TP_RATIO),
        "true_positive_ratio_proposal_over_pipeline": (
            round(tp_ratio, 1) if tp_ratio is not None else None
        ),
        "f1_ratio_proposal_over_best_pipeline": (
            round(proposal["f1"] / best_pipeline_f1, 1) if best_pipeline_f1 else None
        ),
    }

    payload = {
        "schema_version": "dataforge_capability_stage_v1",
        "preregistration": "eval/preregistration/capability_measurement_stage.md",
        "dataset": "hospital",
        "ground_truth_cells": len(ground_truth),
        "published_f1": published_f1,
        "published_f1_source": (
            "eval/results/agent_comparison.json, gated against the code by "
            "scripts/ci/anchor_truth.py -- deliberately not a constant in this file"
        ),
        "scorer": "dataforge.bench.core.score_repairs (imported, not reimplemented)",
        "method": (
            "Three arms over one table, one ground truth and one scorer, differing ONLY in the "
            "stage at which the correction set is taken: proposal_stage reproduces "
            "dataforge/bench/methods.py::_repairs_from_proposed_fixes (no verifier, no safety "
            "filter, no auto-apply gate), while the pipeline arms take run_repair_pipeline's "
            "would-apply set in dry_run mode. Because the scorer is shared, a difference "
            "between arms can only be caused by the stage."
        ),
        "arms": {
            "proposal_stage_published_path": proposal,
            "pipeline_legacy_mined_authority": legacy,
            "pipeline_c4_declared_authority": c4,
        },
        "verdict": verdict,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
