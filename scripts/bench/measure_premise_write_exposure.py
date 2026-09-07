"""P3: what C4 costs and prevents, measured through the shipped write path.

Executes the write-exposure arm of `eval/preregistration/premise_acquisition.md` (P3, P4, P5).
Read that document first. The H1 half is separate and lives in
`measure_premise_acquisition_h1.py`; `rwd` supplies annotations but no dirty/clean pairs, so it
can test H1 and CANNOT test this. Conflating the two would be a scoping error.

## What is measured, and why it goes through the pipeline

Two arms per corpus, differing in exactly one flag:

- **legacy** -- `mined_constraints_grant_write_authority=True`, the pre-2026-09-07 behaviour in
  which accepting a mined candidate in review authorised writes.
- **c4** -- the shipped default, in which it does not.

Both run `run_repair_pipeline` in `dry_run` mode, so nothing is written and the measured
quantity is the set of fixes that WOULD be applied. That is deliberate: the K4 fence harness
(`measure_deductive_coverage.py`) measures the repairer and verifier directly and therefore
cannot see the authority gate at all, which is why P3 needed its own instrument rather than a
new column in an existing one.

The premise is built through the real objects -- `infer_schema`,
`build_constraint_review_artifact`, accept every functional-dependency candidate,
`run_repair_pipeline(constraints=...)`. That is `profile --constraints-out`,
`constraints review --accept`, `repair --constraints`. Using a proxy for the premise is what
produced the defect this measurement exists to quantify.

Scoring follows `measure_deductive_coverage.py` exactly -- `repaired_a_real_error`,
`wrong_value_on_a_real_error`, `no_op_on_a_clean_cell`, `corrupted_a_clean_cell` -- rather than
inventing a fourth vocabulary for the same four outcomes.

Read-only apart from one artifact under `eval/results/` and a temporary CSV per corpus.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.engine.repair import (  # noqa: E402
    RepairPipelineRequest,
    run_repair_pipeline,
)
from dataforge.schema_inference import (  # noqa: E402
    build_constraint_review_artifact,
    infer_schema,
    update_constraint_review_artifact,
)

# `beers` is excluded from the project by the dataset-scope rule and is not added here.
DEFAULT_CORPORA = ("hospital", "flights", "rayyan")
OUTPUT = REPO / "eval" / "results" / "premise_acquisition_write_exposure.json"


def _accept_all_fd_artifact(dirty: pd.DataFrame, source: Path) -> Any:
    """Build the premise a user gets by accepting every mined dependency.

    Constructed through the objects the user's commands run, never by filtering candidates
    directly, for the reason `measure_deductive_coverage.shipped_accept_all_fds` gives: a
    proxy for this premise is what produced the defect being measured.
    """
    import hashlib

    inferred = infer_schema(dirty)
    artifact = build_constraint_review_artifact(
        inferred,
        source_path=source,
        source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
    )
    fd_ids = [
        reviewed.candidate_id
        for reviewed in artifact.candidates
        if reviewed.candidate.kind == "functional_dependency"
    ]
    return update_constraint_review_artifact(artifact, accept_ids=fd_ids), len(fd_ids)


def _truth_by_cell(dirty: pd.DataFrame, clean: pd.DataFrame) -> dict[tuple[int, str], str]:
    """Cells where the dirty frame disagrees with the clean one: the real errors."""
    truth: dict[tuple[int, str], str] = {}
    shared = [column for column in dirty.columns if column in clean.columns]
    for column in shared:
        dirty_values = dirty[column].astype(str)
        clean_values = clean[column].astype(str)
        for row in range(min(len(dirty_values), len(clean_values))):
            if dirty_values.iat[row] != clean_values.iat[row]:
                truth[(row, column)] = clean_values.iat[row]
    return truth


def _score(
    fixes: list[Any],
    dirty: pd.DataFrame,
    clean: pd.DataFrame,
    truth: dict[tuple[int, str], str],
) -> dict[str, Any]:
    """Score a would-apply set with the same four outcomes the K4 harness uses."""
    tally = {
        "writes": 0,
        "repaired_a_real_error": 0,
        "wrong_value_on_a_real_error": 0,
        "no_op_on_a_clean_cell": 0,
        "corrupted_a_clean_cell": 0,
    }
    for fix in fixes:
        row, column, chosen = fix.row, fix.column, str(fix.new_value)
        if column not in clean.columns or row >= len(clean):
            continue
        tally["writes"] += 1
        key = (row, column)
        if key in truth:
            if chosen == truth[key]:
                tally["repaired_a_real_error"] += 1
            else:
                tally["wrong_value_on_a_real_error"] += 1
            continue
        current = str(clean.iat[row, clean.columns.get_loc(column)])
        if chosen == current:
            tally["no_op_on_a_clean_cell"] += 1
        else:
            tally["corrupted_a_clean_cell"] += 1

    repaired = tally["repaired_a_real_error"]
    damaged = tally["corrupted_a_clean_cell"] + tally["wrong_value_on_a_real_error"]
    tally["net_cells_improved"] = repaired - damaged
    # Reported alongside, never alone: precision without the counts hides a zero-write arm.
    tally["write_precision"] = round(repaired / tally["writes"], 4) if tally["writes"] else None
    return tally


def _run_arm(source: Path, artifact: Any, *, trust_mined: bool) -> list[Any]:
    """Return the would-apply fixes for one arm. `dry_run` writes nothing."""
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="dry_run",
            constraints=artifact,
            mined_constraints_grant_write_authority=trust_mined,
        )
    )
    return list(result.fixes)


def main() -> int:
    """Measure both arms on each corpus and emit the trade."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpora", default=",".join(DEFAULT_CORPORA))
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    requested = [name.strip() for name in args.corpora.split(",") if name.strip()]
    if "beers" in requested:
        print(
            "FAIL beers is excluded from this project by the dataset-scope rule.", file=sys.stderr
        )
        return 2

    corpora: dict[str, Any] = {}
    for name in requested:
        try:
            dataset = load_real_world_dataset(name)
        except Exception as error:  # noqa: BLE001 - a missing corpus is reported, not fatal
            corpora[name] = {"status": "not_available", "detail": str(error)}
            print(f"  {name}: not available ({error})")
            continue

        dirty, clean = dataset.dirty_df, dataset.clean_df
        truth = _truth_by_cell(dirty, clean)

        with tempfile.TemporaryDirectory(prefix=f"dataforge-p3-{name}-") as raw_tmp:
            source = Path(raw_tmp) / f"{name}.csv"
            dirty.to_csv(source, index=False)
            artifact, fd_count = _accept_all_fd_artifact(dirty, source)

            legacy = _score(_run_arm(source, artifact, trust_mined=True), dirty, clean, truth)
            c4 = _score(_run_arm(source, artifact, trust_mined=False), dirty, clean, truth)

        corpora[name] = {
            "status": "measured",
            "rows": int(len(dirty)),
            "real_errors_in_shared_columns": len(truth),
            "mined_fds_accepted": fd_count,
            "arms": {"legacy_mined_authority": legacy, "c4_declared_authority": c4},
            "delta": {
                "repairs_given_up": legacy["repaired_a_real_error"] - c4["repaired_a_real_error"],
                "corruptions_prevented": (
                    legacy["corrupted_a_clean_cell"] - c4["corrupted_a_clean_cell"]
                ),
            },
        }
        print(
            f"  {name}: {fd_count} mined FDs | legacy "
            f"{legacy['repaired_a_real_error']}R/{legacy['corrupted_a_clean_cell']}C "
            f"-> c4 {c4['repaired_a_real_error']}R/{c4['corrupted_a_clean_cell']}C"
        )

    payload = {
        "schema_version": "dataforge_premise_write_exposure_v1",
        "preregistration": "eval/preregistration/premise_acquisition.md",
        "predictions_tested": ["P3", "P4", "P5"],
        "method": (
            "run_repair_pipeline in dry_run mode, twice per corpus, differing only in "
            "mined_constraints_grant_write_authority. The premise is built through "
            "infer_schema + build_constraint_review_artifact + accept-all, i.e. the objects "
            "the user's own commands run. Scoring vocabulary matches "
            "scripts/bench/measure_deductive_coverage.py."
        ),
        "note": (
            "The K4 fence harness measures the repairer and verifier directly and cannot see "
            "the authority gate, which is why this arm exists separately rather than as a "
            "column there."
        ),
        "corpora": corpora,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
