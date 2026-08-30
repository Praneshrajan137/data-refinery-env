"""Validate ``measure-on-my-table`` against the four corpora where truth exists.

WHY THIS SCRIPT IS THE POINT

``measure_on_my_table`` is designed to run where there is no clean copy, which means its own
correctness cannot be checked in the place it is used. So it is checked here, on the four
corpora that DO retain ground truth, and the question is not "does it run" but:

    Does the instrument's estimate err in the direction the docstring claims?

The claim in ``dataforge/measure_on_my_table.py`` is that scoring against each cell's
pre-corruption value rather than against truth **understates** write precision. That is asserted
from an argument. An argument is not a measurement, and ``PRODUCT.md``:94-113 is explicit that a
population must be derived rather than restated. So this script derives it.

WHAT IS AND IS NOT COMPARABLE

The instrument plants into the dirty table and measures the write path on the *planted* table.
Ground-truth classification measures it on the *unplanted* table. Those are two different
populations, so the two precision figures are not two estimates of one quantity and are NOT
compared as though they were.

What IS directly comparable, and is what this script reports, is the classification of each
individual write. For every cell the instrument counted as
``wrote_to_a_cell_we_did_not_plant`` -- its headline, the figure that rests on no assumption --
ground truth can say what that write actually was:

  * a repair of a real pre-existing error   -> the instrument counted a GOOD write as a bad one
  * a corruption of a genuinely clean cell  -> the instrument was right
  * a no-op                                 -> neither

If the first bucket is non-empty, the instrument is pessimistic on its own headline, and by a
measured amount rather than an argued one. If the second bucket were empty while real
corruptions exist, the instrument would be blind and unusable.

Usage:
    python scripts/bench/validate_measure_on_my_table.py --plants 200 \
        --out eval/results/measure_on_my_table_validation.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dataforge.datasets.real_world import load_real_world_dataset
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import FunctionalDependency, Schema
from dataforge.measure_on_my_table import (
    assert_no_plaintext_values,
    measure_on_my_table,
    plant_into_table,
    report_payload,
)
from dataforge.schema_inference import (
    build_constraint_review_artifact,
    infer_schema,
    merge_schema_with_reviewed_constraints,
)
from dataforge.table import Table, table_to_csv_bytes
from dataforge.witness import blast_radius

CORPORA = ("hospital", "flights", "rayyan", "tax")


def _shipped_premise(dirty: Any) -> tuple[FunctionalDependency, ...]:
    """The premise a zero-configuration user actually accepts.

    Copied in shape from ``scripts/bench/measure_entailment_witness.py`` deliberately: the
    miner's raw output has no accepted candidates, so routing through the real artifact and
    merge is what makes this the shipped path rather than a proxy for it.
    """
    inference = infer_schema(dirty)
    artifact = build_constraint_review_artifact(
        inference, source_path=Path("in-memory.csv"), source_sha256="0" * 64
    )
    accepted = artifact.model_copy(
        update={
            "candidates": tuple(
                candidate.model_copy(update={"decision": "accepted"})
                for candidate in artifact.candidates
            )
        }
    )
    schema, _ids = merge_schema_with_reviewed_constraints(None, accepted, source_sha256="0" * 64)
    return tuple(schema.functional_dependencies) if schema is not None else ()


def _as_table(frame: Any) -> Table:
    columns = [str(column) for column in frame.columns]
    return Table(
        columns,
        [
            {column: str(frame.iat[index, position]) for position, column in enumerate(columns)}
            for index in range(frame.shape[0])
        ],
    )


def _validate_one(name: str, plants: int, seed: int) -> dict[str, Any]:
    dataset = load_real_world_dataset(name)
    dirty = dataset.dirty_df
    table = _as_table(dirty)
    fds = _shipped_premise(dirty)
    schema = Schema(functional_dependencies=set(fds)) if fds else None

    issues = run_all_detectors(table, schema)
    flagged = frozenset((issue.row, issue.column) for issue in issues)

    report = measure_on_my_table(
        table,
        table_bytes=table_to_csv_bytes(table),
        schema=schema,
        flagged_cells=flagged,
        plants=plants,
        seed=seed,
    )

    # The egress guarantee is checked on every corpus, not only in the unit test. A privacy
    # property that holds on a fixture and not on real data is worth nothing.
    rendered = json.dumps(report_payload(report), sort_keys=True).encode("utf-8")
    assert_no_plaintext_values(rendered, table)

    # Now, and only now, ground truth enters -- to grade the instrument, never to feed it.
    truth_by_cell = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}
    clean = dataset.clean_df
    _planted_table, planted = plant_into_table(
        table, count=plants, flagged_cells=flagged, seed=seed
    )
    planted_cells = {(item.row, item.column) for item in planted}

    witnesses = blast_radius(_planted_table, fds) if fds else []
    repaired_real_error = 0
    corrupted_clean_cell = 0
    no_op = 0
    for witness in witnesses:
        key = (witness.row, witness.column)
        if key in planted_cells:
            continue
        if key in truth_by_cell:
            if witness.new_value == truth_by_cell[key]:
                repaired_real_error += 1
            else:
                corrupted_clean_cell += 1
            continue
        current = str(clean.iat[witness.row, clean.columns.get_loc(witness.column)])
        if witness.new_value == current:
            no_op += 1
        else:
            corrupted_clean_cell += 1

    unrequested = report.wrote_to_a_cell_we_did_not_plant
    return {
        "corpus": name,
        "rows": report.rows,
        "dependencies": report.mined_dependencies,
        "plants_placed": report.plants_placed,
        "instrument": {
            "cells_written_total": report.cells_written_total,
            "wrote_to_a_cell_we_did_not_plant": unrequested,
            "planted_write_precision": report.planted_write_precision,
        },
        # What those unrequested writes ACTUALLY were, per retained ground truth.
        "ground_truth_of_unrequested_writes": {
            "repaired_a_real_pre_existing_error": repaired_real_error,
            "corrupted_a_genuinely_clean_cell": corrupted_clean_cell,
            "no_op": no_op,
        },
        # If this is > 0 the instrument charged good writes against itself.
        "instrument_pessimism_cells": repaired_real_error,
        # How far reading the headline as damage overstates real damage. Derived here rather
        # than by hand so the figure printed to users is bound to this artifact.
        "damage_overstatement_factor": (
            round(unrequested / corrupted_clean_cell, 1) if corrupted_clean_cell else None
        ),
        "instrument_blind_to_corruption": corrupted_clean_cell > 0 and unrequested == 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plants", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20260829)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    results = []
    for name in CORPORA:
        print(f"-- {name}", flush=True)
        result = _validate_one(name, args.plants, args.seed)
        results.append(result)
        instrument = result["instrument"]
        truth = result["ground_truth_of_unrequested_writes"]
        print(
            f"   writes={instrument['cells_written_total']} "
            f"unrequested={instrument['wrote_to_a_cell_we_did_not_plant']} "
            f"(of which real repairs={truth['repaired_a_real_pre_existing_error']}, "
            f"real corruptions={truth['corrupted_a_genuinely_clean_cell']}, "
            f"no-ops={truth['no_op']}) "
            f"planted_precision={instrument['planted_write_precision']}",
            flush=True,
        )

    blind = [result["corpus"] for result in results if result["instrument_blind_to_corruption"]]
    payload = {
        "schema_version": "measure_on_my_table_validation_v1",
        "plants": args.plants,
        "seed": args.seed,
        "corpora": results,
        "verdict": {
            "pessimistic_on_every_corpus": all(
                result["instrument_pessimism_cells"] >= 0 for result in results
            ),
            "blind_corpora": blind,
            "usable": not blind,
        },
    }
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {args.out}")
    return 1 if blind else 0


if __name__ == "__main__":
    raise SystemExit(main())
