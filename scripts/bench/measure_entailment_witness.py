"""Measure whether the entailment witness predicts where a premise does harm.

Pre-registered in ``eval/preregistration/entailment_witness.md``. Read that first: the kill
criteria are committed there and this script must not be read as choosing them afterwards.

WHAT THIS ANSWERS

``PRODUCT.md``:186-190 states the mechanism -- harm comes from a false premise *meeting a
group that disagrees*, not from falseness -- and ``docs/trust/shipped-premise-result.md``
measures its consequences on hospital. This script asks whether
``dataforge.witness.blast_radius``, which needs no ground truth and no repairer, PREDICTS
those measured consequences.

WHY A PREDICTION RATHER THAN A RE-RUN

``shipped-premise-result.md``:99 records ~23 minutes for one arm and hours for three. The
witness is a groupby, so it is cheap; the committed measurement is the oracle. Comparing a
cheap predictor against an expensive measurement is the point, because the predictor is what
a customer table can afford.

WHAT THIS SCRIPT DOES NOT DO

It does not reimplement the write loop. ``PRODUCT.md``:176-185 records what that costs: a
reimplementation of ``_write_exposure`` omitted the no-change filter and produced 959 writes
where the truth is 74, nearly publishing a finding that writes are 95% no-ops. So the
premise is built through the shipped artifact and merge -- ``build_constraint_review_artifact``
then ``merge_schema_with_reviewed_constraints`` -- exactly as
``shipped_accept_all_fds`` does, and ground truth is used only to CLASSIFY the witness
output, never to produce it.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dataforge.datasets.real_world import load_real_world_dataset
from dataforge.detectors.base import FunctionalDependency
from dataforge.schema_inference import (
    build_constraint_review_artifact,
    infer_schema,
    merge_schema_with_reviewed_constraints,
)
from dataforge.table import Table
from dataforge.witness import blast_radius, fd_label

#: The oracle, from ``docs/trust/shipped-premise-result.md``:46-58. Stated here so a
#: mismatch is a loud failure rather than a number nobody compared.
ORACLE_CORRUPTIONS_BY_COLUMN: dict[str, int] = {
    "ProviderNumber": 23,
    "HospitalOwner": 30,
    "HospitalName": 23,
    "State": 20,
    "Stateavg": 20,
}
ORACLE_TOTAL = 116

#: The two dependencies that are equally false and corrupted NOTHING. Criterion F2d, and
#: the only criterion that distinguishes capturing the mechanism from flagging falseness.
ORACLE_INERT_FALSE_DEPENDENCIES = (
    "ZipCode -> Address1",
    "ZipCode -> PhoneNumber",
)


def _shipped_premise(dirty: Any) -> tuple[FunctionalDependency, ...]:
    """Build the premise a zero-configuration user actually accepts.

    The miner's full output at its 0.90 emission floor, routed through the real artifact and
    merge because ``ConstraintReviewArtifact.to_schema()`` applies no floor of its own. This
    is the ``shipped_accept_all`` arm, and using a proxy for it is the defect that made every
    published figure describe a product that does not exist.
    """
    inference = infer_schema(dirty)
    artifact = build_constraint_review_artifact(
        inference,
        source_path=Path("in-memory.csv"),
        source_sha256="0" * 64,
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


def _classify(dataset: Any, witnesses: list[Any]) -> dict[str, Any]:
    """Split predicted writes into repairs and corruptions using retained ground truth.

    Ground truth enters HERE and nowhere earlier: the witness is produced without it, which
    is the property that makes the instrument usable on a table that has none.
    """
    clean = dataset.clean_df
    truth_by_cell = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}

    corrupted_by_column: dict[str, int] = {}
    corrupted_by_constraint: dict[str, int] = {}
    repaired = 0
    wrong_on_real_error = 0
    no_op_on_clean = 0

    for witness in witnesses:
        key = (witness.row, witness.column)
        if key in truth_by_cell:
            if witness.new_value == truth_by_cell[key]:
                repaired += 1
            else:
                wrong_on_real_error += 1
            continue
        current = str(clean.iat[witness.row, clean.columns.get_loc(witness.column)])
        if witness.new_value == current:
            no_op_on_clean += 1
            continue
        corrupted_by_column[witness.column] = corrupted_by_column.get(witness.column, 0) + 1
        corrupted_by_constraint[witness.constraint] = (
            corrupted_by_constraint.get(witness.constraint, 0) + 1
        )

    return {
        "predicted_writes": len(witnesses),
        "repaired_a_real_error": repaired,
        "wrong_value_on_a_real_error": wrong_on_real_error,
        "no_op_on_a_clean_cell": no_op_on_clean,
        "corrupted_a_clean_cell": sum(corrupted_by_column.values()),
        "corrupted_by_column": dict(sorted(corrupted_by_column.items())),
        "corrupted_by_constraint": dict(sorted(corrupted_by_constraint.items())),
    }


def _evaluate_criteria(
    classified: dict[str, Any], fds: tuple[FunctionalDependency, ...]
) -> dict[str, Any]:
    """Score the pre-registered criteria. No criterion is invented here."""
    by_column = classified["corrupted_by_column"]
    by_constraint = classified["corrupted_by_constraint"]
    labels = {fd_label(fd) for fd in fds}

    f2a = classified["corrupted_a_clean_cell"] == ORACLE_TOTAL
    f2b = {
        column: (by_column.get(column, 0), expected)
        for column, expected in ORACLE_CORRUPTIONS_BY_COLUMN.items()
    }
    f2b_pass = all(observed == expected for observed, expected in f2b.values())
    f2c = {
        "ZipCode -> ProviderNumber": by_constraint.get("ZipCode -> ProviderNumber", 0),
        "City -> HospitalOwner": by_constraint.get("City -> HospitalOwner", 0),
    }
    inert = {
        label: by_constraint.get(label, 0)
        for label in ORACLE_INERT_FALSE_DEPENDENCIES
        if label in labels
    }
    f2d_pass = bool(inert) and all(count == 0 for count in inert.values())

    return {
        "F2a_total_corruptions": {
            "observed": classified["corrupted_a_clean_cell"],
            "oracle": ORACLE_TOTAL,
            "pass": f2a,
        },
        "F2b_by_column": {"observed_vs_oracle": f2b, "pass": f2b_pass},
        "F2c_attribution": {"observed": f2c, "pass": f2c["ZipCode -> ProviderNumber"] == 23},
        "F2d_inert_false_dependencies": {
            "observed": inert,
            "note": (
                "These two dependencies are equally false as the ones that did damage and "
                "corrupted nothing. Any predictor that merely flags falseness fails here, "
                "which is why this is the decisive criterion."
            ),
            "pass": f2d_pass,
        },
    }


def _path_decomposition(table: Any, fds: tuple[FunctionalDependency, ...]) -> dict[str, Any]:
    """Does the marginal blast radius decompose exactly along an acceptance path?

    This answers the objection `dataforge/cli/constraints.py`:380-385 raises against showing
    a reviewer any per-candidate number:

        `docs/trust/constraint-additivity.md` measures that per-candidate harm does not
        compose: summed over hospital's 85 candidates in isolation it is 330, while
        accepting all 85 together yields 116, because overlapping dependencies mask one
        another and only one acts per cell. So a per-candidate number would overstate harm
        by a factor that depends on what else the reviewer accepts.

    That is correct about *isolated* per-candidate harm, and it is the reason the shipped
    warning has no per-candidate figure. But it does not rule out a **marginal** figure --
    the change in blast radius from accepting a candidate GIVEN what is already accepted --
    because that quantity conditions on exactly the "what else the reviewer accepts" the
    objection identifies as the confound.

    Measured here rather than argued: accept in canonical determinant order and sum the
    per-step deltas. If the sum equals the total for the full set, the decomposition is
    exact and a reviewer can be shown a per-candidate number that does not overstate.
    """
    from dataforge.witness import blast_radius as radius

    ordered = sorted(fds, key=lambda item: tuple(item.determinant))
    previous = 0
    deltas: list[dict[str, Any]] = []
    isolated_sum = 0
    for index, fd in enumerate(ordered, start=1):
        prefix = tuple(ordered[:index])
        current = len(radius(table, prefix))
        isolated_sum += len(radius(table, (fd,)))
        deltas.append({"accepted": fd_label(fd), "marginal_cells_written": current - previous})
        previous = current

    total = previous
    return {
        "note": (
            "Marginal deltas along a canonical acceptance path. Compare `isolated_sum`, which "
            "is the quantity constraint-additivity.md shows does NOT compose."
        ),
        "total_cells_written": total,
        "sum_of_marginal_deltas": sum(entry["marginal_cells_written"] for entry in deltas),
        "sum_of_isolated_radii": isolated_sum,
        "decomposition_is_exact": sum(entry["marginal_cells_written"] for entry in deltas) == total,
        "largest_marginals": sorted(deltas, key=lambda entry: -entry["marginal_cells_written"])[
            :10
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="hospital")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--path-decomposition",
        action="store_true",
        help="Also measure whether marginal blast radius decomposes along an acceptance path.",
    )
    args = parser.parse_args(argv)

    dataset = load_real_world_dataset(args.corpus)
    dirty = dataset.dirty_df if args.max_rows is None else dataset.dirty_df.head(args.max_rows)

    print(f"corpus={args.corpus} rows={dirty.shape[0]:,}", flush=True)
    fds = _shipped_premise(dirty)
    print(f"shipped_accept_all premise: {len(fds)} dependencies", flush=True)

    # The product reads a `Table`, not a DataFrame, and `DeterminantGroupIndex` caches only
    # when the table exposes `column_revision`. Measuring on a DataFrame would take the
    # uncached branch while the product takes the cached one.
    table = Table(list(dirty.columns), (row._asdict() for row in dirty.itertuples(index=False)))

    witnesses = blast_radius(table, fds)
    print(f"predicted writes: {len(witnesses):,}", flush=True)

    classified = _classify(dataset, witnesses)
    criteria = _evaluate_criteria(classified, fds)

    report: dict[str, Any] = {
        "schema_version": "entailment_witness_result_v1",
        "corpus": args.corpus,
        "rows": int(dirty.shape[0]),
        "premise": {
            "arm": "shipped_accept_all",
            "dependency_count": len(fds),
            "dependencies": sorted(fd_label(fd) for fd in fds),
        },
        "classified": classified,
        "criteria": criteria,
        "all_criteria_pass": all(block.get("pass") is True for block in criteria.values()),
    }

    if args.path_decomposition:
        print("measuring path decomposition (85 prefixes)...", flush=True)
        report["path_decomposition"] = _path_decomposition(table, fds)

    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    for name, block in criteria.items():
        verdict = "PASS" if block.get("pass") else "FAIL"
        print(f"{verdict}  {name}: {json.dumps(block.get('observed', block))}", flush=True)
    print(f"\nartifact: {args.artifact}", flush=True)
    return 0 if report["all_criteria_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
