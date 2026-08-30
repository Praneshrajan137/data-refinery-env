"""Cost of the entailment witness, measured by counted work rather than wall clock.

Kill criterion F5 in ``eval/preregistration/entailment_witness.md``. Wall clock is unusable
as a gate on this project's development machine: the same verifier code measured 42, then
166-249, then 136-143, then 79.8-352.2 ms/fix in one afternoon, so within-configuration
variance exceeded between-configuration difference. Counted work is bit-identical across
runs, which is the property a budget needs. Cachegrind's manual makes the same argument;
neither Cachegrind nor `iai` is available on Windows, which is recorded as an open limit.

The quantity counted is **cell reads**, which is what the witness spends. The prediction is
that blast radius is O(rows x dependencies) after the group-index rewrite, not
O(rows^2 x dependencies) as the first implementation was. A quadratic witness would have
been unusable on exactly the corpus that tests its limits: 200,000 rows squared is 10^10 row
comparisons.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import dataforge.witness as witness_module
from dataforge.datasets.real_world import load_real_world_dataset
from dataforge.table import Table
from dataforge.witness import blast_radius


def _counting_cell_reader() -> tuple[Any, dict[str, int]]:
    """Wrap the module's cell accessor so reads can be counted deterministically."""
    counter = {"cell_reads": 0}
    original = witness_module._cell

    def counted(table: Any, row: int, column: str) -> str:
        counter["cell_reads"] += 1
        return str(original(table, row, column))

    return (original, counted, counter)  # type: ignore[return-value]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="tax")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=[1000, 5000, 25000],
        help="Row counts to measure, so scaling can be read rather than assumed.",
    )
    args = parser.parse_args(argv)

    dataset = load_real_world_dataset(args.corpus)
    full = dataset.dirty_df

    from dataforge.schema_inference import (
        build_constraint_review_artifact,
        infer_schema,
        merge_schema_with_reviewed_constraints,
    )

    # Mine on the FULL frame, then accept every candidate. tax mines four candidates over its
    # 200,000 rows and none over a 25,000-row head, so mining on a subset would measure an
    # empty premise and report a cost of zero -- a green result establishing nothing. And a
    # raw inference has no ACCEPTED candidates, so `to_schema()` on it also yields no
    # dependencies: the premise has to come through the artifact and merge, exactly as the
    # zero-config user's does.
    inference = infer_schema(full)
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
    fds = tuple(schema.functional_dependencies) if schema is not None else ()
    print(f"corpus={args.corpus} total_rows={len(full):,} dependencies={len(fds)}", flush=True)
    if not fds:
        print("no dependencies mined; cost is trivially zero and F5 is not testable here")
        args.artifact.parent.mkdir(parents=True, exist_ok=True)
        args.artifact.write_text(
            json.dumps(
                {
                    "schema_version": "entailment_witness_cost_v1",
                    "corpus": args.corpus,
                    "dependency_count": 0,
                    "note": "no mined dependencies, so blast radius is empty by construction",
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return 0

    original, counted, counter = _counting_cell_reader()
    measurements: list[dict[str, Any]] = []
    try:
        witness_module._cell = counted  # type: ignore[assignment]
        for rows in args.rows:
            if rows > len(full):
                continue
            frame = full.head(rows)
            table = Table(
                list(frame.columns),
                (row._asdict() for row in frame.itertuples(index=False)),
            )
            counter["cell_reads"] = 0
            started = time.perf_counter()
            witnesses = blast_radius(table, fds)
            elapsed = time.perf_counter() - started
            reads = counter["cell_reads"]
            measurements.append(
                {
                    "rows": rows,
                    "cell_reads": reads,
                    "reads_per_row": round(reads / rows, 2),
                    "predicted_writes": len(witnesses),
                    "observed_seconds": round(elapsed, 3),
                }
            )
            print(
                f"rows={rows:>7,} cell_reads={reads:>12,} "
                f"reads_per_row={reads / rows:>8.2f} writes={len(witnesses):,} "
                f"({elapsed:.2f}s observed, NOT a budget)",
                flush=True,
            )
    finally:
        witness_module._cell = original  # type: ignore[assignment]

    # Linearity check: reads_per_row must stay flat. A quadratic implementation makes it grow
    # with the row count, which is the regression this instrument exists to catch.
    per_row = [m["reads_per_row"] for m in measurements]
    linear = len(per_row) < 2 or max(per_row) <= min(per_row) * 1.5

    report = {
        "schema_version": "entailment_witness_cost_v1",
        "corpus": args.corpus,
        "dependency_count": len(fds),
        "measurements": measurements,
        "reads_per_row_is_flat": linear,
        "note": (
            "cell_reads is the deterministic quantity; observed_seconds is recorded for "
            "context and must never be used as a gate on this machine."
        ),
    }
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nreads_per_row flat across scales: {linear}", flush=True)
    print(f"artifact: {args.artifact}", flush=True)
    return 0 if linear else 1


if __name__ == "__main__":
    sys.exit(main())
