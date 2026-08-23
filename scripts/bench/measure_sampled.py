"""Scale-aware sampled benchmark: measure the deterministic stack on a row sample.

Some RAHA datasets (notably ``tax`` at 200k rows) are too large for the
deterministic bench because schema inference is super-linear. This harness uses
``sample_dataset_rows`` to take a deterministic, ground-truth-aligned head sample
so the dataset can be measured HONESTLY at a tractable size, using the SAME
``run_heuristic_episode`` scoring as every other dataset.

The output is explicitly a SAMPLE: the artifact records ``sampled: true`` and the
sample size, so the number can never be mistaken for a full-dataset or
pinned-source result. It is a measurement instrument, not a release artifact.

Usage:
    python scripts/bench/measure_sampled.py --dataset tax --max-rows 3000 \
        --output-json eval/results/heuristic_tax_sampled.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dataforge.bench.methods import run_heuristic_episode
from dataforge.datasets.real_world import load_real_world_dataset, sample_dataset_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--max-rows", type=int, required=True)
    parser.add_argument(
        "--strategy",
        choices=("random", "head"),
        default="random",
        help=(
            "random (default) draws a seeded uniform sample; head takes the leading slice. "
            "head is NOT a sample of a sorted table -- it is a biased stratum -- and is "
            "retained only to reproduce the pre-2026-08-23 tax artifact."
        ),
    )
    parser.add_argument("--sample-seed", type=int, default=20260823)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output-json", type=Path, default=None)
    args = parser.parse_args()

    full = load_real_world_dataset(args.dataset)
    full_rows = len(full.dirty_df.index)
    sample = sample_dataset_rows(full, args.max_rows, strategy=args.strategy, seed=args.sample_seed)
    record = run_heuristic_episode(sample, seed=args.seed)

    artifact = {
        "schema_version": "dataforge_sampled_bench_v1",
        "provenance": {
            "sampled": True,
            "dataset": args.dataset,
            "max_rows": args.max_rows,
            "sampled_rows": len(sample.dirty_df.index),
            "full_rows": full_rows,
            "sampling_strategy": args.strategy,
            "sampling_seed": args.sample_seed if args.strategy == "random" else None,
            "error_provenance": full.metadata.error_provenance,
            "tier": full.metadata.tier,
            "ground_truth_cells_in_sample": len(sample.ground_truth),
            "seed": args.seed,
            "note": (
                f"SAMPLED deterministic-stack measurement ({args.strategy} sample). NOT a "
                "full-dataset result and NOT tied to the pinned source hashes. For "
                "measure-first honesty only; never cite as the full-dataset number. "
                "Recorded strategy and seed so the sample cannot be mistaken for the whole."
            ),
        },
        "record": record.model_dump(mode="json"),
    }

    summary = (
        f"{args.dataset} ({args.strategy} sample {len(sample.dirty_df.index)}/{full_rows}, "
        f"provenance={full.metadata.error_provenance}, tier={full.metadata.tier}): "
        f"correction P/R/F1 = {record.precision:.4f}/{record.recall:.4f}/{record.f1:.4f} "
        f"(tp={record.tp} fp={record.fp} fn={record.fn}); runtime {record.runtime_s}s"
    )
    print(summary)
    for error_class, score in record.by_class.items():
        print(
            f"  {error_class}: detection_recall={score.detection_recall:.3f} "
            f"correction_recall={score.recall:.3f} support={score.support}"
        )

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"wrote {args.output_json}")


if __name__ == "__main__":
    main()
