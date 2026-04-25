"""Generate deterministic 10-row sample CSVs for the playground.

Downloads the full Raha benchmark datasets via the existing DataForge
dataset infrastructure, takes a seeded 10-row slice from each, and writes
them to playground/api/samples/. This script ensures the samples are
reproducible, not magic numbers.

Usage:
    python scripts/playground/build_samples.py
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402, I001


SAMPLES_DIR = PROJECT_ROOT / "playground" / "api" / "samples"
SEED = 42
N_ROWS = 10

DATASETS = ["hospital", "flights", "beers"]


def build_samples() -> None:
    """Build all sample CSVs."""
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    for name in DATASETS:
        print(f"Loading {name}...")
        dataset = load_real_world_dataset(name)
        dirty_df = dataset.dirty_df

        # Deterministic sample
        random.seed(SEED)
        total_rows = len(dirty_df)
        if total_rows <= N_ROWS:
            sample_df = dirty_df.copy()
        else:
            indices = sorted(random.sample(range(total_rows), N_ROWS))
            sample_df = dirty_df.iloc[indices].reset_index(drop=True)

        output_path = SAMPLES_DIR / f"{name}_10rows.csv"
        sample_df.to_csv(output_path, index=False)
        print(f"  Wrote {len(sample_df)} rows to {output_path}")

    print("Done.")


if __name__ == "__main__":
    build_samples()
