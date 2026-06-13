"""Generate deterministic 10-row sample CSVs for the playground.

Downloads the full Raha benchmark datasets via the existing DataForge
dataset infrastructure for secondary samples, takes a seeded 10-row slice
from each, and writes them to playground/api/samples/. The Hospital sample is
curated as the primary product story so the hosted demo reliably shows one
clear decimal-shift repair: row 6 rating 45.0 -> 4.5.

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

HOSPITAL_STORY_CSV = """provider_number,hospital_name,city,state,zip_code,phone_number,rating,mortality_rate,readmission_rate,er_wait_time
PRV001,General Hospital,Springfield,IL,62701,2175550101,4.2,0.023,0.145,28
PRV002,St. Mary Medical Center,Chicago,IL,60601,3125550202,3.8,0.031,0.162,35
PRV001,Springfield Medical,Springfield,IL,62701,2175550303,4.5,0.019,0.138,22
PRV003,Mercy Hospital,Peoria,IL,61602,3095550404,3.5,0.028,0.158,31
PRV004,Northwestern Memorial,Chicago,IL,60611,3125550505,4.1,0.025,0.149,26
PRV005,Rush University MC,Chicago,IL,60612,3125550606,45.0,0.022,0.141,29
PRV006,Advocate Christ,Oak Lawn,IL,60453,7085550707,3.9,0.027,0.155,33
PRV007,Loyola University MC,Maywood,IL,60153,7085550808,4.3,0.020,0.142,25
PRV008,Presence St. Joseph,Joliet,IL,60435,8155550909,4.0,0.026,0.151,30
PRV009,Edward Hospital,Naperville,IL,60540,6305551010,3.7,0.029,0.160,34
"""


def build_samples() -> None:
    """Build all sample CSVs."""
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    for name in DATASETS:
        output_path = SAMPLES_DIR / f"{name}_10rows.csv"
        if name == "hospital":
            output_path.write_text(HOSPITAL_STORY_CSV, encoding="utf-8")
            print(f"  Wrote curated Hospital decimal-shift story to {output_path}")
            continue

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

        sample_df.to_csv(output_path, index=False)
        print(f"  Wrote {len(sample_df)} rows to {output_path}")

    print("Done.")


if __name__ == "__main__":
    build_samples()
