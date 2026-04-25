"""Write citation-only SOTA comparison rows for benchmark reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    """Write citation-only literature rows to JSON."""
    parser = argparse.ArgumentParser(description="Write citation-only SOTA comparison JSON.")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("eval/results/sota_comparison.json"),
    )
    args = parser.parse_args()

    payload = {
        "source": {
            "title": "BClean: A Bayesian Data Cleaning System",
            "table": "Table 4",
            "url": "https://szudseg.cn/assets/papers/vldb2024-qin.pdf",
            "note": "HoloClean 2017 is cited in report narrative only.",
        },
        "rows": [
            {
                "method": "HoloClean",
                "dataset": "hospital",
                "precision": 1.000,
                "recall": 0.456,
                "f1": 0.626,
                "note": "Citation-only literature result.",
            },
            {
                "method": "HoloClean",
                "dataset": "flights",
                "precision": 0.742,
                "recall": 0.352,
                "f1": 0.477,
                "note": "Citation-only literature result.",
            },
            {
                "method": "HoloClean",
                "dataset": "beers",
                "precision": 1.000,
                "recall": 0.024,
                "f1": 0.047,
                "note": "Citation-only literature result.",
            },
            {
                "method": "Raha+Baran",
                "dataset": "hospital",
                "precision": 0.971,
                "recall": 0.585,
                "f1": 0.730,
                "note": "Citation-only literature result.",
            },
            {
                "method": "Raha+Baran",
                "dataset": "flights",
                "precision": 0.829,
                "recall": 0.650,
                "f1": 0.729,
                "note": "Citation-only literature result.",
            },
            {
                "method": "Raha+Baran",
                "dataset": "beers",
                "precision": 0.873,
                "recall": 0.872,
                "f1": 0.873,
                "note": "Citation-only literature result.",
            },
        ],
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
