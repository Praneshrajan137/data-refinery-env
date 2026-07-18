"""Write citation-only SOTA comparison rows for benchmark reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SOTA_SCHEMA_VERSION = "dataforge_sota_citation_v1"
BCLEAN_SOURCE_SHA256 = "40f85c91e20383131488b758be46fa2aae54e591cd5973824688f301d93c2715"
BCLEAN_SOURCE_TITLE = "BClean: A Bayesian Data Cleaning System"
BCLEAN_SOURCE_URL = "https://arxiv.org/abs/2311.06517"
BCLEAN_PDF_URL = "https://arxiv.org/pdf/2311.06517"
BCLEAN_RETRIEVED_AT_UTC = "2026-05-25T00:00:00Z"


def _citation_row(
    *,
    method: str,
    dataset: str,
    precision: float,
    recall: float,
    f1: float,
) -> dict[str, object]:
    """Build one citation-only comparison row."""
    return {
        "method": method,
        "dataset": dataset,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "evidence_kind": "citation_only",
        "source_title": BCLEAN_SOURCE_TITLE,
        "source_url": BCLEAN_SOURCE_URL,
        "source_table": "Table 4",
        "source_page": "Section 7.2.1, Table 4",
        "source_sha256": BCLEAN_SOURCE_SHA256,
        "retrieved_at_utc": BCLEAN_RETRIEVED_AT_UTC,
        "note": "Citation-only literature result; not rerun by this repository.",
    }


def build_sota_payload() -> dict[str, object]:
    """Return citation-only SOTA evidence with source provenance."""
    return {
        "schema_version": SOTA_SCHEMA_VERSION,
        "source": {
            "title": BCLEAN_SOURCE_TITLE,
            "table": "Table 4",
            "page": "Section 7.2.1, Table 4",
            "url": BCLEAN_SOURCE_URL,
            "pdf_url": BCLEAN_PDF_URL,
            "source_sha256": BCLEAN_SOURCE_SHA256,
            "retrieved_at_utc": BCLEAN_RETRIEVED_AT_UTC,
            "note": "HoloClean and Raha+Baran rows are transcribed from BClean Table 4.",
        },
        "rows": [
            _citation_row(
                method="HoloClean",
                dataset="hospital",
                precision=1.000,
                recall=0.456,
                f1=0.626,
            ),
            _citation_row(
                method="HoloClean",
                dataset="flights",
                precision=0.742,
                recall=0.352,
                f1=0.477,
            ),
            _citation_row(
                method="Raha+Baran",
                dataset="hospital",
                precision=0.971,
                recall=0.585,
                f1=0.730,
            ),
            _citation_row(
                method="Raha+Baran",
                dataset="flights",
                precision=0.829,
                recall=0.650,
                f1=0.729,
            ),
        ],
    }


def main() -> int:
    """Write citation-only literature rows to JSON."""
    parser = argparse.ArgumentParser(description="Write citation-only SOTA comparison JSON.")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("eval/results/sota_comparison.json"),
    )
    args = parser.parse_args()

    payload = build_sota_payload()
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
