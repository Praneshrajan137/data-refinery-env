"""Write citation-only SOTA comparison rows for benchmark reports.

This script is the SINGLE SOURCE OF TRUTH for `eval/results/sota_comparison.json`.

It has to be, and the reason is a defect this file previously carried. On 2026-09-01 an
audit found the artifact transcribed only HoloClean and Raha+Baran -- the two weakest rows
of BClean Table 4 -- from a source whose own system reports 0.976 on hospital. The fix added
BClean, BClean (PI/PIP), PClean and GARF **to the JSON by hand and not to this generator**.
The generator therefore still emitted four rows while the artifact held eight, so running the
documented regeneration command would have silently deleted the correction and restored the
mis-citation. `test_sota_payload_is_citation_evidence_not_reproduced_rows` did not catch it,
because it asserted the schema version and the evidence kind, never the row population.

`tests/unit/test_bench_report.py::test_generator_matches_the_committed_artifact` now pins
generator output against the committed file, so that divergence cannot recur silently.

Every row is citation-only: transcribed from a published table, never rerun here. Rows carry
their own provenance because the artifact now cites more than one paper, and a single
top-level source line above rows drawn from two different tables would itself be a
mis-citation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

SOTA_SCHEMA_VERSION = "dataforge_sota_citation_v1"

# --- Sources -----------------------------------------------------------------------------
# `source_sha256` is the SHA-256 of the PDF at `pdf_url`, so a transcription can be audited
# against the exact bytes it was read from.

BCLEAN: dict[str, str] = {
    "source_short": "BClean",
    "source_title": "BClean: A Bayesian Data Cleaning System",
    "source_url": "https://arxiv.org/abs/2311.06517",
    "pdf_url": "https://arxiv.org/pdf/2311.06517",
    "source_table": "Table 4",
    "source_page": "Section 7.2.1, Table 4",
    "source_sha256": "40f85c91e20383131488b758be46fa2aae54e591cd5973824688f301d93c2715",
    "retrieved_at_utc": "2026-05-25T00:00:00Z",
}

COCOON: dict[str, str] = {
    "source_short": "Cocoon",
    "source_title": "Data Cleaning Using Large Language Models",
    "source_url": "https://arxiv.org/abs/2410.15547",
    "pdf_url": "https://arxiv.org/pdf/2410.15547",
    "source_table": "Table 1",
    "source_page": "Section 3.2, Table 1",
    "source_sha256": "da4b1eaf974f33dc4b4d87964b0b851d5343d0270b99a7847cfb1e021b2f82e5",
    "retrieved_at_utc": "2026-09-06T00:00:00Z",
}

_DEFAULT_NOTE = "Citation-only literature result; not rerun by this repository."


def _citation_row(
    *,
    method: str,
    dataset: str,
    precision: float,
    recall: float,
    f1: float,
    source: dict[str, str] = BCLEAN,
    note: str = _DEFAULT_NOTE,
) -> dict[str, Any]:
    """Build one citation-only comparison row, carrying its own source provenance."""
    return {
        "method": method,
        "dataset": dataset,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "evidence_kind": "citation_only",
        "source_short": source["source_short"],
        "source_title": source["source_title"],
        "source_url": source["source_url"],
        "source_table": source["source_table"],
        "source_page": source["source_page"],
        "source_sha256": source["source_sha256"],
        "retrieved_at_utc": source["retrieved_at_utc"],
        "note": note,
    }


def build_sota_payload() -> dict[str, Any]:
    """Return citation-only SOTA evidence with per-row source provenance."""
    return {
        "schema_version": SOTA_SCHEMA_VERSION,
        "source": {
            "title": BCLEAN["source_title"],
            "table": BCLEAN["source_table"],
            "page": BCLEAN["source_page"],
            "url": BCLEAN["source_url"],
            "pdf_url": BCLEAN["pdf_url"],
            "source_sha256": BCLEAN["source_sha256"],
            "retrieved_at_utc": BCLEAN["retrieved_at_utc"],
            "note": (
                "PRIMARY source only. This artifact cites more than one paper: rows carry "
                "their own `source_*` fields and the report renders provenance per row. "
                "Added 2026-09-01: BClean, PClean and GARF -- the table previously "
                "transcribed only HoloClean and Raha+Baran, the two weakest rows, from a "
                "source whose own system reports 0.976 on hospital, which selected against "
                "the source. Omitting the stronger rows of a correctly-cited table is still "
                "a misleading citation. Added 2026-09-06: Cocoon Table 1, which is a "
                "stronger row than ours and an INDEPENDENT re-run of the same Raha+Baran "
                "baseline, so omitting it would repeat that defect."
            ),
        },
        "sources": [BCLEAN, COCOON],
        "rows": [
            # --- BClean Table 4 -------------------------------------------------------
            _citation_row(
                method="BClean",
                dataset="hospital",
                precision=0.998,
                recall=0.956,
                f1=0.976,
                note=(
                    "Citation-only; the source system of Table 4. Above this repository on "
                    "hospital. Note the paper's own abstract headlines 'F-measure of up to "
                    "0.9'; 0.976 is this single table row, so quoting it is the reading "
                    "least favourable to us and most favourable to BClean."
                ),
            ),
            _citation_row(
                method="BClean (PI/PIP)",
                dataset="hospital",
                precision=1.000,
                recall=0.960,
                f1=0.980,
                note="Citation-only; best hospital F1 in Table 4.",
            ),
            _citation_row(
                method="PClean",
                dataset="hospital",
                precision=1.000,
                recall=0.927,
                f1=0.962,
            ),
            _citation_row(
                method="GARF",
                dataset="hospital",
                precision=1.000,
                recall=0.556,
                f1=0.715,
            ),
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
                note=(
                    "Citation-only. NOT a figure Raha or Baran published: BClean configured "
                    "and ran them. Cocoon re-runs the same baseline independently at 0.72."
                ),
            ),
            _citation_row(
                method="Raha+Baran",
                dataset="flights",
                precision=0.829,
                recall=0.650,
                f1=0.729,
            ),
            # --- Cocoon Table 1 -------------------------------------------------------
            # Added 2026-09-06. Every number in Cocoon Table 1 was measured with oracle
            # assistance the paper states plainly: its own detection/cleaning steps "use the
            # LLM provided ground truth", HoloClean was given ground-truth denial
            # constraints, and Baran was given ground-truth feedback on 20 clean cells. That
            # is a premise supplied from outside the table, which is exactly the axis our
            # 0.7926 differs on -- see docs/trust/baseline-protocol-comparability.md.
            _citation_row(
                method="Cocoon",
                dataset="hospital",
                precision=0.870,
                recall=0.930,
                f1=0.900,
                source=COCOON,
                note=(
                    "Citation-only; LLM-based. Above this repository on hospital. Measured "
                    "with ground truth supplied to the cleaning step."
                ),
            ),
            _citation_row(
                method="Cocoon",
                dataset="flights",
                precision=0.910,
                recall=0.420,
                f1=0.570,
                source=COCOON,
                note=(
                    "Citation-only. The paper attributes the low recall to benchmark "
                    "ambiguity in Flight Number -> Actual Arrival Time and argues it is "
                    "'preferable to preserve these to represent the uncertainty' -- an "
                    "independent SOTA system concluding that abstention is correct here."
                ),
            ),
            _citation_row(
                method="Raha+Baran (Cocoon re-run)",
                dataset="hospital",
                precision=0.910,
                recall=0.600,
                f1=0.720,
                source=COCOON,
                note=(
                    "Citation-only. Independent re-run of the same baseline BClean reports "
                    "at 0.730, reproducing it within 0.01 under a different protocol."
                ),
            ),
            _citation_row(
                method="Raha+Baran (Cocoon re-run)",
                dataset="flights",
                precision=0.840,
                recall=0.610,
                f1=0.700,
                source=COCOON,
                note=(
                    "Citation-only. Independent re-run of the baseline BClean reports at "
                    "0.729 on flights."
                ),
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
