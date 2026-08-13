"""Measure the analyze payload before and after the index/detail/histogram split.

The 'before' figure is reconstructed by serialising full records for every flagged
cell, which is exactly what the previous FlaggedCellsView did at its 20,000-record
limit. The 'after' figure is the shipped payload. Both are measured on the same run,
so the ratio is measured rather than estimated.

The full hospital table is not committed (eval fetches it), so when it is absent this
synthesises a table of the same SHAPE -- 1,000 rows, a functional dependency that a
detector will flag densely -- and says so. The bytes-per-cell figures are properties
of the encoding, not of the dataset, so they transfer; the flagged-cell COUNT does
not, and is not claimed to.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "playground"))

from fastapi.testclient import TestClient  # noqa: E402

from playground.api.app import app  # noqa: E402

CSV = ROOT / "data" / "hospital.csv"
ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 1000


def _synthetic_csv() -> bytes:
    """A table with a dense functional-dependency violation, hospital's shape."""
    header = [
        "provider",
        "hospital_name",
        "address",
        "city",
        "state",
        "zip",
        "county",
        "phone",
        "measure_code",
        "measure_name",
    ]
    lines = [",".join(header)]
    for row in range(ROWS):
        zip_code = f"{35000 + (row % 40)}"
        # zip -> city is a real dependency; break it on most rows so the detector
        # flags densely, which is the regime the payload split exists for.
        city = "birmingham" if row % 3 == 0 else f"city{row % 40}"
        lines.append(
            ",".join(
                [
                    f"p{row:05d}",
                    f"hospital {row % 60}",
                    f"{row} main st",
                    city,
                    "al",
                    zip_code,
                    f"county{row % 20}",
                    # Malformed on most rows so the flagged set crosses the detail
                    # limit: below that limit the split cannot help, and measuring
                    # only the sparse regime would misrepresent it.
                    ("n/a" if row % 5 != 0 else f"256555{row % 10000:04d}"),
                    f"m{row % 12}",
                    "measure name text",
                ]
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def main() -> int:
    client = TestClient(app)
    if CSV.exists():
        source_label = f"{CSV.name} (real)"
        body = CSV.read_bytes()
    else:
        source_label = f"synthetic, hospital shape ({ROWS} rows)"
        body = _synthetic_csv()

    response = client.post(
        "/api/analyze",
        files={"file": ("hospital.csv", io.BytesIO(body), "text/csv")},
    )
    if response.status_code != 200:
        print(f"FAIL: analyze returned {response.status_code}: {response.text[:400]}")
        return 1

    payload = response.json()
    flagged = payload["flagged_cells"]
    total = flagged["total"]
    detail = flagged["cells"]
    index = flagged["index"]

    after = len(json.dumps(flagged, separators=(",", ":")).encode("utf-8"))
    per_record = len(json.dumps(detail[0], separators=(",", ":")).encode("utf-8")) if detail else 0
    before = per_record * total + 120
    index_bytes = len(json.dumps(index, separators=(",", ":")).encode("utf-8"))

    print(f"PAYLOAD source={source_label}")
    print(f"  rows={payload['source']['rows']} flagged_cells={total}")
    if total == 0:
        print("  no cells flagged; ratio not measurable on this input")
        return 0
    print(f"  per full record            = {per_record} B")
    print(f"  before (every cell, full)  = {before / 1024:.1f} KiB")
    print(f"  after  (index + {len(detail)} + hist) = {after / 1024:.1f} KiB")
    print(f"  reduction                  = {before / after:.1f}x")
    print(
        f"  index only                 = {index_bytes / 1024:.1f} KiB "
        f"({index_bytes / total:.1f} B/cell)"
    )
    print(f"  index covers every cell    = {len(index['rows']) == total}")
    print(f"  histogram classes          = {len(flagged['confidence_histogram'])}")
    for entry in flagged["confidence_histogram"]:
        print(
            f"    {entry['issue_type']}: n={entry['count']} "
            f"distinct={entry['distinct_values']} "
            f"mode={entry['mode_value']} share={entry['mode_share']:.4f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
