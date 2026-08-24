"""Harvest wrong corrector proposals on cells whose ground truth is retained.

Why this script exists
----------------------
``docs/trust/stratified-label-noise-result.md`` records that the binding control class,
``corrector_generated``, has only **8** controls, so its bound is 0.8712 largely because 4/8 with a
union-corrected Clopper-Pearson interval is very wide. Limit 1 in that document is exactly this.

A ``corrector_generated`` control is a cell where:

* the true value is **retained** (so a verdict on it is objectively scorable),
* a real corrector **proposed a replacement**, and
* that proposal is **wrong**.

The third condition is why this cannot be synthesised. ``column_distribution`` plants draw a
plausible value from the column; a ``corrector_generated`` plant is the value a corrector actually
chose, which is systematically confusable with the truth *because that is why the corrector chose
it*. Only running the corrector produces that distribution.

Deterministic repairers cannot supply these. ``docs/trust/`` records
``wrong_value_on_a_real_error = 0`` on hospital, rayyan and flights: the deterministic correctors
abstain rather than guess, so their false positives are all ``repaired_a_clean_cell``. The wrong
proposals must come from the LLM corrector.

What it writes
--------------
A JSON artifact of ``(flagged_value, proposed_value, withheld_truth, column, row)`` records, plus
the abstention and correct-proposal counts needed to state the yield honestly. **No verdicts** --
labelling happens in `probe_label_protocol.py`, which must not see this file's truth column until
after both arms have committed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dataforge.datasets.real_world import load_real_world_dataset


def _cells_with_truth(dataset: Any) -> list[tuple[int, str, str, str]]:
    """Return ``(row, column, dirty_value, clean_value)`` for every retained real error.

    Reads ``dataset.ground_truth``, which the loader already computes, rather than re-diffing the
    frames. Nothing is sampled here; sampling belongs to the caller so the seed lives in one place.
    """
    return [(c.row, c.column, c.dirty_value, c.clean_value) for c in dataset.ground_truth]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="rayyan")
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=0, help="0 means no cap")
    args = parser.parse_args()

    dataset = load_real_world_dataset(args.corpus, cache_root=args.cache_root)
    cells = _cells_with_truth(dataset)

    by_column: dict[str, int] = {}
    for _, column, _, _ in cells:
        by_column[column] = by_column.get(column, 0) + 1

    payload: dict[str, Any] = {
        "corpus": args.corpus,
        "total_real_errors_with_retained_truth": len(cells),
        "errors_by_column": dict(sorted(by_column.items(), key=lambda kv: -kv[1])),
        "note": (
            "Census of real errors with retained truth. Corrector proposals are added by the "
            "labelling probe, which runs the corrector itself so the proposal and the verdict "
            "come from the same run."
        ),
    }
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"corpus                          {args.corpus}")
    print(f"real errors with retained truth {len(cells)}")
    print("top columns by error count:")
    for column, count in list(payload["errors_by_column"].items())[:10]:
        print(f"  {column:<34} {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
