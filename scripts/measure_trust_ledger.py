"""Measure the Trust Ledger over the adversarial corpus, under two schema premises.

This produces the committed artifact behind the project's outcome claims. It exists because
every prior number described a detector's agreement with a label set; none described what a
run does to a user's data.

The two-premise design is the finding. Both schemas cover every column, so the gate reports
every write as ``proven`` under both. The difference in measured corruption is attributable
entirely to how much the premise actually constrains -- turning
``docs/trust/authority-is-mutable.md``'s prose ("covering a column is not the same as
constraining the value") into a number.

Usage:
    python scripts/measure_trust_ledger.py            # print
    python scripts/measure_trust_ledger.py --write    # commit the artifact
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.detectors.base import Schema  # noqa: E402
from dataforge.engine.repair import VerifyAndApplyRequest, verify_and_apply  # noqa: E402
from dataforge.metrics import TrustLedger  # noqa: E402
from tests.adversarial.corpus import (  # noqa: E402
    PERMISSIVE_SCHEMA,
    TIGHT_SCHEMA,
    build_corpus,
    truth,
    write_table,
)

ARTIFACT = PROJECT_ROOT / "eval" / "results" / "trust_ledger_adversarial.json"


def _poke(source: Path, row: int, column: str, value: str) -> None:
    lines = source.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    index = header.index(column)
    cells = lines[row + 1].split(",")
    cells[index] = value
    lines[row + 1] = ",".join(cells)
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _truth_or_none(row: int, column: str) -> str | None:
    try:
        return truth(row, column)
    except (IndexError, ValueError):
        return None


def measure(schema: Schema, label: str, *, only: str = "all") -> tuple[TrustLedger, dict[str, Any]]:
    """Run attacks individually and build a ledger from the outcomes.

    ``only`` selects the subset, and the subsets are NOT interchangeable:

    * ``discriminable`` -- attacks the stated constraints can reject. This is the gate's
      responsibility, and the only subset where a corruption is a defect.
    * ``undecidable`` -- attacks that satisfy every declared constraint and are merely
      false. These are written by design; the guarantee covering them is reversibility.
    * ``all`` -- both. Reported for completeness, but a corruption RATE over ``all`` is
      close to meaningless here: 16 of 17 proposals are hostile, so the rate measures the
      corpus's malice rather than any property of a workload. It is recorded with that
      warning rather than omitted, because omitting it would invite someone to recompute it
      later without the warning.
    """
    corpus = [
        attack
        for attack in build_corpus()
        if only == "all"
        or (only == "discriminable" and attack.discriminable)
        or (only == "undecidable" and not attack.discriminable)
    ]
    applied = 0
    corrections = 0
    corruptions = 0
    held = 0
    corrupting: list[str] = []
    reversible = True

    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / "readings.csv"
        for attack in corpus:
            write_table(source)
            if attack.pre_corrupt is not None:
                row, column, wrong = attack.pre_corrupt
                _poke(source, row, column, wrong)

            result = verify_and_apply(
                VerifyAndApplyRequest(
                    source_path=source,
                    fixes=[attack.fix],
                    mode="apply",
                    schema=schema,
                    confirm_escalations=True,
                    proposer="adversarial-corpus",
                )
            )
            receipt = result.receipt
            if receipt.applied_fixes:
                applied += len(receipt.applied_fixes)
                if not receipt.reversible:
                    reversible = False
                for fix in receipt.applied_fixes:
                    expected = _truth_or_none(fix.row, fix.column)
                    if expected is None or fix.new_value != expected:
                        corruptions += 1
                        corrupting.append(attack.name)
                    else:
                        corrections += 1
            else:
                held += 1

    # Real errors present: one, introduced by the single legitimate attack's pre_corrupt.
    real_errors = sum(1 for attack in corpus if attack.pre_corrupt is not None)

    ledger = TrustLedger(
        cells_applied=applied,
        corrections=corrections,
        corruptions=corruptions,
        cells_held=held,
        cells_abstained=0,
        real_errors=real_errors,
        reversibility_verified=reversible,
    )
    detail = {
        "premise": label,
        "subset": only,
        "attacks": len(corpus),
        "corrupting_attacks": sorted(set(corrupting)),
    }
    return ledger, detail


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="commit the artifact")
    args = parser.parse_args()

    premises = {
        "tight": (TIGHT_SCHEMA, "tight (typed, bounded, patterned, enumerated)"),
        "permissive": (PERMISSIVE_SCHEMA, "permissive (every column declared str)"),
    }
    subsets = ("discriminable", "undecidable", "all")

    results: dict[str, dict[str, Any]] = {}
    for premise_name, (schema, label) in premises.items():
        results[premise_name] = {}
        print(f"\n=== {premise_name} premise: {label} ===")
        for subset in subsets:
            ledger, detail = measure(schema, label, only=subset)
            results[premise_name][subset] = {**detail, **ledger.as_dict()}
            print(f"\n  [{subset}] n={detail['attacks']}")
            for line in ledger.summary_lines():
                print(f"    {line}")
            if detail["corrupting_attacks"]:
                print(f"    written despite being wrong: {detail['corrupting_attacks']}")

    # The headline is generated rather than written, so it cannot drift from the numbers. It now
    # reports BOTH corruptions and writes, because after 2026-08-25 the corruption counts alone no
    # longer distinguish the premises -- both are zero -- and a sentence quoting only corruptions
    # would read as "the premise does not matter", which is the opposite of the finding.
    headline = (
        "Under a tight premise, "
        f"{results['tight']['discriminable']['corruptions']} of "
        f"{results['tight']['discriminable']['attacks']} constraint-violating attacks were "
        f"written, and {results['tight']['discriminable']['cells_applied']} cell(s) applied in "
        "total. Under a premise that declares every column str and therefore constrains nothing, "
        f"{results['permissive']['discriminable']['corruptions']} of "
        f"{results['permissive']['discriminable']['attacks']} were written, and "
        f"{results['permissive']['discriminable']['cells_applied']} cell(s) applied in total. The "
        "gate is identical in both runs; only the premise differs. Until 2026-08-25 the permissive "
        "premise wrote 10 of 14, because declaring a column str counted as authority over it; "
        "requiring a premise to discriminate before it confers proof "
        "(dataforge.domain.vocabulary.type_discriminates) took that to zero while leaving the "
        "tight premise's one legitimate repair intact. The premise still decides the outcome -- it "
        "now decides whether anything may be written at all, rather than whether corruption occurs."
    )

    payload = {
        "measurement": "trust_ledger_adversarial",
        "headline": headline,
        "note": (
            "Trust Ledger over the adversarial corpus, under two schema premises and split "
            "by subset. 'discriminable' attacks violate a declared constraint and are the "
            "gate's responsibility. 'undecidable' attacks satisfy every declared constraint "
            "and are merely false; no verifier can reject them without ground truth, so "
            "they are written by design and the guarantee covering them is reversibility, "
            "not correctness."
        ),
        "rate_warning": (
            "Do NOT read the 'all' subset's corruption rate as a workload metric. This is "
            "an attack corpus: 16 of 17 proposals are hostile by construction, so that rate "
            "measures the corpus, not any real distribution of proposals."
        ),
        "reproduce": "python scripts/measure_trust_ledger.py",
        "premises": results,
    }

    print(f"\nHEADLINE: {headline}")

    if args.write:
        ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
        ARTIFACT.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
        )
        print(f"\nwrote {ARTIFACT.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
