"""Build the wild-column correction-label file from the maintainer's labelling decisions.

The decisions live here, in source, so the label file is reproducible and every judgement is
auditable next to its rule. Run once; the output is committed.

**No corpus value is ever written to the output.** Upstream publishes no licence
(`dataforge/datasets/registry.py`, `license_spdx=None`), so the bytes may live only in a
fetched, hash-verified cache and a label file containing them would be vendoring. Labels are
keyed on `corpus:column_index:sha256(value)[:16]`, and the `note` field describes values
abstractly rather than quoting them.

Keying on column index as well as value hash is necessary, not belt-and-braces: `'-'`, `'0'`
and two typo values each appear as a labelled error in several different columns, and the same
string can be correctable in one column and not in another.

Enumeration order is fixed: corpora in the order below, columns in corpus order, and within a
column ``sorted(ground_truth)``. The decision table is indexed against that order and the
script refuses to write if the count does not match.

Rules are defined in `eval/preregistration/wild_correction_determinability.md`:

* ``R1`` format -- right information, wrong form, column's dominant form unambiguous
* ``R2`` cruft -- correct value present plus extraneous content, removal unambiguous
* ``R3`` unique typo neighbour -- one small edit from exactly one in-domain value
* ``R4`` placeholder -- sentinel in a column whose absence convention is visible
* ``N1`` absent fact -- the correct value is a different fact, not in the cell
* ``N2`` multiple candidates -- two or more corrections equally plausible
* ``N3`` external authority -- needs a lookup table or real-world fact

Usage::

    python scripts/data/build_wild_correction_labels.py
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataforge.datasets.column_corpus import load_column_benchmark  # noqa: E402

_OUT = ROOT / "dataforge" / "datasets" / "wild_correction_labels.json"
_CORPORA = ("rt_bench", "st_bench")

#: (label, rule, note). Indexed by position in the fixed enumeration, 1-based.
#: Notes must not quote a corpus value.
_RT: dict[int, tuple[str, str, str]] = {
    1: ("correctable", "R3", "misspelt industry-category term; one obvious in-domain spelling"),
    2: ("correctable", "R3", "misspelt continent name; the missing continent is unique"),
    3: ("correctable", "R3", "single omitted letter in a street name within a street address"),
    4: (
        "not_determinable",
        "N2",
        "spreadsheet broken-reference error token in a graded column; any of the six grades "
        "is equally plausible",
    ),
    5: ("correctable", "R3", "misspelt job-title word; one obvious spelling"),
    6: ("correctable", "R3", "transposed letters in an Irish county name; unique nearest county"),
    7: (
        "not_determinable",
        "N1",
        "a business-function word appears in a city column; the actual city is absent",
    ),
    8: (
        "not_determinable",
        "N1",
        "a province designation from a different country's scheme; the intended province is "
        "not recoverable",
    ),
    9: ("not_determinable", "N1", "negative sentinel in an alphanumeric flight-number column"),
    10: (
        "correctable",
        "R2",
        "an encyclopaedia maintenance banner is spliced into a currency amount; the amount is "
        "present and extractable",
    ),
    11: ("not_determinable", "N1", "placeholder prose in a fiscal-year column"),
    12: ("correctable", "R3", "transposed letters in a manufacturer name"),
    13: ("correctable", "R3", "misspelt vehicle model; one nearest real model name"),
    14: ("correctable", "R3", "colour name missing its final letter"),
    15: ("not_determinable", "N1", "single character in a city column; the city is absent"),
    16: ("correctable", "R3", "misspelt Canadian region name"),
    17: ("correctable", "R3", "misspelt month name"),
    18: (
        "correctable",
        "R1",
        "space-separated variant of a business-type value that also appears unspaced in the "
        "same column",
    ),
    19: ("correctable", "R3", "same misspelt job title as an earlier item, different column"),
    20: ("correctable", "R3", "misspelt surname in a surname-then-forename display column"),
    21: (
        "correctable",
        "R1",
        "three-letter country code in a column of full country names; expansion is unique "
        "within the column",
    ),
    22: (
        "not_determinable",
        "N3",
        "a bare numeric code in a survey column of labelled income bands; needs the survey "
        "codebook",
    ),
    23: (
        "not_determinable",
        "N1",
        "question-mark sentinel in a country column with no visible empty convention; the "
        "country is absent",
    ),
    24: (
        "correctable",
        "R1",
        "two-letter state code in a column of mostly full state names; expansion is unique",
    ),
    25: ("not_determinable", "N1", "dash sentinel in a datetime column"),
    26: ("correctable", "R3", "misspelt country name; unique nearest country"),
    27: (
        "not_determinable",
        "N1",
        "the unit word alone in a column of N-months duration strings; the count is absent",
    ),
    28: (
        "correctable",
        "R3",
        "transposed letters in a city name; the nearest real city in that state is unique and "
        "much closer than any value present in the column",
    ),
    29: (
        "correctable",
        "R3",
        "transposed letters in a forename that also appears correctly spelt in the same column",
    ),
    30: ("not_determinable", "N1", "zero sentinel in a column of month abbreviations"),
    31: ("correctable", "R3", "misspelt city inside a full postal address"),
    32: ("correctable", "R3", "misspelt street-type term"),
    33: ("correctable", "R3", "misspelt beverage flavour term"),
    34: ("correctable", "R3", "misspelt US state name"),
    35: (
        "correctable",
        "R3",
        "misspelt category word in a three-valued good/bad/average column",
    ),
    36: (
        "not_determinable",
        "N1",
        "encoding corruption has replaced the characters of a surname; the original bytes are "
        "not recoverable",
    ),
    37: ("correctable", "R3", "misspelt Canadian province name"),
    38: ("not_determinable", "N1", "dash sentinel in a player-name column"),
    39: ("correctable", "R3", "misspelt word in an account-description column"),
    40: (
        "not_determinable",
        "N1",
        "far-future sentinel date in an end-date column; the real end date is absent",
    ),
    41: ("correctable", "R3", "misspelt adjective in a product-name column"),
}

_ST: dict[int, tuple[str, str, str]] = {
    1: ("correctable", "R3", "misspelt component adjective"),
    2: ("not_determinable", "N1", "dash sentinel in a scientific-notation numeric column"),
    3: ("correctable", "R3", "country name missing its final letter"),
    4: ("not_determinable", "N1", "double-dash sentinel in a short-code column"),
    5: ("not_determinable", "N1", "dash sentinel in a gene-name column"),
    6: ("not_determinable", "N1", "double-dash sentinel in a short-code column"),
    7: ("correctable", "R3", "misspelt language name"),
    8: ("correctable", "R3", "transposed and doubled letters in a surname"),
    9: ("correctable", "R3", "transposed letters in a payroll-category word"),
    10: (
        "not_determinable",
        "N1",
        "letters where the column's fixed code pattern requires digits; the code is absent",
    ),
    11: ("correctable", "R3", "wrong adverbial form of an adjective in definition prose"),
    12: ("not_determinable", "N1", "zero sentinel in a vessel-name column"),
    13: ("not_determinable", "N1", "zero sentinel in a purchase-order-reference column"),
    14: ("correctable", "R3", "misspelt job-title noun"),
    15: ("correctable", "R3", "misspelt medical term in a cause-of-death description"),
    16: ("correctable", "R3", "misspelt funding-category noun"),
    17: ("not_determinable", "N1", "dash sentinel in a scientific-notation numeric column"),
    18: ("not_determinable", "N1", "zero sentinel in a short-code column"),
    19: (
        "correctable",
        "R3",
        "misspelt status term in a three-valued endorsement column",
    ),
    20: (
        "not_determinable",
        "N1",
        "an employment-status word appears in a country column; the country is absent",
    ),
    21: (
        "not_determinable",
        "N1",
        "an internal channel label appears in a geographic-region column; the region is absent",
    ),
    22: ("correctable", "R3", "transposed letters in a forename"),
    23: (
        "ambiguous",
        "R1/N2",
        "a bare number in a column of prefixed box references. R1 would add the prefix; N2 "
        "applies because the magnitude is far outside the column's range, so the number may be "
        "a different field's value rather than a malformed one. Both rules apply, so by the "
        "pre-registered taxonomy this resolves to ambiguous rather than to a guess.",
    ),
    24: ("not_determinable", "N1", "question-mark sentinel in a numeric column"),
    25: ("correctable", "R3", "misspelt profession adjective"),
    26: (
        "not_determinable",
        "N1",
        "explanatory prose in an inspection-date column; no date may even exist",
    ),
    27: ("not_determinable", "N1", "not-applicable sentinel in a variant-identifier column"),
    28: ("correctable", "R3", "misspelt noun in a budget-line description"),
    29: ("correctable", "R3", "misspelt noun in a control-description sentence"),
    30: (
        "correctable",
        "R1",
        "a stray punctuation character inside a time range that is otherwise well formed",
    ),
    31: ("correctable", "R3", "transposed letters in a care-facility name"),
    32: ("correctable", "R3", "misspelt word in a column-instruction label"),
    33: ("correctable", "R3", "transposed letters in an Indian state name inside a company name"),
    34: ("correctable", "R3", "final letter wrong in an Irish place name"),
    35: (
        "not_determinable",
        "N1",
        "a country name appears in a US-state-code column; the state is absent",
    ),
    36: (
        "not_determinable",
        "N1",
        "a plain six-digit number where the column uses a mixed letter-digit district code",
    ),
    37: (
        "not_determinable",
        "N1",
        "the account handle is masked out of an otherwise well-formed status URL; the handle is "
        "not in the cell",
    ),
    38: ("correctable", "R3", "misspelt country name"),
    39: ("correctable", "R3", "misspelt dashboard term"),
    40: ("not_determinable", "N1", "zero sentinel in a town-name column"),
    41: (
        "not_determinable",
        "N1",
        "a non-English not-stated placeholder in an email column",
    ),
    42: ("not_determinable", "N1", "single-character placeholder in an email column"),
    43: (
        "not_determinable",
        "N2",
        "two URLs concatenated with the second truncated; which one belongs in the cell is not "
        "determined, so cruft removal is not unambiguous",
    ),
    44: ("correctable", "R3", "misspelt education-stage noun"),
    45: ("correctable", "R3", "misspelt county name"),
    46: (
        "correctable",
        "R3",
        "misspelt hygiene term that also appears correctly spelt elsewhere in the column",
    ),
    47: ("correctable", "R3", "same transposed facility name as an earlier item, other column"),
}

_DECISIONS = {"rt_bench": _RT, "st_bench": _ST}


def main() -> int:
    """Write the label file."""
    labels: dict[str, dict[str, str]] = {}
    counts: dict[str, int] = {}
    for corpus in _CORPORA:
        benchmark = load_column_benchmark(corpus)
        decisions = _DECISIONS[corpus]
        index = 0
        for column in benchmark.columns:
            if not column.ground_truth:
                continue
            for value in sorted(column.ground_truth):
                index += 1
                if index not in decisions:
                    print(f"ERROR: {corpus} item {index} has no decision; refusing to write")
                    return 1
                label, rule, note = decisions[index]
                digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
                key = f"{corpus}:{column.index}:{digest}"
                if key in labels:
                    print(f"ERROR: duplicate key {key}; refusing to write")
                    return 1
                labels[key] = {"label": label, "rule": rule, "note": note}
                counts[label] = counts.get(label, 0) + 1
        if index != len(decisions):
            print(
                f"ERROR: {corpus} enumerated {index} values, decisions table has {len(decisions)}"
            )
            return 1
        print(f"{corpus}: {index} labelled values")

    payload = {
        "schema_version": "dataforge_wild_correction_labels_v1",
        "pre_registration": "eval/preregistration/wild_correction_determinability.md",
        "annotators": 1,
        "key_format": "corpus:column_index:sha256(value)[:16]",
        "contains_corpus_values": False,
        "counts": counts,
        "labels": labels,
    }
    _OUT.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    total = sum(counts.values())
    print(f"\ntotals over {total} values: {counts}")
    for label, count in sorted(counts.items()):
        print(f"  {label:<20} {count:>3}  {count / total:.4f}")
    print(f"wrote {_OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
