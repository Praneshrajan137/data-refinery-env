"""Determinability labels for the 88 real errors in RT-bench and ST-bench.

The first correction-side labels this project has for **wild** columns. They answer a prior
question rather than providing a corrector target:

> Given a real error found in a column of a table in the wild, and the rest of that column, is
> the correct value determinable at all?

Measured: **52 of 88 correctable, 35 not determinable, 1 ambiguous**. See
`docs/trust/wild-correction-determinability.md` and
`eval/preregistration/wild_correction_determinability.md`.

**These are not clean values, and this module deliberately provides none.** A label says whether
a correction is recoverable, not what it is. Storing the corrections would make this a repair
benchmark and would also require storing the erroneous values, which the licence forbids -- see
below.

**No corpus bytes live here.** Upstream publishes no licence (`registry.py`, `license_spdx=None`),
so labels are keyed on ``corpus:column_index:sha256(value)[:16]`` and the join happens at load
time against a freshly fetched, hash-verified corpus. The column index is part of the key
because the same string is a labelled error in several different columns, and a sentinel that is
undeterminable in a date column may be correctable elsewhere.

One annotator, so there is **no inter-annotator agreement and no bound on label noise**. Nothing
here may certify anything: `dataforge/conformal.py` needs `beta` bounded by planted controls and
independent judgements, and this has neither.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

__all__ = [
    "WildCorrectionError",
    "DeterminabilityLabel",
    "WildCorrectionLabel",
    "LABELS_PATH",
    "load_wild_correction_labels",
    "determinability_counts",
    "label_key",
    "lookup_label",
]

DeterminabilityLabel = Literal["correctable", "not_determinable", "ambiguous"]

LABELS_PATH = Path(__file__).resolve().parent / "wild_correction_labels.json"

#: Rules that make a value correctable, and rules that make it not. Mirrors the taxonomy in
#: the pre-registration; a rule outside these sets is a schema violation, not a new category.
_CORRECTABLE_RULES = frozenset({"R1", "R2", "R3", "R4"})
_NOT_DETERMINABLE_RULES = frozenset({"N1", "N2", "N3"})


class WildCorrectionError(RuntimeError):
    """Raised when the label file is absent, malformed, or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class WildCorrectionLabel:
    """One determinability judgement.

    ``note`` describes the value abstractly and never quotes it, so this object can be logged
    or published without redistributing corpus content.
    """

    corpus: str
    column_index: int
    value_sha256_prefix: str
    label: DeterminabilityLabel
    rule: str
    note: str

    @property
    def is_correctable(self) -> bool:
        """Whether a unique replacement was judged determinable."""
        return self.label == "correctable"


def label_key(corpus: str, column_index: int, value: str) -> str:
    """Return the label-file key for one value. Never returns the value itself."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"{corpus}:{column_index}:{digest}"


@lru_cache(maxsize=1)
def load_wild_correction_labels() -> dict[str, WildCorrectionLabel]:
    """Load and validate the label file.

    Returns:
        Mapping from :func:`label_key` output to its label.

    Raises:
        WildCorrectionError: If the file is absent or malformed, if a label/rule pair is
            inconsistent, or if the file claims to contain corpus values. Validation is strict
            because a silently mislabelled entry would propagate into a published proportion.
    """
    if not LABELS_PATH.exists():
        raise WildCorrectionError(f"label file {LABELS_PATH.name} is absent")
    try:
        payload = json.loads(LABELS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WildCorrectionError(f"label file unreadable: {type(exc).__name__}") from exc

    if payload.get("contains_corpus_values") is not False:
        raise WildCorrectionError(
            "label file does not assert contains_corpus_values=false; refusing to load a file "
            "that may be redistributing unlicensed corpus content"
        )
    raw = payload.get("labels")
    if not isinstance(raw, dict) or not raw:
        raise WildCorrectionError("label file carries no labels")

    labels: dict[str, WildCorrectionLabel] = {}
    for key, entry in raw.items():
        parts = key.split(":")
        if len(parts) != 3:
            raise WildCorrectionError(f"malformed key {key!r}")
        corpus, index_text, digest = parts
        label = entry.get("label")
        rule = str(entry.get("rule", ""))
        if label == "correctable":
            if rule not in _CORRECTABLE_RULES:
                raise WildCorrectionError(f"{key}: correctable with non-correctable rule {rule!r}")
        elif label == "not_determinable":
            if rule not in _NOT_DETERMINABLE_RULES:
                raise WildCorrectionError(f"{key}: not_determinable with rule {rule!r}")
        elif label != "ambiguous":
            raise WildCorrectionError(f"{key}: unknown label {label!r}")
        labels[key] = WildCorrectionLabel(
            corpus=corpus,
            column_index=int(index_text),
            value_sha256_prefix=digest,
            label=label,
            rule=rule,
            note=str(entry.get("note", "")),
        )
    return labels


def determinability_counts() -> dict[str, int]:
    """Return the count of each label, over the whole census."""
    counts: dict[str, int] = {}
    for entry in load_wild_correction_labels().values():
        counts[entry.label] = counts.get(entry.label, 0) + 1
    return counts


def lookup_label(corpus: str, column_index: int, value: str) -> WildCorrectionLabel | None:
    """Return the label for one corpus value, or None if it is not a labelled error.

    Args:
        corpus: ``"rt_bench"`` or ``"st_bench"``.
        column_index: The column's index within that corpus.
        value: The value, used only to compute its digest.

    Returns:
        The label, or None. None means "not one of the 88 labelled errors", **not** "clean".
    """
    return load_wild_correction_labels().get(label_key(corpus, column_index, value))
