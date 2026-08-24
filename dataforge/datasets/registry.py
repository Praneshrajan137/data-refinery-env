"""Canonical metadata for real-world benchmark datasets."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class HeaderMismatch(BaseModel):
    """Pair of dirty/clean header names that align by column position."""

    dirty_name: str = Field(min_length=1)
    clean_name: str = Field(min_length=1)

    model_config = {"frozen": True}


class DatasetMetadata(BaseModel):
    """Metadata describing a canonical benchmark dataset.

    ``error_provenance`` and ``tier`` exist because "benchmark dataset" was doing too
    much work. Two of these corpora have errors that were *injected*, and one of those
    was the declared flagship and hard regression anchor:

    * ``natural`` -- errors occurred in the wild and were labelled after the fact.
    * ``injected`` -- errors were programmatically written into an otherwise clean real
      table. ``hospital``'s are a single substituted character (``birminghxm`` ->
      ``birmingham``), which HoloDetect's own authors describe in print as typos made
      "by swapping a character with the character 'x'".
    * ``synthetic`` -- the table itself is generated, not merely the errors.
    * ``contested`` -- errors are natural but the ground truth encodes an arbitrary
      convention, so a system that declines to invent one truth is scored as wrong.

    ``tier`` governs what a number may be *used for*, and is enforced rather than
    documented: only a ``headline`` corpus may source a headline claim in ``README.md``
    or ``PRODUCT.md``. See ``scripts/ci/readme_truth.py``.

    * ``headline`` -- may source a published claim.
    * ``tripwire`` -- regression detection only. A fixed, useful role; not evidence of
      capability.
    * ``diagnostic`` -- investigation only, never a claim.
    """

    name: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    n_rows: int = Field(ge=0)
    n_columns: int = Field(ge=1)
    error_types: tuple[str, ...] = Field(default_factory=tuple)
    error_provenance: Literal["natural", "injected", "synthetic", "contested"]
    tier: Literal["headline", "tripwire", "diagnostic"]
    source_urls: tuple[str, str]
    source_revision: str = Field(min_length=7)
    dirty_sha256: str = Field(min_length=64, max_length=64)
    clean_sha256: str = Field(min_length=64, max_length=64)
    citation: str = Field(min_length=1)
    header_mismatches: tuple[HeaderMismatch, ...] = Field(default_factory=tuple)
    tier_reason: str = Field(min_length=1)
    # A dirty/clean pair ships full tables, so value multiplicities are present and
    # frequency-dependent detectors (outlier, decimal_shift, categorical_normalization) can
    # be honestly scored. Contrast ColumnBenchmarkMetadata, which cannot.
    frequencies_available: Literal[True] = True

    model_config = {"frozen": True}


RAHA_GIT_REVISION = "7be1334b8c7bbdac3f47ef514fb3e1e8c5fc181c"
_BASE_URL = f"https://raw.githubusercontent.com/BigDaMa/raha/{RAHA_GIT_REVISION}/datasets"

DATASET_REGISTRY: dict[str, DatasetMetadata] = {
    "hospital": DatasetMetadata(
        name="hospital",
        domain="healthcare",
        n_rows=1000,
        n_columns=20,
        error_types=("typo", "missing_value", "formatting"),
        error_provenance="injected",
        tier="tripwire",
        tier_reason=(
            "Errors are injected, and the entire error model is one substituted character: "
            "509 of 509 corrupted cells contain an 'x'. HoloClean's and HoloDetect's own "
            "authors call it an easy benchmark in print, and the field has moved 0.83 -> "
            "0.99 while this project's anchor sits at 0.7926. Measured on real errors, the "
            "same detector family runs at precision 0.025-0.037 versus 0.561 here "
            "(docs/trust/real-error-detection-result.md). Retained as a fixed regression "
            "tripwire, which is a real role; demoted from flagship, which it was not."
        ),
        source_urls=(
            f"{_BASE_URL}/hospital/dirty.csv",
            f"{_BASE_URL}/hospital/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="dbc5575b915fe8b5e0ac6dc6172f38ba91e611fdb76d09a8f4a81cb7ea9925ac",
        clean_sha256="ea3ee44998455c0b491750c348509de176c758a3bbf58e4530c0a136bb248b4b",
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Hospital) via the BigDaMa/raha repository."
        ),
    ),
    "flights": DatasetMetadata(
        name="flights",
        domain="aviation",
        n_rows=2376,
        n_columns=7,
        error_types=("missing_value", "formatting", "datetime"),
        error_provenance="contested",
        tier="diagnostic",
        tier_reason=(
            "Errors are natural, but the labels encode an arbitrary convention: the same "
            "flight's arrival time appears upstream as 10:30/10:31/10:28/10:39 and the "
            "ground truth picks one. Under two-way scoring a system that declines to "
            "invent a truth is indistinguishable from one that guesses wrong, which is how "
            "this corpus produces correction F1 0.0000. NOTE (corrected 2026-08-23): this "
            "CANNOT be resolved by re-scoring under specs/SPEC_abstention_scoring.md -- "
            "that rule requires a ground_truth_debatable label class and RAHA ships none. "
            "The only honest routes are relabelling flights with a debatable class, or "
            "measuring abstention on a corpus that already has one."
        ),
        source_urls=(
            f"{_BASE_URL}/flights/dirty.csv",
            f"{_BASE_URL}/flights/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="1b5c1afa10aa0e7c20fd7e14d05c56772715b2771aa0f5fa67ed1709e1eecd46",
        clean_sha256="0acfcfd8985b06fdd363965c9e8d9522c43e7589a93d79ae7dc311e1c37fdf3b",
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Flights) via the BigDaMa/raha repository."
        ),
    ),
    "rayyan": DatasetMetadata(
        name="rayyan",
        domain="bibliographic",
        n_rows=1000,
        n_columns=11,
        error_types=("typo", "missing_value", "formatting"),
        error_provenance="natural",
        tier="diagnostic",
        tier_reason=(
            "Errors are natural and owner-cleaned, which makes this the strongest of the "
            "four RAHA corpora in provenance terms. Diagnostic rather than headline only "
            "because it has never been measured for correction: detection recall floors "
            "exist in eval/thresholds/coverage_floors.json, but no committed correction "
            "baseline does. Promote on measurement, not on provenance."
        ),
        source_urls=(
            f"{_BASE_URL}/rayyan/dirty.csv",
            f"{_BASE_URL}/rayyan/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="7e25e6db262b0c72ca2d9735d5959599cf5a582e1c705459507c7b45d0d1d174",
        clean_sha256="23159f43c0706782388ed8957ad0c74eb7b88bc98f34d65bd49296e186d4673f",
        citation=(
            "Mahdavi et al. Raha benchmark dataset (Rayyan) via the BigDaMa/raha repository."
        ),
    ),
    "tax": DatasetMetadata(
        name="tax",
        domain="finance",
        n_rows=200000,
        n_columns=15,
        error_types=("typo", "formatting", "rule_violation"),
        error_provenance="synthetic",
        tier="diagnostic",
        tier_reason=(
            "Fully synthetic: a generated table on the Fan et al. (TODS 2008) schema with "
            "BART-injected errors. Measured only on a head(3000) slice of 200,000 sorted "
            "rows -- a biased slice, not a sample -- scoring F1 0.0000 with 696 false "
            "positives. No coverage floor was ever seeded for it, deliberately, rather "
            "than fabricating one."
        ),
        source_urls=(
            f"{_BASE_URL}/tax/dirty.csv",
            f"{_BASE_URL}/tax/clean.csv",
        ),
        source_revision=RAHA_GIT_REVISION,
        dirty_sha256="8dd3429ec4791b2ed1a688c308a57a9f3d1a94f77d1f4e98294a67273270b973",
        clean_sha256="201290927ae92e65b3940d776b3df5b4d953c5dfd9abb231715a2e65ecca87b0",
        citation=("Mahdavi et al. Raha benchmark dataset (Tax) via the BigDaMa/raha repository."),
    ),
}


def get_dataset_metadata(name: str) -> DatasetMetadata:
    """Return canonical metadata for a named benchmark dataset.

    Args:
        name: Canonical dataset name.

    Returns:
        The immutable metadata entry for the dataset.

    Raises:
        KeyError: If the dataset is not registered.
    """
    return DATASET_REGISTRY[name]


class ColumnBenchmarkMetadata(BaseModel):
    """Metadata for a column-level detection benchmark with a debatable label class.

    A separate type from :class:`DatasetMetadata` rather than an extension of it,
    because the two are not substitutable and pretending otherwise would invite a
    fabricated number:

    * A :class:`DatasetMetadata` corpus is a row-aligned dirty/clean **pair**, so it
      can score a *repair* (is the written value correct?).
    * A column benchmark ships **no clean values at all**. It can only score
      *detection* (was the right cell flagged?). ``axis`` is therefore a required
      field pinned to ``"detection"``, and there is deliberately nowhere to put a
      correction metric.

    ``license_spdx`` is ``None`` where the upstream repository publishes no licence.
    That is not a formality: with no licence there is no grant to redistribute, so
    the corpus must be fetched and hash-pinned at load time and must never be
    vendored into this repository or into a built artifact.

    ``tier`` is **required with no default**, matching :class:`DatasetMetadata`. It
    defaulted to ``"headline"`` until 2026-08-24, which is the denylist-fails-open
    mistake ``CONSTRAINT_CHECKABLE_DETECTORS`` exists to avoid, one level up: a newly
    registered corpus that simply forgot the field inherited permission to source a
    published claim, and only an equality assertion in
    ``tests/unit/test_corpus_tiering.py`` stood between that and a headline number.
    The argument is written out in ``tests/support/corpora.py`` -- a wrong default must
    not be able to reach a published number -- and it applies here identically.
    """

    name: str = Field(min_length=1)
    kind: Literal["relational_tables", "spreadsheet_tables"]
    axis: Literal["detection"] = "detection"
    error_provenance: Literal["natural"] = "natural"
    tier: Literal["headline", "tripwire", "diagnostic"]
    tier_reason: str = Field(min_length=1)
    # Rows the upstream file declares as content. rt_bench.csv is an Excel export
    # padded to the 1,048,575-row sheet limit; everything past this index is blank.
    declared_columns: int = Field(ge=1)
    source_url: str = Field(min_length=1)
    source_revision: str = Field(min_length=7)
    sha256: str = Field(min_length=64, max_length=64)
    citation: str = Field(min_length=1)
    license_spdx: str | None = None
    scoring_spec: str = Field(min_length=1)
    # `dist_val` is a DISTINCT-value list: multiplicities are absent from the corpus by
    # construction. Pinned False, not defaulted, because a frequency-dependent detector
    # scored against this corpus produces a number describing a distribution that does not
    # exist -- which happened, and was published. See
    # docs/trust/frequency-dependence-correction.md.
    frequencies_available: Literal[False] = False

    model_config = {"frozen": True}


# Pinned to a commit SHA with a verified PGP signature, not a branch, for the same
# reason RAHA_GIT_REVISION is: a mutable ref means a held-out evaluation can change
# silently when upstream moves.
AUTOTEST_GIT_REVISION = "4acf65cf37a506206bf2888dbd45f17e58dce2e2"
_AUTOTEST_BASE_URL = (
    f"https://raw.githubusercontent.com/qixuchen/AutoTest/{AUTOTEST_GIT_REVISION}/benchmarks"
)

_AUTOTEST_CITATION = (
    "Qixu Chen, Yeye He, Raymond Chi-Wing Wong, Weiwei Cui, Song Ge, Haidong Zhang, "
    "Dongmei Zhang, Surajit Chaudhuri. Auto-Test: Learning Semantic-Domain Constraints "
    "for Unsupervised Error Detection in Tables. SIGMOD 2025. arXiv:2504.10762."
)

COLUMN_BENCHMARK_REGISTRY: dict[str, ColumnBenchmarkMetadata] = {
    "rt_bench": ColumnBenchmarkMetadata(
        name="rt_bench",
        kind="relational_tables",
        declared_columns=1200,
        tier="headline",
        tier_reason=(
            "Real errors on real relational columns, manually labelled, with a debatable "
            "class so principled abstention is scored neutrally. Headline for DETECTION "
            "only: no clean values ship, so it can never source a correction claim."
        ),
        source_url=f"{_AUTOTEST_BASE_URL}/rt_bench.csv",
        source_revision=AUTOTEST_GIT_REVISION,
        sha256="57cc995d15275fced84d19abaaa46802dd990492052c08d0cbc7fe76b49cb623",
        citation=_AUTOTEST_CITATION,
        license_spdx=None,
        scoring_spec="specs/SPEC_abstention_scoring.md",
    ),
    "st_bench": ColumnBenchmarkMetadata(
        name="st_bench",
        kind="spreadsheet_tables",
        declared_columns=1200,
        tier="headline",
        tier_reason=(
            "Real errors on real spreadsheet columns, manually labelled, with a debatable "
            "class. Headline for DETECTION only, as for rt_bench."
        ),
        source_url=f"{_AUTOTEST_BASE_URL}/st_bench.csv",
        source_revision=AUTOTEST_GIT_REVISION,
        sha256="6899f46debd284d2167b644fe1f917bb59fa4785c1565cd1499c10359649ecf0",
        citation=_AUTOTEST_CITATION,
        license_spdx=None,
        scoring_spec="specs/SPEC_abstention_scoring.md",
    ),
}


def get_column_benchmark_metadata(name: str) -> ColumnBenchmarkMetadata:
    """Return canonical metadata for a named column-level detection benchmark.

    Args:
        name: Canonical benchmark name.

    Returns:
        The immutable metadata entry.

    Raises:
        KeyError: If the benchmark is not registered.
    """
    return COLUMN_BENCHMARK_REGISTRY[name]


def headline_corpora() -> frozenset[str]:
    """Return every corpus permitted to source a headline claim.

    A published number must trace to one of these. The gate that enforces it lives in
    ``scripts/ci/readme_truth.py``; this function is the single definition it reads, so
    tiering cannot drift between the registry and the check.

    Deliberately spans both registries: the tier question is "may this source a claim",
    which is orthogonal to whether the corpus is a dirty/clean pair or a column list.
    """
    return frozenset(
        {name for name, meta in DATASET_REGISTRY.items() if meta.tier == "headline"}
        | {name for name, meta in COLUMN_BENCHMARK_REGISTRY.items() if meta.tier == "headline"}
    )


def non_headline_corpora() -> frozenset[str]:
    """Return every corpus that must not source a headline claim."""
    return frozenset(
        {name for name, meta in DATASET_REGISTRY.items() if meta.tier != "headline"}
        | {name for name, meta in COLUMN_BENCHMARK_REGISTRY.items() if meta.tier != "headline"}
    )
