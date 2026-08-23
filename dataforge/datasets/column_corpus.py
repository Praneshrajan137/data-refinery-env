"""Loader for column-level detection benchmarks with a debatable label class.

Fetches, hash-pins and parses the ``RT-bench``/``ST-bench`` corpora of Auto-Test
(Chen et al., SIGMOD 2025, arXiv:2504.10762). Scored under
``specs/SPEC_abstention_scoring.md`` by :mod:`dataforge.bench.abstention`.

**Never vendored.** The upstream repository publishes no licence file, so there is no
grant to redistribute these bytes. They are fetched at load time and verified against
a pinned SHA-256, the same pattern :mod:`dataforge.datasets.real_world` uses for RAHA.

Four upstream realities this module exists to handle. Each was found by measurement,
and each would silently corrupt a score if handled naively:

1. **``rt_bench.csv`` is an Excel export padded to the sheet limit.** It has 1,048,575
   rows (2**20 - 1); rows 1201 onward are entirely blank. A loader that trusted the
   row count would score against a million empty columns and report near-zero
   coverage as though it had measured something.
2. **The published field name is misspelled.** ``benchmark_readme.md`` documents
   ``ground_truth_debateable``; the actual CSV header is ``ground_truth_debatable``.
   Reading the documented name yields ``None`` for every row, which would silently
   collapse the neutral zone and reintroduce the abstention penalty the whole
   protocol exists to remove.
3. **Three ``st_bench`` rows do not parse.** One carries a leaked spreadsheet formula
   reference (``['refridgerator'+C1187]``); two have an empty ``dist_val``. They are
   *quarantined and counted*, never silently skipped -- dropping a row with labels in
   it would inflate precision.
4. **The file carries a UTF-8 BOM** and label fields are Python list literals, not
   JSON, so they need ``utf-8-sig`` and :func:`ast.literal_eval`.

The corpora are label-sparse and that is the point. Measured at the pinned revision:
88 unambiguous error values across 166,387 real distinct values, i.e. **0.053%**. This
is a false-positive-rate benchmark on real data at scale with a weak recall signal --
which is the right shape for a system whose measured failure was 263,428 false
rewrites, and the wrong shape for a recall claim. See
``docs/trust/column-benchmark-scope.md``.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import io
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx

from dataforge.datasets.registry import (
    COLUMN_BENCHMARK_REGISTRY,
    ColumnBenchmarkMetadata,
    get_column_benchmark_metadata,
)

__all__ = [
    "BenchmarkColumn",
    "ColumnBenchmark",
    "ColumnBenchmarkError",
    "QuarantinedRow",
    "load_column_benchmark",
]

# The upstream ``dist_val`` field routinely exceeds the stdlib default of 131,072
# characters (one column holds 104 long sentences). Raised at import so a caller
# cannot forget; bounded rather than unbounded so a corrupt file cannot exhaust memory.
_CSV_FIELD_LIMIT = 10_000_000

# The documented spelling is ``ground_truth_debateable``; the shipped header is this.
# Named as a constant so the discrepancy is impossible to reintroduce by autocorrect.
_DEBATABLE_FIELD = "ground_truth_debatable"

_REQUIRED_FIELDS = ("header", "ground_truth", _DEBATABLE_FIELD, "dist_val")


class ColumnBenchmarkError(RuntimeError):
    """Raised when a column benchmark cannot be loaded or verified honestly."""


@dataclass(frozen=True, slots=True)
class QuarantinedRow:
    """A benchmark row that could not be parsed, retained so it can be counted.

    Quarantine is deliberately not the same as omission. A silently dropped row is
    invisible in the denominator; a quarantined one is reported, so a reader can see
    that the measured corpus is smaller than the published one and by how much.
    """

    index: int
    reason: str
    detail: str


@dataclass(frozen=True, slots=True)
class BenchmarkColumn:
    """One real table column with its three-way labels.

    ``ground_truth`` and ``debatable`` are guaranteed disjoint and guaranteed subsets
    of ``distinct_values``; :func:`load_column_benchmark` refuses the corpus otherwise.
    """

    index: int
    header: str
    distinct_values: tuple[str, ...]
    ground_truth: frozenset[str]
    debatable: frozenset[str]
    declared_value_count: int

    @property
    def value_count_matches_declaration(self) -> bool:
        """Whether ``dist_val_count`` agrees with the distinct values actually present.

        Ten rows across the two corpora disagree upstream. Reported rather than
        enforced: the label fields are what this benchmark is for, and refusing a
        corpus over a stale count would discard 2,397 good columns for a cosmetic
        defect.
        """
        return self.declared_value_count == len(set(self.distinct_values))


@dataclass(frozen=True, slots=True)
class ColumnBenchmark:
    """A loaded, hash-verified column benchmark.

    ``sha256`` is the digest of the bytes actually parsed, so an artifact built from
    this object records what was measured rather than what was expected.
    """

    metadata: ColumnBenchmarkMetadata
    columns: tuple[BenchmarkColumn, ...]
    quarantined: tuple[QuarantinedRow, ...]
    sha256: str
    padded_rows_discarded: int
    value_count_mismatches: int

    @property
    def n_columns(self) -> int:
        """Columns successfully parsed and admitted."""
        return len(self.columns)

    @property
    def n_ground_truth_values(self) -> int:
        """Total unambiguous error values across the corpus."""
        return sum(len(column.ground_truth) for column in self.columns)

    @property
    def n_debatable_values(self) -> int:
        """Total debatable values across the corpus."""
        return sum(len(column.debatable) for column in self.columns)

    @property
    def n_distinct_values(self) -> int:
        """Total distinct values across the corpus: the effective negative set."""
        return sum(len(set(column.distinct_values)) for column in self.columns)


def _cache_path(name: str, cache_root: Path | None) -> Path:
    """Return the on-disk cache location for a benchmark's raw bytes."""
    root = cache_root if cache_root is not None else Path.home() / ".dataforge" / "cache"
    return root / "column_benchmarks" / f"{name}.csv"


def _download(url: str) -> bytes:
    """Fetch raw bytes, honouring the shared download timeout envvar."""
    timeout = float(os.environ.get("DATAFORGE_DOWNLOAD_TIMEOUT_S", "30"))
    try:
        response = httpx.get(url, follow_redirects=True, timeout=timeout)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise ColumnBenchmarkError(f"failed to download {url}: {exc}") from exc
    return response.content


def _literal_str_list(raw: str | None, *, field: str) -> list[str]:
    """Parse a Python list literal of strings.

    Raises:
        ValueError: If the field is absent, unparseable, not a list, or contains a
            non-string element. All four are treated identically on purpose: each
            means this row's labels cannot be trusted, and a partially-trusted label
            row is worse than a quarantined one.
    """
    if raw is None:
        raise ValueError(f"field {field!r} is absent")
    parsed = ast.literal_eval(raw)
    if not isinstance(parsed, list):
        raise ValueError(f"field {field!r} is {type(parsed).__name__}, expected list")
    for element in parsed:
        if not isinstance(element, str):
            raise ValueError(f"field {field!r} holds a {type(element).__name__}, expected str")
    return parsed


def _iter_content_rows(text: str, declared: int) -> Iterator[tuple[int, dict[str, str]]]:
    """Yield the declared content rows, refusing to discard anything non-blank.

    Raises:
        ColumnBenchmarkError: If a row past ``declared`` holds any non-whitespace
            value. Truncating an Excel-padded export is safe only while the tail is
            genuinely empty; if upstream ever appends real columns, this must fail
            loudly rather than silently shrink the corpus.
    """
    reader = csv.DictReader(io.StringIO(text))
    missing = sorted(set(_REQUIRED_FIELDS) - set(reader.fieldnames or ()))
    if missing:
        raise ColumnBenchmarkError(
            f"benchmark is missing required field(s) {missing}; got {reader.fieldnames}. "
            f"Note the shipped header is {_DEBATABLE_FIELD!r}, which the upstream "
            "benchmark_readme.md misspells as 'ground_truth_debateable'."
        )
    for position, row in enumerate(reader):
        if position < declared:
            yield position, row
            continue
        if any((value or "").strip() for value in row.values()):
            raise ColumnBenchmarkError(
                f"row {position} lies past the declared {declared} content rows but is "
                "not blank; upstream has changed and the registry's declared_columns "
                "must be re-pinned rather than silently truncating real data"
            )


def load_column_benchmark(
    name: str,
    *,
    cache_root: Path | None = None,
    verify_hash: bool = True,
) -> ColumnBenchmark:
    """Load, verify and parse a registered column-level detection benchmark.

    Args:
        name: Registered benchmark name (``"rt_bench"`` or ``"st_bench"``).
        cache_root: Override for the download cache root.
        verify_hash: Whether to enforce the pinned SHA-256. Defaults to True and
            should stay True; see the note on the raise below.

    Returns:
        The parsed :class:`ColumnBenchmark`.

    Raises:
        KeyError: If the benchmark is not registered.
        ColumnBenchmarkError: If the download fails, the digest does not match, a
            required field is absent, non-blank data lies past the declared row
            count, a row's labels overlap, or a row's labels are not drawn from its
            own distinct-value list.
    """
    metadata = get_column_benchmark_metadata(name)
    path = _cache_path(name, cache_root)

    if path.exists():
        raw = path.read_bytes()
    else:
        raw = _download(metadata.source_url)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    digest = hashlib.sha256(raw).hexdigest()
    if verify_hash and digest != metadata.sha256:
        raise ColumnBenchmarkError(
            f"benchmark {name!r} does not match the pinned Auto-Test revision "
            f"{metadata.source_revision}: expected sha256 {metadata.sha256}, got {digest}"
        )

    # csv's limit is process-global; set it here rather than at import so importing
    # this module cannot change parsing behaviour elsewhere in the process.
    previous_limit = csv.field_size_limit(_CSV_FIELD_LIMIT)
    try:
        text = raw.decode("utf-8-sig")
        columns: list[BenchmarkColumn] = []
        quarantined: list[QuarantinedRow] = []
        rows_seen = 0
        for position, row in _iter_content_rows(text, metadata.declared_columns):
            rows_seen += 1
            try:
                values = _literal_str_list(row.get("dist_val"), field="dist_val")
                truth = _literal_str_list(row.get("ground_truth"), field="ground_truth")
                debated = _literal_str_list(row.get(_DEBATABLE_FIELD), field=_DEBATABLE_FIELD)
            except (ValueError, SyntaxError) as exc:
                quarantined.append(
                    QuarantinedRow(index=position, reason=type(exc).__name__, detail=str(exc))
                )
                continue
            if not values:
                quarantined.append(
                    QuarantinedRow(
                        index=position, reason="EmptyColumn", detail="dist_val holds no values"
                    )
                )
                continue

            truth_set = frozenset(truth)
            debated_set = frozenset(debated)
            # These two are corpus-level invariants, not row defects: a violation
            # means the scoring rule's premises do not hold, so no number derived
            # from this corpus would mean what it claims. Fail closed.
            overlap = truth_set & debated_set
            if overlap:
                raise ColumnBenchmarkError(
                    f"row {position} labels {sorted(overlap)!r} as both unambiguous and "
                    "debatable; the three-way rule requires them disjoint"
                )
            unknown = (truth_set | debated_set) - set(values)
            if unknown:
                raise ColumnBenchmarkError(
                    f"row {position} labels {sorted(unknown)!r} which are absent from its "
                    "own dist_val; a label outside the value set cannot be scored"
                )

            try:
                declared_count = int(row.get("dist_val_count") or 0)
            except ValueError:
                declared_count = 0
            columns.append(
                BenchmarkColumn(
                    index=position,
                    header=row.get("header") or "",
                    distinct_values=tuple(values),
                    ground_truth=truth_set,
                    debatable=debated_set,
                    declared_value_count=declared_count,
                )
            )
    finally:
        csv.field_size_limit(previous_limit)

    if not columns:
        raise ColumnBenchmarkError(
            f"benchmark {name!r} yielded zero admissible columns; an empty corpus would "
            "score as zeros and be indistinguishable from a measured result"
        )

    return ColumnBenchmark(
        metadata=metadata,
        columns=tuple(columns),
        quarantined=tuple(quarantined),
        sha256=digest,
        padded_rows_discarded=max(0, metadata.declared_columns - rows_seen),
        value_count_mismatches=sum(
            1 for column in columns if not column.value_count_matches_declaration
        ),
    )


def registered_column_benchmarks() -> tuple[str, ...]:
    """Return the registered column-benchmark names in a stable order."""
    return tuple(sorted(COLUMN_BENCHMARK_REGISTRY))
