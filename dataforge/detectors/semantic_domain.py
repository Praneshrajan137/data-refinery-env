"""Semantic-domain violation detection from externally learned constraints.

Implements the *Pattern* family of Semantic-Domain Constraints (SDCs) learned offline by
Auto-Test (Chen et al., SIGMOD 2025, arXiv:2504.10762) over a corpus of ~200,000 real table
columns.

An SDC is a pair: a **pre-condition** that decides whether the constraint applies to a
column ("at least 95% of values match this pattern"), and a **post-condition** naming the
violations ("values not matching it"). Each carries a confidence measured offline on held-out
real columns. So unlike every inferred constraint currently in this repository, an SDC's
error rate was estimated on data other than the table it is applied to.

That property is what makes this detector interesting, and it is also exactly why it must
not be allowed to write.

## Why this detector has no repairer, and must not be allowlisted

An SDC is a *statistical* claim about a semantic domain, not a declared constraint. It is
therefore advisory, and `semantic_domain_violation` is deliberately absent from
``CONSTRAINT_CHECKABLE_DETECTORS``.

The tempting argument for allowlisting it is that an SDC *is* an external reference --
evidence from 200,000 other columns rather than from this column's own distribution -- and
so satisfies the soundness axis as ``vocabulary.py`` defines it. The argument fails on a
mechanism: ``verification_strength_for("deterministic", ...)`` returns ``proven``
**regardless of schema**. Allowlisting a detector whose repairer is deterministic would
therefore grant proven-strength writes on statistical evidence, which is precisely the
conflation of determinism with soundness that produced 263,428 false money rewrites.

The resolution is structural rather than procedural: **there is no repairer**, so no write
path exists and the question cannot arise. This mirrors ``outlier``, which also ships
detection-only.

There is a clean convergence worth naming. ``RT-bench``/``ST-bench`` ship no clean values,
so the only corpus that can validate this capability can validate detection and nothing
else. The evidence available and the capability shipped are the same size.

## Why this detector is not in the default ensemble

It requires a fetched SDC artifact, and ``default_detectors()`` must stay offline and
dependency-free. Construct it explicitly with :func:`load_pattern_sdcs`.

## Scope: 60 of 505 SDCs

Only the ``Pattern`` family is implemented. The artifact also holds 361 ``Embedding`` SDCs
(Sentence-BERT distance), 60 ``CTA`` (Sherlock/Doduo column-type annotation) and 24
``Function``. Those need model weights and heavyweight inference that a pure detector must
not take on, and pretending otherwise by silently skipping them would misreport coverage.
:func:`load_pattern_sdcs` therefore reports how many SDCs it declined.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import io
import re
from dataclasses import dataclass
from pathlib import Path

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike

__all__ = [
    "AUTOTEST_SDC_SHA256",
    "AUTOTEST_SDC_URL",
    "PatternSDC",
    "SDCLoadResult",
    "SemanticDomainDetector",
    "load_pattern_sdcs",
    "parse_pattern_sdcs",
]

# Pinned to the same Auto-Test commit as the benchmark corpora. Upstream publishes no
# licence, so this artifact is fetched and hash-verified, never vendored.
_AUTOTEST_REVISION = "4acf65cf37a506206bf2888dbd45f17e58dce2e2"
AUTOTEST_SDC_URL = (
    f"https://raw.githubusercontent.com/qixuchen/AutoTest/{_AUTOTEST_REVISION}"
    "/code/AutoTest/results/SDC/rt_train_selected_sdc.csv"
)
AUTOTEST_SDC_SHA256 = "c51205c93da17d4ddb64b69a361469c98a3231146285e6b1d58741b1dc077e85"

# Minimum distinct values before a pre-condition is trusted. Below this, "95% of values
# match" is satisfied by two coincidences.
_MIN_VALUES = 8

# Cap on violations reported per column. A pattern SDC firing on most of a column is
# describing a column it does not apply to, not finding many errors; the pre-condition is
# meant to prevent that, and this bounds the damage when it does not.
_MAX_VIOLATIONS_PER_COLUMN = 5


@dataclass(frozen=True, slots=True)
class PatternSDC:
    """One learned pattern constraint.

    Attributes:
        pattern: Regex the column's values are expected to match.
        coverage_threshold: Fraction of values that must match for the constraint to
            apply to a column at all.
        confidence: Precision measured offline on held-out real columns. Carried onto the
            emitted issue, which is what makes this the first inferred-constraint family
            in this repository with an externally estimated error rate.
        example: A value from the learning corpus that matched, kept for explanation.
    """

    pattern: str
    coverage_threshold: float
    confidence: float
    example: str

    def compiled(self) -> re.Pattern[str]:
        """Return the compiled regex."""
        return re.compile(self.pattern)


@dataclass(frozen=True, slots=True)
class SDCLoadResult:
    """Loaded pattern SDCs plus what was declined, so coverage is not overstated."""

    sdcs: tuple[PatternSDC, ...]
    total_in_artifact: int
    declined_by_family: dict[str, int]
    sha256: str

    @property
    def declined_total(self) -> int:
        """SDCs present in the artifact but not implemented here."""
        return sum(self.declined_by_family.values())


def parse_pattern_sdcs(raw: bytes) -> SDCLoadResult:
    """Parse the Pattern family out of an Auto-Test SDC artifact.

    The artifact is **tab**-separated despite its ``.csv`` extension, and the ``SDC`` column
    holds a Python tuple literal whose first element is
    ``('pattern', _, example, regex, coverage_threshold)``.

    Args:
        raw: Artifact bytes.

    Returns:
        The :class:`SDCLoadResult`.

    Raises:
        ValueError: If the artifact has no recognisable header, or yields zero pattern
            SDCs. Zero is treated as a failure rather than an empty ensemble: a detector
            silently loaded with no constraints would report perfect precision by never
            firing, which is the vacuous-pass failure mode this project keeps meeting.
    """
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames or "SDC" not in reader.fieldnames:
        raise ValueError(
            f"SDC artifact has no 'SDC' column; got {reader.fieldnames}. Note the file is "
            "tab-separated despite the .csv extension."
        )

    sdcs: list[PatternSDC] = []
    declined: dict[str, int] = {}
    total = 0
    for row in reader:
        total += 1
        family = (row.get("type") or "").strip()
        if family != "Pattern":
            declined[family or "unknown"] = declined.get(family or "unknown", 0) + 1
            continue
        try:
            spec = ast.literal_eval(row["SDC"])
            pre = spec[0]
            _, _, example, pattern, threshold = pre
            confidence = float(row["confidence"])
            re.compile(pattern)
        except (ValueError, SyntaxError, TypeError, IndexError, re.error):
            declined["Pattern (unparseable)"] = declined.get("Pattern (unparseable)", 0) + 1
            continue
        sdcs.append(
            PatternSDC(
                pattern=str(pattern),
                coverage_threshold=float(threshold),
                confidence=min(max(confidence, 0.0), 1.0),
                example=str(example),
            )
        )

    if not sdcs:
        raise ValueError(
            "SDC artifact yielded zero pattern constraints; a detector with no constraints "
            "never fires and would report perfect precision while measuring nothing"
        )
    return SDCLoadResult(
        sdcs=tuple(sdcs),
        total_in_artifact=total,
        declined_by_family=declined,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def load_pattern_sdcs(
    *,
    cache_root: Path | None = None,
    verify_hash: bool = True,
) -> SDCLoadResult:
    """Fetch, verify and parse the pinned Auto-Test SDC artifact.

    Args:
        cache_root: Override for the download cache root.
        verify_hash: Whether to enforce the pinned SHA-256. Defaults to True.

    Returns:
        The :class:`SDCLoadResult`.

    Raises:
        ValueError: If the digest does not match, or parsing yields no constraints.
        RuntimeError: If the download fails.
    """
    import httpx

    root = cache_root if cache_root is not None else Path.home() / ".dataforge" / "cache"
    path = root / "sdc" / "rt_train_selected_sdc.csv"
    if path.exists():
        raw = path.read_bytes()
    else:
        try:
            response = httpx.get(AUTOTEST_SDC_URL, follow_redirects=True, timeout=60.0)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"failed to download SDC artifact: {exc}") from exc
        raw = response.content
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    digest = hashlib.sha256(raw).hexdigest()
    if verify_hash and digest != AUTOTEST_SDC_SHA256:
        raise ValueError(
            f"SDC artifact does not match the pinned Auto-Test revision {_AUTOTEST_REVISION}: "
            f"expected sha256 {AUTOTEST_SDC_SHA256}, got {digest}"
        )
    return parse_pattern_sdcs(raw)


class SemanticDomainDetector:
    """Flag values violating an externally learned semantic-domain constraint.

    Detection only, by construction. There is no ``semantic_domain_violation`` repairer and
    ``semantic_domain_violation`` is not in ``CONSTRAINT_CHECKABLE_DETECTORS``, so this
    detector has no write path on any surface.

    Severity is always ``REVIEW``: never ``SAFE``, which would invite bulk application, and
    never ``UNSAFE``, which would overstate a statistical finding as a structural one.
    """

    def __init__(self, sdcs: tuple[PatternSDC, ...]) -> None:
        """Initialise with learned constraints.

        Args:
            sdcs: Pattern SDCs to apply.

        Raises:
            ValueError: If ``sdcs`` is empty. An empty detector never fires, and a detector
                that never fires is indistinguishable from a precise one.
        """
        if not sdcs:
            raise ValueError(
                "SemanticDomainDetector requires at least one SDC; an empty ensemble never "
                "fires and would look like perfect precision"
            )
        self._compiled = tuple((sdc, sdc.compiled()) for sdc in sdcs)

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect semantic-domain violations.

        For each column, an SDC applies only if at least ``coverage_threshold`` of the
        column's non-empty values match its pattern; the non-matching values are then the
        violations. The highest-confidence applicable SDC wins a cell.

        Args:
            df: The input table.
            schema: Ignored. An SDC is advisory and a declared schema neither strengthens
                nor weakens it, so accepting one here would imply a relationship that does
                not exist.

        Returns:
            One issue per violating cell.
        """
        issues: list[Issue] = []
        for column in df.columns:
            name = str(column)
            series = df[column]
            values = [("" if value is None else str(value)) for value in series]
            non_empty = [value for value in values if value.strip()]
            if len(non_empty) < _MIN_VALUES:
                continue

            best: dict[int, tuple[float, PatternSDC]] = {}
            for sdc, pattern in self._compiled:
                matched = [value for value in non_empty if pattern.match(value)]
                coverage = len(matched) / len(non_empty)
                if coverage < sdc.coverage_threshold:
                    continue
                violations = [
                    index
                    for index, value in enumerate(values)
                    if value.strip() and not pattern.match(value)
                ]
                # A constraint firing on most of a column is describing a column it does
                # not apply to. The pre-condition should prevent this; the cap bounds it.
                if not violations or len(violations) > _MAX_VIOLATIONS_PER_COLUMN:
                    continue
                for index in violations:
                    current = best.get(index)
                    if current is None or sdc.confidence > current[0]:
                        best[index] = (sdc.confidence, sdc)

            for index, (confidence, sdc) in sorted(best.items()):
                issues.append(
                    Issue(
                        row=index,
                        column=name,
                        issue_type="semantic_domain_violation",
                        severity=Severity.REVIEW,
                        confidence=confidence,
                        expected=None,
                        actual=values[index],
                        reason=(
                            f"Value does not match the learned semantic domain of this column "
                            f"({sdc.coverage_threshold:.0%} of values match "
                            f"{sdc.pattern!r}, e.g. {sdc.example!r}). Advisory: this "
                            f"constraint was learned statistically, not declared, so it is "
                            f"never a basis for an automatic write."
                        ),
                    )
                )
        return issues
