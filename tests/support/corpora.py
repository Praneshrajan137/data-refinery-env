"""Shared builders for benchmark-corpus metadata in tests.

Exists for the reason ``tests/support/tables.py`` exists: the same fixture literal was
copy-pasted across five test files, so a required field added to
:class:`~dataforge.datasets.registry.DatasetMetadata` broke thirty-six tests at once and
had to be fixed in eight places.

The tier fields are deliberately **required** on the real model rather than defaulted --
a default ``tier`` would let a newly registered corpus inherit permission to source a
published claim, which is the denylist-fails-open mistake ``CONSTRAINT_CHECKABLE_DETECTORS``
exists to avoid. The defaults live here, in test support, where a wrong one cannot reach
a published number.
"""

from __future__ import annotations

from dataforge.datasets.registry import DatasetMetadata

__all__ = ["build_fixture_metadata"]


def build_fixture_metadata(
    *,
    name: str,
    domain: str,
    n_rows: int,
    n_columns: int,
    error_types: tuple[str, ...],
    source_revision: str,
    dirty_sha256: str,
    clean_sha256: str,
    citation: str = "fixture",
    source_urls: tuple[str, str] = ("dirty", "clean"),
    error_provenance: str = "injected",
    tier: str = "diagnostic",
) -> DatasetMetadata:
    """Build a :class:`DatasetMetadata` for a synthetic test corpus.

    Defaults to ``injected``/``diagnostic``: a fixture is neither natural data nor
    evidence of anything, so the honest default is the one that authorises the least. A
    test needing headline-tier behaviour must ask for it explicitly, which keeps that
    request visible in review.

    Args:
        name: Corpus name.
        domain: Corpus domain.
        n_rows: Declared row count.
        n_columns: Declared column count.
        error_types: Declared error families.
        source_revision: Pinned revision string.
        dirty_sha256: Dirty-file digest.
        clean_sha256: Clean-file digest.
        citation: Citation string.
        source_urls: Dirty and clean URLs.
        error_provenance: One of natural/injected/synthetic/contested.
        tier: One of headline/tripwire/diagnostic.

    Returns:
        The frozen metadata model.
    """
    return DatasetMetadata(
        name=name,
        domain=domain,
        n_rows=n_rows,
        n_columns=n_columns,
        error_types=error_types,
        error_provenance=error_provenance,  # type: ignore[arg-type]
        tier=tier,  # type: ignore[arg-type]
        tier_reason="synthetic test fixture; never a source of a published claim",
        source_urls=source_urls,
        source_revision=source_revision,
        dirty_sha256=dirty_sha256,
        clean_sha256=clean_sha256,
        citation=citation,
    )
