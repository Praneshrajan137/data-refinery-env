"""Audit canonical real-world benchmark source bytes before trajectory generation."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.real_world import (  # noqa: E402
    DatasetDownloadError,
    load_real_world_dataset,
)
from dataforge.datasets.registry import DATASET_REGISTRY  # noqa: E402

REPORT_SCHEMA_VERSION = "dataforge_real_world_source_audit_v1"
# Derived from the registry rather than hardcoded. The previous literal
# ("hospital", "flights", "beers") raised KeyError on the default invocation for six
# weeks after `beers` was de-registered on 2026-07-12: a hardcoded default cannot be
# de-registered, so it silently became a crash instead of a corpus.
DEFAULT_DATASETS = tuple(sorted(DATASET_REGISTRY))


@dataclass(frozen=True, slots=True)
class DatasetSourceAudit:
    """One dataset source-audit row."""

    dataset: str
    status: str
    domain: str | None = None
    source_revision: str | None = None
    dirty_sha256: str | None = None
    clean_sha256: str | None = None
    rows: int | None = None
    columns: int | None = None
    ground_truth_cells: int | None = None
    expected_rows: int | None = None
    expected_columns: int | None = None
    expected_dirty_sha256: str | None = None
    expected_clean_sha256: str | None = None
    error: str | None = None


def audit_real_world_sources(
    *,
    datasets: tuple[str, ...] = DEFAULT_DATASETS,
    cache_root: Path | None = None,
) -> dict[str, Any]:
    """Return a JSON-ready source provenance report for canonical datasets."""
    rows: list[DatasetSourceAudit] = []
    for dataset_name in datasets:
        expected = DATASET_REGISTRY[dataset_name]
        try:
            loaded = load_real_world_dataset(
                dataset_name,
                cache_root=cache_root,
                verify_hashes=True,
                allow_embedded_fallback=False,
            )
        except (DatasetDownloadError, OSError, ValueError) as exc:
            rows.append(
                DatasetSourceAudit(
                    dataset=dataset_name,
                    status="block",
                    domain=expected.domain,
                    source_revision=expected.source_revision,
                    expected_rows=expected.n_rows,
                    expected_columns=expected.n_columns,
                    expected_dirty_sha256=expected.dirty_sha256,
                    expected_clean_sha256=expected.clean_sha256,
                    error=str(exc),
                )
            )
            continue
        status = "pass"
        errors: list[str] = []
        if len(loaded.dirty_df.index) != expected.n_rows:
            status = "block"
            errors.append(f"rows {len(loaded.dirty_df.index)} != expected {expected.n_rows}")
        if len(loaded.canonical_columns) != expected.n_columns:
            status = "block"
            errors.append(
                f"columns {len(loaded.canonical_columns)} != expected {expected.n_columns}"
            )
        rows.append(
            DatasetSourceAudit(
                dataset=dataset_name,
                status=status,
                domain=loaded.metadata.domain,
                source_revision=loaded.metadata.source_revision,
                dirty_sha256=loaded.dirty_sha256,
                clean_sha256=loaded.clean_sha256,
                rows=len(loaded.dirty_df.index),
                columns=len(loaded.canonical_columns),
                ground_truth_cells=len(loaded.ground_truth),
                expected_rows=expected.n_rows,
                expected_columns=expected.n_columns,
                expected_dirty_sha256=expected.dirty_sha256,
                expected_clean_sha256=expected.clean_sha256,
                error="; ".join(errors) if errors else None,
            )
        )
    payload = [asdict(row) for row in rows]
    blockers = [row["dataset"] for row in payload if row["status"] != "pass"]
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": "pass" if not blockers else "block",
        "ok": not blockers,
        "cache_root": str(cache_root) if cache_root is not None else None,
        "datasets": payload,
        "blockers": blockers,
        "limitations": [
            "This verifies canonical source bytes and row/column counts; it is not model-quality evidence.",
            "Embedded fixtures and unverified cached CSVs are not accepted for release trajectory generation.",
        ],
    }


def _parse_datasets(value: str) -> tuple[str, ...]:
    """Parse and validate a comma-separated dataset list."""
    datasets = tuple(item.strip() for item in value.split(",") if item.strip())
    if not datasets:
        raise argparse.ArgumentTypeError("at least one dataset is required")
    unknown = sorted(set(datasets) - set(DATASET_REGISTRY))
    if unknown:
        raise argparse.ArgumentTypeError("unknown dataset(s): " + ", ".join(unknown))
    return datasets


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasets", type=_parse_datasets, default=DEFAULT_DATASETS)
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--fail-on-block", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the source-audit CLI."""
    args = _build_parser().parse_args(argv)
    report = audit_real_world_sources(datasets=args.datasets, cache_root=args.cache_root)
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    if args.fail_on_block and not report["ok"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
