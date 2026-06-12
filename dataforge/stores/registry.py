"""Table-store URI parsing and adapter selection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from dataforge.stores.base import TableStore, TableStoreError
from dataforge.stores.cloud import CloudWarehouseStore
from dataforge.stores.duckdb import DuckDBStore

_CLOUD_BACKENDS = {"snowflake", "bigquery", "databricks", "databricks_delta"}


@dataclass(frozen=True)
class TableStoreSpec:
    """Parsed table-store URI."""

    backend: str
    target: str
    relation: str
    database_path: Path | None
    row_identity_columns: tuple[str, ...]


def is_table_store_uri(raw: str) -> bool:
    """Return whether a CLI target string names a DataForge table store."""
    return raw.startswith("warehouse://")


def parse_table_store_uri(raw: str, *, row_ids: tuple[str, ...] = ()) -> TableStoreSpec:
    """Parse a ``warehouse://`` URI into an adapter spec.

    Supported local form:
    ``warehouse://duckdb?database=/tmp/dev.duckdb&relation=main.model&row_id=id``.
    """
    parsed = urlparse(raw)
    if parsed.scheme != "warehouse":
        raise TableStoreError("Table-store URIs must use the warehouse:// scheme.")
    backend = (parsed.netloc or parsed.path.strip("/").split("/", 1)[0]).lower()
    if not backend:
        raise TableStoreError("Warehouse URI must include a backend name.")
    query = {key: values[-1] for key, values in parse_qs(parsed.query).items() if values}
    relation = unquote(query.get("relation", ""))
    if not relation:
        raise TableStoreError("Warehouse URI must include relation=<schema.table>.")
    database = query.get("database") or query.get("path")
    row_id_query = query.get("row_id") or query.get("key")
    resolved_row_ids = row_ids
    if row_id_query:
        resolved_row_ids = tuple(part.strip() for part in row_id_query.split(",") if part.strip())
    return TableStoreSpec(
        backend=backend,
        target=raw,
        relation=relation,
        database_path=Path(unquote(database)).expanduser() if database else None,
        row_identity_columns=resolved_row_ids,
    )


def store_from_uri(raw: str, *, row_ids: tuple[str, ...] = ()) -> TableStore:
    """Create a table-store adapter from a CLI URI."""
    spec = parse_table_store_uri(raw, row_ids=row_ids)
    if spec.backend == "duckdb":
        if spec.database_path is None:
            raise TableStoreError("DuckDB warehouse URI requires database=<path>.")
        return DuckDBStore(
            database_path=spec.database_path,
            relation=spec.relation,
            row_identity_columns=spec.row_identity_columns,
            target=spec.target,
        )
    if spec.backend in _CLOUD_BACKENDS:
        backend = "databricks" if spec.backend == "databricks_delta" else spec.backend
        return CloudWarehouseStore(
            backend=backend,
            target=spec.target,
            relation=spec.relation,
            row_identity_columns=spec.row_identity_columns,
        )
    raise TableStoreError(f"Unsupported warehouse backend: {spec.backend}")
