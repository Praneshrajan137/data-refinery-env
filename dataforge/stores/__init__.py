"""Backend-neutral table-store and patch-plan API."""

from __future__ import annotations

from dataforge.stores.base import (
    StoreApplyReceipt,
    StoreRevertReceipt,
    TableStore,
    TableStoreError,
)
from dataforge.stores.cloud import CloudWarehouseStore
from dataforge.stores.csv import CSVStore
from dataforge.stores.duckdb import DuckDBStore
from dataforge.stores.patch_plan import (
    CostEstimate,
    PatchOperation,
    PatchPlan,
    RowIdentity,
)
from dataforge.stores.registry import (
    TableStoreSpec,
    is_table_store_uri,
    parse_table_store_uri,
    store_from_uri,
)
from dataforge.stores.repair import TableStoreRepairResult, run_table_store_repair

__all__ = [
    "CSVStore",
    "CloudWarehouseStore",
    "CostEstimate",
    "DuckDBStore",
    "PatchOperation",
    "PatchPlan",
    "RowIdentity",
    "StoreApplyReceipt",
    "StoreRevertReceipt",
    "TableStore",
    "TableStoreError",
    "TableStoreRepairResult",
    "TableStoreSpec",
    "is_table_store_uri",
    "parse_table_store_uri",
    "run_table_store_repair",
    "store_from_uri",
]
