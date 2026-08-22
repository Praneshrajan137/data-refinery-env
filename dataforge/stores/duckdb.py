"""DuckDB table-store implementation for local warehouse repair proofs."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataforge.detectors.base import Schema
from dataforge.engine.repair import authoritative_columns
from dataforge.repairers.base import ProposedFix
from dataforge.stores.base import StoreApplyReceipt, TableStore, TableStoreError
from dataforge.stores.patch_plan import (
    PatchOperation,
    PatchPlan,
    RowIdentity,
    enforce_plan_constraint_checkable_only,
    enforce_plan_proven_only,
)
from dataforge.stores.sql import ensure_safe_relation, quote_identifier, sql_literal
from dataforge.table import Table, TableLike, cell_value, column_names, row_count
from dataforge.transactions.log import (
    append_applied_event,
    append_created_transaction,
    append_reverted_event,
    load_transaction,
    sha256_bytes,
)
from dataforge.transactions.txn import RepairTransaction, generate_txn_id


class DuckDBStore(TableStore):
    """Local DuckDB relation adapter with real apply and rollback."""

    backend = "duckdb"

    def __init__(
        self,
        *,
        database_path: Path,
        relation: str,
        row_identity_columns: tuple[str, ...] = (),
        target: str | None = None,
    ) -> None:
        self.database_path = database_path.resolve()
        self.relation = ensure_safe_relation(relation)
        self.row_identity_columns = row_identity_columns
        self.target = (
            target or f"warehouse://duckdb?database={self.database_path}&relation={relation}"
        )

    def _connect(self, *, read_only: bool) -> Any:
        try:
            import duckdb
        except ImportError as exc:
            raise TableStoreError("DuckDB table-store support requires duckdb.") from exc
        return duckdb.connect(str(self.database_path), read_only=read_only)

    def read_table(self) -> TableLike:
        """Read the configured relation into the DataForge table surface."""
        with self._connect(read_only=True) as connection:
            cursor = connection.execute(f"SELECT * FROM {self.relation}")
            columns = [str(item[0]) for item in cursor.description]
            return Table(
                columns, (dict(zip(columns, row, strict=True)) for row in cursor.fetchall())
            )

    def _identity_for_row(self, table: TableLike, row: int) -> RowIdentity:
        columns = column_names(table)
        if not self.row_identity_columns:
            return RowIdentity(
                kind="unavailable",
                stable=False,
                reason="Warehouse apply requires explicit row identity columns.",
            )
        missing = [column for column in self.row_identity_columns if column not in columns]
        if missing:
            return RowIdentity(
                kind="unavailable",
                columns=self.row_identity_columns,
                stable=False,
                reason="Missing row identity columns: " + ", ".join(missing),
            )
        return RowIdentity(
            kind="column_values",
            columns=self.row_identity_columns,
            values={column: cell_value(table, row, column) for column in self.row_identity_columns},
            stable=True,
            reason="Explicit row identity columns are present in the relation snapshot.",
        )

    def _where_sql(
        self,
        identity: RowIdentity,
        *,
        column: str | None = None,
        value: str | None = None,
    ) -> str:
        clauses = [
            f"{quote_identifier(key)} = {sql_literal(identity.values[key])}"
            for key in identity.columns
        ]
        if column is not None and value is not None:
            clauses.append(f"{quote_identifier(column)} = {sql_literal(value)}")
        return " AND ".join(clauses)

    def build_patch_plan(
        self,
        fixes: list[ProposedFix],
        *,
        schema: Schema | None,
        safety_verdict: str,
        touched_constraints: tuple[str, ...] = (),
        smt_obligations: tuple[str, ...] = (),
    ) -> PatchPlan:
        """Build SQL patch and rollback statements for verified fixes."""
        authoritative_schema_present = schema is not None
        covered_columns = authoritative_columns(schema)
        table = self.read_table()
        operations: list[PatchOperation] = []
        for proposed in fixes:
            fix = proposed.fix
            identity = self._identity_for_row(table, fix.row)
            precondition_sql = forward_sql = rollback_sql = None
            verification_sql: tuple[str, ...] = ()
            if identity.stable:
                old_where = self._where_sql(identity, column=fix.column, value=fix.old_value)
                new_where = self._where_sql(identity, column=fix.column, value=fix.new_value)
                precondition_sql = f"SELECT COUNT(*) FROM {self.relation} WHERE {old_where}"
                forward_sql = (
                    f"UPDATE {self.relation} SET {quote_identifier(fix.column)} = "
                    f"{sql_literal(fix.new_value)} WHERE {old_where}"
                )
                rollback_sql = (
                    f"UPDATE {self.relation} SET {quote_identifier(fix.column)} = "
                    f"{sql_literal(fix.old_value)} WHERE {new_where}"
                )
                verification_sql = (f"SELECT COUNT(*) FROM {self.relation} WHERE {new_where}",)
            operations.append(
                PatchOperation.from_cell_fix(
                    fix,
                    relation=self.relation,
                    row_identity=identity,
                    reason=proposed.reason,
                    confidence=proposed.confidence,
                    provenance=proposed.provenance,
                    precondition_sql=precondition_sql,
                    forward_sql=forward_sql,
                    rollback_sql=rollback_sql,
                    verification_sql=verification_sql,
                )
            )

        reason = (
            "DuckDB patch plan is apply-ready."
            if operations and all(operation.row_identity.stable for operation in operations)
            else "DuckDB patch plan is dry-run only until row identity is configured."
        )
        return PatchPlan.new(
            backend=self.backend,
            target=self.target,
            relation=self.relation,
            row_identity_columns=self.row_identity_columns,
            operations=tuple(operations),
            safety_verdict=safety_verdict,
            rows_scanned=row_count(table),
            reason=reason,
            touched_constraints=touched_constraints,
            smt_obligations=smt_obligations,
            audit_metadata={"database": str(self.database_path)},
            authoritative_schema_present=authoritative_schema_present,
            authoritative_columns=tuple(sorted(covered_columns)),
        )

    def _relation_rows(self, connection: Any) -> list[dict[str, str]]:
        order_by = ""
        if self.row_identity_columns:
            quoted = ", ".join(quote_identifier(column) for column in self.row_identity_columns)
            order_by = f" ORDER BY {quoted}"
        cursor = connection.execute(f"SELECT * FROM {self.relation}{order_by}")
        columns = [str(item[0]) for item in cursor.description]
        return [
            {
                column: "" if value is None else str(value)
                for column, value in zip(columns, row, strict=True)
            }
            for row in cursor.fetchall()
        ]

    def _snapshot_bytes(self, plan: PatchPlan) -> bytes:
        with self._connect(read_only=True) as connection:
            rows = self._relation_rows(connection)
        payload = {
            "schema_version": "table_store_snapshot_v1",
            "backend": self.backend,
            "target": self.target,
            "relation": self.relation,
            "patch_plan_sha256": plan.sha256(),
            "rows": rows,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def _write_snapshot(self, state_root: Path, txn_id: str, payload: bytes) -> Path:
        snapshot_path = state_root.resolve() / ".dataforge" / "snapshots" / f"{txn_id}.bin"
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with snapshot_path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as exc:
            raise TableStoreError(f"Transaction snapshot already exists: {snapshot_path}") from exc
        return snapshot_path

    def _execute_scalar_int(self, connection: Any, sql: str) -> int:
        value = connection.execute(sql).fetchone()[0]
        return int(value)

    def apply_patch_plan(
        self,
        plan: PatchPlan,
        *,
        state_root: Path | None = None,
        allow_unproven_autoapply: bool = False,
    ) -> StoreApplyReceipt:
        """Apply a verified DuckDB patch plan inside a transaction."""
        if plan.backend != self.backend:
            raise TableStoreError(f"Patch plan backend {plan.backend!r} does not match DuckDB.")
        if not plan.apply_supported or not plan.reversible:
            raise TableStoreError(plan.reason)
        enforce_plan_proven_only(plan, allow_unproven_autoapply=allow_unproven_autoapply)
        enforce_plan_constraint_checkable_only(plan)
        state_dir = (state_root or Path.cwd()).resolve()
        txn_id = generate_txn_id()
        snapshot_bytes = self._snapshot_bytes(plan)
        snapshot_path = self._write_snapshot(state_dir, txn_id, snapshot_bytes)
        transaction = RepairTransaction(
            txn_id=txn_id,
            created_at=datetime.now(UTC),
            source_path=self.target,
            source_sha256=sha256_bytes(snapshot_bytes),
            source_snapshot_path=str(snapshot_path),
            fixes=[],
            applied=False,
            source_kind="table_store",
            backend=self.backend,
            patch_plan=plan.model_dump(mode="json"),
        )
        try:
            log_path = append_created_transaction(transaction, log_root=state_dir)
        except Exception:
            snapshot_path.unlink(missing_ok=True)
            raise

        with self._connect(read_only=False) as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                for sql in plan.preflight_probes:
                    if self._execute_scalar_int(connection, sql) != 1:
                        raise TableStoreError(
                            f"Preflight probe did not match exactly one row: {sql}"
                        )
                for sql in plan.forward_sql:
                    connection.execute(sql)
                for sql in plan.verification_queries:
                    if self._execute_scalar_int(connection, sql) != 1:
                        raise TableStoreError(f"Verification query failed: {sql}")
                post_rows = self._relation_rows(connection)
                post_sha256 = sha256_bytes(
                    json.dumps(post_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
                )
                connection.execute("COMMIT")
                append_applied_event(log_path, txn_id, post_sha256=post_sha256)
            except Exception:
                connection.execute("ROLLBACK")
                raise

        return StoreApplyReceipt(
            ok=True,
            txn_id=txn_id,
            backend=self.backend,
            target=self.target,
            patch_plan_sha256=plan.sha256(),
            post_state_sha256=post_sha256,
            reason=f"Applied {len(plan.operations)} DuckDB operation(s).",
        )

    def revert_transaction(self, transaction: RepairTransaction, *, log_path: Path) -> None:
        """Execute rollback SQL for a recorded DuckDB table-store transaction."""
        if transaction.patch_plan is None:
            raise TableStoreError("Table-store transaction is missing its patch plan.")
        plan = PatchPlan.model_validate(transaction.patch_plan)
        if plan.backend != self.backend:
            raise TableStoreError(f"Patch plan backend {plan.backend!r} does not match DuckDB.")
        with self._connect(read_only=False) as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                for sql in reversed(plan.rollback_sql):
                    connection.execute(sql)
                connection.execute("COMMIT")
                append_reverted_event(log_path, transaction.txn_id)
            except Exception:
                connection.execute("ROLLBACK")
                raise


def load_duckdb_transaction(log_path: Path) -> tuple[DuckDBStore, RepairTransaction]:
    """Load a DuckDB table-store transaction and recreate its store."""
    transaction = load_transaction(log_path)
    if transaction.patch_plan is None:
        raise TableStoreError("Transaction is missing a patch plan.")
    plan = PatchPlan.model_validate(transaction.patch_plan)
    database = plan.audit_metadata.get("database")
    if not database:
        raise TableStoreError("DuckDB transaction is missing database metadata.")
    return (
        DuckDBStore(
            database_path=Path(database),
            relation=plan.relation,
            row_identity_columns=plan.row_identity_columns,
            target=plan.target,
        ),
        transaction,
    )
