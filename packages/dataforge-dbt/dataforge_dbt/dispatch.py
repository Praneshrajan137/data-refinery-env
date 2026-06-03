"""Python entrypoint used by DataForge dbt macros and integration tests."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from dataforge import (
    DuckDBStore,
    Issue,
    Severity,
    TableStoreError,
    read_csv,
    run_all_detectors,
    run_table_store_repair,
)
from dataforge.table import TableLike
from pydantic import BaseModel, Field

from dataforge_dbt.config import DataForgeDbtConfigError, load_config

_LOGGER = logging.getLogger("dataforge_dbt")


class DataForgeDbtDispatchError(RuntimeError):
    """Raised when a DataForge dbt dispatch invocation cannot complete.

    Args:
        message: User-facing failure reason.
    """


@dataclass(frozen=True)
class DispatchRequest:
    """Inputs for one DataForge dbt post-hook invocation.

    Args:
        relation: dbt relation identifier for logs and transaction metadata.
        column: Column to inspect for DataForge findings.
        mode: Hook behavior requested by the dbt macro.
        input_csv: Optional CSV export of the model relation. When omitted,
            DataForge exports the relation from dbt's adapter artifacts.
        target_path: dbt target directory.
        project_dir: dbt project directory.
        select: Optional dbt selector to run before exporting.
        run_dbt: Whether to run ``dbt run`` before exporting.
        profiles_path: Optional dbt profiles.yml path.
        profile_name: Optional dbt profile name.
    """

    relation: str
    column: str
    mode: str
    input_csv: Path | None
    target_path: Path
    project_dir: Path | None = None
    select: str | None = None
    run_dbt: bool = False
    profiles_path: Path | None = None
    profile_name: str | None = None
    row_identity_columns: tuple[str, ...] = ()


class DbtTransaction(BaseModel):
    """Audit artifact written by ``mode='apply'`` for dbt hook runs.

    Args:
        relation: dbt relation that was inspected.
        column: Column passed to ``dataforge_repair``.
        mode: DataForge dbt mode that produced the artifact.
        created_at: UTC timestamp for the audit artifact.
        issues: Serialized DataForge issues detected in the relation.
    """

    relation: str = Field(min_length=1)
    column: str = Field(min_length=1)
    mode: str = Field(pattern="^apply$")
    created_at: datetime
    issues: list[dict[str, object]] = Field(default_factory=list)
    patch_plan: dict[str, object] | None = None
    apply_receipt: dict[str, object] | None = None


def dispatch(request: DispatchRequest) -> list[Issue]:
    """Run DataForge detection for a dbt relation export.

    Args:
        request: Validated dispatch inputs from the macro or test harness.

    Returns:
        DataForge issues detected for the requested column.

    Raises:
        DataForgeDbtDispatchError: If inputs are missing or refuse mode blocks the run.
        DataForgeDbtConfigError: If configuration is invalid.
    """
    config = load_config(
        mode=request.mode,
        target_path=request.target_path,
        profiles_path=request.profiles_path,
        profile_name=request.profile_name,
    )
    if request.input_csv is None and request.project_dir is not None:
        return _dispatch_table_store_relation(request, config)

    input_csv = request.input_csv or _export_relation_csv(request)
    table = _read_input_csv(input_csv)
    if request.column not in table.columns:
        raise DataForgeDbtDispatchError(
            f"Column '{request.column}' does not exist in dbt relation '{request.relation}'."
        )

    issues = [
        issue for issue in run_all_detectors(table, schema=None) if issue.column == request.column
    ]
    _log_issues(relation=request.relation, column=request.column, issues=issues)

    if config.mode == "refuse" and any(issue.severity == Severity.UNSAFE for issue in issues):
        raise DataForgeDbtDispatchError(
            f"DataForge refuse mode blocked dbt relation '{request.relation}' because UNSAFE issues were detected."
        )

    if config.mode == "apply" and issues:
        _write_transaction(config.transaction_dir, request=request, issues=issues)

    return issues


def main(argv: list[str] | None = None) -> int:
    """Run the DataForge dbt dispatch command-line entrypoint.

    Args:
        argv: Optional argument vector for tests. When omitted, ``sys.argv`` is used.

    Returns:
        Process exit code.
    """
    _configure_logging()
    parser = _build_parser()
    args = parser.parse_args(argv)
    request = DispatchRequest(
        relation=args.relation,
        column=args.column,
        mode=args.mode,
        input_csv=Path(args.input_csv) if args.input_csv else None,
        target_path=Path(args.target_path),
        project_dir=Path(args.project_dir) if args.project_dir else None,
        select=args.select,
        run_dbt=args.run_dbt,
        profiles_path=Path(args.profiles_path) if args.profiles_path else None,
        profile_name=args.profile_name,
        row_identity_columns=tuple(args.row_id or ()),
    )
    try:
        dispatch(request)
    except (DataForgeDbtConfigError, DataForgeDbtDispatchError) as exc:
        _LOGGER.error("DATAFORGE_DBT error relation=%s message=%s", request.relation, exc)
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for ``dataforge-dbt``.

    Returns:
        Configured argument parser.
    """
    parser = argparse.ArgumentParser(prog="dataforge-dbt")
    parser.add_argument("--relation", required=True)
    parser.add_argument("--column", required=True)
    parser.add_argument("--mode", required=True, choices=["dry_run", "apply", "refuse"])
    parser.add_argument("--input-csv")
    parser.add_argument("--target-path", required=True)
    parser.add_argument("--project-dir")
    parser.add_argument("--select")
    parser.add_argument("--run-dbt", action="store_true")
    parser.add_argument("--profiles-path")
    parser.add_argument("--profile-name")
    parser.add_argument("--row-id", action="append", help="Stable row identity column.")
    return parser


def _configure_logging() -> None:
    """Configure stable process logging for dbt and pytest capture.

    Returns:
        None.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


def _read_input_csv(path: Path) -> TableLike:
    """Read a dbt relation CSV export using DataForge-safe dtype handling.

    Args:
        path: CSV file produced from a dbt model relation.

    Returns:
        String-preserving DataForge table.

    Raises:
        DataForgeDbtDispatchError: If the CSV does not exist or cannot be read.
    """
    if not path.exists():
        raise DataForgeDbtDispatchError(f"DataForge dbt input CSV does not exist: {path}")
    try:
        return read_csv(path)
    except OSError as exc:
        raise DataForgeDbtDispatchError(
            f"Could not read DataForge dbt input CSV '{path}': {exc}"
        ) from exc


_SAFE_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*|\.\"[^\"]+\")*$")


def _export_relation_csv(request: DispatchRequest) -> Path:
    """Export a dbt relation to a temporary CSV through the configured adapter."""
    project_dir = request.project_dir
    if project_dir is None:
        raise DataForgeDbtDispatchError(
            "Either --input-csv or --project-dir must be supplied for DataForge dbt dispatch."
        )
    profiles_path = _resolve_profiles_path(request.profiles_path)
    profile_name = request.profile_name or _read_project_profile(project_dir)
    if request.run_dbt:
        _run_dbt(project_dir, profiles_path.parent, request.select)
    output_dir = request.target_path / "dataforge_exports"
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_relation = "".join(
        ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in request.relation
    )
    export_path = output_dir / f"{safe_relation}.csv"
    _export_duckdb_relation(
        relation=request.relation,
        output_path=export_path,
        profiles_path=profiles_path,
        profile_name=profile_name,
    )
    return export_path


def _dispatch_table_store_relation(
    request: DispatchRequest,
    config: Any,
) -> list[Issue]:
    """Run a dbt relation through the table-store patch-plan path."""
    project_dir = request.project_dir
    if project_dir is None:
        raise DataForgeDbtDispatchError(
            "Either --input-csv or --project-dir must be supplied for DataForge dbt dispatch."
        )
    profiles_path = _resolve_profiles_path(request.profiles_path)
    profile_name = request.profile_name or _read_project_profile(project_dir)
    if request.run_dbt:
        _run_dbt(project_dir, profiles_path.parent, request.select)

    output = _read_duckdb_output(profiles_path, profile_name)
    if output.get("type") != "duckdb":
        raise DataForgeDbtDispatchError(
            "Native DataForge dbt apply currently supports dbt-duckdb only."
        )
    database_path = output.get("path")
    if not isinstance(database_path, str) or not database_path:
        raise DataForgeDbtDispatchError("dbt-duckdb profile output is missing path.")

    row_ids = request.row_identity_columns or config.row_identity_columns or ("id",)
    try:
        store = DuckDBStore(
            database_path=Path(database_path),
            relation=request.relation,
            row_identity_columns=row_ids,
        )
        store_result = run_table_store_repair(
            store,
            mode="dry_run" if config.mode == "refuse" else config.mode,
            schema=None,
            state_root=request.target_path,
            only_column=request.column,
        )
    except TableStoreError as exc:
        raise DataForgeDbtDispatchError(str(exc)) from exc

    issues = store_result.issues
    _log_issues(relation=request.relation, column=request.column, issues=issues)

    if config.mode == "refuse" and any(issue.severity == Severity.UNSAFE for issue in issues):
        raise DataForgeDbtDispatchError(
            f"DataForge refuse mode blocked dbt relation '{request.relation}' because UNSAFE issues were detected."
        )

    if config.mode == "apply" and issues:
        _write_transaction(
            config.transaction_dir,
            request=request,
            issues=issues,
            patch_plan=store_result.patch_plan.model_dump(mode="json"),
            apply_receipt=store_result.apply_receipt.model_dump(mode="json")
            if store_result.apply_receipt is not None
            else None,
        )
    return issues


def _resolve_profiles_path(profiles_path: Path | None) -> Path:
    """Resolve dbt profiles.yml from a file path, directory, or default location."""
    if profiles_path is None:
        candidate = Path.home() / ".dbt" / "profiles.yml"
    elif profiles_path.is_dir():
        candidate = profiles_path / "profiles.yml"
    else:
        candidate = profiles_path
    if not candidate.exists():
        raise DataForgeDbtDispatchError(f"dbt profiles.yml not found: {candidate}")
    return candidate


def _read_project_profile(project_dir: Path) -> str:
    """Read the dbt profile name from dbt_project.yml."""
    project_file = project_dir / "dbt_project.yml"
    if not project_file.exists():
        raise DataForgeDbtDispatchError(f"dbt_project.yml not found: {project_file}")
    payload = yaml.safe_load(project_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not payload.get("profile"):
        raise DataForgeDbtDispatchError("dbt_project.yml must define a profile name.")
    return str(payload["profile"])


def _run_dbt(project_dir: Path, profiles_dir: Path, selector: str | None) -> None:
    """Run dbt before exporting the selected relation."""
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "run",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    if selector:
        command.extend(["--select", selector])
    env = os.environ.copy()
    result = subprocess.run(
        command,
        cwd=project_dir,
        env=env,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    if result.returncode != 0:
        raise DataForgeDbtDispatchError(
            "dbt run failed before DataForge export.\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )


def _export_duckdb_relation(
    *,
    relation: str,
    output_path: Path,
    profiles_path: Path,
    profile_name: str,
) -> None:
    """Export a relation from a dbt-duckdb profile."""
    if not _safe_relation(relation):
        raise DataForgeDbtDispatchError(f"Unsafe relation identifier: {relation}")
    output = _read_duckdb_output(profiles_path, profile_name)
    if output.get("type") != "duckdb":
        raise DataForgeDbtDispatchError(
            "DataForge dbt relation export currently supports dbt-duckdb profiles only."
        )
    database_path = output.get("path")
    if not isinstance(database_path, str) or not database_path:
        raise DataForgeDbtDispatchError("dbt-duckdb profile output is missing path.")
    try:
        import duckdb
    except ImportError as exc:
        raise DataForgeDbtDispatchError(
            "DuckDB export requires dbt-duckdb or duckdb to be installed."
        ) from exc
    try:
        fd, temp_name = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        temp_output = Path(temp_name)
        temp_output.unlink(missing_ok=True)
        with duckdb.connect(database_path, read_only=True) as connection:
            escaped_output = str(temp_output).replace("'", "''")
            connection.execute(
                f"COPY (SELECT * FROM {relation}) TO '{escaped_output}' (HEADER, DELIMITER ',')"
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_output.replace(output_path)
    except Exception as exc:
        with suppress(NameError, FileNotFoundError):
            temp_output.unlink()
        raise DataForgeDbtDispatchError(
            f"Could not export dbt relation '{relation}' from DuckDB: {exc}"
        ) from exc


def _read_duckdb_output(profiles_path: Path, profile_name: str) -> dict[str, Any]:
    """Read the active output block from dbt profiles.yml."""
    payload = yaml.safe_load(profiles_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DataForgeDbtDispatchError("dbt profiles.yml must be a mapping.")
    profile = payload.get(profile_name)
    if not isinstance(profile, dict):
        raise DataForgeDbtDispatchError(f"dbt profile '{profile_name}' not found.")
    target = str(profile.get("target", "dev"))
    outputs = profile.get("outputs")
    if not isinstance(outputs, dict) or not isinstance(outputs.get(target), dict):
        raise DataForgeDbtDispatchError(
            f"dbt profile '{profile_name}' output '{target}' not found."
        )
    return dict(outputs[target])


def _safe_relation(relation: str) -> bool:
    """Return whether a dbt relation identifier is safe to embed in SQL."""
    return bool(_SAFE_RELATION_RE.match(relation))


def _log_issues(*, relation: str, column: str, issues: list[Issue]) -> None:
    """Log detected issues in a stable dbt-testable format.

    Args:
        relation: dbt relation that was inspected.
        column: Column passed to the macro.
        issues: DataForge issues to log.

    Returns:
        None.
    """
    if not issues:
        _LOGGER.info("DATAFORGE_DBT no_issues relation=%s column=%s", relation, column)
        return

    for issue in issues:
        _LOGGER.warning(
            "DATAFORGE_DBT issue relation=%s column=%s row=%s type=%s severity=%s confidence=%.2f reason=%s",
            relation,
            column,
            issue.row,
            issue.issue_type,
            issue.severity.value,
            issue.confidence,
            issue.reason,
        )


def _write_transaction(
    transaction_dir: Path,
    *,
    request: DispatchRequest,
    issues: list[Issue],
    patch_plan: dict[str, object] | None = None,
    apply_receipt: dict[str, object] | None = None,
) -> Path:
    """Write a dbt-scoped DataForge transaction artifact.

    Args:
        transaction_dir: Directory under dbt target for transaction files.
        request: Original dispatch request.
        issues: Issues detected during the hook.

    Returns:
        Path to the written JSONL transaction artifact.

    Raises:
        DataForgeDbtDispatchError: If the transaction file cannot be written.
    """
    transaction_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC)
    safe_relation = "".join(
        ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in request.relation
    )
    output_path = transaction_dir / f"dataforge-dbt-{safe_relation}-{timestamp:%Y%m%d%H%M%S}.jsonl"
    transaction = DbtTransaction(
        relation=request.relation,
        column=request.column,
        mode="apply",
        created_at=timestamp,
        issues=[issue.model_dump(mode="json") for issue in issues],
        patch_plan=patch_plan,
        apply_receipt=apply_receipt,
    )
    try:
        output_path.write_text(
            json.dumps(transaction.model_dump(mode="json"), sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError as exc:
        raise DataForgeDbtDispatchError(
            f"Could not write DataForge dbt transaction file: {exc}"
        ) from exc
    _LOGGER.info("DATAFORGE_DBT transaction_written path=%s", output_path)
    return output_path


if __name__ == "__main__":
    sys.exit(main())
