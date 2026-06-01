"""CLI subcommand: ``dataforge revert <txn_id>``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from dataforge.transactions.log import sha256_file, verify_transaction_log
from dataforge.transactions.revert import TransactionRevertError, revert_transaction

_console = Console(stderr=True)


def revert(
    txn_id: Annotated[
        str,
        typer.Argument(help="Transaction identifier to revert."),
    ],
    search_root: Annotated[
        Path | None,
        typer.Option(
            "--search-root",
            help="Root directory used to locate the transaction log.",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print the revert receipt as JSON."),
    ] = False,
) -> None:
    """Revert a previously applied DataForge repair transaction."""
    try:
        transaction = revert_transaction(txn_id, search_root=search_root)
    except TransactionRevertError as exc:
        if json_output:
            typer.echo(
                json.dumps(
                    {
                        "schema_version": "repair_revert_receipt_v1",
                        "ok": False,
                        "txn_id": txn_id,
                        "error": str(exc),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            raise typer.Exit(code=1) from exc
        _console.print(Panel(f"[bold red]{exc}[/bold red]", title="Revert Error", style="red"))
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        if json_output:
            typer.echo(
                json.dumps(
                    {
                        "schema_version": "repair_revert_receipt_v1",
                        "ok": False,
                        "txn_id": txn_id,
                        "error": str(exc),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            raise typer.Exit(code=2) from exc
        _console.print(Panel(f"[bold red]{exc}[/bold red]", title="Revert Error", style="red"))
        raise typer.Exit(code=2) from exc

    audit_report = verify_transaction_log(txn_id, search_root=search_root)
    if json_output:
        if transaction.source_kind == "table_store":
            typer.echo(
                json.dumps(
                    {
                        "schema_version": "repair_revert_receipt_v1",
                        "ok": True,
                        "txn_id": transaction.txn_id,
                        "source_kind": transaction.source_kind,
                        "backend": transaction.backend,
                        "source_path": transaction.source_path,
                        "expected_source_sha256": transaction.source_sha256,
                        "restored_source_sha256": None,
                        "reverted_at": transaction.reverted_at.isoformat()
                        if transaction.reverted_at is not None
                        else None,
                        "audit_verdict": audit_report.verdict.value,
                        "revert_event_sha256": audit_report.head_sha256,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            raise typer.Exit(code=0)
        source_path = Path(transaction.source_path)
        typer.echo(
            json.dumps(
                {
                    "schema_version": "repair_revert_receipt_v1",
                    "ok": True,
                    "txn_id": transaction.txn_id,
                    "source_path": transaction.source_path,
                    "expected_source_sha256": transaction.source_sha256,
                    "restored_source_sha256": sha256_file(source_path),
                    "reverted_at": transaction.reverted_at.isoformat()
                    if transaction.reverted_at is not None
                    else None,
                    "audit_verdict": audit_report.verdict.value,
                    "revert_event_sha256": audit_report.head_sha256,
                },
                indent=2,
                sort_keys=True,
            )
        )
        raise typer.Exit(code=0)

    title = (
        "Table Store Revert Complete"
        if transaction.source_kind == "table_store"
        else "Revert Complete"
    )
    Console().print(
        Panel(
            (
                f"[green]Source restored successfully.[/green]\n"
                f"Transaction: [bold]{transaction.txn_id}[/bold]"
            ),
            title=title,
            style="green",
        )
    )
