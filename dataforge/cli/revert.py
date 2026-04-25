"""CLI subcommand: ``dataforge revert <txn_id>``."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from dataforge.transactions.revert import TransactionRevertError, revert_transaction

_console = Console(stderr=True)


def revert(
    txn_id: Annotated[
        str,
        typer.Argument(help="Transaction identifier to revert."),
    ],
) -> None:
    """Revert a previously applied DataForge repair transaction."""
    try:
        transaction = revert_transaction(txn_id)
    except TransactionRevertError as exc:
        _console.print(Panel(f"[bold red]{exc}[/bold red]", title="Revert Error", style="red"))
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        _console.print(Panel(f"[bold red]{exc}[/bold red]", title="Revert Error", style="red"))
        raise typer.Exit(code=2) from exc

    Console().print(
        Panel(
            (
                f"[green]Source restored successfully.[/green]\n"
                f"Transaction: [bold]{transaction.txn_id}[/bold]"
            ),
            title="Revert Complete",
            style="green",
        )
    )
