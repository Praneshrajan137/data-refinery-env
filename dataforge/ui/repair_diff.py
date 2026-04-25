"""Rich rendering for repair proposals and transaction output."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dataforge.repairers.base import ProposedFix


def render_repair_diff(
    fixes: list[ProposedFix],
    console: Console | None = None,
    *,
    file_path: str = "",
) -> None:
    """Render a rich table describing proposed repairs."""
    target_console = console or Console()
    title = "Proposed Repairs"
    if file_path:
        title = f"{title}  |  {file_path}"
    target_console.print(Panel(title, style="bold cyan", expand=True))

    if not fixes:
        target_console.print(
            Panel("[yellow]No fixes proposed.[/yellow]", title="Result", style="yellow")
        )
        return

    table = Table(title="Repair Diff", show_lines=True, header_style="bold magenta")
    table.add_column("Row", justify="right", width=5)
    table.add_column("Column", style="cyan", min_width=12)
    table.add_column("Old", min_width=12)
    table.add_column("New", min_width=12)
    table.add_column("Detector", min_width=14)
    table.add_column("Confidence", justify="right", min_width=10)
    table.add_column("Provenance", min_width=13)

    for proposed in fixes:
        table.add_row(
            str(proposed.fix.row),
            proposed.fix.column,
            proposed.fix.old_value,
            proposed.fix.new_value,
            proposed.fix.detector_id,
            f"{proposed.confidence:.0%}",
            proposed.provenance,
        )

    target_console.print(table)
