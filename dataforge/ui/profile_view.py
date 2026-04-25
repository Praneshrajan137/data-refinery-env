"""Rich-based terminal rendering for the ``dataforge profile`` command.

This module contains **rendering only** — no business logic, no data loading,
no detector invocation.  It receives a list of :class:`Issue` objects and
renders them as a Rich table with color-coded severity.
"""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dataforge.detectors.base import Issue, Severity

# Severity-to-color mapping for the Rich table.
_SEVERITY_STYLE = {
    Severity.SAFE: "green",
    Severity.REVIEW: "yellow",
    Severity.UNSAFE: "bold red",
}

# Severity sort key: UNSAFE first, then REVIEW, then SAFE.
_SEVERITY_ORDER = {Severity.UNSAFE: 0, Severity.REVIEW: 1, Severity.SAFE: 2}


def render_profile_table(
    issues: list[Issue],
    console: Console | None = None,
    *,
    file_path: str = "",
) -> None:
    """Render detected issues as a rich-formatted terminal table.

    The table is sorted by severity (UNSAFE first) then confidence
    (highest first).  Each severity level is color-coded.

    Args:
        issues: List of Issue objects to display.
        console: Optional Rich Console instance (for testing / capture).
            If None, a new Console is created.
        file_path: Optional file path to display in the header panel.

    Example:
        >>> from dataforge.detectors.base import Issue, Severity
        >>> issues = [
        ...     Issue(row=3, column="price", issue_type="decimal_shift",
        ...           severity=Severity.REVIEW, confidence=0.92,
        ...           actual="1020.0", expected="102.0",
        ...           reason="Value appears 10x too large"),
        ... ]
        >>> render_profile_table(issues)  # doctest: +SKIP
    """
    if console is None:
        console = Console()

    # Header panel.
    header_text = "DataForge Profile Report"
    if file_path:
        header_text += f"  |  {file_path}"
    console.print(
        Panel(
            header_text,
            style="bold cyan",
            expand=True,
        )
    )

    if not issues:
        console.print(
            Panel(
                "[green]No issues detected.[/green]",
                title="Result",
                style="green",
            )
        )
        return

    # Sort: UNSAFE first, then REVIEW, then SAFE; highest confidence first.
    sorted_issues = sorted(
        issues,
        key=lambda i: (_SEVERITY_ORDER[i.severity], -i.confidence),
    )

    # Build the table.
    table = Table(
        title="Detected Issues",
        show_lines=True,
        header_style="bold magenta",
        title_style="bold white",
    )
    table.add_column("Row", style="dim", justify="right", width=5)
    table.add_column("Column", style="cyan", min_width=12)
    table.add_column("Issue Type", style="white", min_width=14)
    table.add_column("Severity", justify="center", min_width=8)
    table.add_column("Confidence", justify="right", min_width=10)
    table.add_column("Reason", min_width=30)

    for issue in sorted_issues:
        severity_style = _SEVERITY_STYLE[issue.severity]
        severity_text = Text(issue.severity.value.upper(), style=severity_style)

        confidence_pct = f"{issue.confidence:.0%}"

        table.add_row(
            str(issue.row),
            issue.column,
            issue.issue_type,
            severity_text,
            confidence_pct,
            issue.reason,
        )

    console.print(table)

    # Summary panel.
    total = len(sorted_issues)
    by_severity: dict[Severity, int] = {}
    for issue in sorted_issues:
        by_severity[issue.severity] = by_severity.get(issue.severity, 0) + 1

    summary_parts: list[str] = [f"[bold]{total}[/bold] issues found"]
    for sev in (Severity.UNSAFE, Severity.REVIEW, Severity.SAFE):
        count = by_severity.get(sev, 0)
        if count > 0:
            style = _SEVERITY_STYLE[sev]
            summary_parts.append(f"[{style}]{sev.value.upper()}: {count}[/{style}]")

    console.print(
        Panel(
            "  |  ".join(summary_parts),
            title="Summary",
            style="dim",
        )
    )
