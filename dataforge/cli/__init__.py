"""DataForge15 CLI - Modern, service-based interface.

Rebuilt from scratch with clean separation of concerns:
- Services handle business logic
- CLI layer handles user interaction
- Exceptions provide structured error handling
"""

import typer
from rich.console import Console

from dataforge.config import DataForgeConfig

# Create console for output
console = Console()

# Create Typer app
app = typer.Typer(
    help="DataForge15 - AI-powered data-quality detection and repair.",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)


@app.callback(invoke_without_command=True)
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show version and exit.",
        is_eager=True,
    ),
) -> None:
    """DataForge15 - Intelligent CSV data quality and repair."""
    if version:
        from dataforge import __version__

        typer.echo(f"dataforge15 {__version__}")
        raise typer.Exit()


@app.command()
def profile(
    csv_file: str = typer.Argument(
        ..., help="Path to CSV file to profile"
    ),
    schema_file: str = typer.Option(
        None, "--schema", "-s", help="Path to schema file (optional)"
    ),
    output_format: str = typer.Option(
        "table", "--format", "-f", help="Output format (table, json, csv)"
    ),
) -> None:
    """Profile a CSV file for data quality issues."""
    from dataforge.io import CSVReader, SchemaStore
    from dataforge.services import ProfileService

    try:
        config = DataForgeConfig(input_csv=csv_file)
        profiler = ProfileService(config)

        # Load schema if provided
        schema = None
        if schema_file:
            schema = SchemaStore.load(schema_file)

        # Profile the file
        result = profiler.profile_file(csv_file, schema)

        # Display results
        console.print(
            f"[bold cyan]Profile Results[/bold cyan] "
            f"({result.row_count} rows, {result.column_count} columns)"
        )

        if result.schema:
            console.print(f"\n[bold]Schema[/bold]\n{result.schema}")

        if result.total_issues > 0:
            console.print(f"\n[bold yellow]Issues Found: {result.total_issues}[/bold yellow]")
            for detector_name, issues in result.issues.items():
                if issues:
                    console.print(f"\n{detector_name}:")
                    for issue in issues[:10]:  # Show first 10
                        console.print(f"  • {issue}")
        else:
            console.print("\n[bold green]No issues detected![/bold green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def repair(
    csv_file: str = typer.Argument(
        ..., help="Path to CSV file to repair"
    ),
    output_file: str = typer.Option(
        None, "--output", "-o", help="Path to output repaired CSV"
    ),
    schema_file: str = typer.Option(
        None, "--schema", "-s", help="Path to schema file"
    ),
    auto_apply: bool = typer.Option(
        False, "--auto", "-a", help="Automatically apply all repairs"
    ),
) -> None:
    """Detect and repair data quality issues."""
    from dataforge.io import CSVReader, CSVWriter, SchemaStore
    from dataforge.services import ProfileService, RepairEngine

    try:
        config = DataForgeConfig(input_csv=csv_file, output_csv=output_file)
        profiler = ProfileService(config)
        repair_engine = RepairEngine(config)
        csv_writer = CSVWriter()

        # Load schema if provided
        schema = None
        if schema_file:
            schema = SchemaStore.load(schema_file)

        # Profile file
        profile_result = profiler.profile_file(csv_file, schema)
        console.print(f"[cyan]Detected {profile_result.total_issues} issues[/cyan]")

        # Get all issues
        all_issues = profile_result.get_all_issues()

        # Create repair plan
        plan = repair_engine.plan_repairs(profile_result.data, all_issues, schema)
        console.print(
            f"[cyan]Proposed {len(plan.proposed_fixes)} fixes "
            f"({plan.get_total_confidence():.0%} confidence)[/cyan]"
        )

        # Apply repairs
        outcome = repair_engine.apply_repairs(profile_result.data, plan, auto_apply)

        if outcome.success:
            console.print(f"[green]✓ {repair_engine.get_repair_summary(outcome)}[/green]")

            # Save repaired data
            if output_file:
                csv_writer.write(output_file, outcome.repaired_data, overwrite=True)
                console.print(f"[green]Repaired CSV saved to {output_file}[/green]")
        else:
            console.print(
                f"[yellow]⚠ Some repairs failed: {repair_engine.get_repair_summary(outcome)}[/yellow]"
            )

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def revert(
    audit_file: str = typer.Argument(
        ..., help="Path to audit log file"
    ),
    txn_id: str = typer.Option(
        None, "--id", help="Transaction ID to revert"
    ),
) -> None:
    """Revert a previous repair transaction."""
    from dataforge.services import AuditService

    try:
        audit = AuditService()
        audit.initialize_log(audit_file)

        if not txn_id:
            # Show recent transactions
            recent = audit.get_history(10)
            console.print("[bold]Recent Transactions[/bold]")
            for txn in recent:
                console.print(f"  {txn.id[:8]}... {audit.get_transaction_summary(txn)}")
            return

        # Revert specific transaction
        if not audit.can_revert(txn_id):
            console.print(f"[red]Transaction {txn_id} not found[/red]")
            raise typer.Exit(1)

        reverse_txn = audit.revert_transaction(txn_id)
        console.print(f"[green]✓ Reverted transaction {txn_id}[/green]")
        console.print(f"  Reverse: {audit.get_transaction_summary(reverse_txn)}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def audit(
    audit_file: str = typer.Argument(
        ..., help="Path to audit log file"
    ),
    count: int = typer.Option(
        10, "--count", "-c", help="Number of recent transactions"
    ),
) -> None:
    """View audit trail of repairs."""
    from dataforge.services import AuditService

    try:
        audit = AuditService()
        audit.initialize_log(audit_file)

        history = audit.get_history(count)
        if not history:
            console.print("[yellow]No transactions recorded[/yellow]")
            return

        console.print("[bold cyan]Audit History[/bold cyan]")
        for txn in history:
            console.print(f"\n{audit.get_transaction_summary(txn)}")
            for row, col, old_val, new_val in txn.changes[:5]:
                console.print(f"  • Row {row}, '{col}': {old_val!r} → {new_val!r}")
            if len(txn.changes) > 5:
                console.print(f"  • ... and {len(txn.changes) - 5} more changes")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
