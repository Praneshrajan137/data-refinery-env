"""Typer application entrypoint for DataForge.

Each CLI subcommand is defined in its own module under ``dataforge.cli.*``
and registered here. The ``app`` object is the entry point referenced by
``[project.scripts]`` in ``pyproject.toml``.
"""

import typer

from dataforge.cli.bench import bench
from dataforge.cli.profile import profile
from dataforge.cli.repair import repair
from dataforge.cli.revert import revert

app: typer.Typer = typer.Typer(
    help="DataForge — AI-powered data-quality detection and repair.",
    no_args_is_help=True,
)


@app.callback(invoke_without_command=True)
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show version and exit.",
        is_eager=True,
    ),
) -> None:
    """DataForge — AI-powered data-quality detection and repair."""
    if version:
        from dataforge import __version__

        typer.echo(f"dataforge {__version__}")
        raise typer.Exit()


app.command(name="profile")(profile)
app.command(name="repair")(repair)
app.command(name="revert")(revert)
app.command(name="bench")(bench)
