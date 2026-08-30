"""Typer application entrypoint for DataForge.

Each CLI subcommand is defined in its own module under ``dataforge.cli.*``. They are registered
**lazily**: the module is imported only when its command is actually invoked.

Why, with the measurement. Until 2026-08-28 this file imported all twelve subcommand modules at
module scope so Typer could read their signatures. That made ``dataforge --version`` cost
891-940 ms against a ~100 ms bare interpreter, because registering ``repair`` pulls in
``dataforge.engine.repair``, registering ``audit`` pulls in ``dataforge.transactions.txn``, and
registering ``constraints`` pulls in the whole ``textual`` TUI framework -- to print a version
string. The suite launches the CLI as a subprocess 39 times and the release gate 22 more, so that
was a first-order term in the verification loop, not a micro-optimisation.

Deferring individual heavy imports inside those modules was measured and rejected: the cost is
spread across many product modules (``transactions.txn`` 24 ms self, ``engine.repair`` 14 ms,
``verifier.schema`` 10 ms, plus z3 and pydantic model construction), so no small set of deferrals
recovers it. The only thing that does is not importing the product at all.

The table below is a second source of truth about what commands exist, which is a real risk. Two
things hold it to reality:

* ``tests/unit/test_cli_import_cost.py`` resolves every entry, so a wrong module path fails a
  test rather than a user's invocation;
* ``scripts/ci/readme_truth.py`` verifies each command claimed in the README resolves here. That
  check got *stronger* with this change, not weaker -- it previously confirmed only that a name
  was registered, and now confirms the target imports and builds.

``--help`` on the top-level group necessarily resolves every command to show its short help, so
it still pays the full import. That is the correct trade: help is interactive and rare, while
``--version`` and single-command invocations are what automation runs in a loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import typer
import typer.core
import typer.main
import typer.models

if TYPE_CHECKING:
    import click

#: Command name -> ``"module:attribute"`` for a plain function command.
_LAZY_COMMANDS: Final[dict[str, str]] = {
    "profile": "dataforge.cli.profile:profile",
    "calibrate": "dataforge.cli.calibrate:calibrate",
    "quickstart": "dataforge.cli.quickstart:quickstart",
    "repair": "dataforge.cli.repair:repair",
    "verify-apply": "dataforge.cli.verify_apply:verify_apply",
    "revert": "dataforge.cli.revert:revert",
    "audit": "dataforge.cli.audit:audit",
    "bench": "dataforge.cli.bench:bench",
    "watch": "dataforge.cli.watch:watch",
    "measure-on-my-table": "dataforge.cli.measure:measure_on_my_table_command",
}

#: Command name -> ``"module:attribute"`` for a nested ``typer.Typer`` sub-application.
_LAZY_GROUPS: Final[dict[str, str]] = {
    "attest": "dataforge.cli.attest:attest_app",
    "constraints": "dataforge.cli.constraints:constraints_app",
    "release": "dataforge.cli.release:release_app",
}


def command_names() -> set[str]:
    """Return every command name, without importing any subcommand module.

    This is the cheap surface. Callers that need the real command object -- to inspect options,
    or to prove the table is not lying -- use :func:`resolve_command`, which pays the import.
    """
    return set(_LAZY_COMMANDS) | set(_LAZY_GROUPS)


def _load(target: str) -> Any:
    """Import ``"module:attribute"`` and return the attribute.

    Returns ``Any`` because the two call sites want different types -- a ``typer.Typer`` for a
    group and a callable for a command -- and each narrows immediately, one by ``isinstance`` and
    one by handing the result to ``CommandInfo``.
    """
    module_name, _, attribute = target.partition(":")
    module = __import__(module_name, fromlist=[attribute or "__name__"])
    return getattr(module, attribute)


def resolve_command(name: str) -> click.Command | None:
    """Build the click command for ``name``, importing its module.

    Returns:
        The command, or ``None`` if the name is not declared.
    """
    if name in _LAZY_GROUPS:
        sub_app = _load(_LAZY_GROUPS[name])
        assert isinstance(sub_app, typer.Typer)
        # get_group, NOT get_command. `get_command` COLLAPSES a single-command sub-application
        # into that bare command, so `dataforge constraints review <path>` parsed "review" as a
        # positional argument and exited 2. `constraints` has exactly one command today
        # (`review`), while `attest` and `release` have three and four -- so `get_command` would
        # have worked for two of the three groups and silently broken the third, and would break
        # any group that later dropped to one command. `get_group` always returns a group.
        group = typer.main.get_group(sub_app)
        group.name = name
        return group
    if name in _LAZY_COMMANDS:
        callback = _load(_LAZY_COMMANDS[name])
        info = typer.models.CommandInfo(name=name, callback=callback)
        return typer.main.get_command_from_info(
            info,
            pretty_exceptions_short=app.pretty_exceptions_short,
            rich_markup_mode=app.rich_markup_mode,
        )
    return None


class _LazyTyperGroup(typer.core.TyperGroup):
    """A Typer group that resolves subcommands on demand.

    Both methods fall through to the eager registry first, so a command added the ordinary way
    with ``@app.command()`` keeps working. That matters for third parties and for any future
    command that genuinely needs eager registration.
    """

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Return every command name, eager and lazy, without importing the lazy ones."""
        return sorted(set(super().list_commands(ctx)) | command_names())

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """Return one command, importing its module only if it is lazy and requested."""
        eager = super().get_command(ctx, cmd_name)
        if eager is not None:
            return eager
        return resolve_command(cmd_name)


app: typer.Typer = typer.Typer(
    cls=_LazyTyperGroup,
    help="DataForge - AI-powered data-quality detection and repair.",
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
    """DataForge - AI-powered data-quality detection and repair."""
    if version:
        from dataforge import __version__

        typer.echo(f"dataforge {__version__}")
        raise typer.Exit()


def as_click_command() -> click.Group:
    """Return the underlying click group.

    Exposed so tests and truth checks can ask the group what commands it lists, rather than
    trusting the table above to agree with it.
    """
    command = typer.main.get_command(app)
    assert isinstance(command, typer.core.TyperGroup)
    return command
