"""``dataforge --version`` must not import the product.

Measured on 2026-08-28, before this guard existed: ``dataforge --version`` took 891-940 ms, of
which ~100 ms was the interpreter. The other ~800 ms was ``dataforge.cli`` importing all twelve
subcommand modules to register them, which transitively pulled in ``dataforge.engine.repair``,
``dataforge.transactions.txn``, ``dataforge.verifier.schema``, z3 and the full ``textual`` TUI
framework -- to print a version string.

That cost is paid 39 times by the test suite's subprocess launches and 22 more times by the
release gate's CLI smokes, so it is a first-order term in the verification loop rather than a
micro-optimisation.

The guard asserts on a **class** of modules rather than on ``textual`` alone. Pinning the one
offender that motivated the work would let the next eager import in unchallenged, and the
regression is silent: nothing about a slow import fails a test.

Note what is deliberately NOT asserted: a millisecond threshold. Wall clock on a shared machine
is not reproducible enough to gate -- the claim ledger removed its only timing claim for exactly
that reason. Module presence is a discrete, reproducible proxy for the same property.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

#: Modules that must not be loaded by a command that does no data work. Each entry is here
#: because it was measurably imported by ``--version`` before the CLI became lazy.
FORBIDDEN_ON_VERSION: frozenset[str] = frozenset(
    {
        "textual",
        "textual.app",
        "z3",
        "dataforge.engine.repair",
        "dataforge.transactions.txn",
        "dataforge.transactions.log",
        "dataforge.verifier.schema",
        "dataforge.stores.patch_plan",
        "dataforge.calibration_session",
        "dataforge.schema_inference",
        "dataforge.release.full_vision",
        "pandas",
    }
)

_PROBE_TEMPLATE = """import json, sys, runpy
sys.argv = {argv!r}
try:
    runpy.run_module('dataforge', run_name='__main__')
except SystemExit:
    pass
print('MODULES:' + json.dumps(sorted(sys.modules)))
"""


def _modules_after(argv: list[str]) -> set[str]:
    """Return ``sys.modules`` after running the CLI with ``argv``, in a fresh interpreter."""
    result = subprocess.run(
        [sys.executable, "-c", _PROBE_TEMPLATE.format(argv=argv)],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    assert result.returncode == 0, f"probe failed:\n{result.stdout}\n{result.stderr}"
    line = next(
        (ln for ln in result.stdout.splitlines() if ln.startswith("MODULES:")),
        None,
    )
    assert line is not None, f"probe produced no module list:\n{result.stdout}"
    return set(json.loads(line[len("MODULES:") :]))


class TestVersionDoesNotImportTheProduct:
    def test_no_heavy_module_is_loaded(self) -> None:
        loaded = _modules_after(["dataforge", "--version"])

        offenders = sorted(FORBIDDEN_ON_VERSION & loaded)
        assert offenders == [], (
            f"'dataforge --version' imported {offenders}. Registering a subcommand eagerly makes "
            "every CLI launch pay for the whole product: 39 subprocess launches in this suite and "
            "22 in the release gate. Register the command in dataforge.cli's lazy table instead."
        )

    def test_the_probe_actually_observes_imports(self) -> None:
        """Non-vacuity: the probe must be able to SEE a heavy module when one is loaded.

        Without this, a probe that silently reported an empty module set would make the guard
        above pass forever.
        """
        loaded = _modules_after(["dataforge", "repair", "--help"])

        assert "dataforge.engine.repair" in loaded, (
            "the probe did not observe engine.repair even when running 'repair --help', so it "
            "cannot be trusted to observe it when running --version"
        )

    def test_version_still_works(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "dataforge", "--version"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )

        assert result.returncode == 0
        assert "dataforge" in result.stdout


class TestEveryLazyCommandResolves:
    """A lazy table is a second source of truth, so it must be proven to match reality."""

    def test_every_declared_command_imports_and_builds(self) -> None:
        from dataforge import cli

        unresolved: list[str] = []
        for name in sorted(cli.command_names()):
            try:
                command = cli.resolve_command(name)
            except Exception as exc:  # noqa: BLE001 - the failure is the finding
                unresolved.append(f"{name}: {type(exc).__name__}: {exc}")
                continue
            if command is None:
                unresolved.append(f"{name}: resolved to None")

        assert unresolved == [], (
            "a lazy command name does not resolve to a real command. A typo in the table would "
            f"otherwise surface only when a user ran it: {unresolved}"
        )

    def test_the_declared_names_match_what_the_cli_exposes(self) -> None:
        """The table and the click group must agree, or ``--help`` and reality diverge."""
        import click

        from dataforge import cli

        group = click.Group.__call__  # keep the import used and explicit
        assert group is not None

        command = cli.as_click_command()
        ctx = click.Context(command)

        assert set(command.list_commands(ctx)) == cli.command_names()

    @pytest.mark.parametrize("name", ["profile", "repair", "revert", "audit", "verify-apply"])
    def test_the_documented_commands_are_present(self, name: str) -> None:
        """A spot check on the commands the release gate smokes, by name."""
        from dataforge import cli

        assert name in cli.command_names()
