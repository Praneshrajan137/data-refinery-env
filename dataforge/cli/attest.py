"""CLI subcommand: ``dataforge attest verify <attestation.json>``.

The consumer entry point the certificate never had. Before this, nothing in the CLI could
verify a certificate at all -- ``verify_certificate`` and ``reverify_certificate`` were
reachable only from tests and the playground API, which means the artifact the product
calls its portable proof had no way to be consumed from a terminal or a CI job.

Exits non-zero on any failure, with the failing check named, so it works as a pipeline
gate rather than only as a human report.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from dataforge.attestation import verify_attestation

_console = Console(stderr=True)

attest_app = typer.Typer(
    no_args_is_help=True,
    help="Build and verify portable repair attestations.",
)


def _load_public_key(path: Path) -> bytes:
    """Read a raw or PEM Ed25519 public key.

    Raw 32-byte keys are accepted so a verifier needs no cryptography stack to produce
    one; PEM is accepted because that is what most tooling emits.
    """
    raw = path.read_bytes()
    if len(raw) == 32:  # noqa: PLR2004 - a raw Ed25519 public key is exactly 32 bytes
        return raw
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from cryptography.hazmat.primitives.serialization import load_pem_public_key

    key = load_pem_public_key(raw)
    if not isinstance(key, Ed25519PublicKey):
        message = f"{path} is not an Ed25519 public key; this format signs with Ed25519 only"
        raise typer.BadParameter(message)
    return key.public_bytes_raw()


@attest_app.command(name="verify")
def verify(
    attestation: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Path to the attestation (a bare in-toto Statement or a DSSE envelope).",
        ),
    ],
    data: Annotated[
        Path | None,
        typer.Option(
            "--data",
            exists=True,
            dir_okay=False,
            readable=True,
            help="The file this attestation describes. Without it the digest claim is SKIPPED.",
        ),
    ] = None,
    pubkey: Annotated[
        Path | None,
        typer.Option(
            "--pubkey",
            exists=True,
            dir_okay=False,
            readable=True,
            help="Ed25519 public key (raw 32 bytes or PEM). Without it a signature is unverified.",
        ),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit the full check list as JSON on stdout."),
    ] = False,
) -> None:
    """Verify an attestation without trusting the tool that produced it.

    This runs the NORMATIVE tier only: hashes, structure, closed vocabularies, and
    strength derived from provenance and column authority. It does not re-run the SMT
    verifier, and it does not need DataForge's engine, a solver, or the original schema --
    the constraints travel inside the attestation.
    """
    document: Any = json.loads(attestation.read_text(encoding="utf-8"))
    data_bytes = data.read_bytes() if data is not None else None
    public_key_raw = _load_public_key(pubkey) if pubkey is not None else None

    result = verify_attestation(
        document,
        data_bytes=data_bytes,
        public_key_raw=public_key_raw,
    )

    if as_json:
        typer.echo(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        table = Table(title="Attestation verification", show_lines=False)
        table.add_column("check")
        table.add_column("result")
        table.add_column("detail", overflow="fold")
        for check in result.checks:
            if check.skipped:
                verdict = "[yellow]skipped[/yellow]"
            elif check.ok:
                verdict = "[green]pass[/green]"
            else:
                verdict = "[red]FAIL[/red]"
            table.add_row(check.name, verdict, check.detail)
        _console.print(table)

        if result.skipped:
            # Naming the skips matters: a verifier that reported the same result for
            # "all checks passed" and "all runnable checks passed, two skipped" would be
            # overstating what was established.
            names = ", ".join(check.name for check in result.skipped)
            _console.print(f"[yellow]Not checked:[/yellow] {names}")

    if not result.ok:
        failures = ", ".join(check.name for check in result.failures)
        _console.print(f"[red]Attestation REJECTED[/red]: {failures}")
        raise typer.Exit(code=1)

    _console.print("[green]Attestation verified[/green] (normative tier)")


@attest_app.command(name="inspect")
def inspect(
    attestation: Annotated[
        Path,
        typer.Argument(exists=True, dir_okay=False, readable=True, help="Path to inspect."),
    ],
) -> None:
    """Print the predicate without verifying it.

    Useful when triaging a rejection: it shows what the attestation claims, which is a
    different question from whether the claim holds.
    """
    document = json.loads(attestation.read_text(encoding="utf-8"))
    if "payload" in document:
        document = json.loads(base64.b64decode(document["payload"]))
    typer.echo(json.dumps(document.get("predicate", {}), indent=2, sort_keys=True))
