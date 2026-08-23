"""CLI subcommands: ``dataforge attest build|verify|inspect``.

The consumer entry point the certificate never had, and since 2026-08-23 the producer too.
Before ``verify``, nothing in the CLI could verify a certificate at all --
``verify_certificate`` and ``reverify_certificate`` were reachable only from tests and the
playground API, which means the artifact the product calls its portable proof had no way to
be consumed from a terminal or a CI job. Before ``build``, ``build_attestation`` had no CLI
caller either, so a user could verify a certificate but never obtain one.

Exits non-zero on any failure, with the failing check named, so it works as a pipeline
gate rather than only as a human report. ``build`` self-verifies and refuses to emit
anything this tool's own normative verifier would reject.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from datetime import UTC, datetime
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


def _load_private_key(path: Path) -> object:
    """Read a raw or PEM Ed25519 private key."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    raw = path.read_bytes()
    if len(raw) == 32:  # noqa: PLR2004 - a raw Ed25519 private key is exactly 32 bytes
        return Ed25519PrivateKey.from_private_bytes(raw)
    key = load_pem_private_key(raw, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        message = f"{path} is not an Ed25519 private key; this format signs with Ed25519 only"
        raise typer.BadParameter(message)
    return key


def _extract_receipt(document: Mapping[str, Any]) -> Mapping[str, Any]:
    """Find the receipt in a repair-result document, or accept a bare receipt.

    ``dataforge repair --json`` emits the whole :class:`RepairPipelineResult`, with the
    receipt nested under ``receipt``. Accepting both shapes means a user does not have to
    know which one they have, and guessing wrong is caught rather than silently producing
    an attestation over the wrong object.
    """
    if "receipt" in document and isinstance(document["receipt"], Mapping):
        return document["receipt"]
    if document.get("schema_version") == "repair_receipt_v1":
        return document
    raise typer.BadParameter(
        "input is neither a repair result (with a 'receipt' key) nor a bare "
        "repair_receipt_v1 receipt. Produce one with `dataforge repair --json`."
    )


@attest_app.command(name="build")
def build(
    receipt_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Repair result or receipt JSON, as emitted by `dataforge repair --json`.",
        ),
    ],
    subject_name: Annotated[
        str,
        typer.Option("--subject-name", help="Name recorded for the attested subject."),
    ],
    data: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            readable=True,
            help=(
                "The repaired file. Strongly recommended: without it the digest claim is "
                "built but never checked, so the emitted certificate carries an unverified "
                "data_identity."
            ),
        ),
    ] = None,
    constraints: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Schema JSON to embed in full. Required for any fix proven only by schema.",
        ),
    ] = None,
    journal_head: Annotated[
        str | None,
        typer.Option("--journal-head", help="Journal head SHA-256 to record."),
    ] = None,
    sign_key: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Ed25519 private key (raw 32 bytes or PEM). Emits a DSSE envelope.",
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write here instead of stdout."),
    ] = None,
) -> None:
    """Build a portable attestation from a repair receipt, and verify it before emitting.

    The producer this command group advertised and did not have. ``build_attestation`` had
    no CLI caller, so a user could verify a certificate but never obtain one -- and a
    certificate without a consumer is a log line.

    The build **self-verifies and fails closed**. Emitting an attestation that this tool's
    own normative verifier would reject is the one outcome worth preventing at any cost:
    every over-trust defect recorded in ``dataforge/certificate.py`` and
    ``dataforge/attestation/__init__.py`` was a verifier believing more than a receipt
    supported, and shipping a producer that can emit unverifiable output would reintroduce
    that class from the other side.

    ``--data`` is optional but its absence is reported, never folded into success, matching
    how ``attest verify`` treats a skipped ``data_identity``.
    """
    from dataforge import __version__
    from dataforge.attestation import build_attestation, sign_attestation

    document = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not isinstance(document, Mapping):
        raise typer.BadParameter(f"{receipt_path} does not contain a JSON object")
    receipt = _extract_receipt(document)

    constraint_payload: Mapping[str, Any] | None = None
    if constraints is not None:
        loaded = json.loads(constraints.read_text(encoding="utf-8"))
        if not isinstance(loaded, Mapping):
            raise typer.BadParameter(f"{constraints} does not contain a JSON object")
        constraint_payload = loaded

    statement = build_attestation(
        receipt,
        tool_version=__version__,
        produced_at=datetime.now(UTC).isoformat(),
        subject_name=subject_name,
        constraints=constraint_payload,
        journal_head_sha256=journal_head,
    )

    envelope: dict[str, Any] = statement
    if sign_key is not None:
        envelope = sign_attestation(statement, private_key=_load_private_key(sign_key))

    data_bytes = data.read_bytes() if data is not None else None
    verification = verify_attestation(envelope, data_bytes=data_bytes)
    if not verification.ok:
        failures = ", ".join(check.name for check in verification.failures)
        _console.print(
            f"[red]Refusing to emit:[/red] the built attestation does not verify ({failures}). "
            "This is a defect in the receipt or in the inputs, not something to work around."
        )
        raise typer.Exit(code=1)

    rendered = json.dumps(envelope, indent=2, sort_keys=True)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
        _console.print(f"[green]Attestation written[/green] to {output}")
    else:
        typer.echo(rendered)

    if verification.skipped:
        names = ", ".join(check.name for check in verification.skipped)
        _console.print(
            f"[yellow]Built, but not fully checked:[/yellow] {names}. "
            "Pass --data to check the digest claim."
        )
    if sign_key is None:
        _console.print(
            "[yellow]Unsigned:[/yellow] a consumer will report this as `unsigned`, never "
            "`verified`. Pass --sign-key to emit a DSSE envelope."
        )


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
