"""End-to-end test for ``dataforge attest build``: the producer the CLI advertised.

``cli/attest.py`` declared the group as "Build and verify portable repair attestations"
while only ``verify`` and ``inspect`` existed, so ``build_attestation`` had no CLI caller and
a user could verify a certificate but never obtain one. Per ``docs/STRATEGY.md``, quoted in
``PRODUCT.md``: "A certificate with a named consumer is a product; one without is a log
line." The consumer could not get one.

The load-bearing assertion here is not that ``build`` produces output. It is that ``build``
**refuses** to produce output this tool's own normative verifier would reject. Every
over-trust defect recorded in ``dataforge/certificate.py`` was a verifier believing more
than a receipt supported; a producer that can emit unverifiable certificates is the same
class of defect from the other side.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dataforge.cli import app
from tests.support.tables import build_premised_repairable_table

runner = CliRunner()


@pytest.fixture
def repaired(tmp_path: Path) -> tuple[Path, Path]:
    """Apply a real repair and return (data path, receipt path).

    Uses the shared premised fixture rather than a local literal: its name carries the
    disposition, and it verifies its own premise, so this test cannot silently become a test
    of a table where nothing is applied.
    """
    table = build_premised_repairable_table(tmp_path / "premised.csv")
    result = runner.invoke(
        app,
        [
            "repair",
            str(table.csv_path),
            "--apply",
            "--schema",
            str(table.schema_path),
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    receipt_path = tmp_path / "result.json"
    receipt_path.write_text(result.stdout, encoding="utf-8")

    payload = json.loads(result.stdout)
    assert payload["fixes"], "precondition: a fix must have been applied for this to be a test"
    return table.csv_path, receipt_path


class TestBuildProducesAVerifiableCertificate:
    """The lifecycle a consumer actually needs: repair -> build -> verify."""

    def test_build_then_verify_round_trip(
        self, tmp_path: Path, repaired: tuple[Path, Path]
    ) -> None:
        data_path, receipt_path = repaired
        attestation = tmp_path / "attestation.json"

        built = runner.invoke(
            app,
            [
                "attest",
                "build",
                str(receipt_path),
                "--subject-name",
                data_path.name,
                "--data",
                str(data_path),
                "--output",
                str(attestation),
            ],
        )
        assert built.exit_code == 0, built.output
        assert attestation.exists()

        verified = runner.invoke(
            app, ["attest", "verify", str(attestation), "--data", str(data_path), "--json"]
        )
        assert verified.exit_code == 0, verified.output
        report = json.loads(verified.stdout)
        assert report["ok"] is True

    def test_built_attestation_is_a_wellformed_statement(
        self, tmp_path: Path, repaired: tuple[Path, Path]
    ) -> None:
        data_path, receipt_path = repaired
        attestation = tmp_path / "a.json"
        runner.invoke(
            app,
            [
                "attest",
                "build",
                str(receipt_path),
                "--subject-name",
                data_path.name,
                "--data",
                str(data_path),
                "--output",
                str(attestation),
            ],
        )
        document = json.loads(attestation.read_text(encoding="utf-8"))
        assert document["_type"] == "https://in-toto.io/Statement/v1"
        assert document["subject"][0]["name"] == data_path.name
        predicate = document["predicate"]
        assert predicate["applied"] is True
        assert predicate["reversible"] is True
        assert predicate["journal"]["txn_id"]
        assert predicate["revert_command"]

    def test_accepts_a_bare_receipt_as_well_as_a_full_result(
        self, tmp_path: Path, repaired: tuple[Path, Path]
    ) -> None:
        """A user should not have to know which shape they are holding."""
        data_path, receipt_path = repaired
        bare = tmp_path / "bare.json"
        bare.write_text(
            json.dumps(json.loads(receipt_path.read_text(encoding="utf-8"))["receipt"]),
            encoding="utf-8",
        )
        built = runner.invoke(
            app,
            [
                "attest",
                "build",
                str(bare),
                "--subject-name",
                data_path.name,
                "--data",
                str(data_path),
            ],
        )
        assert built.exit_code == 0, built.output
        assert json.loads(built.stdout)["_type"] == "https://in-toto.io/Statement/v1"


class TestFailClosed:
    """What build refuses, which is the point of it."""

    def test_refuses_to_emit_when_the_data_does_not_match(
        self, tmp_path: Path, repaired: tuple[Path, Path]
    ) -> None:
        """The self-verification must actually gate the write, not merely run."""
        data_path, receipt_path = repaired
        wrong = tmp_path / "wrong.csv"
        wrong.write_text("id,amount\n1,999\n", encoding="utf-8")
        output = tmp_path / "should_not_exist.json"

        built = runner.invoke(
            app,
            [
                "attest",
                "build",
                str(receipt_path),
                "--subject-name",
                data_path.name,
                "--data",
                str(wrong),
                "--output",
                str(output),
            ],
        )
        assert built.exit_code == 1
        assert "Refusing to emit" in built.output
        assert not output.exists(), (
            "a refused build must not leave a file behind; a rejected certificate on disk "
            "will eventually be read as a valid one"
        )

    def test_rejects_input_that_is_not_a_receipt(self, tmp_path: Path) -> None:
        stray = tmp_path / "stray.json"
        stray.write_text(json.dumps({"hello": "world"}), encoding="utf-8")
        built = runner.invoke(app, ["attest", "build", str(stray), "--subject-name", "x"])
        assert built.exit_code != 0
        assert "repair_receipt_v1" in built.output


class TestHonestReporting:
    """Absences are named, never folded into success."""

    def test_missing_data_flag_is_reported_as_unchecked(
        self, tmp_path: Path, repaired: tuple[Path, Path]
    ) -> None:
        """Without --data the digest claim is built but never checked."""
        data_path, receipt_path = repaired
        built = runner.invoke(
            app, ["attest", "build", str(receipt_path), "--subject-name", data_path.name]
        )
        assert built.exit_code == 0, built.output
        assert "not fully checked" in built.output
        assert "--data" in built.output

    def test_unsigned_output_says_so(self, tmp_path: Path, repaired: tuple[Path, Path]) -> None:
        """An unsigned certificate is reported `unsigned`, never `verified`."""
        data_path, receipt_path = repaired
        built = runner.invoke(
            app,
            [
                "attest",
                "build",
                str(receipt_path),
                "--subject-name",
                data_path.name,
                "--data",
                str(data_path),
            ],
        )
        assert "Unsigned" in built.output
        assert "--sign-key" in built.output


class TestSigning:
    """DSSE signing, when a key is supplied."""

    def test_signed_output_is_a_dsse_envelope_that_verifies(
        self, tmp_path: Path, repaired: tuple[Path, Path]
    ) -> None:
        cryptography = pytest.importorskip("cryptography")
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        assert cryptography  # keep the import meaningful to linters

        private = Ed25519PrivateKey.generate()
        key_path = tmp_path / "key.raw"
        key_path.write_bytes(private.private_bytes_raw())
        pub_path = tmp_path / "key.pub"
        pub_path.write_bytes(private.public_key().public_bytes_raw())

        data_path, receipt_path = repaired
        attestation = tmp_path / "signed.json"
        built = runner.invoke(
            app,
            [
                "attest",
                "build",
                str(receipt_path),
                "--subject-name",
                data_path.name,
                "--data",
                str(data_path),
                "--sign-key",
                str(key_path),
                "--output",
                str(attestation),
            ],
        )
        assert built.exit_code == 0, built.output
        envelope = json.loads(attestation.read_text(encoding="utf-8"))
        assert envelope["payloadType"] == "application/vnd.in-toto+json"
        assert envelope["signatures"]

        verified = runner.invoke(
            app,
            [
                "attest",
                "verify",
                str(attestation),
                "--data",
                str(data_path),
                "--pubkey",
                str(pub_path),
                "--json",
            ],
        )
        assert verified.exit_code == 0, verified.output
        report = json.loads(verified.stdout)
        assert report["ok"] is True
        signature_checks = [c for c in report["checks"] if c["name"] == "signature"]
        assert signature_checks and signature_checks[0]["ok"] is True
