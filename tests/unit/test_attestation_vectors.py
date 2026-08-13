"""Run the Python attestation verifier against every committed golden vector.

The vectors record the verdict an implementation MUST reach. This is the Python half of
the conformance suite; ``playground/web/src/attestation/verify.test.ts`` is the other, and
``scripts/ci/attestation_conformance.py`` proves the two agree.

Encoding the expected FAILING CHECK NAMES, not just ok/not-ok, is deliberate. A verifier
that rejects everything for the wrong reason would satisfy a boolean assertion while being
useless -- and an implementation that rejects a vector via a different check has not
implemented the same specification.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from dataforge.attestation import verify_attestation

VECTORS_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "attestation" / "vectors.json"


def _load() -> dict[str, Any]:
    return json.loads(VECTORS_PATH.read_text(encoding="utf-8"))


VECTORS = _load()
PUBLIC_KEY = bytes.fromhex(VECTORS["test_public_key_hex"])


def test_the_vector_file_is_a_real_suite() -> None:
    """Guard against a vacuous suite.

    A conformance file that drifted down to two happy-path vectors would still pass every
    parametrized test below while proving nothing. Pin the shape instead.
    """
    vectors = VECTORS["vectors"]
    assert len(vectors) >= 15, f"only {len(vectors)} vectors; the suite has lost coverage"
    accepted = [name for name, case in vectors.items() if case["expect_ok"]]
    rejected = [name for name, case in vectors.items() if not case["expect_ok"]]
    assert len(accepted) >= 3
    assert len(rejected) >= 10
    # Every distinct normative check that can fail should be exercised by some vector.
    exercised = {check for case in vectors.values() for check in case["expect_failures"]}
    for required in (
        "envelope_recognised",
        "version_recognised",
        "schema_complete",
        "vocabulary_closed",
        "subject_matches_post_state",
        "reversibility_complete",
        "verdicts_accepting",
        "strength_is_earned",
        "constraints_present",
        "signature",
    ):
        assert required in exercised, f"no vector exercises a {required} failure"


@pytest.mark.parametrize("name", sorted(VECTORS["vectors"]))
def test_vector(name: str) -> None:
    case = VECTORS["vectors"][name]
    document = case["document"]
    signed = "payload" in document
    result = verify_attestation(
        document,
        public_key_raw=PUBLIC_KEY if signed else None,
    )

    assert result.ok is case["expect_ok"], (
        f"{name}: expected ok={case['expect_ok']}, got {result.ok}; "
        f"failures={[c.as_dict() for c in result.failures]}"
    )
    actual_failures = sorted(check.name for check in result.failures)
    assert actual_failures == case["expect_failures"], (
        f"{name}: rejected by the wrong checks. "
        f"expected {case['expect_failures']}, got {actual_failures}"
    )


def test_the_test_key_is_only_a_fixture() -> None:
    """The committed key must be the documented deterministic seed, never a real key."""
    expected = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
    assert expected.public_key().public_bytes_raw() == PUBLIC_KEY


def test_no_vector_embeds_an_absolute_path() -> None:
    """Vectors must be machine-independent.

    The transaction journal embeds absolute local paths inside its hash preimage, so two
    runs on identical bytes in different directories produce different chains. A fixture
    with the same property could not be shared across implementations or machines.
    """
    raw = VECTORS_PATH.read_text(encoding="utf-8")
    for marker in ("C:\\\\", "C:/", "/home/", "/Users/", "/tmp/"):
        assert marker not in raw, f"vectors contain a machine-specific path: {marker}"

    # Signed payloads hide their content in base64, so decode and check those too.
    for case in VECTORS["vectors"].values():
        document = case["document"]
        if "payload" in document:
            decoded = base64.b64decode(document["payload"]).decode("utf-8")
            for marker in ("C:\\", "C:/", "/home/", "/Users/", "/tmp/"):
                assert marker not in decoded
