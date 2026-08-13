"""Generate the committed golden attestation vectors.

These vectors are the conformance suite. Two independent implementations (Python and
TypeScript) must agree on every one, including every rejection -- which is what makes
``dataforge.repair.attestation/v1`` a specification rather than one program's behaviour.

Rules the vectors must obey:

* **No absolute paths anywhere.** The transaction journal already demonstrates why: it
  embeds absolute local paths inside its hash preimage, so two runs on identical bytes in
  different directories produce different chains. A vector that is machine-dependent
  cannot be a shared fixture.
* **A fixed signing key and fixed timestamps.** Vectors must be byte-reproducible, so
  nothing may come from the clock or from a random keypair.
* **One vector per distinct rejection reason**, because a suite that only proves the happy
  path proves almost nothing.

Usage:
    python scripts/ci/generate_attestation_vectors.py --write
    python scripts/ci/generate_attestation_vectors.py --check
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

from dataforge.attestation import (  # noqa: E402
    ATTESTATION_VERSION,
    PREDICATE_TYPE,
    STATEMENT_TYPE,
    sign_attestation,
)

VECTOR_DIR = PROJECT_ROOT / "tests" / "fixtures" / "attestation"

# A fixed, published-in-the-repo test key. It signs test vectors and nothing else; it is
# not a trust root and must never be used to sign a real attestation.
TEST_SEED = bytes(range(32))

POST_DIGEST = "b" * 64
SOURCE_DIGEST = "a" * 64
JOURNAL_HEAD = "c" * 64
CONSTRAINTS_DIGEST = "d" * 64


def _base_predicate() -> dict[str, Any]:
    return {
        "attestation_version": ATTESTATION_VERSION,
        "tool": {
            "name": "dataforge",
            "version": "0.1.0",
            "contract_version": "repair_contract_v2",
        },
        "produced_at": "2026-08-13T00:00:00Z",
        "mode": "apply",
        "applied": True,
        "reversible": True,
        "source": {"digest": {"sha256": SOURCE_DIGEST}},
        "post": {"digest": {"sha256": POST_DIGEST}},
        "authority": {
            "authoritative_columns": [],
            "accepted_constraint_ids": [],
            "constraints_digest": None,
            "constraints": None,
        },
        "fixes": [
            {
                "row": 7,
                "column": "amount",
                "detector_id": "decimal_shift",
                "provenance": "deterministic",
                "verification_strength": "proven",
            }
        ],
        "held": [],
        "verdicts": {
            "verifier": "accept",
            "safety": "allow",
            "independent_verification": "not_run",
        },
        "journal": {"txn_id": "txn-2026-08-13-vector", "head_sha256": JOURNAL_HEAD},
        "revert_command": "dataforge revert txn-2026-08-13-vector",
        "model": None,
        "limitations": [],
        "verification": {"checks_available": []},
    }


def _statement(predicate: dict[str, Any], *, subject_digest: str | None = None) -> dict[str, Any]:
    digest = subject_digest
    if digest is None:
        digest = POST_DIGEST if predicate.get("applied") else SOURCE_DIGEST
    return {
        "_type": STATEMENT_TYPE,
        "subject": [{"name": "data.csv", "digest": {"sha256": digest}}],
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def _with_authority(predicate: dict[str, Any], *, constraints: object) -> dict[str, Any]:
    predicate["authority"] = {
        "authoritative_columns": ["amount"],
        "accepted_constraint_ids": ["amount-int"],
        "constraints_digest": {"sha256": CONSTRAINTS_DIGEST},
        "constraints": constraints,
    }
    predicate["fixes"] = [
        {
            "row": 7,
            "column": "amount",
            "detector_id": "external",
            "provenance": "external",
            "verification_strength": "proven",
        }
    ]
    return predicate


def build_vectors() -> dict[str, dict[str, Any]]:
    """Every vector, with the verdict each implementation must reach."""
    vectors: dict[str, dict[str, Any]] = {}

    def add(name: str, document: Any, *, expect_ok: bool, expect_failures: list[str]) -> None:
        vectors[name] = {
            "description": name.replace("-", " "),
            "expect_ok": expect_ok,
            "expect_failures": sorted(expect_failures),
            "document": document,
        }

    add(
        "valid-deterministic-applied",
        _statement(_base_predicate()),
        expect_ok=True,
        expect_failures=[],
    )

    dry = _base_predicate()
    dry["mode"] = "dry_run"
    dry["applied"] = False
    dry["post"] = None
    dry["fixes"] = []
    dry["revert_command"] = None
    dry["journal"] = {"txn_id": None, "head_sha256": None}
    dry["verdicts"] = {
        "verifier": "not_run",
        "safety": "allow",
        "independent_verification": "not_run",
    }
    add("valid-dry-run", _statement(dry), expect_ok=True, expect_failures=[])

    add(
        "valid-external-on-covered-column",
        _statement(_with_authority(_base_predicate(), constraints={"columns": {"amount": "int"}})),
        expect_ok=True,
        expect_failures=[],
    )

    off = _with_authority(_base_predicate(), constraints={"columns": {"amount": "int"}})
    off["authority"]["authoritative_columns"] = ["id"]
    add(
        "reject-external-off-authority",
        _statement(off),
        expect_ok=False,
        expect_failures=["strength_is_earned"],
    )

    dangling = _with_authority(_base_predicate(), constraints=None)
    add(
        "reject-schema-proven-without-embedded-constraints",
        _statement(dangling),
        expect_ok=False,
        expect_failures=["constraints_present"],
    )

    consensus = _base_predicate()
    consensus["fixes"] = [
        {
            "row": 0,
            "column": "city",
            "detector_id": "entity_consensus",
            "provenance": "entity_consensus",
            "verification_strength": "proven",
        }
    ]
    add(
        "reject-entity-consensus-claimed-proven",
        _statement(consensus),
        expect_ok=False,
        expect_failures=["strength_is_earned"],
    )

    downgraded = _with_authority(_base_predicate(), constraints={"columns": {"amount": "int"}})
    downgraded["fixes"][0]["provenance"] = "llm_live"
    downgraded["fixes"][0]["verification_strength"] = "plausibility_only"
    add(
        "reject-honest-downgrade-must-not-be-upgraded",
        _statement(downgraded),
        expect_ok=False,
        expect_failures=["strength_is_earned"],
    )

    unknown_provenance = _base_predicate()
    unknown_provenance["fixes"][0]["provenance"] = "some_future_corrector"
    add(
        "reject-unknown-provenance",
        _statement(unknown_provenance),
        expect_ok=False,
        expect_failures=["strength_is_earned", "vocabulary_closed"],
    )

    missing_strength = _base_predicate()
    missing_strength["fixes"][0]["verification_strength"] = None
    add(
        "reject-missing-verification-strength",
        _statement(missing_strength),
        expect_ok=False,
        expect_failures=["vocabulary_closed"],
    )

    unknown_reason = _base_predicate()
    unknown_reason["held"] = [{"row": 1, "column": "amount", "review_reason": "because_i_said_so"}]
    add(
        "reject-unknown-review-reason",
        _statement(unknown_reason),
        expect_ok=False,
        expect_failures=["vocabulary_closed"],
    )

    bad_version = _base_predicate()
    bad_version["attestation_version"] = "2"
    add(
        "reject-unrecognised-version",
        _statement(bad_version),
        expect_ok=False,
        expect_failures=["version_recognised"],
    )

    wrong_predicate = _statement(_base_predicate())
    wrong_predicate["predicateType"] = "https://example.com/SomethingElse/v1"
    add(
        "reject-unrecognised-predicate-type",
        wrong_predicate,
        expect_ok=False,
        expect_failures=["envelope_recognised"],
    )

    no_version = _base_predicate()
    del no_version["tool"]["version"]
    add(
        "reject-missing-tool-version",
        _statement(no_version),
        expect_ok=False,
        expect_failures=["schema_complete"],
    )

    add(
        "reject-subject-digest-mismatch",
        _statement(_base_predicate(), subject_digest="9" * 64),
        expect_ok=False,
        expect_failures=["subject_matches_post_state"],
    )

    no_revert = _base_predicate()
    no_revert["revert_command"] = None
    add(
        "reject-applied-without-revert-command",
        _statement(no_revert),
        expect_ok=False,
        expect_failures=["reversibility_complete"],
    )

    rejected_verdict = _base_predicate()
    rejected_verdict["verdicts"]["verifier"] = "reject"
    add(
        "reject-verifier-rejected",
        _statement(rejected_verdict),
        expect_ok=False,
        expect_failures=["verdicts_accepting"],
    )

    # Signed vectors. The signature check is verified only when a key is supplied, so the
    # unsigned-report path is a vector too.
    key = Ed25519PrivateKey.from_private_bytes(TEST_SEED)
    signed = sign_attestation(_statement(_base_predicate()), private_key=key)
    add("valid-signed", copy.deepcopy(signed), expect_ok=True, expect_failures=[])

    tampered = copy.deepcopy(signed)
    payload = json.loads(__import__("base64").b64decode(tampered["payload"]))
    payload["predicate"]["applied"] = False
    tampered["payload"] = (
        __import__("base64")
        .b64encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        .decode("ascii")
    )
    add(
        "reject-tampered-signed-payload",
        tampered,
        expect_ok=False,
        expect_failures=["signature"],
    )

    return vectors


def render() -> str:
    payload = {
        "format": "dataforge.repair.attestation/v1",
        "note": (
            "Conformance vectors. Every implementation must reach the recorded verdict for "
            "every vector. Signed vectors use the fixed test key below, which is a test "
            "fixture and not a trust root."
        ),
        "test_public_key_hex": Ed25519PrivateKey.from_private_bytes(TEST_SEED)
        .public_key()
        .public_bytes_raw()
        .hex(),
        "vectors": build_vectors(),
    }
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true")
    group.add_argument("--write", action="store_true")
    args = parser.parse_args()

    target = VECTOR_DIR / "vectors.json"
    rendered = render()

    if args.write:
        VECTOR_DIR.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"wrote {target.relative_to(PROJECT_ROOT)} ({len(build_vectors())} vectors)")
        return 0

    if not target.exists():
        print(f"MISSING: {target.relative_to(PROJECT_ROOT)}")
        return 1
    if target.read_text(encoding="utf-8") != rendered:
        print(f"STALE: {target.relative_to(PROJECT_ROOT)} does not match the generator.")
        return 1
    print(f"Attestation vectors are current ({len(build_vectors())} vectors).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
