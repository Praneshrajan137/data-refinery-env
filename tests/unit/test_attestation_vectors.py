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


class TestMultiSubjectRuleCoversSomethingTheOldRuleCouldNot:
    """A gate nobody has seen fail on a case it newly covers has not been shown to cover it.

    `PRODUCT.md`:110-113 states the rule. Both verifiers read `subject[0]` and ignored the
    rest until 2026-08-29, so this reconstructs the OLD predicate and asserts it accepts the
    smuggling vector that the new one rejects. Without this, the new vectors could pass for
    reasons unrelated to the change.
    """

    @staticmethod
    def _subject_zero_verdict(statement: dict[str, object]) -> bool:
        """The retired rule: read subject[0], ignore every other subject."""
        predicate = statement["predicate"]
        assert isinstance(predicate, dict)
        applied = predicate.get("applied") is True
        section = predicate.get("post") if applied else predicate.get("source")
        assert isinstance(section, dict)
        digest = section.get("digest")
        assert isinstance(digest, dict)
        expected = digest.get("sha256")

        subjects = statement["subject"]
        assert isinstance(subjects, list) and subjects
        first = subjects[0]
        assert isinstance(first, dict)
        first_digest = first.get("digest")
        assert isinstance(first_digest, dict)
        return bool(first_digest.get("sha256") == expected)

    def test_the_old_rule_accepts_the_smuggled_subject(self) -> None:
        """The attack, and the proof the old rule was vulnerable to it."""
        case = VECTORS["vectors"]["reject-extra-subject-naming-a-different-artifact"]
        statement = case["document"]

        assert self._subject_zero_verdict(statement) is True, (
            "the vector no longer exercises the subject[0] hole"
        )
        assert case["expect_ok"] is False, "the new rule must reject what the old one accepted"

    def test_the_new_rule_rejects_it(self) -> None:
        from dataforge.attestation import verify_attestation

        statement = VECTORS["vectors"]["reject-extra-subject-naming-a-different-artifact"][
            "document"
        ]

        report = verify_attestation(statement)

        assert not report.ok
        assert any(check.name == "subject_matches_post_state" for check in report.failures)

    def test_duplicate_subjects_naming_the_same_artifact_are_allowed(self) -> None:
        """Non-vacuity in the other direction: the rule must not reject all N>1 statements.

        in-toto permits several subjects, and listing the same artifact twice asserts nothing
        extra. Refusing it would be a format restriction with no safety argument behind it.
        """
        from dataforge.attestation import verify_attestation

        statement = VECTORS["vectors"]["accept-multiple-subjects-naming-the-same-artifact"][
            "document"
        ]

        assert verify_attestation(statement).ok


class TestTheWitnessBreaksTheStrengthCircularity:
    """The reason the witness is in the predicate at all.

    `_check_strength` calls `verification_strength_for` -- the same function object the engine
    calls to stamp `verification_strength`. Within one language it therefore validates field
    consistency, not the rule: a wrong trust model is invisible to it, which is exactly the
    axis `decimal_shift` lived on. An attestation from that window would have verified clean.

    A witness states arithmetic that can contradict itself, so a wrong claim is catchable
    WITHOUT re-running the rule that produced it. These tests assert that the strength check
    accepts the very statement the witness check rejects -- which is what makes the two
    independent rather than redundant.
    """

    PLURALITY = "reject-witness-without-a-strict-majority"

    def test_the_strength_check_accepts_what_the_witness_check_rejects(self) -> None:
        """Both checks run on the same statement and disagree. That is the point."""
        from dataforge.attestation import _check_strength, _check_witness

        statement = VECTORS["vectors"][self.PLURALITY]["document"]
        predicate = statement["predicate"]

        strength = _check_strength(predicate)
        witness = _check_witness(predicate)

        assert all(entry.ok for entry in strength), (
            "the fix is deterministic and proven, so the strength rule has no objection"
        )
        assert not all(entry.ok for entry in witness), (
            "the witness arithmetic must contradict the write the strength rule accepted"
        )

    def test_the_rejection_names_the_strict_majority_rule(self) -> None:
        from dataforge.attestation import verify_attestation

        statement = VECTORS["vectors"][self.PLURALITY]["document"]

        report = verify_attestation(statement)

        assert not report.ok
        detail = " ".join(check.detail for check in report.failures)
        assert "strict majority" in detail

    def test_an_unwitnessed_fix_is_reported_skipped_not_passed(self) -> None:
        """Absence of evidence must not read as evidence.

        A fix with no witness is one whose derivation nobody can check. Folding that into
        `ok` would be the same over-claim the `unsigned` and `data_identity` reports exist to
        prevent.
        """
        from dataforge.attestation import verify_attestation

        statement = VECTORS["vectors"]["valid-deterministic-applied"]["document"]

        report = verify_attestation(statement)

        names = [check.name for check in report.skipped]
        assert "witness_is_coherent" in names, [c.name for c in report.checks]

    def test_a_coherent_witness_is_reported_as_checked(self) -> None:
        """Non-vacuity: the check must be able to actually pass, not only skip or fail."""
        from dataforge.attestation import verify_attestation

        statement = VECTORS["vectors"]["accept-coherent-entailment-witness"]["document"]

        report = verify_attestation(statement)

        assert report.ok, [check.detail for check in report.failures]
        checked = [c for c in report.checks if c.name == "witness_is_coherent"]
        assert checked and not checked[0].skipped

    def test_no_plaintext_cell_value_reaches_the_witness(self) -> None:
        """The privacy property, asserted rather than trusted.

        The predicate deliberately carries no cell values. A witness stating a group's
        distribution in plaintext would reverse that silently, so every value in a witness is
        published as sha256(value)[:16] and this plants a recognisable string to prove it.
        """
        from dataforge.witness import EntailmentWitness, GroupDistribution

        sentinel = "PATIENT-NAME-SENTINEL-9f3a"
        witness = EntailmentWitness(
            row=0,
            column="name",
            constraint="id -> name",
            constraint_kind="functional_dependency",
            determinant=(("id", sentinel),),
            distribution=GroupDistribution(group_size=3, values=(("ok", 2), (sentinel, 1))),
            old_value=sentinel,
            new_value="ok",
            support=2,
        )

        payload = json.dumps(witness.to_attestation_payload())

        assert sentinel not in payload, "a plaintext cell value reached the attestation"
