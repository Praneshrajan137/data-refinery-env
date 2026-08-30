"""The published attestation JSON Schema must describe real attestations.

``specs/SPEC_repair_attestation.md``:24 promised "one normative wire format with a published
JSON Schema", and line 154 declined to restate the enums on the grounds that they live in that
schema. The schema did not exist. So a published specification cited a normative artifact that
was never shipped, and readers were pointed at nothing.

Publishing one creates a second risk in place of the first: a schema that exists but does not
describe the artifact is worse than an absent one, because it can be conformed to. These tests
close that by validating real attestations and every committed conformance vector against it.

The schema is GENERATED from the verifier's own ``REQUIRED_PREDICATE_FIELDS`` and the closed
vocabularies, so it cannot disagree with the verifier about what is required. What it can still
get wrong is its optional structure, and that is what validating real documents catches.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

REPO = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO / "specs" / "repair_attestation.schema.json"
VECTORS_PATH = REPO / "tests" / "fixtures" / "attestation" / "vectors.json"
FIXTURES = REPO / "dataforge" / "fixtures"


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    assert SCHEMA_PATH.is_file(), (
        "the JSON Schema promised at specs/SPEC_repair_attestation.md is absent; "
        "run scripts/ci/generate_attestation_schema.py --write"
    )
    payload = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _real_attestation(tmp_path: Path) -> dict[str, Any]:
    """Produce an attestation the way a user does: run a repair and take what it emits."""
    from dataforge.cli.common import load_schema
    from dataforge.cli.repair import _attest_result
    from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline

    source = tmp_path / "premised_fd_10rows.csv"
    schema_file = tmp_path / "premised_fd_10rows.schema.yaml"
    source.write_bytes((FIXTURES / "premised_fd_10rows.csv").read_bytes())
    schema_file.write_bytes((FIXTURES / "premised_fd_10rows.schema.yaml").read_bytes())

    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="apply",
            schema=load_schema(schema_file),
            allow_llm=False,
        )
    )
    payload = _attest_result(result, source_path=source, schema_path=schema_file)
    statement = payload.get("attestation")
    assert isinstance(statement, dict), payload.get("attestation_unavailable")
    return statement


class TestTheSchemaIsValidAndDescribesRealAttestations:
    def test_the_schema_is_itself_a_valid_json_schema(self, schema: dict[str, Any]) -> None:
        """A malformed schema would silently accept everything."""
        jsonschema.Draft202012Validator.check_schema(schema)

    def test_a_real_emitted_attestation_validates(
        self, schema: dict[str, Any], tmp_path: Path
    ) -> None:
        """The claim that matters: the published format describes what the product emits."""
        jsonschema.validate(_real_attestation(tmp_path), schema)

    def test_every_accepting_vector_validates(self, schema: dict[str, Any]) -> None:
        """Structural validity is necessary, so anything the verifier accepts must validate.

        The converse does NOT hold and must not be asserted: verification additionally
        re-derives strength, checks the data digest, and rejects foreign subjects, so a
        rejected vector may well be structurally fine.
        """
        vectors = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))["vectors"]
        accepting = {name: case["document"] for name, case in vectors.items() if case["expect_ok"]}
        assert accepting, "no accepting vectors; the suite would be vacuous here"

        for name, document in accepting.items():
            statement = document
            if "payload" in document:
                import base64

                statement = json.loads(base64.b64decode(document["payload"]))
            try:
                jsonschema.validate(statement, schema)
            except jsonschema.ValidationError as exc:
                pytest.fail(f"accepting vector {name!r} does not satisfy the schema: {exc.message}")


class TestTheSchemaIsNotVacuous:
    """A schema that accepts anything is worse than none: it can be conformed to."""

    def test_an_empty_object_is_rejected(self, schema: dict[str, Any]) -> None:
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({}, schema)

    def test_a_wrong_predicate_type_is_rejected(
        self, schema: dict[str, Any], tmp_path: Path
    ) -> None:
        statement = _real_attestation(tmp_path)
        statement["predicateType"] = "https://example.invalid/Other/v1"

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(statement, schema)

    def test_a_missing_required_predicate_field_is_rejected(
        self, schema: dict[str, Any], tmp_path: Path
    ) -> None:
        from dataforge.attestation import REQUIRED_PREDICATE_FIELDS

        statement = _real_attestation(tmp_path)
        del statement["predicate"][REQUIRED_PREDICATE_FIELDS[0]]

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(statement, schema)

    def test_an_unknown_verification_strength_is_rejected(
        self, schema: dict[str, Any], tmp_path: Path
    ) -> None:
        """The closed vocabulary, enforced structurally as SPEC:154 claimed it was."""
        statement = _real_attestation(tmp_path)
        assert statement["predicate"]["fixes"], "the fixture must produce at least one fix"
        statement["predicate"]["fixes"][0]["verification_strength"] = "definitely_fine"

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(statement, schema)

    def test_a_non_hex_digest_is_rejected(self, schema: dict[str, Any], tmp_path: Path) -> None:
        statement = _real_attestation(tmp_path)
        statement["subject"][0]["digest"]["sha256"] = "not-a-digest"

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(statement, schema)

    def test_an_empty_subject_list_is_rejected(
        self, schema: dict[str, Any], tmp_path: Path
    ) -> None:
        statement = _real_attestation(tmp_path)
        statement["subject"] = []

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(statement, schema)


class TestTheSchemaIsDerivedFromTheVerifier:
    """It must be impossible for the schema and the verifier to disagree on what is required."""

    def test_required_fields_come_from_the_verifier(self, schema: dict[str, Any]) -> None:
        from dataforge.attestation import REQUIRED_PREDICATE_FIELDS

        required = schema["properties"]["predicate"]["required"]

        assert required == list(REQUIRED_PREDICATE_FIELDS)

    def test_the_generator_check_mode_detects_drift(self) -> None:
        """The gate that keeps this true, exercised rather than assumed."""
        import sys

        sys.path.insert(0, str(REPO / "scripts" / "ci"))
        import generate_attestation_schema as generator

        assert generator.main(["--check"]) == 0

    def test_the_schema_says_validity_is_not_verification(self, schema: dict[str, Any]) -> None:
        """A reader must not mistake structural conformance for a verified repair.

        This is the same over-claim the certificate work has corrected repeatedly, arriving
        by a new route: a published schema invites "it validates, therefore it is proven".
        """
        description = schema["description"]

        assert "NOT SUFFICIENT" in description
        assert "verify_attestation" in description
