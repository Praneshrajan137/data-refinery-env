"""The repair attestation: a portable statement that a mutation was verified.

Implements ``dataforge.repair.attestation/v1`` as specified in
``specs/SPEC_repair_attestation.md``. Three pieces:

* :func:`build_attestation` -- project a :class:`RepairReceipt` into the wire format,
  embedding the constraints and the tool version the receipt never carried.
* :func:`verify_attestation` -- the NORMATIVE tier. Pure standard library, no solver, no
  inference. Reimplementable in any language from the spec; a TypeScript implementation
  runs the same golden vectors.
* :func:`sign_attestation` / :func:`verify_signature` -- optional DSSE signing.

Why this exists
---------------
The receipt was already honest, but it was not *consumable*. It went to stdout, was
reprojected lossily over HTTP, wrapped a third way by the browser, carried no tool
version or timestamp, embedded none of the constraints it was verified against, referred
to the transaction journal by a bare string, and was unsigned. A proof nobody else can
check is a log line.

Two design rules, both learned the hard way
-------------------------------------------
**Fail closed.** An unknown enum value, a missing required field, or an unrecognised
version is a REJECT. The previous verifier tested membership of an *untrusted* denylist,
so any provenance nobody had thought of read as trustworthy; and nullable trust fields
meant an absent label read as proven.

**Never claim more than the receipt.** Strength is re-derived from provenance and the
column's authority to catch a receipt claiming more than it earned -- but a receipt that
honestly records ``plausibility_only`` is always believed, because the engine downgrades
for reasons a certificate reader cannot see.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, Final

from dataforge.domain.vocabulary import (
    PROVENANCE_ORDER as _PROVENANCE_ORDER,
)
from dataforge.domain.vocabulary import (
    REVIEW_REASONS,
    SAFETY_VERDICTS,
    VERIFICATION_STRENGTHS,
    VERIFIER_VERDICTS,
    is_trusted_provenance,
    verification_strength_for,
)

__all__ = [
    "ATTESTATION_VERSION",
    "PAYLOAD_TYPE",
    "PREDICATE_TYPE",
    "STATEMENT_TYPE",
    "AttestationCheck",
    "AttestationVerification",
    "build_attestation",
    "pae",
    "sign_attestation",
    "verify_attestation",
]

STATEMENT_TYPE: Final = "https://in-toto.io/Statement/v1"
PREDICATE_TYPE: Final = "https://dataforge.dev/RepairAttestation/v1"
PAYLOAD_TYPE: Final = "application/vnd.in-toto+json"
ATTESTATION_VERSION: Final = "1"

_HEX64_LENGTH: Final = 64
_PROVENANCES: Final[frozenset[str]] = frozenset(_PROVENANCE_ORDER)
_STRENGTHS: Final[frozenset[str]] = frozenset(VERIFICATION_STRENGTHS)
_REVIEW_REASONS: Final[frozenset[str]] = frozenset(REVIEW_REASONS)
_VERIFIER_VERDICTS: Final[frozenset[str]] = frozenset(VERIFIER_VERDICTS)
_SAFETY_VERDICTS: Final[frozenset[str]] = frozenset(SAFETY_VERDICTS)


class AttestationCheck:
    """One normative check, reported individually.

    Deliberately not a pydantic model: this module is the normative tier and stays
    importable with nothing but the standard library.
    """

    __slots__ = ("detail", "name", "ok", "skipped")

    def __init__(self, name: str, ok: bool, detail: str, *, skipped: bool = False) -> None:
        self.name = name
        self.ok = ok
        self.detail = detail
        self.skipped = skipped

    def as_dict(self) -> dict[str, object]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail, "skipped": self.skipped}

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        state = "skipped" if self.skipped else ("ok" if self.ok else "FAIL")
        return f"<AttestationCheck {self.name} {state}>"


class AttestationVerification:
    """The result of normatively verifying an attestation.

    ``ok`` is true only when every non-skipped check passed. ``skipped`` is reported
    separately and never folded into ``ok``: a verifier that returns the same answer for
    "all checks passed" and "all runnable checks passed, two were skipped" is
    misreporting, which is the defect this replaces.
    """

    __slots__ = ("checks",)

    def __init__(self, checks: list[AttestationCheck]) -> None:
        self.checks = checks

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks if not check.skipped)

    @property
    def failures(self) -> list[AttestationCheck]:
        return [check for check in self.checks if not check.ok and not check.skipped]

    @property
    def skipped(self) -> list[AttestationCheck]:
        return [check for check in self.checks if check.skipped]

    def as_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "checks": [check.as_dict() for check in self.checks]}


def _sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _is_hex64(value: object) -> bool:
    if not isinstance(value, str) or len(value) != _HEX64_LENGTH:
        return False
    return all(character in "0123456789abcdef" for character in value)


def pae(payload_type: str, body: bytes) -> bytes:
    """DSSE Pre-Authentication Encoding.

    ``PAE(type, body) = "DSSEv1" + SP + LEN(type) + SP + type + SP + LEN(body) + SP + body``

    Signing this rather than the JSON text is what removes canonicalisation from the
    trust boundary. The transaction journal used to show the alternative failing: it hashed
    a compact-separator preimage but wrote the line with default separators, so the bytes on
    disk were not the bytes hashed and a second implementation had to rediscover that. Fixed
    2026-08-23 -- the writer now uses the canonical options, so the only difference between a
    line and its preimage is the removal of the single ``event_sha256`` key. Asserted on
    bytes in ``tests/unit/test_journal_preimage.py``.

    The argument for PAE stands regardless: it holds even when both sides agree on a
    canonical form, because it removes the need for them to agree.
    """
    type_bytes = payload_type.encode("utf-8")
    return b"DSSEv1 %d %s %d %s" % (len(type_bytes), type_bytes, len(body), body)


def _canonical_json(value: object) -> bytes:
    """Serialise deterministically for digesting the embedded constraint set.

    Only used for the ``constraints_digest``, never for a signature. Sorted keys and
    compact separators, documented here so a reimplementation can reproduce the digest.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def build_attestation(
    receipt: Mapping[str, object],
    *,
    tool_version: str,
    produced_at: str,
    subject_name: str,
    constraints: Mapping[str, object] | None = None,
    journal_head_sha256: str | None = None,
    model: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    """Project a repair receipt into an in-toto Statement.

    ``constraints`` is embedded in full rather than referenced. A digest and an id list
    are dangling pointers: a third party holding only the attestation cannot resolve
    them, and the previous verifier responded to a missing schema by re-inferring
    constraints from the repaired data, which is circular.
    """
    applied = bool(receipt.get("applied"))
    post_digest = receipt.get("post_sha256")
    source_digest = receipt.get("source_sha256")
    subject_digest = post_digest if applied else source_digest

    fixes = _sequence_of_mappings(receipt.get("applied_fixes"))
    held = _sequence_of_mappings(receipt.get("suggested_fixes"))

    constraints_payload = dict(constraints) if constraints is not None else None
    constraints_digest = (
        _sha256_hex(_canonical_json(constraints_payload)) if constraints_payload else None
    )

    predicate: dict[str, Any] = {
        "attestation_version": ATTESTATION_VERSION,
        "tool": {
            "name": "dataforge",
            "version": tool_version,
            "contract_version": str(receipt.get("contract_version", "")),
        },
        "produced_at": produced_at,
        "mode": str(receipt.get("mode", "dry_run")),
        "applied": applied,
        "reversible": bool(receipt.get("reversible")),
        "source": {"digest": {"sha256": source_digest}},
        "post": {"digest": {"sha256": post_digest}} if applied else None,
        "authority": {
            "authoritative_columns": sorted(
                str(column) for column in _sequence_of_str(receipt.get("authoritative_columns"))
            ),
            "accepted_constraint_ids": [
                str(item) for item in _sequence_of_str(receipt.get("accepted_constraint_ids"))
            ],
            "constraints_digest": {"sha256": constraints_digest} if constraints_digest else None,
            "constraints": constraints_payload,
        },
        "fixes": [
            {
                "row": int(fix.get("row", -1)),  # type: ignore[call-overload]
                "column": str(fix.get("column", "")),
                "detector_id": str(fix.get("detector_id", "")),
                "provenance": str(fix.get("provenance", "")),
                "verification_strength": str(fix.get("verification_strength") or ""),
            }
            for fix in fixes
        ],
        "held": [
            {
                "row": int(item.get("row", -1)),  # type: ignore[call-overload]
                "column": str(item.get("column", "")),
                "review_reason": str(item.get("review_reason") or ""),
            }
            for item in held
        ],
        "verdicts": {
            "verifier": str(receipt.get("verifier_verdict", "not_run")),
            "safety": str(receipt.get("safety_verdict", "allow")),
            "independent_verification": str(receipt.get("independent_verification", "not_run")),
        },
        "journal": {
            "txn_id": receipt.get("txn_id"),
            "head_sha256": journal_head_sha256,
        },
        "revert_command": receipt.get("revert_command"),
        "model": dict(model) if model is not None else None,
        "limitations": [str(item) for item in _sequence_of_str(receipt.get("limitations"))],
        "verification": {
            "checks_available": [
                "envelope_recognised",
                "version_recognised",
                "schema_complete",
                "vocabulary_closed",
                "subject_matches_post_state",
                "data_identity",
                "reversibility_complete",
                "verdicts_accepting",
                "strength_is_earned",
                "constraints_present",
            ]
        },
    }

    return {
        "_type": STATEMENT_TYPE,
        "subject": [{"name": subject_name, "digest": {"sha256": subject_digest}}],
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def _sequence_of_mappings(value: object) -> list[Mapping[str, object]]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _sequence_of_str(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return list(value)
    return []


def sign_attestation(statement: Mapping[str, object], *, private_key: object) -> dict[str, Any]:
    """Wrap a Statement in a DSSE envelope signed with Ed25519.

    Signing proves a keyholder produced this payload. It does NOT establish that the
    keyholder is trustworthy: key distribution and trust roots are deployment policy and
    explicitly out of scope, so a verifier that is handed no public key must report
    ``unsigned`` rather than implying authenticity.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if not isinstance(private_key, Ed25519PrivateKey):
        message = "private_key must be an Ed25519PrivateKey"
        raise TypeError(message)

    body = _canonical_json(statement)
    signature = private_key.sign(pae(PAYLOAD_TYPE, body))
    public_bytes = private_key.public_key().public_bytes_raw()
    return {
        "payload": base64.b64encode(body).decode("ascii"),
        "payloadType": PAYLOAD_TYPE,
        "signatures": [
            {
                "keyid": _sha256_hex(public_bytes)[:16],
                "sig": base64.b64encode(signature).decode("ascii"),
            }
        ],
    }


def _verify_dsse(
    envelope: Mapping[str, object],
    *,
    public_key_raw: bytes | None,
) -> tuple[AttestationCheck, dict[str, Any] | None]:
    """Verify a DSSE envelope and return the payload parsed from the VERIFIED bytes.

    Per the DSSE spec, the payload handed to the application must be the same bytes that
    were verified -- never a re-parse of the envelope afterwards.
    """
    payload_b64 = envelope.get("payload")
    payload_type = envelope.get("payloadType")
    if not isinstance(payload_b64, str) or not isinstance(payload_type, str):
        return AttestationCheck("signature", False, "envelope is missing payload fields"), None
    if payload_type != PAYLOAD_TYPE:
        return (
            AttestationCheck("signature", False, f"unexpected payloadType {payload_type!r}"),
            None,
        )
    try:
        body = base64.b64decode(payload_b64, validate=True)
    except (ValueError, TypeError):
        return AttestationCheck("signature", False, "payload is not valid base64"), None

    try:
        statement = json.loads(body)
    except json.JSONDecodeError:
        return AttestationCheck("signature", False, "payload is not valid JSON"), None
    if not isinstance(statement, dict):
        return AttestationCheck("signature", False, "payload is not an object"), None

    if public_key_raw is None:
        return (
            AttestationCheck(
                "signature",
                True,
                "unsigned: no public key supplied, so authenticity was NOT established",
                skipped=True,
            ),
            statement,
        )

    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    signatures = envelope.get("signatures")
    if not isinstance(signatures, Sequence) or not signatures:
        return AttestationCheck("signature", False, "envelope carries no signatures"), None

    public_key = Ed25519PublicKey.from_public_bytes(public_key_raw)
    preauth = pae(PAYLOAD_TYPE, body)
    for entry in signatures:
        if not isinstance(entry, Mapping):
            continue
        raw_sig = entry.get("sig")
        if not isinstance(raw_sig, str):
            continue
        try:
            public_key.verify(base64.b64decode(raw_sig, validate=True), preauth)
        except (InvalidSignature, ValueError, TypeError):
            continue
        return AttestationCheck("signature", True, "DSSE signature verified"), statement

    return AttestationCheck("signature", False, "no signature verified against the key"), None


def verify_attestation(
    document: Mapping[str, object],
    *,
    data_bytes: bytes | None = None,
    public_key_raw: bytes | None = None,
) -> AttestationVerification:
    """Normatively verify an attestation. Pure stdlib apart from optional signing.

    Accepts either a bare Statement or a DSSE envelope. Fails closed: anything
    unrecognised is a rejection, never a pass with a warning.
    """
    checks: list[AttestationCheck] = []

    statement: Mapping[str, object] | None = document
    if "payload" in document and "payloadType" in document:
        signature_check, parsed = _verify_dsse(document, public_key_raw=public_key_raw)
        checks.append(signature_check)
        if parsed is None:
            return AttestationVerification(checks)
        statement = parsed

    assert statement is not None  # noqa: S101 - narrowing for type checkers

    type_ok = statement.get("_type") == STATEMENT_TYPE
    predicate_ok = statement.get("predicateType") == PREDICATE_TYPE
    checks.append(
        AttestationCheck(
            "envelope_recognised",
            type_ok and predicate_ok,
            f"_type={statement.get('_type')!r} predicateType={statement.get('predicateType')!r}",
        )
    )
    if not (type_ok and predicate_ok):
        return AttestationVerification(checks)

    predicate = statement.get("predicate")
    if not isinstance(predicate, Mapping):
        checks.append(AttestationCheck("schema_complete", False, "predicate is not an object"))
        return AttestationVerification(checks)

    version = predicate.get("attestation_version")
    checks.append(
        AttestationCheck(
            "version_recognised",
            version == ATTESTATION_VERSION,
            f"attestation_version={version!r}",
        )
    )
    if version != ATTESTATION_VERSION:
        return AttestationVerification(checks)

    checks.extend(_check_schema(predicate, statement))
    checks.extend(_check_vocabulary(predicate))
    checks.extend(_check_identity(predicate, statement, data_bytes))
    checks.extend(_check_reversibility(predicate))
    checks.extend(_check_verdicts(predicate))
    checks.extend(_check_strength(predicate))

    return AttestationVerification(checks)


def _check_schema(
    predicate: Mapping[str, object], statement: Mapping[str, object]
) -> list[AttestationCheck]:
    missing: list[str] = []
    for field in (
        "tool",
        "produced_at",
        "mode",
        "applied",
        "reversible",
        "source",
        "authority",
        "fixes",
        "held",
        "verdicts",
        "journal",
        "limitations",
    ):
        if field not in predicate:
            missing.append(field)

    tool = predicate.get("tool")
    if isinstance(tool, Mapping):
        for field in ("name", "version"):
            value = tool.get(field)
            if not isinstance(value, str) or not value:
                missing.append(f"tool.{field}")
    else:
        missing.append("tool")

    subject = statement.get("subject")
    if not isinstance(subject, Sequence) or not subject:
        missing.append("subject")

    return [
        AttestationCheck(
            "schema_complete",
            not missing,
            "all required fields present" if not missing else f"missing/invalid: {missing}",
        )
    ]


def _check_vocabulary(predicate: Mapping[str, object]) -> list[AttestationCheck]:
    """Every enum value must be a member of its published set. Unknown is a REJECT."""
    problems: list[str] = []

    for fix in _sequence_of_mappings(predicate.get("fixes")):
        provenance = fix.get("provenance")
        if provenance not in _PROVENANCES:
            problems.append(f"fix provenance {provenance!r}")
        strength = fix.get("verification_strength")
        if strength not in _STRENGTHS:
            problems.append(f"fix verification_strength {strength!r}")

    for item in _sequence_of_mappings(predicate.get("held")):
        reason = item.get("review_reason")
        if reason not in _REVIEW_REASONS:
            problems.append(f"held review_reason {reason!r}")

    verdicts = predicate.get("verdicts")
    if isinstance(verdicts, Mapping):
        if verdicts.get("verifier") not in _VERIFIER_VERDICTS:
            problems.append(f"verifier verdict {verdicts.get('verifier')!r}")
        if verdicts.get("safety") not in _SAFETY_VERDICTS:
            problems.append(f"safety verdict {verdicts.get('safety')!r}")
        if verdicts.get("independent_verification") not in {"agreed", "not_run"}:
            problems.append(
                f"independent_verification {verdicts.get('independent_verification')!r}"
            )
    else:
        problems.append("verdicts is not an object")

    mode = predicate.get("mode")
    if mode not in {"apply", "dry_run"}:
        problems.append(f"mode {mode!r}")

    return [
        AttestationCheck(
            "vocabulary_closed",
            not problems,
            "every enum value is a published member"
            if not problems
            else f"unrecognised values: {problems}",
        )
    ]


def _check_identity(
    predicate: Mapping[str, object],
    statement: Mapping[str, object],
    data_bytes: bytes | None,
) -> list[AttestationCheck]:
    checks: list[AttestationCheck] = []
    applied = bool(predicate.get("applied"))

    source = predicate.get("source")
    source_digest = source.get("digest", {}).get("sha256") if isinstance(source, Mapping) else None
    post = predicate.get("post")
    post_digest = post.get("digest", {}).get("sha256") if isinstance(post, Mapping) else None

    expected = post_digest if applied else source_digest

    subject_list = statement.get("subject")
    subject_digest = None
    if isinstance(subject_list, Sequence) and subject_list:
        first = subject_list[0]
        if isinstance(first, Mapping):
            digest = first.get("digest")
            if isinstance(digest, Mapping):
                subject_digest = digest.get("sha256")

    checks.append(
        AttestationCheck(
            "subject_matches_post_state",
            _is_hex64(subject_digest) and subject_digest == expected,
            f"subject={subject_digest!r} expected={expected!r}",
        )
    )

    if data_bytes is None:
        checks.append(
            AttestationCheck(
                "data_identity",
                True,
                "no data supplied, so the digest claim was NOT checked",
                skipped=True,
            )
        )
    else:
        actual = _sha256_hex(data_bytes)
        checks.append(
            AttestationCheck(
                "data_identity",
                actual == expected,
                f"sha256(data)={actual} expected={expected!r}",
            )
        )

    return checks


def _check_reversibility(predicate: Mapping[str, object]) -> list[AttestationCheck]:
    if not predicate.get("applied"):
        return [
            AttestationCheck(
                "reversibility_complete",
                True,
                "dry run: nothing was written, so there is nothing to reverse",
            )
        ]
    journal = predicate.get("journal")
    txn_id = journal.get("txn_id") if isinstance(journal, Mapping) else None
    problems = []
    if not predicate.get("reversible"):
        problems.append("reversible is false for an applied run")
    if not isinstance(txn_id, str) or not txn_id:
        problems.append("journal.txn_id is missing")
    revert = predicate.get("revert_command")
    if not isinstance(revert, str) or not revert:
        problems.append("revert_command is missing")
    return [
        AttestationCheck(
            "reversibility_complete",
            not problems,
            "applied run is fully reversible" if not problems else f"{problems}",
        )
    ]


def _check_verdicts(predicate: Mapping[str, object]) -> list[AttestationCheck]:
    verdicts = predicate.get("verdicts")
    if not isinstance(verdicts, Mapping):
        return [AttestationCheck("verdicts_accepting", False, "verdicts missing")]
    verifier = verdicts.get("verifier")
    safety = verdicts.get("safety")
    if not predicate.get("applied"):
        return [
            AttestationCheck(
                "verdicts_accepting",
                safety != "deny",
                f"dry run: safety={safety!r}",
            )
        ]
    ok = verifier == "accept" and safety == "allow"
    return [
        AttestationCheck(
            "verdicts_accepting",
            ok,
            f"verifier={verifier!r} safety={safety!r}",
        )
    ]


def _check_strength(predicate: Mapping[str, object]) -> list[AttestationCheck]:
    """Re-derive strength per fix, and require embedded constraints for untrusted proof."""
    authority = predicate.get("authority")
    covered: frozenset[str] = frozenset()
    constraints: object = None
    if isinstance(authority, Mapping):
        covered = frozenset(
            str(column) for column in _sequence_of_str(authority.get("authoritative_columns"))
        )
        constraints = authority.get("constraints")

    applied = bool(predicate.get("applied"))
    unproven: list[str] = []
    needs_constraints = False

    for fix in _sequence_of_mappings(predicate.get("fixes")):
        column = str(fix.get("column", ""))
        provenance = fix.get("provenance")
        recorded = fix.get("verification_strength")

        if recorded == "plausibility_only":
            unproven.append(f"{column} (recorded plausibility_only)")
            continue

        on_authority = column in covered
        derived = verification_strength_for(
            str(provenance) if isinstance(provenance, str) else None,
            authoritative_schema_present=on_authority,
        )
        if derived != "proven":
            unproven.append(f"{column}:{provenance!r} (recorded {recorded!r})")
        elif not is_trusted_provenance(provenance if isinstance(provenance, str) else None):
            # Proven only because a schema covered the column, so the attestation must
            # carry that schema for anyone else to check it.
            needs_constraints = True

    checks = [
        AttestationCheck(
            "strength_is_earned",
            not (applied and unproven),
            "every applied fix is proven by provenance or by column authority"
            if not (applied and unproven)
            else f"unproven applied writes: {unproven}",
        )
    ]

    if needs_constraints:
        has_constraints = isinstance(constraints, Mapping) and bool(constraints)
        checks.append(
            AttestationCheck(
                "constraints_present",
                has_constraints,
                "embedded constraints support the schema-proven writes"
                if has_constraints
                else "a write is proven only by schema authority, but no constraints are embedded",
            )
        )
    else:
        checks.append(
            AttestationCheck(
                "constraints_present",
                True,
                "no write depends on schema authority",
            )
        )

    return checks
