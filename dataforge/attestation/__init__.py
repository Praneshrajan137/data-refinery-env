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


class AttestationEmission:
    """The outcome of trying to attest a completed repair automatically.

    Two states, and neither is silent. Either an envelope was built and self-verified, or
    ``reason`` says why not. There is deliberately no third state where the caller gets
    ``None`` and no explanation: an agent that receives nothing cannot distinguish "this
    repair is not attestable" from "attestation is not implemented", and until 2026-08-29
    the second was true and the first was what it looked like.

    Not a pydantic model, for the same reason as :class:`AttestationCheck`: this module is
    the normative tier and stays importable with nothing but the standard library.
    """

    __slots__ = ("envelope", "reason")

    def __init__(self, envelope: dict[str, Any] | None, reason: str | None) -> None:
        self.envelope = envelope
        self.reason = reason

    @property
    def ok(self) -> bool:
        return self.envelope is not None

    def as_dict(self) -> dict[str, Any]:
        """Render for a JSON payload, carrying the reason when there is no envelope."""
        if self.envelope is not None:
            return {"attestation": self.envelope}
        return {"attestation_unavailable": self.reason}


def attest_repair(
    receipt: Mapping[str, Any],
    *,
    subject_name: str,
    tool_version: str,
    produced_at: str,
    constraints: Mapping[str, Any] | None = None,
    journal_head_sha256: str | None = None,
    data_bytes: bytes | None = None,
    witnesses: Mapping[tuple[int, str], Mapping[str, Any]] | None = None,
) -> AttestationEmission:
    """Build and self-verify an attestation for a completed repair.

    The single implementation shared by ``dataforge repair`` and the MCP server, so the
    portable proof cannot reach one surface and not the other. Before 2026-08-29 it reached
    neither automatically: ``build_attestation`` had a CLI caller only through a separate
    ``attest build`` command needing three hand-supplied arguments, and the string ``attest``
    appeared nowhere in ``dataforge-mcp``. So the differentiator -- an in-toto/DSSE
    attestation a third party can verify offline with no solver, no network and no schema --
    could not reach an agent at all.

    **Fails closed and says so.** An attestation this module's own verifier would reject is
    never returned; the reason is. Every over-trust defect recorded in this file was a
    verifier believing more than a receipt supported, and returning unverifiable output would
    reintroduce that class from the producer side.

    ``data_bytes`` is optional and its absence is not folded into success: without it the
    digest claim is built but never checked, which the returned envelope records in
    ``verification.checks_available`` exactly as ``attest verify`` reports a skipped check.
    """
    try:
        statement = build_attestation(
            receipt,
            tool_version=tool_version,
            produced_at=produced_at,
            subject_name=subject_name,
            constraints=constraints,
            journal_head_sha256=journal_head_sha256,
            witnesses=witnesses,
        )
    except Exception as exc:  # noqa: BLE001 - a build failure must not fail the repair
        return AttestationEmission(None, f"the attestation could not be built: {exc}")

    verification = verify_attestation(statement, data_bytes=data_bytes)
    if not verification.ok:
        failures = ", ".join(check.name for check in verification.failures)
        return AttestationEmission(
            None,
            (
                f"the built attestation does not verify ({failures}), so it was withheld. "
                "This is a defect in the receipt or its inputs, not something to work around."
            ),
        )
    return AttestationEmission(statement, None)


def _witness_for(
    witnesses: Mapping[tuple[int, str], Mapping[str, object]] | None,
    fix: Mapping[str, object],
) -> dict[str, Any]:
    """Return the ``witness`` key for one fix, or nothing if none was supplied.

    Omitted rather than set to ``None`` so that "unwitnessed" is one state in the wire format
    instead of two indistinguishable ones.
    """
    if not witnesses:
        return {}
    try:
        key = (int(fix.get("row", -1)), str(fix.get("column", "")))  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return {}
    witness = witnesses.get(key)
    return {"witness": dict(witness)} if witness is not None else {}


def build_attestation(
    receipt: Mapping[str, object],
    *,
    tool_version: str,
    produced_at: str,
    subject_name: str,
    constraints: Mapping[str, object] | None = None,
    journal_head_sha256: str | None = None,
    model: Mapping[str, object] | None = None,
    witnesses: Mapping[tuple[int, str], Mapping[str, object]] | None = None,
) -> dict[str, Any]:
    """Project a repair receipt into an in-toto Statement.

    ``constraints`` is embedded in full rather than referenced. A digest and an id list
    are dangling pointers: a third party holding only the attestation cannot resolve
    them, and the previous verifier responded to a missing schema by re-inferring
    constraints from the repaired data, which is circular.

    ``witnesses`` maps ``(row, column)`` to the entailment witness for that fix, as produced
    by :meth:`dataforge.witness.EntailmentWitness.to_attestation_payload`. It is optional and
    absence is reported by the verifier rather than folded into success: a fix with no witness
    is one whose derivation nobody can check. Values inside a witness are hashed, so embedding
    one does not turn a shareable document into a data disclosure -- see that method for why
    that preserves rather than weakens third-party verification.
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
                **_witness_for(witnesses, fix),
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
    checks.extend(_check_witness(predicate))

    return AttestationVerification(checks)


#: Predicate fields a conforming attestation must carry.
#:
#: Hoisted out of :func:`_check_schema` 2026-08-29 so the published JSON Schema can be
#: GENERATED from it rather than restated beside it. A hand-written schema and a verifier
#: are two specifications, and `PRODUCT.md`:94-113 records what happens when a gate restates
#: a population instead of deriving it: the prose and the gate agree with each other and both
#: disagree with the code.
REQUIRED_PREDICATE_FIELDS: Final = (
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
)

#: Fields the ``tool`` block must carry, checked as non-empty strings.
REQUIRED_TOOL_FIELDS: Final = ("name", "version")


def _check_witness(predicate: Mapping[str, object]) -> list[AttestationCheck]:
    """Check each fix's entailment witness for internal coherence.

    WHY THIS IS NOT THE SAME CHECK AS ``strength_is_earned``

    ``_check_strength`` re-derives ``verification_strength`` by calling
    :func:`verification_strength_for` -- the *same function object* the engine calls to stamp
    the field. Within one language it therefore validates field consistency, not the rule: a
    wrong trust model is invisible to it, which is precisely the axis ``decimal_shift`` lived
    on. An attestation from that window would have verified clean.

    A witness is a different kind of claim. It states the evidence a constraint-derived write
    rested on -- which constraint acted, how large the determinant group was, how the values
    were distributed, and how much support the written value had -- and those are **arithmetic
    facts that can contradict each other**. A fix claiming a write whose own witness shows no
    strict majority is refused here regardless of its provenance, its column authority, or
    what any labelling rule says. That is a rejection the strength check cannot express.

    WHAT THIS DOES NOT DO, AND WHY THAT IS THE POINT

    It does not recompute the distribution from the data. Doing so would make the normative
    verifier a CSV parser, and two implementations would then have to agree byte-for-byte on
    quoting, encodings and line endings -- ``docs/trust/apply-rewrites-line-endings.md``
    records that this project has already been bitten there. So the normative tier stays
    integer arithmetic, and the *data* check is left to whoever holds the table: values are
    published as ``sha256(value)[:16]``, so a third party hashes their own group and compares
    counts, in SQL or any language, with no DataForge code. Verification of the derivation is
    therefore possible **without trusting our rule and without running our code**, which is
    what a portable proof is for.

    A fix with no witness is reported as unwitnessed rather than failed. Not every write is
    constraint-derived, and treating absence as failure would make the check unrunnable on
    honest input.
    """
    fixes = _sequence_of_mappings(predicate.get("fixes"))
    if not fixes:
        return [
            AttestationCheck(
                "witness_is_coherent",
                True,
                "no fixes, so there is no witness to check",
                skipped=True,
            )
        ]

    problems: list[str] = []
    witnessed = 0
    for fix in fixes:
        witness = fix.get("witness")
        label = f"{fix.get('column')}@{fix.get('row')}"
        if witness is None:
            continue
        if not isinstance(witness, Mapping):
            problems.append(f"{label}: witness is not an object")
            continue
        witnessed += 1

        group_size = witness.get("group_size")
        support = witness.get("support")
        if not isinstance(group_size, int) or not isinstance(support, int):
            problems.append(f"{label}: group_size and support must be integers")
            continue
        if group_size < 1 or support < 1:
            problems.append(f"{label}: group_size={group_size} support={support} must be >= 1")
            continue
        if support > group_size:
            problems.append(f"{label}: support {support} exceeds group_size {group_size}")
            continue
        # The shipped decision rule is a STRICT majority, not a plurality. Mutant M16 records
        # the difference: plurality writes on 2 votes of 5 across four distinct values, with
        # `deterministic` provenance that bypasses calibration, and is worse on every measured
        # axis. A witness that does not clear this bar contradicts the rule the write claimed.
        if support * 2 <= group_size:
            problems.append(
                f"{label}: support {support} of {group_size} is not a strict majority, so the "
                "written value was not entailed by the rule the product implements"
            )
            continue

        entries = witness.get("value_digests")
        if not isinstance(entries, Sequence) or isinstance(entries, str | bytes):
            problems.append(f"{label}: value_digests must be a list")
            continue
        counts: dict[str, int] = {}
        for entry in entries:
            if (
                isinstance(entry, Sequence)
                and not isinstance(entry, str | bytes)
                and len(entry) == 2
                and isinstance(entry[0], str)
                and isinstance(entry[1], int)
            ):
                counts[entry[0]] = entry[1]
        if sum(counts.values()) > group_size:
            problems.append(f"{label}: value counts exceed group_size {group_size}")
            continue
        if not witness.get("truncated") and sum(counts.values()) != group_size:
            problems.append(
                f"{label}: value counts sum to {sum(counts.values())} but group_size is "
                f"{group_size} and the distribution is not marked truncated"
            )
            continue

        new_digest = witness.get("new_value_digest")
        old_digest = witness.get("old_value_digest")
        if counts.get(str(new_digest)) != support:
            problems.append(
                f"{label}: the written value's recorded count does not equal its support"
            )
            continue
        if new_digest == old_digest:
            problems.append(f"{label}: the write replaces a value with itself")
            continue
        if old_digest is not None and str(old_digest) not in counts:
            problems.append(f"{label}: the replaced value does not appear in its own group")

    if problems:
        return [
            AttestationCheck(
                "witness_is_coherent",
                False,
                "; ".join(problems[:5])
                + (f"; and {len(problems) - 5} more" if len(problems) > 5 else ""),
            )
        ]
    if witnessed == 0:
        return [
            AttestationCheck(
                "witness_is_coherent",
                True,
                (
                    f"no entailment witness on any of {len(fixes)} fix(es); the derivation was "
                    "NOT checked"
                ),
                skipped=True,
            )
        ]
    return [
        AttestationCheck(
            "witness_is_coherent",
            True,
            f"{witnessed} of {len(fixes)} fix(es) carry a coherent entailment witness",
        )
    ]


def _check_schema(
    predicate: Mapping[str, object], statement: Mapping[str, object]
) -> list[AttestationCheck]:
    missing: list[str] = []
    for field in REQUIRED_PREDICATE_FIELDS:
        if field not in predicate:
            missing.append(field)

    tool = predicate.get("tool")
    if isinstance(tool, Mapping):
        for field in REQUIRED_TOOL_FIELDS:
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
    subject_digests: list[object] = []
    if isinstance(subject_list, Sequence) and not isinstance(subject_list, str | bytes):
        for entry in subject_list:
            if isinstance(entry, Mapping):
                digest = entry.get("digest")
                subject_digests.append(
                    digest.get("sha256") if isinstance(digest, Mapping) else None
                )
            else:
                subject_digests.append(None)

    # in-toto v1 permits N subjects. Both verifiers used to read `subject[0]` and ignore the
    # rest, which is a smuggling hole rather than a cosmetic gap: append a second subject
    # naming a malicious file's digest to an otherwise valid attestation and a consumer that
    # checks the first subject reports `verified`, while the statement now also asserts
    # something about a file nobody attested.
    #
    # This predicateType describes ONE repair of ONE artifact, so there is no honest reading
    # under which extra subjects are verifiable here. The rule is therefore a refusal, not a
    # wider read: every subject must carry the expected digest. Duplicates are harmless and
    # allowed; a subject naming a different artifact fails closed. Widening the predicate to
    # describe several artifacts would be a new format, not a verifier change.
    unexpected = [digest for digest in subject_digests if digest != expected]
    if not subject_digests:
        detail = "no subject present"
    elif unexpected:
        detail = (
            f"{len(unexpected)} of {len(subject_digests)} subject(s) name a different "
            f"artifact than the predicate describes: {unexpected[:3]!r} expected={expected!r}"
        )
    else:
        detail = (
            f"all {len(subject_digests)} subject(s) match, subject={expected!r}"
            if len(subject_digests) > 1
            else f"subject={expected!r}"
        )

    checks.append(
        AttestationCheck(
            "subject_matches_post_state",
            bool(subject_digests) and not unexpected and _is_hex64(expected),
            detail,
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
