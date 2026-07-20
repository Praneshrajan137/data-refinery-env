"""Self-verifiable trust certificate for a DataForge repair run.

The repair receipt already records what was applied, why, and how to reverse it.
This module makes that receipt *independently checkable*: anyone holding the data
and the certificate can confirm the certificate describes that exact data and that
its trust invariants hold -- without re-running DataForge or trusting the tool.
That is what makes trust portable: it travels with the data.

Pure and dependency-free (stdlib ``hashlib`` only). It does not re-execute repairs;
it re-checks the claims that can be verified from the certificate plus the data:
cryptographic identity (SHA-256), reversibility completeness, verifier acceptance,
and provenance honesty (whether the auto-applied set is proven-deterministic or a
policy-permitted plausible LLM write).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import cast

from pydantic import BaseModel

__all__ = [
    "CertificateCheck",
    "CertificateVerification",
    "reverify_certificate",
    "verify_certificate",
]

_LLM_PROVENANCE = frozenset({"llm_live", "llm_cache", "external"})


class CertificateCheck(BaseModel):
    """One independently re-checked claim from a repair certificate."""

    name: str
    ok: bool
    detail: str

    model_config = {"frozen": True}


class CertificateVerification(BaseModel):
    """Result of independently re-verifying a repair certificate."""

    ok: bool
    checks: list[CertificateCheck]

    model_config = {"frozen": True}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _check(name: str, ok: bool, detail: str) -> CertificateCheck:  # noqa: FBT001
    return CertificateCheck(name=name, ok=ok, detail=detail)


def verify_certificate(
    receipt: Mapping[str, object],
    *,
    data_bytes: bytes | None = None,
) -> CertificateVerification:
    """Independently re-verify a repair certificate (the receipt) against data.

    Args:
        receipt: The repair receipt (``RepairReceipt.model_dump(mode="json")``).
        data_bytes: Optional raw bytes of the file the certificate describes. When
            provided, the SHA-256 identity claim is checked: for an applied run the
            bytes must match ``post_sha256``; for a dry run they must match
            ``source_sha256``. Tampering with either the data or the recorded hash
            is detected here.

    Returns:
        A ``CertificateVerification`` whose ``ok`` is True only if every check
        passed. Each check is itemized so a caller can see exactly what held.
    """
    checks: list[CertificateCheck] = []

    schema_version = receipt.get("schema_version")
    checks.append(
        _check(
            "schema_recognized",
            schema_version == "repair_receipt_v1",
            f"schema_version={schema_version!r}",
        )
    )

    applied = bool(receipt.get("applied"))
    source_sha = receipt.get("source_sha256")
    post_sha = receipt.get("post_sha256")

    if data_bytes is not None:
        actual = _sha256(data_bytes)
        expected = post_sha if applied else source_sha
        state = "post_sha256" if applied else "source_sha256"
        checks.append(
            _check(
                "data_identity",
                actual == expected,
                f"sha256(data)={actual} vs {state}={expected!r}",
            )
        )

    if applied:
        # An applied run must be fully reversible and cryptographically pinned.
        checks.append(
            _check(
                "reversibility_complete",
                bool(receipt.get("reversible"))
                and bool(post_sha)
                and bool(receipt.get("txn_id"))
                and bool(receipt.get("revert_command")),
                "applied run carries reversible flag, post hash, txn id, revert command",
            )
        )
        checks.append(
            _check(
                "verifier_accepted",
                receipt.get("verifier_verdict") == "accept",
                f"verifier_verdict={receipt.get('verifier_verdict')!r}",
            )
        )
        checks.append(
            _check(
                "safety_allowed",
                receipt.get("safety_verdict") == "allow",
                f"safety_verdict={receipt.get('safety_verdict')!r}",
            )
        )

    # Proof honesty: is the auto-applied set PROVEN (deterministic OR verified
    # against an authoritative schema), or did a policy permit a plausibility-only
    # (not proven) write? Prefer the per-fix ``verification_strength`` when present
    # (authoritative: a schema-verified external/LLM value is genuinely proven),
    # falling back to raw provenance for legacy/hand-built receipts without it.
    applied_fixes_raw = receipt.get("applied_fixes")
    applied_fixes = (
        [f for f in applied_fixes_raw if isinstance(f, Mapping)]
        if isinstance(applied_fixes_raw, Sequence)
        and not isinstance(applied_fixes_raw, str | bytes)
        else []
    )
    if applied_fixes:
        unproven_applied = applied and any(
            fix.get("verification_strength") == "plausibility_only" for fix in applied_fixes
        )
        checks.append(
            _check(
                "auto_apply_is_proven_deterministic",
                not unproven_applied,
                (
                    "auto-applied set is proven (deterministic or authoritative-schema-verified)"
                    if not unproven_applied
                    else "auto-applied set includes a policy-permitted plausibility-only (unproven) write"
                ),
            )
        )
    else:
        provenance = receipt.get("candidate_provenance")
        if isinstance(provenance, Sequence) and not isinstance(provenance, str | bytes):
            provenances = [str(p) for p in provenance]
            untrusted_applied = applied and any(p in _LLM_PROVENANCE for p in provenances)
            checks.append(
                _check(
                    "auto_apply_is_proven_deterministic",
                    not untrusted_applied,
                    (
                        "auto-applied set is deterministic (proven)"
                        if not untrusted_applied
                        else f"auto-applied set includes policy-permitted LLM/external writes: {provenances}"
                    ),
                )
            )

    return CertificateVerification(ok=all(c.ok for c in checks), checks=checks)


def reverify_certificate(
    receipt: Mapping[str, object],
    *,
    data_bytes: bytes,
    schema: object | None = None,
) -> CertificateVerification:
    """Deep, independent re-verification: re-derive ACCEPT for every applied cell.

    Beyond the hash/structural checks of ``verify_certificate``, this reconstructs
    the applied fixes from the certificate and re-runs the REAL verifier on the
    repaired data -- per fix, mirroring the engine's guard selection (an LLM value
    with no authoritative schema is re-checked by the advisory inferred guard;
    deterministic and authoritative-schema fixes are checked against the schema).
    It also confirms the recorded ``verification_strength`` labels are truthful.

    Independence: this is independent in DATA and EXECUTION (a fresh invocation
    against the certified bytes, trusting none of the engine's stored verdicts).
    On the AUTHORITATIVE-schema path it is ALSO independent in IMPLEMENTATION: the
    ACCEPT re-derivation runs two independently-written constraint checkers -- the
    z3-backed ``SMTVerifier`` and the direct-evaluation ``DirectVerifier`` -- combined
    fail-closed, so a bug in either implementation is caught rather than passing
    twice. For the schema-less advisory path the inferred guard is re-run with a
    single implementation (the diverse checker intentionally does not re-implement
    the heuristic inferred guard). This catches tampering, drift, receipt/data
    mismatch, and single-implementation verifier bugs.

    Args:
        receipt: The repair receipt (``RepairReceipt.model_dump(mode="json")``).
        data_bytes: Raw bytes of the repaired (post-state) file the certificate
            describes.
        schema: Optional authoritative ``Schema`` used at repair time. When None,
            constraints are re-inferred from the data for the advisory re-check.
    """
    # Local imports keep the lightweight verify_certificate pure/stdlib.
    import tempfile
    from pathlib import Path

    from dataforge.repairers.base import ProposedFix
    from dataforge.schema_inference import infer_verification_schema
    from dataforge.table import read_csv, table_to_csv_bytes
    from dataforge.transactions.txn import CellFix
    from dataforge.verifier import SMTVerifier, VerificationVerdict
    from dataforge.verifier.differential import differential_verify
    from dataforge.verifier.schema import Schema

    base = verify_certificate(receipt, data_bytes=data_bytes)
    # The strict "auto_apply_is_proven_deterministic" flag is informational here:
    # an honestly-recorded unproven opt-in is a VALID certificate. reverify judges
    # constraint re-derivation and truthfulness of the recorded labels instead.
    checks: list[CertificateCheck] = [
        c for c in base.checks if c.name != "auto_apply_is_proven_deterministic"
    ]

    applied_raw = receipt.get("applied_fixes")
    applied: list[Mapping[str, object]] = (
        [f for f in applied_raw if isinstance(f, Mapping)]
        if isinstance(applied_raw, Sequence) and not isinstance(applied_raw, str | bytes)
        else []
    )
    if not applied:
        checks.append(_check("reverify_applied_fixes", True, "no applied fixes to re-verify"))
        return CertificateVerification(ok=all(c.ok for c in checks), checks=checks)

    with tempfile.TemporaryDirectory() as temp_dir:
        post_path = Path(temp_dir) / "post.csv"
        post_path.write_bytes(data_bytes)
        post_df = read_csv(post_path)
        # Confirm the loaded table round-trips to the same bytes (parse integrity).
        checks.append(
            _check(
                "reverify_parse_integrity",
                table_to_csv_bytes(post_df) == data_bytes,
                "repaired data parses back to identical bytes",
            )
        )

        verification_schema = infer_verification_schema(post_df) if schema is None else None
        verifier = SMTVerifier()
        constraints_ok = True
        strength_truthful = True
        independent_agreement_ok = True
        cross_checked = schema is not None
        detail_parts: list[str] = []
        agreement_details: list[str] = []
        for fix in applied:
            row = int(fix["row"])  # type: ignore[call-overload]
            column = str(fix["column"])
            new_value = str(fix["new_value"])
            old_value = str(fix.get("old_value", ""))
            provenance = str(fix.get("provenance", "deterministic"))
            strength = fix.get("verification_strength")
            is_llm = provenance in _LLM_PROVENANCE

            # Re-run the real verifier on the repaired cell value, selecting the
            # same guard the engine used: the advisory inferred guard applies only
            # to LLM values with no authoritative schema.
            guard = verification_schema if (schema is None and is_llm) else None
            proposed = ProposedFix(
                fix=CellFix(
                    row=row,
                    column=column,
                    old_value=old_value,
                    new_value=new_value,
                    detector_id=str(fix.get("detector_id", "reverify")),
                ),
                reason="reverify",
                confidence=1.0,
                provenance=provenance,  # type: ignore[arg-type]
            )
            if schema is not None:
                # Authoritative path: re-derive with TWO independent checkers,
                # fail-closed. Diversity in implementation, not just data/execution.
                differential = differential_verify(post_df, [proposed], cast(Schema, schema))
                verdict = differential.verdict
                reason = differential.reason
                if not differential.agreement:
                    independent_agreement_ok = False
                    agreement_details.append(
                        f"({row},{column}) primary={differential.primary_verdict.value} "
                        f"diverse={differential.secondary_verdict.value}"
                    )
            else:
                result = verifier.verify(post_df, [proposed], schema, verification_schema=guard)
                verdict = result.verdict
                reason = result.reason
            if verdict != VerificationVerdict.ACCEPT:
                constraints_ok = False
                detail_parts.append(f"({row},{column}) {verdict.value}: {reason}")

            # Truthfulness: a fix recorded "proven" must be deterministic or backed
            # by an authoritative schema. "plausibility_only" is always truthful.
            if strength == "proven" and is_llm and schema is None:
                strength_truthful = False
                detail_parts.append(f"({row},{column}) claims proven but is unverified LLM value")

        checks.append(
            _check(
                "reverify_constraints_accept",
                constraints_ok,
                "every applied cell re-derives ACCEPT"
                if constraints_ok
                else "; ".join(detail_parts),
            )
        )
        if cross_checked:
            checks.append(
                _check(
                    "reverify_independent_agreement",
                    independent_agreement_ok,
                    "two independent verifiers agree on every applied cell"
                    if independent_agreement_ok
                    else "; ".join(agreement_details),
                )
            )
        checks.append(
            _check(
                "reverify_recorded_strength_truthful",
                strength_truthful,
                "recorded verification_strength labels are truthful",
            )
        )

    return CertificateVerification(ok=all(c.ok for c in checks), checks=checks)
