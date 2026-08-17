"""Generate the TypeScript twin of the domain vocabulary from its Python source.

The browser needs the same closed vocabularies the engine reasons about. It used to get
them by hand-transcription, and the copies disagreed three times -- once in the function
every trust surface routes through, and once in the certificate a third party reads.

So the vocabulary is written once, in ``dataforge/domain/vocabulary.py``, and this
script projects it into TypeScript. The generated file is committed (so the frontend
builds without Python) and verified byte-for-byte by ``--check`` in CI, which is what
makes the two sides provably identical rather than nominally in sync.

Usage:
    python scripts/ci/generate_domain_vocabulary.py --check
    python scripts/ci/generate_domain_vocabulary.py --write
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.domain import vocabulary as vocab  # noqa: E402

TARGET = PROJECT_ROOT / "playground" / "web" / "src" / "domain" / "vocabulary.generated.ts"
SOURCE = PROJECT_ROOT / "dataforge" / "domain" / "vocabulary.py"


def _source_fingerprint() -> str:
    """SHA-256 of the Python source, normalised to LF.

    Embedded in the generated file so a Node-only environment can detect staleness
    without running Python: the frontend build hashes the source itself and compares.
    Normalised because this checkout stores CRLF, and a line-ending difference is not a
    vocabulary change.
    """
    raw = SOURCE.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(raw).hexdigest()


HEADER = """/**
 * GENERATED FILE -- DO NOT EDIT BY HAND.
 *
 * Source:      dataforge/domain/vocabulary.py
 * Source hash: sha256:{fingerprint}
 * Generator:   scripts/ci/generate_domain_vocabulary.py
 * Verify:      python scripts/ci/generate_domain_vocabulary.py --check
 *              (or, without Python: npm run audit:vocabulary)
 *
 * These are the closed vocabularies the engine reasons about. They are generated
 * rather than transcribed because transcription failed three times: `entity_consensus`
 * went missing from the untrusted-provenance set (so an untrusted value reported as
 * `proven` in the one function every trust surface routes through), the review-reason
 * humanizer carried 12 of 13 reasons (so a held fix rendered as a raw machine token),
 * and the certificate verifier carried a three-member set against the engine's four.
 *
 * A constant in two places is a constant that will disagree.
 */
"""


def _union(name: str, values: tuple[str, ...], doc: str) -> str:
    members = "\n".join(f'  | "{value}"' for value in values)
    return f"/** {doc} */\nexport type {name} =\n{members};\n"


def _string_set(name: str, values: frozenset[str], doc: str) -> str:
    members = ", ".join(f'"{value}"' for value in sorted(values))
    return f"/** {doc} */\nexport const {name}: ReadonlySet<string> = new Set([{members}]);\n"


def _ordered_array(name: str, values: tuple[str, ...], element_type: str, doc: str) -> str:
    members = ", ".join(f'"{value}"' for value in values)
    return f"/** {doc} */\nexport const {name}: readonly {element_type}[] = [{members}] as const;\n"


def _record(name: str, mapping: dict[str, str], doc: str) -> str:
    lines = []
    for key, value in mapping.items():
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'  {key}: "{escaped}",')
    body = "\n".join(lines)
    return f"/** {doc} */\nexport const {name}: Record<string, string> = {{\n{body}\n}};\n"


def render() -> str:
    parts: list[str] = [HEADER.format(fingerprint=_source_fingerprint())]

    parts.append(
        _union(
            "VerificationStrength",
            vocab.VERIFICATION_STRENGTHS,
            "How strong the claim behind an applied value is. The product's core distinction.",
        )
    )
    parts.append(
        _ordered_array(
            "VERIFICATION_STRENGTHS",
            vocab.VERIFICATION_STRENGTHS,
            "VerificationStrength",
            "Every strength, for runtime membership checks in a verifier.",
        )
    )
    parts.append(
        _union(
            "Provenance",
            vocab.PROVENANCE_ORDER,
            "Where a proposed value came from.",
        )
    )
    parts.append(
        _ordered_array(
            "PROVENANCE_ORDER",
            vocab.PROVENANCE_ORDER,
            "Provenance",
            "Every provenance, in declaration order.",
        )
    )
    parts.append(
        _string_set(
            "TRUSTED_PROVENANCE",
            vocab.TRUSTED_PROVENANCE,
            "Provenances correct by construction. Trust decisions read THIS set, never a denylist.",
        )
    )
    parts.append(
        _string_set(
            "UNTRUSTED_PROVENANCE",
            vocab.UNTRUSTED_PROVENANCE,
            "Provenances that cannot be proven without an authoritative schema.",
        )
    )
    parts.append(
        _string_set(
            "CALIBRATED_PROVENANCE",
            vocab.CALIBRATED_PROVENANCE,
            "Strict subset of untrusted that additionally needs a calibrated threshold.",
        )
    )
    parts.append(
        """/**
 * True only for a provenance known to be correct by construction.
 *
 * Written against the allowlist deliberately. Testing membership of an untrusted
 * denylist fails OPEN: a provenance added by a future corrector, a typo, or a missing
 * value would all read as trustworthy.
 */
export function isTrustedProvenance(provenance: string | null | undefined): boolean {
  if (provenance === null || provenance === undefined) {
    return false;
  }
  return TRUSTED_PROVENANCE.has(provenance);
}
"""
    )
    parts.append(
        """/**
 * Derive how strong a claim is from where it came from.
 *
 * `authoritativeSchemaPresent` must be decided for the fix's OWN column. A table-level
 * boolean once granted authority over columns the schema never mentioned, which let a
 * garbage external value be applied and stamped `proven`.
 */
export function verificationStrengthFor(
  provenance: string | null | undefined,
  { authoritativeSchemaPresent }: { authoritativeSchemaPresent: boolean },
): VerificationStrength {
  if (isTrustedProvenance(provenance) || authoritativeSchemaPresent) {
    return "proven";
  }
  return "plausibility_only";
}
"""
    )
    parts.append(
        _union(
            "ReviewReason",
            vocab.REVIEW_REASONS,
            "Why a proposal was not applied. The machine contract behind every held fix.",
        )
    )
    parts.append(
        _record(
            "REVIEW_REASON_HUMAN",
            vocab.REVIEW_REASON_HUMAN,
            "The sentence a human reads for each review reason. Identical to the terminal's.",
        )
    )
    parts.append(
        _ordered_array(
            "REVIEW_REASONS",
            vocab.REVIEW_REASONS,
            "ReviewReason",
            "Every review reason, for runtime membership checks in a verifier.",
        )
    )
    parts.append(_union("Severity", vocab.SEVERITY_ORDER, "Issue severity, ascending."))
    parts.append(
        _ordered_array(
            "SEVERITY_ORDER", vocab.SEVERITY_ORDER, "Severity", "Ascending severity order."
        )
    )
    parts.append(
        _union("VerifierVerdict", vocab.VERIFIER_VERDICTS, "What the independent verifier decided.")
    )
    parts.append(
        _ordered_array(
            "VERIFIER_VERDICTS",
            vocab.VERIFIER_VERDICTS,
            "VerifierVerdict",
            "Every verifier verdict, for runtime membership checks.",
        )
    )
    parts.append(
        _record(
            "VERIFIER_VERDICT_HUMAN",
            vocab.VERIFIER_VERDICT_HUMAN,
            "What a human reads for each verifier verdict. The browser rendered the raw token.",
        )
    )
    parts.append(
        _union("SafetyVerdict", vocab.SAFETY_VERDICTS, "What the safety constitution decided.")
    )
    parts.append(
        _ordered_array(
            "SAFETY_VERDICTS",
            vocab.SAFETY_VERDICTS,
            "SafetyVerdict",
            "Every safety verdict, for runtime membership checks.",
        )
    )
    parts.append(
        _record(
            "SAFETY_VERDICT_HUMAN",
            vocab.SAFETY_VERDICT_HUMAN,
            "What a human reads for each safety verdict.",
        )
    )
    parts.append(
        _record(
            "INDEPENDENT_VERIFICATION_HUMAN",
            vocab.INDEPENDENT_VERIFICATION_HUMAN,
            "What a human reads for the second verifier's outcome. 'not_run' is not a failure.",
        )
    )
    parts.append(
        _record(
            "PROVENANCE_HUMAN",
            vocab.PROVENANCE_HUMAN,
            "Where a proposed value came from, in words rather than implementation tokens.",
        )
    )
    parts.append(
        _union(
            "Rung",
            vocab.RUNG_ORDER,
            "The epistemic ladder, weakest to strongest. Perceptual intensity is monotonic in it.",
        )
    )
    parts.append(
        _ordered_array(
            "RUNG_ORDER",
            vocab.RUNG_ORDER,
            "Rung",
            "Weakest to strongest. Index order is meaningful and load-bearing.",
        )
    )

    return "\n".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="fail if the target is stale")
    group.add_argument("--write", action="store_true", help="regenerate the target")
    args = parser.parse_args()

    rendered = render()

    if args.write:
        TARGET.parent.mkdir(parents=True, exist_ok=True)
        TARGET.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"wrote {TARGET.relative_to(PROJECT_ROOT)}")
        return 0

    if not TARGET.exists():
        print(f"MISSING: {TARGET.relative_to(PROJECT_ROOT)} has never been generated.")
        print("Run: python scripts/ci/generate_domain_vocabulary.py --write")
        return 1

    current = TARGET.read_text(encoding="utf-8")
    if current != rendered:
        print(
            f"STALE: {TARGET.relative_to(PROJECT_ROOT)} does not match "
            "dataforge/domain/vocabulary.py."
        )
        print("The browser and the engine would disagree about the trust vocabulary.")
        print("Run: python scripts/ci/generate_domain_vocabulary.py --write")
        return 1

    print("Domain vocabulary parity verified (TypeScript matches Python).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
