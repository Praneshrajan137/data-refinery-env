"""Mutation harness for the domain-vocabulary gates.

A gate whose mutants survive is not a gate. Every assertion added to protect the
ubiquitous language is exercised here by breaking the source deliberately, running the
verifier that is supposed to catch it, and requiring a non-zero exit. Files are always
restored, and every verifier is re-run afterwards to prove the restore worked.

This exists because the drifts this vocabulary was extracted to prevent had all been
"protected" by prose comments asserting parity -- three of them -- and every one of
those comments was already false when it was read. A comment is not a gate, and an
unverified gate is a comment.

Mutants are routed to the verifier that can actually see them: a static text audit
cannot detect a change in runtime arithmetic, and the fingerprint tripwire cannot see a
semantic change that leaves the source hash matching.

Usage:
    python scripts/ci/mutate_domain_vocabulary.py
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WEB = PROJECT_ROOT / "playground" / "web"

VOCABULARY = PROJECT_ROOT / "dataforge" / "domain" / "vocabulary.py"
GENERATED = WEB / "src" / "domain" / "vocabulary.generated.ts"
CERTIFICATE = PROJECT_ROOT / "dataforge" / "certificate.py"


@dataclass(frozen=True)
class Mutant:
    """One deliberate defect, and the verifier that must catch it."""

    name: str
    path: Path
    runner: str
    apply: Callable[[str], str]


def _python(*args: str) -> list[str]:
    return [sys.executable, *args]


RUNNERS: dict[str, list[str]] = {
    # Full text regeneration comparison: catches any vocabulary change not projected.
    "parity": _python("scripts/ci/generate_domain_vocabulary.py", "--check"),
    # Node-side fingerprint tripwire: catches staleness without Python.
    "fingerprint": ["node", "playground/web/scripts/audit_vocabulary.mjs"],
    # Runtime semantics: fail-closed predicates, partition invariants, ladder order.
    "tests": _python("-m", "pytest", "tests/unit/test_trust_vocab.py", "-q"),
    # The trust artifact itself: overtrust must be rejected.
    "certificate": _python("-m", "pytest", "tests/unit/test_certificate.py", "-q"),
}


MUTANTS: tuple[Mutant, ...] = (
    Mutant(
        name="a new review reason is added without regenerating TypeScript",
        path=VOCABULARY,
        runner="parity",
        apply=lambda s: s.replace(
            '    "invalid_target",\n]',
            '    "invalid_target",\n    "newly_invented_reason",\n]',
        ),
    ),
    Mutant(
        name="a review reason's phrasing is reworded on the Python side only",
        path=VOCABULARY,
        runner="parity",
        apply=lambda s: s.replace(
            '"invalid_target": "The proposed value failed the target\'s constraints.",',
            '"invalid_target": "Reworded on one side only.",',
        ),
    ),
    Mutant(
        name="a provenance is added to the literal but not to any trust partition",
        path=VOCABULARY,
        runner="tests",
        apply=lambda s: s.replace(
            '    "entity_consensus",\n]',
            '    "entity_consensus",\n    "some_future_corrector",\n]',
        ),
    ),
    Mutant(
        name="the trust predicate is rewritten to read a denylist (fails open)",
        path=VOCABULARY,
        runner="tests",
        apply=lambda s: s.replace(
            "    return provenance in TRUSTED_PROVENANCE",
            "    return provenance not in UNTRUSTED_PROVENANCE",
        ),
    ),
    Mutant(
        name="the rung ladder is reordered so a weaker rung outranks a stronger one",
        path=VOCABULARY,
        runner="tests",
        apply=lambda s: s.replace(
            '    "plausibility_only",\n    "downgraded",\n    "held",\n    "proven",',
            '    "proven",\n    "downgraded",\n    "held",\n    "plausibility_only",',
        ),
    ),
    Mutant(
        name="a missing strength is mapped UP the ladder instead of down",
        path=VOCABULARY,
        runner="tests",
        apply=lambda s: s.replace(
            '    if strength == "plausibility_only":\n        return "plausibility_only"\n'
            '    return "plausibility_only"',
            '    if strength == "plausibility_only":\n        return "plausibility_only"\n'
            '    return "held"',
        ),
    ),
    Mutant(
        name="the generated TypeScript is hand-edited",
        path=GENERATED,
        runner="parity",
        apply=lambda s: s.replace(
            'export const TRUSTED_PROVENANCE: ReadonlySet<string> = new Set(["deterministic"]);',
            "export const TRUSTED_PROVENANCE: ReadonlySet<string> = "
            'new Set(["deterministic", "entity_consensus"]);',
        ),
    ),
    Mutant(
        name="the generated file's source fingerprint is stripped",
        path=GENERATED,
        runner="fingerprint",
        apply=lambda s: s.replace(" * Source hash: sha256:", " * Source hash: removed:"),
    ),
    Mutant(
        name="the certificate trusts the recorded strength label instead of deriving it",
        path=CERTIFICATE,
        runner="certificate",
        apply=lambda s: s.replace(
            '            if recorded == "plausibility_only":',
            '            if recorded == "definitely_not_this_value":',
        ),
    ),
    Mutant(
        name="the certificate credits authority table-wide instead of per column",
        path=CERTIFICATE,
        runner="certificate",
        apply=lambda s: s.replace(
            "                authoritative_schema_present=column in authoritative,",
            "                authoritative_schema_present=bool(authoritative),",
        ),
    ),
)


def run(runner: str) -> tuple[int, str]:
    completed = subprocess.run(  # noqa: S603
        RUNNERS[runner],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout + completed.stderr


def main() -> int:
    for runner in RUNNERS:
        code, output = run(runner)
        if code != 0:
            print(f"BASELINE FAILS for {runner}; fix it before mutating:\n{output}")
            return 1
        print(f"baseline: {runner} passes on clean source")

    survivors: list[str] = []
    for mutant in MUTANTS:
        # TWO reads, deliberately. `read_text` applies universal-newline translation, so a working
        # copy stored with CRLF is matched by anchors written with "\n" -- that is why the
        # anchors below are "\n"-terminated and why they work. `read_bytes` captures the file
        # exactly as it is on disk so the restore is byte-identical.
        #
        # Restoring via `write_text(..., newline="")` is NOT a round trip: it emits LF where the
        # file was CRLF, leaving the tree "modified" with an empty content diff. Worse, it made
        # this harness state-dependent -- a first run on a fresh CRLF checkout would match, write
        # LF, and every later run saw LF -- so a genuinely stale anchor could pass on the second
        # run and fail on the first. That phantom-modification state is also exactly where a real
        # leftover mutation would hide.
        original_bytes = mutant.path.read_bytes()
        original = mutant.path.read_text(encoding="utf-8")
        mutated = mutant.apply(original)
        if mutated == original:
            print(f"NO-OP     {mutant.name} ({mutant.path.name} unchanged)")
            survivors.append(mutant.name)
            continue
        mutant.path.write_text(mutated, encoding="utf-8", newline="")
        try:
            code, _ = run(mutant.runner)
            if code == 0:
                print(f"SURVIVED  {mutant.name} [{mutant.runner}]")
                survivors.append(mutant.name)
            else:
                print(f"killed    {mutant.name} [{mutant.runner}]")
        finally:
            mutant.path.write_bytes(original_bytes)

    for runner in RUNNERS:
        code, output = run(runner)
        if code != 0:
            print(f"RESTORE FAILED: {runner} does not pass after restore:\n{output}")
            return 1

    killed = len(MUTANTS) - len(survivors)
    print(f"\n{killed}/{len(MUTANTS)} mutants killed")
    if survivors:
        print("survivors mean a claimed guarantee is not enforced:")
        for name in survivors:
            print(f"  - {name}")
        return 1
    print("source restored, all verifiers green")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
