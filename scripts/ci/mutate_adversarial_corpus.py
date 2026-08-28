"""Mutation harness for the adversarial corpus.

A corpus that passes whether or not the gate works is decoration. This breaks the defences
one at a time and requires the corpus to fail each time.

The repo has already paid for the lesson this prevents:
``tests/property/test_no_corruption_invariant.py`` records a test that asserted unchanged
bytes and passed for a trivial reason -- the corrector was never called -- and SURVIVED
having the gate removed. Its docstring calls that "the definition of a worthless test".

This harness immediately earned its keep. The first version of the corpus always supplied a
schema, so every attack was stopped by CONSTRAINT CHECKING and the proven-only gate was
never reached. All four mutants survived: the corpus was measuring the SMT verifier while
claiming to measure the trust guarantee. Two runs were added -- one with no schema, one with
a schema covering a different column -- and the same mutants now die.

Mutants target distinct defences on purpose. If several mutants can only be killed by the
same test, the corpus has one gate under test, not four.

Usage:
    python scripts/ci/mutate_adversarial_corpus.py
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPAIR = PROJECT_ROOT / "dataforge" / "engine" / "repair.py"
VOCABULARY = PROJECT_ROOT / "dataforge" / "domain" / "vocabulary.py"

# The corpus alone cannot kill a mutant in the write PRIMITIVE. With no authoritative
# schema, proposals are partitioned into "held" before any write is attempted, so
# `enforce_proven_only` is a defence-in-depth backstop on a path the corpus never reaches.
# The suite therefore also runs the property test that calls the primitive directly. That is
# the honest composition: the corpus proves the partition holds, the property test proves the
# primitive refuses, and the harness proves neither can be removed silently.
SUITE = [
    sys.executable,
    "-m",
    "pytest",
    "tests/adversarial",
    "tests/property/test_no_corruption_invariant.py",
    "-q",
    "-x",
]


@dataclass(frozen=True)
class Mutant:
    name: str
    path: Path
    apply: Callable[[str], str]
    defence: str


MUTANTS: tuple[Mutant, ...] = (
    Mutant(
        name="the proven-only write gate becomes a no-op",
        defence="proven-only invariant inside the mutation primitive",
        path=REPAIR,
        apply=lambda s: s.replace(
            "def enforce_proven_only(",
            "def enforce_proven_only(  # mutated\n    *_mutation_args: object,\n"
            "    **_mutation_kwargs: object,\n) -> None:\n    return\n\n\ndef _unused_enforce(",
        ),
    ),
    Mutant(
        name="strength is hardcoded to proven instead of derived",
        defence="strength derivation",
        path=REPAIR,
        apply=lambda s: s.replace(
            "    return _domain_verification_strength_for(",
            '    return "proven"  # mutated\n    return _domain_verification_strength_for(',
        ),
    ),
    Mutant(
        name="every provenance is treated as trusted",
        defence="fail-closed trust predicate",
        path=VOCABULARY,
        apply=lambda s: s.replace(
            "    return provenance in TRUSTED_PROVENANCE",
            "    return True  # mutated",
        ),
    ),
    Mutant(
        name="authority is granted over every column",
        defence="column-scoped authority",
        path=REPAIR,
        # Anchor refreshed 2026-08-28. It had read `covered: set[str] = set(schema.columns)`,
        # which no longer exists: `authoritative_columns` was narrowed to cover a column only
        # when the schema declares a DISCRIMINATING type for it. So the mutant was a NO-OP and
        # reported as a survivor -- "the corpus does not actually test the gate" -- when in fact
        # the corpus was never given a mutation to notice. Nobody saw it because this harness was
        # invoked by nothing: it appeared only in the Makefile's mypy argument list, so it was
        # type-checked and never run. Found the day it was wired into the backend gate.
        #
        # The anchor is deliberately a SINGLE LINE, like its three siblings. A multi-line anchor
        # written with "\n" does not match a working copy stored with CRLF, and the resulting
        # NO-OP is indistinguishable from a real survivor. My first attempt made exactly that
        # mistake and appeared to pass, because an earlier run had left the file LF-terminated.
        #
        # `bool(declared)` covers every declared column, including a bare `str`, which is the
        # absence of a type rather than a constraint -- the precise defect the narrowing fixed.
        apply=lambda s: s.replace(
            "if type_discriminates(declared)",
            "if bool(declared)  # mutated",
        ),
    ),
)


def _run() -> tuple[int, str]:
    completed = subprocess.run(  # noqa: S603
        SUITE,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout + completed.stderr


def main() -> int:
    code, output = _run()
    if code != 0:
        print(f"BASELINE FAILS; fix the corpus before mutating:\n{output[-2000:]}")
        return 1
    print("baseline: the adversarial corpus passes on clean source")

    survivors: list[str] = []
    for mutant in MUTANTS:
        # TWO reads, deliberately. `read_text` applies universal-newline translation, so anchors
        # written with "\n" match a working copy stored with CRLF. `read_bytes` captures the file
        # exactly as it is on disk so the restore is byte-identical.
        #
        # Restoring via `write_text(..., newline="")` is NOT a round trip: it emits LF where the
        # file was CRLF, leaving the tree "modified" with an empty content diff. Worse, it made
        # this harness state-dependent -- a first run on a fresh CRLF checkout wrote LF, and every
        # later run then saw LF -- so a multi-line anchor could fail on the first run and pass on
        # the second. That phantom-modification state is also exactly where a real leftover
        # mutation would hide.
        original_bytes = mutant.path.read_bytes()
        original = mutant.path.read_text(encoding="utf-8")
        mutated = mutant.apply(original)
        if mutated == original:
            print(f"NO-OP     {mutant.name} ({mutant.path.name} unchanged)")
            survivors.append(mutant.name)
            continue
        mutant.path.write_text(mutated, encoding="utf-8", newline="")
        try:
            code, _ = _run()
            if code == 0:
                print(f"SURVIVED  {mutant.name}")
                survivors.append(mutant.name)
            else:
                print(f"killed    {mutant.name}")
        finally:
            mutant.path.write_bytes(original_bytes)

    code, output = _run()
    if code != 0:
        print(f"RESTORE FAILED:\n{output[-2000:]}")
        return 1

    killed = len(MUTANTS) - len(survivors)
    print(f"\n{killed}/{len(MUTANTS)} mutants killed")
    if survivors:
        print("survivors mean the corpus does not actually test the gate:")
        for name in survivors:
            print(f"  - {name}")
        return 1
    print("source restored, corpus green")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
