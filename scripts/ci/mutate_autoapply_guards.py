"""Mutation-test the guards added on 2026-08-22.

Each mutant is applied to a real source file, the suite subset that should notice is run,
and the file is restored. A mutant that SURVIVES means the guard is not actually pinned by
any test.

Critical discipline, learned the hard way in this repo: **assert the file CHANGED before
trusting the result.** Two earlier mutation runs silently no-op'd (one from wrong
indentation, one from PowerShell mangling the quotes) and were briefly recorded as
"mutant killed" when nothing had been mutated at all.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PY = REPO / ".venv" / "Scripts" / "python.exe"


@dataclass(frozen=True)
class Mutant:
    name: str
    target: str
    old: str
    new: str
    tests: tuple[str, ...]
    why: str
    also: tuple[tuple[str, str], ...] = ()
    """Additional (old, new) pairs applied in the SAME mutant.

    Needed when two guards are redundant: reverting either one alone leaves the suite
    green, so a single-line mutant survives for a legitimate reason (defence in depth)
    rather than a missing test. Mutating both together is the honest question.
    """


MUTANTS: tuple[Mutant, ...] = (
    Mutant(
        name="M1-primitive-gate-removed",
        target="dataforge/engine/repair.py",
        old="    enforce_constraint_checkable_only(fixes)\n",
        new="",
        tests=("tests/integration/test_autoapply_decision_table.py",),
        why="the CSV mutation primitive stops refusing uncheckable detectors",
    ),
    Mutant(
        name="M2-allowlist-becomes-denylist",
        target="dataforge/engine/repair.py",
        old="        and fix.fix.detector_id not in CONSTRAINT_CHECKABLE_DETECTORS",
        new="        and fix.fix.detector_id in CONSTRAINT_CHECKABLE_DETECTORS",
        tests=("tests/integration/test_autoapply_decision_table.py",),
        why="the allowlist sense is inverted, so only trusted detectors are refused",
    ),
    Mutant(
        name="M3-agent-hold-removed",
        target="dataforge/agent/controller.py",
        old=(
            "        if (\n"
            '            fix.provenance == "deterministic"\n'
            "            and fix.fix.detector_id not in CONSTRAINT_CHECKABLE_DETECTORS\n"
            "        ):\n"
            "            return True\n"
        ),
        new="",
        tests=(
            "tests/integration/test_autoapply_decision_table.py",
            "tests/unit/test_agent_gate.py",
        ),
        why="the agent stops holding proactively; this was the live bypass",
    ),
    Mutant(
        name="M4-opt-in-unlocks-uncheckable-write",
        target="dataforge/engine/repair.py",
        old="def enforce_constraint_checkable_only(fixes: list[ProposedFix]) -> None:",
        new=(
            "def enforce_constraint_checkable_only(\n"
            "    fixes: list[ProposedFix], allow: bool = True\n"
            ") -> None:\n"
            "    if allow:\n"
            "        return"
        ),
        tests=("tests/integration/test_autoapply_decision_table.py",),
        why="an opt-in is threaded in for symmetry with the strength gate",
    ),
    Mutant(
        name="M5-non-vacuity-guard-removed",
        target="dataforge/bench/agent_gate.py",
        old=" and self.non_vacuous",
        new="",
        tests=("tests/unit/test_agent_gate.py",),
        why="all_parity accepts three zeros again",
    ),
    Mutant(
        name="M6-non-vacuity-threshold-off-by-one",
        target="dataforge/bench/agent_gate.py",
        old="        return any(item.floor_fix_count > 0 for item in self.fixtures)",
        new="        return any(item.floor_fix_count >= 0 for item in self.fixtures)",
        tests=("tests/unit/test_agent_gate.py",),
        why="the threshold is satisfied by a zero floor, making the guard vacuous itself",
    ),
    Mutant(
        name="M7-warehouse-gate-removed",
        target="dataforge/stores/duckdb.py",
        old="        enforce_plan_constraint_checkable_only(plan)\n",
        new="",
        tests=(
            "tests/unit/test_warehouse_allowlist_gate.py",
            "tests/unit/test_table_store_patch_plan.py",
        ),
        why="the SQL write primitive stops refusing uncheckable detectors",
    ),
    Mutant(
        name="M8-fixture-premise-check-removed",
        target="tests/support/tables.py",
        old="    table.verify_premise()\n    return table",
        new="    return table",
        tests=("tests/unit/test_shared_fixtures_verify_themselves.py",),
        why="fixtures stop asserting they exercise what they claim",
    ),
    Mutant(
        name="M9-label-noise-floor-reverted",
        target="dataforge/conformal.py",
        old="            min_support=effective_min_support,",
        new="            min_support=min_support,",
        also=(("        if n < effective_min_support:", "        if n < min_support:"),),
        tests=("tests/unit/test_label_noise_certification.py",),
        why=(
            "the noise-adjusted support floor reverts to the naive one, so the fixed "
            "sequence breaks on a threshold it cannot satisfy (both guards reverted: "
            "either alone suffices, so a single-line mutant survives by design)"
        ),
    ),
    Mutant(
        name="M10-corpus-tier-default-restored",
        target="dataforge/datasets/registry.py",
        old='    tier: Literal["headline", "tripwire", "diagnostic"]\n    tier_reason',
        new='    tier: Literal["headline", "tripwire", "diagnostic"] = "headline"\n    tier_reason',
        tests=("tests/unit/test_corpus_tiering.py",),
        why=(
            "a corpus that omits its tier silently becomes headline-tier, i.e. permitted "
            "to source a published claim. This is the denylist-fails-open mistake one "
            "level up from the write allowlist, and it shipped for a day"
        ),
    ),
    Mutant(
        name="M11-label-noise-beta-repooled",
        target="dataforge/calibration_session.py",
        old="            0, 1, controls_by_class=controls_by_origin, delta=delta",
        new="            0, 1, controls_by_class={\"p\": (false_accepts, controls)}, delta=delta",
        tests=("tests/unit/test_label_noise_certification.py",),
        why=(
            "re-pooling the two planted-control origins drops the certified beta from 0.8712 "
            "to 0.3125 on measured data, which is the difference between a pre-registered "
            "kill criterion firing and not firing. Pooling is not a weaker bound, it is a "
            "different and anti-conservative quantity, and it lets a dirty control class be "
            "hidden by padding the control set with easy plants"
        ),
    ),
)


def run(mutant: Mutant) -> str:
    path = REPO / mutant.target
    original = path.read_text(encoding="utf-8")
    if mutant.old not in original:
        return "NOT_APPLIED (anchor text absent -- mutant is stale)"
    mutated = original.replace(mutant.old, mutant.new, 1)
    for old, new in mutant.also:
        if old not in mutated:
            return f"NOT_APPLIED (secondary anchor absent: {old[:40]!r})"
        mutated = mutated.replace(old, new, 1)
    if mutated == original:
        return "NOT_APPLIED (replacement was a no-op)"
    path.write_text(mutated, encoding="utf-8")
    try:
        if path.read_text(encoding="utf-8") == original:
            return "NOT_APPLIED (file unchanged on disk)"
        proc = subprocess.run(
            [
                str(PY),
                "-m",
                "pytest",
                *mutant.tests,
                "-x",
                "-q",
                "--no-header",
                "-p",
                "no:randomly",
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            timeout=900,
        )
        return "KILLED" if proc.returncode != 0 else "SURVIVED"
    finally:
        path.write_text(original, encoding="utf-8")


def main() -> int:
    results: list[tuple[Mutant, str]] = []
    for mutant in MUTANTS:
        verdict = run(mutant)
        results.append((mutant, verdict))
        print(f"{verdict:12s} {mutant.name}  -- {mutant.why}", flush=True)

    print()
    bad = [(m, v) for m, v in results if v != "KILLED"]
    if bad:
        print("PROBLEMS:")
        for mutant, verdict in bad:
            print(f"  {verdict}: {mutant.name} ({mutant.target})")
        return 1
    print(f"All {len(results)} mutants killed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
