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
        new='            0, 1, controls_by_class={"p": (false_accepts, controls)}, delta=delta',
        tests=("tests/unit/test_label_noise_certification.py",),
        why=(
            "re-pooling the two planted-control origins drops the certified beta from 0.8712 "
            "to 0.3125 on measured data, which is the difference between a pre-registered "
            "kill criterion firing and not firing. Pooling is not a weaker bound, it is a "
            "different and anti-conservative quantity, and it lets a dirty control class be "
            "hidden by padding the control set with easy plants"
        ),
    ),
    Mutant(
        name="M12-label-source-dispatch-defaulted",
        target="dataforge/calibration_session.py",
        old='    match artifact.label_source:\n        case "oracle":\n            human = False',
        new='    match artifact.label_source:\n        case "llm_probe" | "oracle":\n'
        "            human = False",
        tests=("tests/unit/test_label_noise_certification.py",),
        why=(
            "an LLM label source falling through to the oracle path certifies with NO noise "
            "adjustment at all. Measured: an LLM ratifier accepts 95.83% of wrong proposals, so "
            "this is the fails-open shape of `label_source == 'human'` that the exhaustive match "
            "replaced -- a source the function was never taught to reason about must refuse, "
            "not default to the branch that assumes beta = 0"
        ),
    ),
    Mutant(
        name="M13-certificate-provenance-check-removed",
        target="dataforge/calibration_session.py",
        old="        if not tallies:\n            raise ValueError(\n"
        '                "beta_upper recorded with no per-class control tallies',
        new="        if tallies and not tallies:\n            raise ValueError(\n"
        '                "beta_upper recorded with no per-class control tallies',
        tests=("tests/unit/test_label_noise_certification.py",),
        why=(
            "a certificate carrying a bare scalar beta_upper with no per-class provenance is "
            "readable again, which is exactly the pooled shape the stratified bound retired. "
            "Nothing this module builds can reach that state, so the check looks dead -- but a "
            "hand-edited or older artifact on disk reaches it, and by shape alone it is "
            "indistinguishable from a sound certificate while licensing auto-apply against an "
            "error budget nothing measured. Measured stakes: pooled reads 0.3125 where the "
            "binding class reads 0.8712"
        ),
    ),
    Mutant(
        name="M14-control-class-composition-floor-removed",
        target="dataforge/calibration_session.py",
        old="    if human and CERTIFYING_CONTROL_ORIGIN not in controls_by_origin:",
        new="    if False and CERTIFYING_CONTROL_ORIGIN not in controls_by_origin:",
        tests=("tests/unit/test_label_noise_certification.py",),
        why=(
            "stratifying beta stopped two declared control classes being POOLED and never stopped "
            "one being OMITTED, which is the reachable case: plant_controls hardcodes "
            "origin='column_distribution' and is the only PlantedControl construction site in "
            "dataforge/, so the harder class exists only in a docstring's promise. Measured, the "
            "easy class bounds beta at 0.2445 against 0.8712 for the hard one, and a single-class "
            "session of 30 clean easy plants certifies at 82 labels on a beta of 0.1157. Omission "
            "is doubly rewarded: the binding class disappears and the survivor stops paying its "
            "share of the union correction"
        ),
    ),
    Mutant(
        name="M15-str-declaration-confers-authority-again",
        target="dataforge/domain/vocabulary.py",
        old="    return declared_type.strip().lower() not in NON_DISCRIMINATING_COLUMN_TYPES",
        new="    return True",
        tests=(
            "tests/adversarial/test_corpus_gate.py",
            "tests/unit/test_column_scoped_authority.py",
        ),
        why=(
            "a premise that merely NAMES a column is treated as authority over it again. Every CSV "
            "cell is already a string and read_csv runs with dtype=str, so 'str' is the absence of "
            "a type. Measured in eval/results/trust_ledger_adversarial.json: declaring every column "
            "str admitted 10 of 14 constraint-violating attacks against 0 of 14 under a premise "
            "that actually constrained, and the gate stamped every write 'proven' in both runs. "
            "This is the same defect authoritative_columns was already narrowed once to fix, when "
            "one accepted column_type on 'id' granted blanket authority over 'city'"
        ),
    ),
    Mutant(
        name="M16-fd-majority-reverted-to-plurality",
        target="dataforge/repairers/fd_violation.py",
        old="        if top_count * 2 > sum(counts.values()):",
        new="        if top_count > (ranked[1][1] if len(ranked) > 1 else 0):",
        tests=("tests/unit/test_repairers.py",),
        why=(
            "2 votes of 5 across four distinct values writes again, with 'deterministic' "
            "provenance that bypasses calibration entirely, so no threshold downstream can catch "
            "it. The function's docstring claimed strict majority throughout while the code "
            "implemented a plurality. Measured in eval/results/deductive_coverage_flights.json: "
            "the rules diverge on 1732 cells and plurality is worse on every axis -- write "
            "precision 0.5618 against 0.6602, clean cells corrupted 731 against 344, net cells "
            "improved +404 against +579. Hospital cannot see this mutant at all, which is why it "
            "sat undetected"
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
