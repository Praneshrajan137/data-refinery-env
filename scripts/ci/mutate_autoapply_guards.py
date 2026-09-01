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

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _resolve_python() -> str:
    """Return the interpreter to run mutant suites with.

    This was hardcoded to ``.venv/Scripts/python.exe`` -- a Windows-only path -- so the
    gate raised ``FileNotFoundError`` on the first mutant under Linux CI and the whole
    harness had **never once executed there**. It failed closed, so no guard was silently
    unpinned, but "18/18 killed" was a number only ever produced on one machine.

    The hardcoding was not arbitrary: an earlier version invoked a bare ``python``, every
    mutant "died" of ``ModuleNotFoundError: textual``, and the run was recorded as a clean
    sweep. That is why the venv is still preferred over ``sys.executable`` -- but a missing
    venv must degrade to the running interpreter, not to a crash. The real defence against
    the rubber-stamp failure is ``_baseline_verdict`` below, not the shape of this path.
    """
    override = os.environ.get("DATAFORGE_MUTATE_PYTHON")
    if override:
        return override
    venv_python = REPO / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


PY = _resolve_python()

# pytest's documented exit codes. Only TESTS_FAILED means a mutant was noticed; every other
# non-zero value means the harness itself broke, which is NOT evidence about the guard.
PYTEST_ALL_PASSED = 0
PYTEST_TESTS_FAILED = 1


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
        target="dataforge/stores/patch_plan.py",
        old="    enforce_plan_constraint_checkable_only(plan)\n",
        new="",
        tests=(
            "tests/unit/test_warehouse_allowlist_gate.py",
            "tests/unit/test_table_store_patch_plan.py",
            "tests/unit/test_table_store_proven_gate.py",
        ),
        why=(
            "the SQL write primitive stops refusing uncheckable detectors. Retargeted "
            "2026-08-29: the call moved from DuckDBStore.apply_patch_plan into "
            "enforce_plan_write_gates when the three warehouse preconditions were composed "
            "into one entry point. The harness reported NOT_APPLIED rather than scoring a "
            "stale anchor, which is the behaviour the green-baseline work was added for"
        ),
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
    Mutant(
        name="M18-abstention-policy-accepts-extra-fields-again",
        target="dataforge/calibration.py",
        old='    model_config = ConfigDict(extra="forbid", frozen=True)\n\n    def threshold_for',
        new="    model_config = ConfigDict(frozen=True)\n\n    def threshold_for",
        tests=("tests/unit/test_certificate_policy_gap.py",),
        why=(
            "a payload this model does not recognise is silently accepted instead of refused, "
            "and the only alternative to refusing an unknown field is guessing a threshold. "
            "Measured: wrapping a SessionCertification in a 'policy' block -- the obvious fix "
            "for the fact that `calibrate --certify` prints a certificate `repair` cannot read "
            "-- was ACCEPTED, its certified thresholds dropped as unrecognised, and "
            "default_threshold fell back to 0.90. At confidence 0.95 that flips the decision "
            "from 'review' to 'auto_apply': a write against a threshold nobody certified, with "
            "no error and no log line, where the conservative default for this path is 1.01 "
            "meaning never. The guard is weakest exactly where it is most needed -- the "
            "permissive model sat at the boundary a well-intentioned wiring attempt arrives "
            "through, so silence there converted an abstention into a write"
        ),
    ),
    Mutant(
        name="M17-type-mismatch-regains-its-bypass",
        target="dataforge/domain/vocabulary.py",
        old='        "fd_violation",\n        "missing_value",\n    }\n)',
        new='        "fd_violation",\n        "missing_value",\n        "type_mismatch",\n    }\n)',
        tests=(
            "tests/integration/test_autoapply_decision_table.py",
            "tests/integration/test_playground_smoke.py",
        ),
        why=(
            "a detector with no committed write measurement regains the right to skip the "
            "calibration threshold. Measured in docs/trust/bypass-allowlist-evidence.md: 156 flags "
            "and ZERO proposals across hospital, rayyan and flights -- 4,376 rows and 6,377 real "
            "errors -- so nothing establishes what it does when it writes. Zero writes is not a "
            "safety result: decimal_shift was benchmark-quiet too at 39, 92 and 112 flags with "
            "precision 0.0000, and what removed it was a fourth dataset where it would have "
            "rewritten 263,428 values. This detector's firing population, a missing sentinel in a "
            "mostly-numeric column, is absent from all three corpora while being among the "
            "commonest shapes of real dirty data. Restoring it also re-fills the schema-free write "
            "path, so 'no declared premise, no write' stops holding"
        ),
    ),
    Mutant(
        name="M19-confirmation-flags-remerged",
        target="dataforge/safety/constitutions/default.yaml",
        old="    confirm_flag: confirm_untrusted_write",
        new="    confirm_flag: confirm_escalations",
        also=(
            ("    confirm_flag: confirm_high_volume", "    confirm_flag: confirm_escalations"),
            ("    confirm_flag: confirm_aggregate_break", "    confirm_flag: confirm_escalations"),
            ("    confirm_flag: confirm_injection_text", "    confirm_flag: confirm_escalations"),
        ),
        tests=("tests/unit/test_safety_filter.py",),
        why=(
            "one boolean regains control of four unrelated soft rules. The untrusted-write "
            "guard inspects a fix's ORIGIN LABEL and nothing else -- not the value, not the "
            "premise, not any constraint -- while NO_HIGH_VOLUME_AUTO_APPLY is a blast-radius "
            "budget. Under the merged flag, clearing the first silently disabled the second, so "
            "docs/trust/agent-throughput-decomposition.md could not recommend defaulting the "
            "origin-label rule on without also disabling a guard nobody had argued about. The "
            "coupling was worse interactively: engine/repair.py:787 reassigns the resolver's "
            "returned context into its loop variable, so a single 'y' at one prompt disabled all "
            "four guards for every remaining issue in the run -- the operator was asked about one "
            "cell and answered for the whole table"
        ),
    ),
    Mutant(
        name="M20-warehouse-reversibility-precondition-removed",
        target="dataforge/stores/patch_plan.py",
        old="    enforce_plan_reversible(plan)\n",
        new="",
        tests=("tests/unit/test_table_store_proven_gate.py",),
        why=(
            "the composite write gate stops refusing an irreversible plan, so a plan whose "
            "own reason says it cannot be undone is applied anyway. This check used to live in "
            "DuckDBStore.apply_patch_plan one line ABOVE the strength gate, which is the "
            "calling-surface pattern the other two gates were deliberately moved away from: a "
            "second backend adapter calling the two functions that look like 'the write gates' "
            "inherited neither the reversibility precondition nor any error saying so. "
            "CloudWarehouseStore sets reversible=False and all four cloud backends route to it, "
            "so this predicate is what makes 'no irreversible warehouse write' true by "
            "construction rather than by each adapter remembering"
        ),
    ),
    Mutant(
        name="M21-csv-snapshot-recoverability-unchecked",
        target="dataforge/engine/repair.py",
        old="        enforce_snapshot_recoverable(transaction)\n",
        new="",
        tests=("tests/unit/test_snapshot_recoverable_gate.py",),
        why=(
            "the CSV mutation primitive stops verifying that the snapshot it just wrote can "
            "actually restore the file, so reversibility reverts to a property of the ORDERING "
            "of steps rather than a checked precondition. PRODUCT.md ranks reversibility above "
            "proven-only, and the failure mode is silent in the worst direction: a truncated or "
            "missing snapshot is indistinguishable from a good one until the revert that needs "
            "it, which is exactly the moment the user has no other copy. "
            "docs/trust/write-surface-uniformity.md records that Round 1 of the uniformity work "
            "missed precisely 'the one with a stronger promise and no test'"
        ),
    ),
    Mutant(
        name="M22-duckdb-revert-verifies-nothing",
        target="dataforge/stores/duckdb.py",
        old=(
            "                if restored_rows != expected_rows:\n"
            "                    raise TableStoreError(\n"
            '                        "Revert failed integrity verification: the restored relation does not "\n'
            "                        f\"match the snapshot recorded for transaction '{transaction.txn_id}'.\"\n"
            "                    )\n"
        ),
        new="",
        also=(
            (
                "                if transaction.post_sha256 is not None:\n"
                "                    current = self._post_state_sha256(self._relation_rows(connection))\n"
                "                    if current != transaction.post_sha256:\n"
                "                        raise TableStoreError(\n"
                '                            "Refusing to revert because the relation no longer matches the "\n'
                '                            "recorded post-state hash. The table may have been modified after "\n'
                '                            "apply, so the recorded rollback statements no longer describe it."\n'
                "                        )\n",
                "",
            ),
            (
                "                    changed = self._execute_dml_rows_changed(connection, sql)\n"
                "                    if changed != 1:\n"
                "                        raise TableStoreError(\n"
                '                            f"Rollback statement changed {changed} row(s) instead of exactly "\n'
                '                            f"one, so the revert is not the inverse of the apply: {sql}"\n'
                "                        )\n",
                "                    connection.execute(sql)\n",
            ),
        ),
        tests=("tests/unit/test_duckdb_revert_integrity.py",),
        why=(
            "the DuckDB revert goes back to firing plan.rollback_sql blind -- the exact state "
            "before 2026-08-29, when this surface had no test of any kind. All three checks are "
            "reverted together because they are redundant by design and removing any one alone "
            "leaves the suite green for a legitimate reason (defence in depth), which is what "
            "the `also` mechanism is for. Restoring the pre-fix behaviour reproduces the "
            "committed symptom in docs/evidence/dbt_duckdb/commands.log: '\"ok\": true' and "
            '\'"audit_verdict": "verified"\' printed beside \'"restored_source_sha256": null\'. '
            "Verified against DuckDB: an UPDATE matching nothing returns [(0,)], so a rollback "
            "that changed no rows and one that worked were indistinguishable, and "
            "append_reverted_event recorded both as success. The CSV revert has had the "
            "equivalent pre- and post-state checks all along, so this was also a "
            "surface-uniformity gap on the promise PRODUCT.md ranks highest"
        ),
    ),
    Mutant(
        name="M23-blast-radius-budget-counts-rows-again",
        target="dataforge/safety/constitution.py",
        old="    return len({(fix.fix.row, fix.fix.column) for fix in fixes}) > HIGH_VOLUME_CELL_BUDGET",
        new="    return len({fix.fix.row for fix in fixes}) > HIGH_VOLUME_CELL_BUDGET",
        tests=("tests/unit/test_safety_filter.py",),
        why=(
            "the blast-radius budget goes back to counting DISTINCT ROWS instead of cells, "
            "which is the state before 2026-08-29 and has a measurable blind spot: a batch "
            "rewriting 90 rows across 50 columns is 4,500 cells and passes, because it touches "
            "90 rows. A cell is the unit this product writes, reverts, proves and attests, so "
            "it is the unit a blast-radius budget must use. Note the threshold VALUE is "
            "unchanged at 100 in both versions -- the defect was the unit, and no measurement "
            "exists that would justify moving the number, so moving it would be fitting a "
            "parameter to nothing"
        ),
    ),
    Mutant(
        name="M24-egress-scan-never-refuses",
        target="dataforge/measure_on_my_table.py",
        old="    leaked = sorted(value for value in seen if value in haystack)\n",
        new="    leaked: list[str] = []\n",
        tests=("tests/unit/test_measure_on_my_table.py",),
        why=(
            "the egress scan on the design-partner report stops finding anything, so it "
            "always passes and the sentinel test it backs becomes a test of nothing. This is "
            "the one mutant in this file whose blast radius is outside the repository: "
            "measure-on-my-table is the instrument a design partner runs on a table we are "
            "never allowed to see, and its report is the artifact they send back. A value-leak "
            "there cannot be fixed after shipping -- the data has already moved -- and it "
            "would be discovered by the partner, not by us. The report is value-free by "
            "CONSTRUCTION as well, every field being an int, a float or a digest, so this scan "
            "is the second of two independent guarantees rather than the only one; the reason "
            "it exists anyway is that the structural argument is true of today's fields and "
            "one future field makes it false"
        ),
    ),
    Mutant(
        name="M25-legacy-journal-revert-loses-its-byte-checks",
        target="dataforge/transactions/revert.py",
        old=(
            "                atomic_write_bytes(source_path, snapshot_path.read_bytes())\n"
            "                reverted_sha256 = sha256_file(source_path)\n"
            "                if reverted_sha256 != transaction.source_sha256:\n"
            "                    atomic_write_bytes(source_path, current_bytes)\n"
            "                    raise TransactionRevertError(\n"
            "                        f\"Revert failed integrity verification for transaction '{txn_id}'.\"\n"
            "                    )\n"
        ),
        new="                atomic_write_bytes(source_path, snapshot_path.read_bytes())\n",
        also=(
            (
                "                if current_sha256 != transaction.post_sha256:\n"
                "                    raise TransactionRevertError(\n"
                '                        "Refusing to revert because the current file no longer matches the recorded "\n'
                '                        "post-state hash. The file may have been edited after apply."\n'
                "                    )\n",
                "",
            ),
        ),
        tests=(
            "tests/unit/test_legacy_journal_revert.py",
            "tests/unit/test_transactions.py",
        ),
        why=(
            "the two byte-level checks that carry the revert guarantee are removed together, "
            "which matters most on the LEGACY_UNVERIFIED path. revert_transaction admits a v1 "
            "journal that has no hash chain at all, so for those transactions these checks are "
            "not defence in depth -- they are the entire guarantee, and nothing else in the "
            "product stands behind a legacy restore. Both are mutated together for the reason "
            "stated at the top of this file: either alone leaves the other catching the case, so "
            "a single-line mutant would survive for a legitimate reason. Until 2026-09-01 no "
            "test reverted a LEGACY_UNVERIFIED transaction -- every revert_transaction call site "
            "in the suite built a v2 log, and the committed v1 fixture was only ever fed to "
            "verify_transaction_log -- so this was a write path authorised by an unverifiable "
            "journal with neither a test nor a mutant"
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
        return _verdict_for(proc.returncode)
    finally:
        path.write_text(original, encoding="utf-8")


def _verdict_for(returncode: int) -> str:
    """Map a pytest exit code to a mutant verdict.

    This was ``"KILLED" if returncode != 0 else "SURVIVED"``, which counted **any** non-zero
    exit as a kill -- a collection error, an import error, a usage error, or a missing
    dependency all read as "the guard is pinned". That is precisely the rubber-stamp this
    harness exists to avoid, and it is how a bare-``python`` run once scored 18/18 while
    every mutant had actually died of ``ModuleNotFoundError``.

    Only exit code 1 (tests ran and at least one failed) is evidence that a test noticed the
    mutation. Everything else is a harness failure and must not be scored as a kill.
    """
    if returncode == PYTEST_TESTS_FAILED:
        return "KILLED"
    if returncode == PYTEST_ALL_PASSED:
        return "SURVIVED"
    return f"HARNESS_ERROR (pytest exit {returncode} -- not a kill)"


def baseline_verdict() -> tuple[bool, str]:
    """Run every mutant's test subset against UNMUTATED source.

    Without this, a kill verdict is unfalsifiable: if the suite is red for any unrelated
    reason -- a broken import, a missing dependency, a syntax error left behind by an earlier
    aborted run -- then every mutant "fails" its subset and the harness reports a clean sweep
    while proving nothing. Asserting the file changed on disk (see ``run``) catches a mutant
    that was never applied; it cannot catch a suite that was already failing.

    One run over the union of subsets is logically sufficient: if the union is green on clean
    source, any subsequent failure under mutation is attributable to the mutation.
    """
    union = sorted({test for mutant in MUTANTS for test in mutant.tests})
    proc = subprocess.run(
        [PY, "-m", "pytest", *union, "-q", "--no-header", "-p", "no:randomly"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if proc.returncode == PYTEST_ALL_PASSED:
        return True, f"baseline green over {len(union)} test path(s)"
    tail = (proc.stdout or proc.stderr or "").strip().splitlines()
    detail = tail[-1] if tail else "no output"
    return False, f"pytest exit {proc.returncode}: {detail}"


def main() -> int:
    ok, detail = baseline_verdict()
    print(f"baseline: {detail}", flush=True)
    if not ok:
        print(
            "\nREFUSING TO SCORE MUTANTS: the suite is not green on unmutated source, so a "
            "failure under mutation would prove nothing about the guard.",
        )
        return 1

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
