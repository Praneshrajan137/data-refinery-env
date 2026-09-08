"""H4: why does FD repair write almost nothing, and does premise SIZE cause it?

Executes `eval/preregistration/fd_repair_yield_mechanism.md`. Read that first.

## The question

`docs/trust/declared-premise-capability.md` reported a bare zero: a declared premise writes 0 cells
on hospital while the oracle premise writes 54, the declared premise proposes MORE candidates (399
against 397), and its 13 dependent columns already include all 10 the oracle wrote to. It named
redundancy in the dependency set as necessary and not sufficient, and declined to name a mechanism.

A bare zero is not actionable. It does not say whether the ceiling is architectural, a defect that
has been silently costing repairs, or liftable at all.

## H4, and how this harness tries to break it

**A single-cell FD repair can be verified only when it is the last remaining violation in its
determinant group.** `dataforge/verifier/direct.py:245-260` rejects a candidate if ANY other row
sharing its determinant value disagrees on the post-fix dependent value.

The test is a **structural predictor** that never runs the pipeline: accept iff
``remaining_disagreements(row, column, value) == 0``. If a predictor with no pipeline in it
reproduces the pipeline's write count on four premises, the mechanism is established rather than
argued. That is **P5**, and **K3** refuses to rescue it by adding terms after seeing the residual.

The predictor is a partial reimplementation of the shipped verifier, which is exactly how a
mechanism gets confirmed for the wrong reason. **K2** therefore checks it against the shipped
``DirectVerifier`` per proposal and refuses to report P5 below 99% agreement.

## What is imported rather than rebuilt

Proposals come from the shipped ``FDViolationRepairer``; verdicts from the shipped
``DirectVerifier``; the table from ``dataforge.table.read_csv``, which is what
``dataforge/engine/repair.py:1773`` itself calls, so no arm compares a ``Table`` measurement against
a pandas one. pandas is used ONLY to discover the oracle premise from the clean frame, which is
premise construction and not a measured quantity.

Read-only apart from one artifact under ``eval/results/``, plus a temporary CSV.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from dataforge.bench.core import BenchmarkRepair, score_repairs  # noqa: E402
from dataforge.cli.common import load_schema  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors.base import FunctionalDependency, Schema  # noqa: E402
from dataforge.detectors.fd_violation import FDViolationDetector  # noqa: E402
from dataforge.engine.repair import RepairPipelineRequest, run_repair_pipeline  # noqa: E402
from dataforge.repairers.fd_violation import FDViolationRepairer  # noqa: E402
from dataforge.table import Table, column_names, column_values, read_csv  # noqa: E402
from dataforge.verifier.differential import differential_verify  # noqa: E402
from dataforge.verifier.direct import DirectVerifier  # noqa: E402
from dataforge.verifier.result import VerificationVerdict  # noqa: E402
from dataforge.verifier.smt import SMTVerifier  # noqa: E402
from scripts.bench.measure_declared_premise_capability import (  # noqa: E402
    thin_to_one_determinant,
)
from scripts.bench.measure_deductive_coverage import discover_oracle_fds  # noqa: E402

CORPUS = "hospital"
PREMISE = REPO / "eval" / "premises" / "hospital_declared.yaml"
DECLARED_ARTIFACT = REPO / "eval" / "results" / "declared_premise_capability.json"
OUTPUT = REPO / "eval" / "results" / "fd_repair_yield_mechanism.json"

#: K2 floor: below this the predictor has drifted from the shipped verifier and P5 is unreportable.
K2_MIN_AGREEMENT = 0.99
#: P5 tolerance, and K3's refutation threshold. Fixed before the residual was seen.
P5_TOLERANCE = 0.10
K3_REFUTATION_TOLERANCE = 0.25
#: P4's probe adds this dependency to the declared premise. `Address1` sorts before every other
#: determinant in that premise, so it wins `fd_violation.py:158`'s sort for `City`. It adds no new
#: dependent column -- `City` was already covered by `ProviderNumber -> City` -- and it is entailed
#: by the oracle premise's `Address1 -> ProviderNumber` plus `ProviderNumber -> City`.
P4_REDUNDANT_FD = FunctionalDependency(determinant=("Address1",), dependent="City")


def committed_writes() -> dict[str, int]:
    """Read the two write counts K1 binds against, from the artifact and never from a constant."""
    payload = json.loads(DECLARED_ARTIFACT.read_text(encoding="utf-8"))
    return {
        "declared": int(payload["arms"]["pipeline_declared_premise"]["writes"]),
        "oracle": int(payload["arms"]["pipeline_oracle_premise"]["writes"]),
    }


class Frame:
    """Column-major snapshot of a Table, so the row scans below are O(1) per cell.

    Built once per arm. `column_values` is read through the shipped accessor; nothing here
    re-parses the CSV, and no pandas frame is compared against a Table measurement.
    """

    def __init__(self, table: Table) -> None:
        self.columns = {name: list(column_values(table, name)) for name in column_names(table)}
        self.rows = len(next(iter(self.columns.values()))) if self.columns else 0

    def at(self, row: int, column: str) -> str:
        return str(self.columns[column][row])


def relevant_fds(schema: Schema, column: str) -> tuple[FunctionalDependency, ...]:
    """Mirror the verifier's scoping exactly: dependent OR anywhere in the determinant.

    `direct.py:129-133` and `smt.py:245-248`. Note this is NOT the whole schema -- the repairer's
    docstring at `fd_violation.py:99-103` says it is, and that sentence is wrong.
    """
    return tuple(
        fd
        for fd in schema.functional_dependencies
        if column == fd.dependent or column in fd.determinant
    )


def remaining_disagreements(
    frame: Frame, schema: Schema, row: int, column: str, value: str
) -> tuple[int, list[str]]:
    """Count rows that would still violate a relevant dependency after writing `value`.

    This is the structural predictor of P5 and the partial reimplementation K2 polices. It
    substitutes `value` at (row, column) exactly as `direct.py`'s local `read` closure does,
    including the case where the fixed column is itself part of a determinant -- there the
    substitution moves the candidate row into a different determinant group, which is why the
    determinant is read through the substitution rather than from the raw frame.
    """

    def read(index: int, col: str) -> str:
        return value if (index == row and col == column) else frame.at(index, col)

    total = 0
    witnesses: list[str] = []
    for fd in relevant_fds(schema, column):
        if fd.dependent not in frame.columns:
            continue
        if any(det not in frame.columns for det in fd.determinant):
            continue
        candidate_det = tuple(read(row, det) for det in fd.determinant)
        candidate_dep = read(row, fd.dependent)
        for index in range(frame.rows):
            if index == row:
                continue
            if tuple(read(index, det) for det in fd.determinant) != candidate_det:
                continue
            if read(index, fd.dependent) != candidate_dep:
                total += 1
                if len(witnesses) < 3:
                    witnesses.append(f"{'+'.join(fd.determinant)}->{fd.dependent}@row{index}")
    return total, witnesses


def shipped_proposals(table: Table, schema: Schema) -> list[Any]:
    """Every proposal the SHIPPED detector and repairer produce, in detector order."""
    issues = FDViolationDetector().detect(table, schema)
    repairer = FDViolationRepairer(cache_dir=None, allow_llm=False)
    proposals = []
    for issue in issues:
        proposal = repairer.propose(issue, table, schema, None)
        if proposal is not None:
            proposals.append(proposal)
    return proposals


def classify_failures(result: Any) -> dict[str, int]:
    """P3: bucket the final disposition of every issue that did not end accepted.

    Derived from `result.failures`, which already exists, rather than from a new field on
    `RepairReceipt` -- K4 forbids touching that structure, because `schema_version`, the attestation
    vectors and the conformance gate all key off it.
    """
    buckets: Counter[str] = Counter()
    fd_core = 0
    for failure in result.failures or []:
        status = str(getattr(failure, "status", "unknown"))
        reason = str(getattr(failure, "reason", ""))
        buckets[status] += 1
        if "functional dependency" in reason.lower() or "fd::" in reason:
            fd_core += 1
    out = dict(sorted(buckets.items()))
    out["reason_names_a_functional_dependency"] = fd_core
    return out


def jointly_repairable(frame: Frame, schema: Schema, proposals: list[Any]) -> tuple[int, int]:
    """P6 headroom: cells that a JOINT check would accept but a single-cell check cannot.

    A co-violator is a row that shares the candidate's determinant and disagrees on the proposed
    value. If every co-violator carries a shipped proposal for the SAME value, then applying them
    together satisfies the dependency, and a joint verifier would accept all of them where the
    sequential one accepts none.

    **This measures headroom for a configuration that does not exist.** `direct.py:104-111` applies
    a fix list sequentially, verifying each against a frame in which the others are not yet applied,
    so passing the group as a list today still fails.
    """
    proposed: dict[tuple[int, str], str] = {
        (p.fix.row, p.fix.column): str(p.fix.new_value) for p in proposals
    }
    single = 0
    joint = 0
    for (row, column), value in proposed.items():
        remaining, _ = remaining_disagreements(frame, schema, row, column, value)
        if remaining == 0:
            single += 1
            joint += 1
            continue
        covered = True
        for fd in relevant_fds(schema, column):
            if fd.dependent not in frame.columns:
                continue
            if any(det not in frame.columns for det in fd.determinant):
                continue
            candidate_det = tuple(
                value if det == column else frame.at(row, det) for det in fd.determinant
            )
            for index in range(frame.rows):
                if index == row:
                    continue
                det = tuple(
                    value if (index == row and d == column) else frame.at(index, d)
                    for d in fd.determinant
                )
                if det != candidate_det:
                    continue
                if frame.at(index, fd.dependent) == value:
                    continue
                if proposed.get((index, fd.dependent)) != value:
                    covered = False
                    break
            if not covered:
                break
        if covered:
            joint += 1
    return single, joint


def violation_group_shape(frame: Frame, schema: Schema, proposals: list[Any]) -> dict[str, int]:
    """P1/P2: how many proposals sit in a group with exactly one differing cell."""
    shape: Counter[str] = Counter()
    for proposal in proposals:
        remaining, _ = remaining_disagreements(
            frame, schema, proposal.fix.row, proposal.fix.column, str(proposal.fix.new_value)
        )
        if remaining == 0:
            shape["singleton_violation_groups"] += 1
        elif remaining == 1:
            shape["groups_with_two_violations"] += 1
        else:
            shape["groups_with_three_or_more_violations"] += 1
    return dict(sorted(shape.items()))


def measure_arm(
    name: str,
    schema: Schema,
    source: Path,
    clean: pd.DataFrame,
    truth: dict[tuple[int, str], str] | None = None,
) -> dict[str, Any]:
    """Run one premise through the pipeline and through the structural predictor."""
    table = read_csv(source)
    frame = Frame(table)

    result = run_repair_pipeline(
        RepairPipelineRequest(source_path=source, mode="dry_run", schema=schema)
    )
    actual_writes = len(result.fixes)

    proposals = shipped_proposals(table, schema)
    verifier = DirectVerifier()
    smt = SMTVerifier()

    predicted = 0
    agree = 0
    disagreements: list[dict[str, Any]] = []
    # H5 (AMENDMENT 1): localise the rejection to a verifier LEG. Counted per proposal so the
    # fail-closed combination can be attributed rather than inferred.
    legs: Counter[str] = Counter()
    differential_accepts = 0
    # P9: are the repairs the Direct leg proves actually RIGHT? This decides whether a lost
    # capability is lost value or lost corruption, and it is the only question here that matters
    # to a user. Ground truth GRADES; it never feeds a verdict above.
    direct_accepted_correct = 0
    direct_accepted_wrong = 0
    direct_accepted_on_clean_cell = 0

    for proposal in proposals:
        row = proposal.fix.row
        column = proposal.fix.column
        value = str(proposal.fix.new_value)
        remaining, witnesses = remaining_disagreements(frame, schema, row, column, value)
        predictor_accepts = remaining == 0
        predicted += int(predictor_accepts)

        shipped = verifier.verify(table, [proposal], schema)
        shipped_accepts = shipped.verdict == VerificationVerdict.ACCEPT
        if predictor_accepts == shipped_accepts:
            agree += 1
        elif len(disagreements) < 5:
            disagreements.append(
                {
                    "row": row,
                    "column": column,
                    "predictor_accepts": predictor_accepts,
                    "shipped_accepts": shipped_accepts,
                    "shipped_verdict": shipped.verdict.value,
                    "remaining_disagreements": remaining,
                    "witnesses": witnesses,
                }
            )

        if not shipped_accepts:
            continue

        # Only proposals the Direct leg PROVES are interesting for H5: those are the ones whose
        # loss would be a loss of proven capability.
        smt_result = smt.verify(table, [proposal], schema)
        differential = differential_verify(table, [proposal], schema)
        legs[f"direct_accept__smt_{smt_result.verdict.value}"] += 1
        if differential.verdict == VerificationVerdict.ACCEPT:
            differential_accepts += 1

        if truth is not None:
            expected = truth.get((row, column))
            if expected is None:
                direct_accepted_on_clean_cell += 1
            elif value == expected:
                direct_accepted_correct += 1
            else:
                direct_accepted_wrong += 1

    agreement = round(agree / len(proposals), 4) if proposals else None
    single, joint = jointly_repairable(frame, schema, proposals)
    per_dependent = Counter(fd.dependent for fd in schema.functional_dependencies)
    direct_accepted = (
        direct_accepted_correct + direct_accepted_wrong + direct_accepted_on_clean_cell
    )

    return {
        "fd_count": len(schema.functional_dependencies),
        "dependent_columns": len(per_dependent),
        "max_determinants_per_dependent": max(per_dependent.values()) if per_dependent else 0,
        "every_fd_holds_on_clean": all(
            _holds_on_clean(clean, fd) for fd in schema.functional_dependencies
        ),
        "pipeline_actual_writes": actual_writes,
        "shipped_proposals": len(proposals),
        "predictor_predicted_writes": predicted,
        "predictor_vs_shipped_agreement": agreement,
        "predictor_vs_shipped_disagreements": disagreements,
        "violation_group_shape": violation_group_shape(frame, schema, proposals),
        "joint_verification_headroom": {
            "single_cell_acceptable": single,
            "joint_acceptable": joint,
            "additional_cells_a_joint_check_would_accept": joint - single,
        },
        "verifier_leg_attribution": {
            "direct_leg_accepts": direct_accepted,
            "differential_accepts": differential_accepts,
            "by_smt_verdict_among_direct_accepts": dict(sorted(legs.items())),
        },
        "correctness_of_direct_accepted_proposals": {
            "repaired_a_real_error": direct_accepted_correct,
            "wrong_value_on_a_real_error": direct_accepted_wrong,
            "would_touch_a_clean_cell": direct_accepted_on_clean_cell,
            "precision_against_ground_truth": (
                round(direct_accepted_correct / direct_accepted, 4) if direct_accepted else None
            ),
        },
        "failure_dispositions": classify_failures(result),
        "arm": name,
    }


def _holds_on_clean(clean: pd.DataFrame, fd: FunctionalDependency) -> bool:
    """Whether a dependency holds with no exceptions on ground truth."""
    determinant = list(fd.determinant)
    if any(col not in clean.columns for col in [*determinant, fd.dependent]):
        return False
    grouped = clean.groupby(determinant, sort=False)[fd.dependent]
    return int(grouped.nunique(dropna=False).max()) == 1


def escalation_gate_arms(
    source: Path,
    schemas: dict[str, Schema],
    ground_truth: Any,
) -> dict[str, Any]:
    """AMENDMENT 2/3: measure the batch VOLUME CAP directly, with and without confirmation.

    `dataforge/engine/repair.py:1920-1932` runs `SafetyFilter().evaluate_batch(...)` and, on a
    non-ALLOW verdict, sets ``accepted_fixes = []``. The discard is total and silent -- the fixes
    reach neither ``result.fixes`` nor ``receipt.suggested_fixes`` -- which is why every earlier
    instrument saw a bare zero with nothing held for review.

    Scored with `dataforge.bench.core.score_repairs`, the same scorer as the published anchor and
    the declared-premise result, so these F1s are directly comparable to 0.8352 and to 0.0000.
    """
    out: dict[str, Any] = {}
    for name, schema in schemas.items():
        for confirmed in (False, True):
            result = run_repair_pipeline(
                RepairPipelineRequest(
                    source_path=source,
                    mode="dry_run",
                    schema=schema,
                    confirm_escalations=confirmed,
                )
            )
            metrics = score_repairs(
                ground_truth,
                [
                    BenchmarkRepair(
                        row=fix.row,
                        column=fix.column,
                        new_value=str(fix.new_value),
                        reason="pipeline_auto_apply",
                    )
                    for fix in result.fixes
                ],
            )
            key = f"{name}__confirm_escalations_{str(confirmed).lower()}"
            out[key] = {
                "confirm_escalations": confirmed,
                "batch_safety_verdict": result.receipt.safety_verdict,
                "batch_safety_reason": result.receipt.reason,
                "writes": len(result.fixes),
                "tp": metrics.tp,
                "fp": metrics.fp,
                "fn": metrics.fn,
                "precision": round(metrics.precision, 4),
                "recall": round(metrics.recall, 4),
                "f1": round(metrics.f1, 4),
                "write_precision": (
                    round(metrics.tp / len(result.fixes), 4) if result.fixes else None
                ),
            }
            print(
                f"    {key:52s} verdict={out[key]['batch_safety_verdict']:9s} "
                f"writes={out[key]['writes']:>4} tp={out[key]['tp']:>4} fp={out[key]['fp']:>3} "
                f"F1={out[key]['f1']}"
            )
    return out


def main() -> int:
    """Measure four premises through the pipeline and through the structural predictor."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    dataset = load_real_world_dataset(CORPUS)
    dirty, clean = dataset.dirty_df, dataset.clean_df
    declared_schema = load_schema(PREMISE)
    all_str = dict.fromkeys((str(column) for column in dirty.columns), "str")
    oracle_fds = discover_oracle_fds(clean, columns=tuple(str(c) for c in dirty.columns))
    # Ground truth GRADES the Direct leg's accepted proposals (P9) and never feeds a verdict.
    truth = {(cell.row, cell.column): cell.clean_value for cell in dataset.ground_truth}

    condition_extra = tuple(
        fd
        for fd in oracle_fds
        if fd.dependent == "Condition"
        and (tuple(fd.determinant), fd.dependent)
        not in {
            (tuple(f.determinant), f.dependent) for f in declared_schema.functional_dependencies
        }
    )

    arms: dict[str, Schema] = {
        "declared": declared_schema,
        "oracle": Schema(columns=all_str, functional_dependencies=oracle_fds),
        "oracle_thinned_to_one_determinant": Schema(
            columns=all_str, functional_dependencies=thin_to_one_determinant(oracle_fds)
        ),
        "declared_plus_condition_redundancy": Schema(
            columns=all_str,
            functional_dependencies=(*declared_schema.functional_dependencies, *condition_extra),
        ),
        "declared_plus_earlier_sorting_determinant": Schema(
            columns=all_str,
            functional_dependencies=(
                *declared_schema.functional_dependencies,
                P4_REDUNDANT_FD,
            ),
        ),
    }

    measured: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="dataforge-yield-") as raw_tmp:
        source = Path(raw_tmp) / f"{CORPUS}.csv"
        dirty.to_csv(source, index=False)
        for name, schema in arms.items():
            print(f"  measuring {name} ({len(schema.functional_dependencies)} FDs) ...")
            measured[name] = measure_arm(name, schema, source, clean, truth)
            arm = measured[name]
            legs = arm["verifier_leg_attribution"]
            correctness = arm["correctness_of_direct_accepted_proposals"]
            print(
                f"    actual {arm['pipeline_actual_writes']:>3} | direct-leg accepts "
                f"{legs['direct_leg_accepts']:>3} | differential accepts "
                f"{legs['differential_accepts']:>3} | of direct accepts: "
                f"{correctness['repaired_a_real_error']}R / "
                f"{correctness['wrong_value_on_a_real_error']}W / "
                f"{correctness['would_touch_a_clean_cell']}C | precision "
                f"{correctness['precision_against_ground_truth']}"
            )
            print(f"      smt among direct accepts: {legs['by_smt_verdict_among_direct_accepts']}")

        print("  measuring the batch volume cap (AMENDMENT 2/3) ...")
        escalation = escalation_gate_arms(
            source,
            {"declared": declared_schema, "oracle": arms["oracle"]},
            dataset.ground_truth,
        )

    # --- K1: the instrument must reproduce the committed write counts. ---
    committed = committed_writes()
    k1 = (
        measured["declared"]["pipeline_actual_writes"] == committed["declared"]
        and measured["oracle"]["pipeline_actual_writes"] == committed["oracle"]
    )
    if not k1:
        print(
            f"FAIL K1: this harness measures declared="
            f"{measured['declared']['pipeline_actual_writes']} oracle="
            f"{measured['oracle']['pipeline_actual_writes']}, but the committed artifact records "
            f"{committed}. The instrument does not reproduce the result it builds on, so NOTHING "
            "may be reported. Fix the harness first.",
            file=sys.stderr,
        )
        return 3

    # --- K2: the predictor must agree with the shipped verifier. ---
    agreements = [
        arm["predictor_vs_shipped_agreement"]
        for arm in measured.values()
        if arm["predictor_vs_shipped_agreement"] is not None
    ]
    worst_agreement = min(agreements) if agreements else None
    k2 = worst_agreement is not None and worst_agreement >= K2_MIN_AGREEMENT
    if not k2:
        print(
            f"FAIL K2: the structural predictor agrees with the shipped DirectVerifier on only "
            f"{worst_agreement} of proposals on its worst arm, below the {K2_MIN_AGREEMENT} floor. "
            "The predictor has drifted from the code it models, so P5 is UNREPORTABLE and the "
            "disagreeing cells are the finding instead.",
            file=sys.stderr,
        )

    def relative_error(arm: dict[str, Any]) -> float | None:
        actual = arm["pipeline_actual_writes"]
        predicted = arm["predictor_predicted_writes"]
        if actual == 0:
            return 0.0 if predicted == 0 else 1.0
        return round(abs(predicted - actual) / actual, 4)

    errors = {name: relative_error(arm) for name, arm in measured.items()}
    worst_error = max(value for value in errors.values() if value is not None)

    verdict = {
        "k1_instrument_reproduces_committed_write_counts": k1,
        "k2_predictor_agrees_with_shipped_verifier": k2,
        "k2_worst_agreement": worst_agreement,
        "k3_h4_refuted": worst_error > K3_REFUTATION_TOLERANCE,
        "p1_oracle_writes_are_singleton_violation_groups": (
            measured["oracle"]["violation_group_shape"].get("singleton_violation_groups", 0)
            == measured["oracle"]["pipeline_actual_writes"]
        ),
        "p2_declared_has_no_singleton_violation_groups": (
            measured["declared"]["violation_group_shape"].get("singleton_violation_groups", 0) == 0
        ),
        "p3_dominant_declared_disposition": max(
            (
                (count, status)
                for status, count in measured["declared"]["failure_dispositions"].items()
                if status != "reason_names_a_functional_dependency"
            ),
            default=(0, "none"),
        )[1],
        "p4_membership_changes_write_count": (
            measured["declared_plus_earlier_sorting_determinant"]["pipeline_actual_writes"]
            != measured["declared"]["pipeline_actual_writes"]
        ),
        "p5_predictor_within_tolerance_on_every_arm": worst_error <= P5_TOLERANCE,
        "p5_relative_error_by_arm": errors,
        "p5_worst_relative_error": worst_error,
        "p7_direct_leg_accepts_more_than_differential": (
            measured["declared"]["verifier_leg_attribution"]["direct_leg_accepts"]
            > measured["declared"]["verifier_leg_attribution"]["differential_accepts"]
        ),
        "p7_declared_differential_accepts": (
            measured["declared"]["verifier_leg_attribution"]["differential_accepts"]
        ),
        "p8_dominant_smt_verdict_among_direct_accepts": max(
            (
                (count, label)
                for label, count in measured["declared"]["verifier_leg_attribution"][
                    "by_smt_verdict_among_direct_accepts"
                ].items()
            ),
            default=(0, "none"),
        )[1],
        "p9_direct_accepted_precision_at_least_70pc": (
            (
                measured["declared"]["correctness_of_direct_accepted_proposals"][
                    "precision_against_ground_truth"
                ]
                or 0.0
            )
            >= 0.70
        ),
        "p9_declared_direct_accepted_precision": (
            measured["declared"]["correctness_of_direct_accepted_proposals"][
                "precision_against_ground_truth"
            ]
        ),
        "p10_oracle_writes_within_direct_accepts": (
            measured["oracle"]["pipeline_actual_writes"]
            <= measured["oracle"]["verifier_leg_attribution"]["direct_leg_accepts"]
        ),
        "k7_loss_is_downstream_of_the_verifier": (
            measured["declared"]["verifier_leg_attribution"]["differential_accepts"]
            > measured["declared"]["pipeline_actual_writes"]
        ),
        "p11_confirming_the_escalation_releases_more_than_100_writes": (
            escalation["declared__confirm_escalations_true"]["writes"] > 100
        ),
        "p12_released_writes_at_least_95pc_correct": (
            escalation["declared__confirm_escalations_true"]["precision"] >= 0.95
        ),
        "p13_oracle_batch_verdict_is_allow": (
            escalation["oracle__confirm_escalations_false"]["batch_safety_verdict"] == "allow"
        ),
        "h6_capability_was_gated_only_by_the_volume_cap": (
            escalation["declared__confirm_escalations_false"]["writes"] == 0
            and escalation["declared__confirm_escalations_true"]["writes"] > 100
        ),
        "declared_end_to_end_f1_with_escalation_confirmed": (
            escalation["declared__confirm_escalations_true"]["f1"]
        ),
        "p6_joint_check_would_accept_more": any(
            arm["joint_verification_headroom"]["additional_cells_a_joint_check_would_accept"] > 0
            for arm in measured.values()
        ),
    }

    payload = {
        "schema_version": "dataforge_fd_repair_yield_v1",
        "preregistration": "eval/preregistration/fd_repair_yield_mechanism.md",
        "dataset": CORPUS,
        "premise_file": "eval/premises/hospital_declared.yaml",
        "hypothesis": (
            "H4: a single-cell FD repair verifies only when it is the last remaining violation in "
            "its determinant group (dataforge/verifier/direct.py:245-260), so yield equals the "
            "number of determinant groups containing exactly one differing cell, and premise SIZE "
            "matters only through which determinant wins the sort at "
            "dataforge/repairers/fd_violation.py:158."
        ),
        "method": (
            "Five premises over one table. For each: the shipped pipeline in dry_run mode supplies "
            "the actual write count; the shipped FDViolationDetector and FDViolationRepairer supply "
            "the proposals; and a STRUCTURAL PREDICTOR that never runs the pipeline (accept iff no "
            "other row sharing the determinant would still disagree) supplies a predicted write "
            "count. K2 checks the predictor against the shipped DirectVerifier per proposal, "
            "because a drifted reimplementation would confirm the hypothesis for the wrong reason."
        ),
        "verifier_scoping_correction": (
            "dataforge/repairers/fd_violation.py:99-103 states the differential verifier 'checks a "
            "candidate against the WHOLE schema'. It does not: direct.py:129-133 and "
            "smt.py:245-248 both scope to dependencies where the fixed column is the dependent or "
            "appears in the determinant. The check IS global over rows, which is the mechanism, but "
            "it is not global over the schema."
        ),
        "committed_referents": committed,
        "committed_referents_source": (
            "eval/results/declared_premise_capability.json /arms/*/writes -- read from the "
            "artifact, deliberately not a constant in this harness"
        ),
        "arms": measured,
        "escalation_gate": escalation,
        "p6_caveat": (
            "joint_verification_headroom describes a configuration that DOES NOT EXIST. "
            "direct.py:104-111 applies a fix list sequentially, verifying each against a frame in "
            "which the others are not yet applied, so passing a group as a list today still fails. "
            "Any use of this number as a capability claim would be the proposal-stage defect again."
        ),
        "verdict": verdict,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nWrote {args.output}")
    return 0 if k2 else 4


if __name__ == "__main__":
    raise SystemExit(main())
