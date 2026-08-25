"""Measure what the label-free repair path can actually prove, and how often it is right.

Why this script exists
----------------------
``DECISIONS.md`` records that when human-labelled certification died, "the honest product is
soundness-plus-reversibility (which needs no labels) plus advisory triage". That fallback has been
asserted repeatedly and never given a denominator. The only coverage figures anywhere in the repo
are incidental: ``specs/SPEC_autoapply_decision.md`` reports a deterministic floor of **1** cell on
``hospital_10rows.csv`` which drops to **zero** once a schema is declared, and
``eval/results/trust_ledger_adversarial.json`` writes **1** cell out of 14 attack proposals on a
corpus containing exactly one real error. Neither can measure coverage: one is a fixture built to
be non-zero, the other has a denominator of one.

This script supplies the missing denominator on a real corpus with retained ground truth, and it
tests the mechanism the fallback rests on.

Why two premise arms, and why that is the whole point
----------------------------------------------------
``eval/results/trust_ledger_adversarial.json`` already showed that the write gate's verdict is a
property of the *premise*, not of the fix: **0 of 14** attacks were written under a tight premise
and **10 of 14** under a premise declaring every column ``str``, with every write labelled
``proven`` in both runs. So a single-premise coverage number would be uninterpretable.

* **oracle** -- functional dependencies discovered from the **clean** frame, admitted only if they
  hold exactly on ground truth. No user has this. It is the ceiling: if the mechanism misfires
  here, no amount of schema authoring rescues it.
* **mined** -- functional dependencies from the product's own miner,
  :func:`dataforge.schema_inference.infer_verification_schema`, run on the **dirty** frame at its
  shipped 0.95 confidence threshold. **This is not a default-reachable configuration.**
  ``--fd-detection`` does default to ``accepted``, but that flag *filters* dependencies already in
  the effective schema and mines nothing. A mined FD reaches the repairer only after
  ``profile --constraints-out``, an explicit ``constraints review --accept`` over a printed
  queue-cost warning, and ``repair --constraints``. This arm models a user who took those three
  steps and accepted the miner's output.

The mined arm is the one that matters for safety. An FD-derived repair carries ``deterministic``
provenance, and ``partition_auto_apply`` lets ``deterministic`` fixes on allowlisted detectors
**bypass calibration entirely** -- no threshold, no confidence, no labels. So a false mined FD is a
route to a confident wrong write with nothing downstream to catch it. Whether that happens is a
measurement, not an opinion, and it is the FD-path analogue of the 10-of-14 result.

What is measured
----------------
``FDViolationRepairer`` is called directly -- not reimplemented -- so the numbers describe shipped
behaviour. Per real error in a dependent column: whether the path proposes, whether the proposal
equals retained truth, and the shape of the vote that produced it.

``_deterministic_choice`` accepts ``ranked[0] > ranked[1]``, which is a **plurality**; its docstring
says "strict majority". Those differ exactly when the winner holds at most half the group, which is
where a dirty-data plurality can outvote the truth. The gap is counted rather than assumed, so the
fix can be justified by evidence instead of by tidiness.

Coverage is reported against real errors in FD-covered columns and against all real errors in the
table, because a path that repairs most of a small slice is a different product from one that
repairs most of the table.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from dataforge.datasets.real_world import load_real_world_dataset
from dataforge.detectors.base import FunctionalDependency, Issue, Schema, Severity
from dataforge.detectors.fd_violation import FDViolationDetector
from dataforge.repairers.fd_violation import FDViolationRepairer
from dataforge.schema_inference import infer_verification_schema

#: A determinant group needs this many rows before a vote is possible. A singleton group makes
#: ``len(counts) <= 1`` true, so the repairer skips it and it can contribute neither coverage
#: nor error.
MIN_GROUP_SIZE = 2


def _fd_label(fd: FunctionalDependency) -> str:
    """Render an FD as a stable, readable key."""
    return f"{' + '.join(fd.determinant)} -> {fd.dependent}"


def _sorted_fds(fds: object) -> tuple[FunctionalDependency, ...]:
    """Order FDs deterministically so 'first matching FD wins' is reproducible."""
    return tuple(
        sorted(
            fds,  # type: ignore[call-overload]
            key=lambda fd: (fd.dependent, tuple(fd.determinant)),
        )
    )


def fd_holds_on_clean(clean: pd.DataFrame, fd: FunctionalDependency) -> bool:
    """Return whether this dependency holds with no exceptions on ground truth."""
    determinant = list(fd.determinant)
    if any(column not in clean.columns for column in [*determinant, fd.dependent]):
        return False
    grouped = clean.groupby(determinant, sort=False)[fd.dependent]
    return int(grouped.nunique(dropna=False).max()) == 1


def discover_oracle_fds(
    clean: pd.DataFrame, *, columns: tuple[str, ...]
) -> tuple[FunctionalDependency, ...]:
    """Return every single-column FD that holds exactly on the clean frame.

    Admission requires the dependency to hold with no exceptions, at least one determinant group of
    ``MIN_GROUP_SIZE`` rows so a vote is reachable, and a non-constant dependent -- a single-valued
    column is determined by everything and would inflate coverage with cells no premise worked for.

    Multi-column determinants are out of scope for this arm; the mined arm supplies them.
    """
    discovered: list[FunctionalDependency] = []
    for dependent in columns:
        if clean[dependent].nunique(dropna=False) <= 1:
            continue
        for determinant in columns:
            if determinant == dependent:
                continue
            grouped = clean.groupby(determinant, sort=False)[dependent]
            if int(grouped.nunique(dropna=False).max()) != 1:
                continue
            if int(grouped.size().max()) < MIN_GROUP_SIZE:
                continue
            discovered.append(FunctionalDependency(determinant=(determinant,), dependent=dependent))
    return _sorted_fds(discovered)


def mined_fds(dirty: pd.DataFrame) -> tuple[FunctionalDependency, ...]:
    """Return the FDs the product's own miner accepts on the dirty frame.

    Uses :func:`infer_verification_schema`, which applies the shipped 0.95 confidence floor. No
    threshold is chosen here, because inventing one would make this arm measure the author rather
    than the product.
    """
    return _sorted_fds(infer_verification_schema(dirty).functional_dependencies)


def _acting_group(
    dirty: pd.DataFrame, row: int, fds: tuple[FunctionalDependency, ...], column: str
) -> tuple[FunctionalDependency, Counter[str]] | None:
    """Return the FD the repairer will act on for this cell, plus that group's value counts.

    Mirrors ``FDViolationRepairer._propose``'s selection exactly: the first FD whose dependent is
    this column and whose determinant group shows more than one distinct dependent value. Groups
    with a single distinct value are skipped by ``continue`` there, so they are skipped here.
    """
    for fd in fds:
        if fd.dependent != column:
            continue
        determinant = list(fd.determinant)
        if any(col not in dirty.columns for col in [*determinant, column]):
            continue
        mask = pd.Series(True, index=dirty.index)
        for col in determinant:
            mask &= dirty[col] == dirty.iat[row, dirty.columns.get_loc(col)]
        group = dirty[mask]
        if group.empty:
            continue
        counts = Counter(str(value) for value in group[column])
        if len(counts) <= 1:
            continue
        return fd, counts
    return None


def _vote_shape(counts: Counter[str], old_value: str) -> dict[str, Any]:
    """Classify the vote: is the winner a true majority, and do the other rows agree?"""
    ranked = counts.most_common()
    group_size = sum(counts.values())
    top_value, top_count = ranked[0]
    second_count = ranked[1][1] if len(ranked) > 1 else 0

    others = Counter(counts)
    others[old_value] -= 1
    others = +others  # drops non-positive entries

    return {
        "group_size": group_size,
        "distinct_values": len(counts),
        "top_value": top_value,
        "top_count": top_count,
        "second_count": second_count,
        # What the repairer accepts today. Its docstring claims this is a strict majority.
        "plurality_wins": top_count > second_count,
        # What "strict majority" actually means: more than half the group.
        "is_true_majority": top_count * 2 > group_size,
        # The only condition under which premise plus data entail a unique value for this cell.
        "others_unanimous": len(others) == 1,
        "unanimous_value": next(iter(others)) if len(others) == 1 else None,
    }


def _run_arm(
    dataset: Any,
    fds: tuple[FunctionalDependency, ...],
) -> list[dict[str, Any]]:
    """Replay every real error in an FD-covered column through the shipped repairer."""
    dirty = dataset.dirty_df
    fd_columns = {fd.dependent for fd in fds}
    schema = _schema_for(dirty, fds)
    repairer = FDViolationRepairer(cache_dir=None, allow_llm=False)

    records: list[dict[str, Any]] = []
    for cell in dataset.ground_truth:
        if cell.column not in fd_columns:
            continue
        issue = Issue(
            row=cell.row,
            column=cell.column,
            issue_type="fd_violation",
            severity=Severity.REVIEW,
            confidence=0.9,
            actual=cell.dirty_value,
            reason="ground-truth real error, replayed through the shipped FD repairer",
        )
        proposal = repairer.propose(issue, dirty, schema, None)
        acting = _acting_group(dirty, cell.row, fds, cell.column)
        records.append(
            {
                "row": cell.row,
                "column": cell.column,
                "dirty_value": cell.dirty_value,
                "clean_value": cell.clean_value,
                "acting_fd": _fd_label(acting[0]) if acting is not None else None,
                "proposed": proposal is not None,
                "proposed_value": proposal.fix.new_value if proposal is not None else None,
                "provenance": proposal.provenance if proposal is not None else None,
                "correct": proposal is not None and proposal.fix.new_value == cell.clean_value,
                "vote": _vote_shape(acting[1], cell.dirty_value) if acting is not None else None,
            }
        )
    return records


def _schema_for(dirty: pd.DataFrame, fds: tuple[FunctionalDependency, ...]) -> Schema:
    """Build the premise for an arm.

    Every column is typed ``str`` deliberately: this isolates the FD mechanism. A tighter type
    premise would also narrow writes, and attributing that narrowing to the FD vote would overstate
    what the dependency did.
    """
    return Schema(
        columns=dict.fromkeys((str(column) for column in dirty.columns), "str"),
        functional_dependencies=fds,
    )


def _rule_choice(rule: str, counts: Counter[str], old_value: str) -> str | None:
    """What each candidate decision rule would write, before the no-change check.

    ``majority`` is what ``FDViolationRepairer._deterministic_choice`` implements as of
    2026-08-25, and it is what its docstring always claimed. ``plurality`` is what that function
    actually did until then, kept as a counterfactual so the cost of the change stays measurable
    rather than becoming folklore. ``unanimity`` requires every other row in the group to agree,
    which is the only one of the three under which the premise plus the data entail a unique value
    rather than merely favouring one -- and which measured worse than both.
    """
    ranked = counts.most_common()
    if not ranked:
        return None
    group_size = sum(counts.values())
    top_value, top_count = ranked[0]
    second_count = ranked[1][1] if len(ranked) > 1 else 0

    if rule == "plurality":
        if len(ranked) < 2:
            return top_value
        return top_value if top_count > second_count else None
    if rule == "majority":
        return top_value if top_count * 2 > group_size else None
    if rule == "unanimity":
        others = Counter(counts)
        others[old_value] -= 1
        others = +others
        return next(iter(others)) if len(others) == 1 else None
    raise ValueError(f"unknown rule {rule!r}")


#: ``majority`` is the shipped rule; the other two are counterfactuals measured on the same flags.
_RULES = ("plurality", "majority", "unanimity")
_SHIPPED_RULE = "majority"


def _write_exposure(
    dataset: Any,
    fds: tuple[FunctionalDependency, ...],
    schema: Schema,
) -> dict[str, Any]:
    """Run the real detector, repair every flag, and count what would hit a CLEAN cell.

    This is the measurement that decides whether the label-free path is safe, and it is not the
    same as the per-error precision above. That number is conditional on the cell already being a
    real error, so it can only report how good the repairs are on cells that needed one. It is
    silent on the failure that actually costs a user data: proposing a change to a cell that was
    already correct.

    So the pipeline is replayed as it ships -- ``FDViolationDetector`` produces the queue,
    ``FDViolationRepairer`` proposes on each flag -- and every proposal is classified against
    retained ground truth. A proposal on a clean cell is a **corruption**: FD repairs carry
    ``deterministic`` provenance, and ``partition_auto_apply`` lets those bypass calibration
    entirely, so nothing downstream would hold it back.

    Two counterfactual rules are evaluated on the identical flag set, so the choice of decision rule
    can be made on measured corruption rather than on which one sounds strictest.
    ``replication_mismatches`` must be zero: it checks the reimplemented ``shipped`` rule against
    the real repairer's own output, so the counterfactuals are known to be computed the same way.
    """
    dirty = dataset.dirty_df
    clean = dataset.clean_df
    truth_by_cell = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}

    issues = FDViolationDetector().detect(dirty, schema)
    repairer = FDViolationRepairer(cache_dir=None, allow_llm=False)

    tallies: dict[str, dict[str, int]] = {
        rule: {
            "proposals": 0,
            "repaired_a_real_error": 0,
            "wrong_value_on_a_real_error": 0,
            "corrupted_a_clean_cell": 0,
            "no_op_on_a_clean_cell": 0,
        }
        for rule in _RULES
    }
    abstained = 0
    replication_mismatches = 0
    corruption_examples: list[dict[str, Any]] = []
    # A cell is written at most once, so the unit of account is the distinct cell, not the flag.
    # With a mined premise the same cell is flagged once per FD naming its column, which inflates
    # flag counts several-fold and would inflate every rate computed against them. ``propose``
    # already loops every FD internally, so one call per distinct cell is complete.
    seen: set[tuple[int, str]] = set()

    for issue in issues:
        key = (issue.row, issue.column)
        if key in seen:
            continue
        seen.add(key)

        proposal = repairer.propose(issue, dirty, schema, None)
        acting = _acting_group(dirty, issue.row, fds, issue.column)
        old_value = str(dirty.iat[issue.row, dirty.columns.get_loc(issue.column)])

        shipped_value: str | None = None
        if acting is not None:
            candidate = _rule_choice(_SHIPPED_RULE, acting[1], old_value)
            shipped_value = None if candidate == old_value else candidate
        real_value = proposal.fix.new_value if proposal is not None else None
        if shipped_value != real_value:
            replication_mismatches += 1

        if proposal is None:
            abstained += 1
        if acting is None:
            continue

        for rule in _RULES:
            chosen = _rule_choice(rule, acting[1], old_value)
            if chosen is None or chosen == old_value:
                continue
            tally = tallies[rule]
            tally["proposals"] += 1
            if key in truth_by_cell:
                if chosen == truth_by_cell[key]:
                    tally["repaired_a_real_error"] += 1
                else:
                    tally["wrong_value_on_a_real_error"] += 1
                continue
            # The cell was already correct. Writing anything other than its existing value
            # destroys good data; writing the same value is a harmless no-op.
            current = str(clean.iat[issue.row, clean.columns.get_loc(issue.column)])
            if chosen == current:
                tally["no_op_on_a_clean_cell"] += 1
            else:
                tally["corrupted_a_clean_cell"] += 1
                if rule == _SHIPPED_RULE and len(corruption_examples) < 25:
                    corruption_examples.append(
                        {
                            "row": issue.row,
                            "column": issue.column,
                            "acting_fd": _fd_label(acting[0]),
                            "was_already_correct": current,
                            "would_be_written": chosen,
                            "group_size": sum(acting[1].values()),
                        }
                    )

    def _rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    by_rule: dict[str, Any] = {}
    for rule, tally in tallies.items():
        harmful = tally["wrong_value_on_a_real_error"] + tally["corrupted_a_clean_cell"]
        by_rule[rule] = {
            **tally,
            "write_precision": _rate(tally["repaired_a_real_error"], tally["proposals"]),
            "harmful_write_rate": _rate(harmful, tally["proposals"]),
            "net_cells_improved": tally["repaired_a_real_error"] - harmful,
            "coverage_of_all_table_errors": _rate(
                tally["repaired_a_real_error"], len(dataset.ground_truth)
            ),
        }

    return {
        "note": (
            "Unconditional on error status. This is what a user's data is exposed to, and the only "
            "figure here that can go badly wrong."
        ),
        "detector_flags": len(issues),
        "distinct_cells_flagged": len(seen),
        "abstained_on_flag": abstained,
        "replication_mismatches": replication_mismatches,
        "by_rule": by_rule,
        "corruption_examples": corruption_examples,
    }


def _summarise_arm(
    dataset: Any,
    fds: tuple[FunctionalDependency, ...],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reduce per-cell records to the numbers the product claim needs."""
    clean = dataset.clean_df
    proposed = [r for r in records if r["proposed"]]
    correct = [r for r in proposed if r["correct"]]
    wrong = [r for r in proposed if not r["correct"]]

    voted = [r for r in records if r["vote"] is not None]
    majority = [r for r in voted if r["vote"]["is_true_majority"]]
    plurality_only = [
        r for r in voted if r["vote"]["plurality_wins"] and not r["vote"]["is_true_majority"]
    ]
    unanimous = [r for r in voted if r["vote"]["others_unanimous"]]
    unanimous_correct = [r for r in unanimous if r["vote"]["unanimous_value"] == r["clean_value"]]

    # How many proposals a true-majority rule would keep, and whether any were wrong. Zero
    # difference from the shipped rule means the docstring/code gap is latent here, not harmless
    # everywhere.
    majority_proposals = [r for r in proposed if r["vote"] and r["vote"]["is_true_majority"]]
    plurality_only_proposals = [
        r for r in proposed if r["vote"] and not r["vote"]["is_true_majority"]
    ]

    true_fds = [fd for fd in fds if fd_holds_on_clean(clean, fd)]
    exposure = _write_exposure(dataset, fds, _schema_for(dataset.dirty_df, fds))

    def _rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    total_errors = len(dataset.ground_truth)
    return {
        "premise": {
            "fd_count": len(fds),
            "fd_count_holding_on_clean": len(true_fds),
            "fd_set_precision": _rate(len(true_fds), len(fds)),
            "fd_covered_columns": sorted({fd.dependent for fd in fds}),
            "functional_dependencies": [_fd_label(fd) for fd in fds],
        },
        "population": {
            "real_errors_in_table": total_errors,
            "real_errors_in_fd_columns": len(records),
            "fd_column_share_of_errors": _rate(len(records), total_errors),
        },
        "shipped_rule": {
            "rule": _SHIPPED_RULE,
            "proposed": len(proposed),
            "correct": len(correct),
            "wrong": len(wrong),
            "abstained": len(records) - len(proposed),
            "precision": _rate(len(correct), len(proposed)),
            "coverage_of_fd_column_errors": _rate(len(correct), len(records)),
            "coverage_of_all_table_errors": _rate(len(correct), total_errors),
        },
        "vote_shape": {
            "cells_with_a_visible_conflict": len(voted),
            "true_majority": len(majority),
            "plurality_only_not_majority": len(plurality_only),
            "others_unanimous": len(unanimous),
        },
        "true_majority_counterfactual": {
            "note": (
                "Make the code match its docstring: require more than half the group. Cells the "
                "shipped plurality rule proposes on but a majority rule would not."
            ),
            "proposals_kept": len(majority_proposals),
            "proposals_dropped": len(plurality_only_proposals),
            "dropped_that_were_wrong": sum(1 for r in plurality_only_proposals if not r["correct"]),
        },
        "unanimity_counterfactual": {
            "note": (
                "Propose only when every other row in the determinant group agrees -- the "
                "condition under which premise plus data entail a unique value rather than "
                "merely favouring one."
            ),
            "would_propose": len(unanimous),
            "would_be_correct": len(unanimous_correct),
            "would_be_wrong": len(unanimous) - len(unanimous_correct),
            "precision": _rate(len(unanimous_correct), len(unanimous)),
            "coverage_of_all_table_errors": _rate(len(unanimous_correct), total_errors),
        },
        "write_exposure": exposure,
        "wrong_proposals": [
            {
                "row": r["row"],
                "column": r["column"],
                "acting_fd": r["acting_fd"],
                "dirty": r["dirty_value"],
                "proposed": r["proposed_value"],
                "truth": r["clean_value"],
                "group_size": r["vote"]["group_size"] if r["vote"] else None,
                "top_count": r["vote"]["top_count"] if r["vote"] else None,
                "is_true_majority": r["vote"]["is_true_majority"] if r["vote"] else None,
            }
            for r in wrong
        ][:40],
    }


def measure(corpus: str, *, cache_root: Path | None) -> dict[str, Any]:
    """Run both premise arms and return a single comparable artifact."""
    dataset = load_real_world_dataset(corpus, cache_root=cache_root)
    columns = tuple(str(column) for column in dataset.dirty_df.columns)

    arms = {
        "oracle": discover_oracle_fds(dataset.clean_df, columns=columns),
        "mined": mined_fds(dataset.dirty_df),
    }
    return {
        "schema": "dataforge_deductive_coverage_v2",
        "corpus": corpus,
        "rows": int(dataset.dirty_df.shape[0]),
        "dirty_sha256": dataset.dirty_sha256,
        "clean_sha256": dataset.clean_sha256,
        "arm_definitions": {
            "oracle": (
                "single-column FDs holding exactly on the CLEAN frame; no user has this; the "
                "ceiling on the mechanism"
            ),
            "mined": (
                "infer_verification_schema on the DIRTY frame at the shipped 0.95 floor. NOT a "
                "default-reachable configuration: --fd-detection defaults to 'accepted', but that "
                "flag FILTERS dependencies already in the effective schema and does not mine any. "
                "A mined FD reaches the repairer only after profile --constraints-out, an explicit "
                "constraints review --accept over a printed queue-cost warning, and repair "
                "--constraints. This arm models a user who accepted the miner's output"
            ),
        },
        "arms": {
            name: _summarise_arm(dataset, fds, _run_arm(dataset, fds)) for name, fds in arms.items()
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="hospital")
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()

    payload = measure(args.corpus, cache_root=args.cache_root)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(f"corpus {payload['corpus']}  rows {payload['rows']}")
    for name, arm in payload["arms"].items():
        premise = arm["premise"]
        shipped = arm["shipped_rule"]
        shape = arm["vote_shape"]
        majority = arm["true_majority_counterfactual"]
        unanimity = arm["unanimity_counterfactual"]
        print(f"\n=== arm: {name} ===")
        print(
            f"FDs                             {premise['fd_count']} "
            f"({premise['fd_count_holding_on_clean']} hold on clean, "
            f"precision {premise['fd_set_precision']})"
        )
        print(
            f"real errors in FD columns       {arm['population']['real_errors_in_fd_columns']}"
            f" of {arm['population']['real_errors_in_table']}"
        )
        print(
            f"proposed / correct / WRONG      {shipped['proposed']} / "
            f"{shipped['correct']} / {shipped['wrong']}"
        )
        print(f"precision                       {shipped['precision']}")
        print(f"coverage of all table errors    {shipped['coverage_of_all_table_errors']}")
        print(f"plurality-only (not majority)   {shape['plurality_only_not_majority']}")
        print(
            f"majority rule would drop        {majority['proposals_dropped']} "
            f"(wrong among dropped: {majority['dropped_that_were_wrong']})"
        )
        print(
            f"unanimity coverage / precision  "
            f"{unanimity['coverage_of_all_table_errors']} / {unanimity['precision']}"
        )
        exposure = arm["write_exposure"]
        print("  -- unconditional write exposure (the number that can go wrong) --")
        print(f"  distinct cells flagged        {exposure['distinct_cells_flagged']}")
        print(f"  replication mismatches        {exposure['replication_mismatches']} (must be 0)")
        header = f"  {'rule':<11}{'writes':>8}{'repaired':>10}{'WRONG':>7}{'CORRUPT':>9}"
        print(f"{header}{'precision':>11}{'net':>7}{'coverage':>10}")
        for rule, stats in exposure["by_rule"].items():
            print(
                f"  {rule:<11}{stats['proposals']:>8}{stats['repaired_a_real_error']:>10}"
                f"{stats['wrong_value_on_a_real_error']:>7}{stats['corrupted_a_clean_cell']:>9}"
                f"{str(stats['write_precision']):>11}{stats['net_cells_improved']:>7}"
                f"{str(stats['coverage_of_all_table_errors']):>10}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
