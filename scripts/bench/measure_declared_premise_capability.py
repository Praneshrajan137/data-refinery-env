"""H3: what does a DECLARED premise repair on hospital, through the shipped write path?

Executes `eval/preregistration/declared_premise_capability.md`. Read that first.

## The gap this closes

`measure_capability_stage.py` measured hospital at three points and left the fourth explicitly
open. Its write-up says: *"The obvious missing measurement is the fourth arm -- the pipeline under
a declared premise -- which is where the product's real claim lives and which no instrument
currently reports end-to-end."*

Two cells of the premise-by-stage matrix were empty at registration:

- **declared x pipeline** -- the product's central promise. *No instrument passed a hand-declared
  schema to ``run_repair_pipeline`` on a real corpus.*
- **oracle x pipeline** -- the honest ceiling through the real write path.

The widely quoted **393 repairs / 0 corruptions** is NOT a declared premise, whatever
``dataforge/cli/repair.py`` used to tell users. ``discover_oracle_fds`` admits a dependency only
if it holds exactly on the clean frame, and its own docstring says "No user has this. It is the
ceiling."

## Why the arms are built the way they are

Four arms, one table, one ground truth, one scorer:

- **proposal_stage_anchor** -- reproduces the published path. Binds **K1a**.
- **pipeline_mined_c4** -- the shipped default on a mined premise. Binds **K1b** against the
  committed stage artifact. Two independently gated referents, because one constant already
  rotted for 54 days.
- **pipeline_declared_premise** -- NEW. ``eval/premises/hospital_declared.yaml`` loaded by the
  shipped ``load_schema``, i.e. through the same door as ``dataforge repair --schema``.
- **pipeline_oracle_premise** -- NEW. ``discover_oracle_fds`` over the CLEAN frame, passed as
  ``repair_schema``. The ceiling, through the write path.

``score_repairs`` is imported from ``dataforge.bench.core``, and the four-outcome tally
(``repaired_a_real_error`` / ``wrong_value_on_a_real_error`` / ``no_op_on_a_clean_cell`` /
``corrupted_a_clean_cell``) is imported from ``measure_premise_write_exposure``. Neither is
reimplemented, so no arm can differ from another by its scoring and every number here is
comparable to the published ones.

## The two traps this harness is built to avoid

1. **A refusal is not a wrong answer.** ``score_repairs`` returns ``precision = 0.0`` when
   ``tp + fp == 0``. Both are reported: that shared-scorer precision for comparability, and a
   separate ``write_precision`` that is ``None`` when an arm writes nothing.
2. **A vacuous premise is not a refusal.** A premise that never bound anything and a premise that
   bound and then declined produce the same zero. **K2** separates them before scoring.

Read-only apart from one artifact under ``eval/results/``, plus a temporary CSV.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from dataforge.bench.core import BenchmarkRepair, score_repairs  # noqa: E402
from dataforge.bench.methods import _repairs_from_proposed_fixes  # noqa: E402
from dataforge.cli.common import load_schema  # noqa: E402
from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors.base import FunctionalDependency, Schema  # noqa: E402
from dataforge.detectors.fd_violation import FDViolationDetector  # noqa: E402
from dataforge.engine.repair import (  # noqa: E402
    RepairPipelineRequest,
    authoritative_columns,
    run_repair_pipeline,
)
from dataforge.schema_inference import (  # noqa: E402
    build_constraint_review_artifact,
    infer_schema,
    update_constraint_review_artifact,
)
from scripts.bench.measure_deductive_coverage import (  # noqa: E402
    _fd_label,
    discover_oracle_fds,
    fd_holds_on_clean,
)
from scripts.bench.measure_premise_write_exposure import (  # noqa: E402
    _score as tally_write_outcomes,
)
from scripts.bench.measure_premise_write_exposure import (  # noqa: E402
    _truth_by_cell,
)

CORPUS = "hospital"
PREMISE = REPO / "eval" / "premises" / "hospital_declared.yaml"
PREREGISTRATION = REPO / "eval" / "preregistration" / "declared_premise_capability.md"
ANCHOR_ARTIFACT = REPO / "eval" / "results" / "agent_comparison.json"
STAGE_ARTIFACT = REPO / "eval" / "results" / "capability_measurement_stage.json"
OUTPUT = REPO / "eval" / "results" / "declared_premise_capability.json"

#: K1a tolerance on the anchor F1. Inherited from the stage pre-registration, not invented here.
K1_TOLERANCE = 0.0001
#: The anchor decision rule pre-committed in the pre-registration: below this, the declared arm
#: is a rounding artefact of a handful of writes rather than a capability. Not a behaviour
#: threshold -- nothing in the product reads it -- so K5 is not violated.
ANCHOR_MATERIALITY_F1 = 0.05


def _display(path: Path) -> str:
    """Render a path repo-relative when it is inside the repo, absolutely when it is not.

    A bare ``relative_to(REPO)`` raises ``ValueError`` for any path outside the tree, which meant
    the K3 failure branch crashed with a ValueError instead of raising the SystemExit it was
    written to raise. A kill criterion whose failure path is itself broken cannot be trusted to
    fire, so this is not cosmetic. Caught by
    ``tests/unit/test_declared_premise_harness.py::test_a_preregistration_without_a_hash_is_refused``.
    """
    try:
        return str(path.relative_to(REPO)).replace("\\", "/")
    except ValueError:
        return str(path)


def registered_premise_sha256() -> str:
    """Return the premise hash the pre-registration recorded, read from that document.

    Deliberately NOT a constant in this file. K1's referents are read from gated artifacts
    because a documented constant is exactly what rotted for 54 days, and the same reasoning
    applies to K3: the hash that matters is the one a reader of the pre-registration sees, so
    the harness must fail when the two drift rather than silently attest to its own copy.
    """
    text = PREREGISTRATION.read_text(encoding="utf-8")
    found = re.search(r"sha256 \*\*`([0-9a-f]{64})`\*\*", text)
    if found is None:
        raise SystemExit(
            f"FAIL K3: {_display(PREREGISTRATION)} records no premise sha256. "
            "The premise cannot be shown to have been frozen before measurement."
        )
    return found.group(1)


def premise_sha256(path: Path) -> str:
    """Hash a premise file's text with line endings normalised to ``\\n``.

    Byte hashing would be wrong here, and quietly so. ``core.autocrlf=true`` on a Windows
    checkout leaves CRLF in the worktree while storing LF in the object database, so the same
    committed file hashes differently on this machine and in Linux CI -- voiding every run under
    K3 for a reason that has nothing to do with the premise. `eval/premises/.gitattributes` pins
    ``eol=lf`` as well; this is belt to that braces.
    """
    return hashlib.sha256(
        path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")
    ).hexdigest()


def anchor_f1() -> float:
    """Read the published anchor F1 from the artifact `anchor_truth.py` gates against the code."""
    payload = json.loads(ANCHOR_ARTIFACT.read_text(encoding="utf-8"))
    for record in payload.get("records", []):
        if record.get("method") == "heuristic" and record.get("dataset") == CORPUS:
            return round(float(record["f1"]), 4)
    raise SystemExit(f"FAIL K1a: no committed heuristic/{CORPUS} record to take the anchor from.")


def stage_mined_c4() -> dict[str, int]:
    """Read the committed mined-premise C4 arm: writes/tp/fp that K1b must reproduce exactly.

    Note the referent arm's name. `pipeline_c4_declared_authority` means *authority requires
    declaration* and runs a MINED premise -- one word away from the conflation this measurement
    exists to correct. The arms here are named after their premise, never after the authority
    rule, and this function is where the two vocabularies meet.
    """
    payload = json.loads(STAGE_ARTIFACT.read_text(encoding="utf-8"))
    arm = payload["arms"]["pipeline_c4_declared_authority"]
    return {"writes": int(arm["writes"]), "tp": int(arm["tp"]), "fp": int(arm["fp"])}


def as_repairs(fixes: list[Any]) -> list[BenchmarkRepair]:
    """Convert a would-apply set into the scorer's input type."""
    return [
        BenchmarkRepair(
            row=fix.row,
            column=fix.column,
            new_value=str(fix.new_value),
            reason=getattr(fix, "reason", "pipeline_auto_apply"),
        )
        for fix in fixes
    ]


def schema_for(dirty: pd.DataFrame, fds: tuple[FunctionalDependency, ...]) -> Schema:
    """Build a premise the same way `measure_deductive_coverage._schema_for` does.

    Every column `str`, which `type_discriminates` rejects, so authority comes only from the
    dependencies. Matching that helper matters: the oracle arm here has to be the same premise
    the published 393 was measured on, differing only in the stage it is taken at.
    """
    return Schema(
        columns=dict.fromkeys((str(column) for column in dirty.columns), "str"),
        functional_dependencies=fds,
    )


def pipeline_fixes(
    source: Path,
    *,
    repair_schema: Schema | None = None,
    constraints: Any = None,
    trust_mined: bool = False,
) -> tuple[list[Any], dict[str, int]]:
    """Return what the shipped pipeline WOULD write, plus the counts that explain a zero.

    `dry_run` writes nothing; `result.fixes` is the would-apply set. The receipt counts are
    returned alongside because a zero in `fixes` has several possible causes, and the counts
    separate them: `issues_count` shows whether detection fired, `candidate_repairs` shows
    whether the repairer proposed, and `suggested_fixes` shows what a gate held for review. The
    declared arm's 399 candidates against 0 writes is the single most important number in this
    measurement, and it must be in the artifact rather than in a discarded diagnostic.
    """
    result = run_repair_pipeline(
        RepairPipelineRequest(
            source_path=source,
            mode="dry_run",
            schema=repair_schema,
            constraints=constraints,
            mined_constraints_grant_write_authority=trust_mined,
        )
    )
    receipt = result.receipt
    counts = {
        "issues_detected": int(receipt.issues_count),
        "candidate_repairs_proposed": len(receipt.candidate_repairs or []),
        "held_for_review": len(receipt.suggested_fixes or []),
        "abstentions": len(receipt.abstentions or []),
        "authoritative_columns": len(receipt.authoritative_columns or []),
    }
    return list(result.fixes), counts


def by_detector(
    fixes: list[Any], dirty: pd.DataFrame, clean: pd.DataFrame, truth: dict[Any, str]
) -> dict[str, Any]:
    """Split an arm's writes by the detector that produced them.

    Without this, a win driven by some other detector operating inside the declared FDs' covered
    columns could be silently credited to the declared dependencies. The declared premise covers
    15 columns; `fd_violation` is only one of the detectors that can act on them.
    """
    grouped: dict[str, list[Any]] = {}
    for fix in fixes:
        grouped.setdefault(str(getattr(fix, "detector_id", "unknown")), []).append(fix)
    return {
        detector: tally_write_outcomes(group, dirty, clean, truth)
        for detector, group in sorted(grouped.items())
    }


def premise_audit(
    fds: tuple[FunctionalDependency, ...], clean: pd.DataFrame, raw_premise: Path | None
) -> dict[str, Any]:
    """Report which declared dependencies ground truth REFUTES, as an outcome.

    This is the asymmetry that makes a declared premise not an oracle. The oracle arm uses
    `fd_holds_on_clean` as an ADMISSION FILTER -- ground truth feeds it. Here the same predicate
    is used only to GRADE a premise that was frozen beforehand, and a refuted dependency stays in
    the arm and its damage is counted. A non-zero `refuted` is a finding about what declaring
    costs, never a licence to edit the premise file.
    """
    holding = tuple(fd for fd in fds if fd_holds_on_clean(clean, fd))
    refuted = tuple(fd for fd in fds if fd not in holding)
    audit: dict[str, Any] = {
        "fd_count": len(fds),
        "fd_count_holding_on_clean": len(holding),
        "fd_set_precision": round(len(holding) / len(fds), 4) if fds else None,
        "declared_fds_refuted_by_ground_truth": [_fd_label(fd) for fd in refuted],
        "fd_covered_columns": sorted({fd.dependent for fd in fds}),
        "functional_dependencies": [_fd_label(fd) for fd in fds],
    }
    if raw_premise is not None:
        warrants: dict[str, int] = {}
        parsed = load_premise_warrants(raw_premise)
        for fd in fds:
            warrants[parsed.get(_fd_label(fd), "unlabelled")] = (
                warrants.get(parsed.get(_fd_label(fd), "unlabelled"), 0) + 1
            )
        audit["warrants"] = dict(sorted(warrants.items()))
        audit["refuted_by_warrant"] = dict(
            sorted(
                {
                    warrant: sum(
                        1 for fd in refuted if parsed.get(_fd_label(fd), "unlabelled") == warrant
                    )
                    for warrant in warrants
                }.items()
            )
        )
    return audit


def load_premise_warrants(path: Path) -> dict[str, str]:
    """Map each declared dependency to the warrant its own file records.

    `load_schema` ignores keys it does not know, so `warrant` travels inside the premise file and
    the justification cannot drift away from the dependency it justifies -- one artifact, one
    hash. Parsed here rather than trusted: a mislabelled warrant would misreport the split
    between documented and format-evident dependencies.
    """
    import yaml

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mapping: dict[str, str] = {}
    for entry in raw.get("functional_dependencies", []) or []:
        if not isinstance(entry, dict):
            continue
        determinant = tuple(str(value) for value in entry.get("determinant", []))
        dependent = str(entry.get("dependent", ""))
        label = _fd_label(FunctionalDependency(determinant=determinant, dependent=dependent))
        mapping[label] = str(entry.get("warrant", "unlabelled"))
    return mapping


def check_vacuity(declared: Schema, dirty: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    """K2: prove the premise BINDS before any zero is read as a refusal.

    A premise that never bound anything and a premise that bound and then declined produce the
    identical numeral. Every harness built for this project that skipped a vacuity check was
    later found to need one, so the three ways this premise could silently fail to bind are
    asserted rather than assumed: a column that is not in the frame, a dependent that is constant
    (nothing can violate an FD into a single-valued column), and a premise that raises no issue
    at all.
    """
    missing = sorted(column for column in declared.columns if column not in dirty.columns)
    dependents = sorted({fd.dependent for fd in declared.functional_dependencies})
    constant = sorted(
        column
        for column in dependents
        if column in dirty.columns and int(dirty[column].nunique(dropna=False)) <= 1
    )
    issues = FDViolationDetector().detect(dirty, declared)
    on_dependents = sum(1 for issue in issues if issue.column in set(dependents))
    report = {
        "columns_absent_from_frame": missing,
        "declared_dependents_that_are_constant": constant,
        "fd_issues_raised_on_declared_dependents": on_dependents,
        "authoritative_columns": sorted(authoritative_columns(declared)),
        "declared_columns": len(declared.columns),
    }
    bound = not missing and not constant and on_dependents > 0
    return bound, report


def score_arm(
    fixes: list[Any],
    ground_truth: Any,
    dirty: pd.DataFrame,
    clean: pd.DataFrame,
    truth: dict[Any, str],
    counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Score one arm with BOTH the published scorer and the four-outcome tally.

    `score_repairs` gives the tp/fp/fn/F1 that is comparable to the anchor and to the stage
    artifact. The tally gives the outcome vocabulary the premise harnesses use, and crucially a
    `write_precision` of `None` rather than `0.0` on a zero-write arm -- reporting `0/0` as
    `0.0` describes a refusal as a wrong answer.
    """
    metrics = score_repairs(ground_truth, as_repairs(fixes))
    outcomes = tally_write_outcomes(fixes, dirty, clean, truth)
    return {
        "pipeline_counts": counts,
        "writes": len(fixes),
        "tp": metrics.tp,
        "fp": metrics.fp,
        "fn": metrics.fn,
        "precision": round(metrics.precision, 4),
        "recall": round(metrics.recall, 4),
        "f1": round(metrics.f1, 4),
        "write_precision": outcomes["write_precision"],
        "repaired_a_real_error": outcomes["repaired_a_real_error"],
        "wrong_value_on_a_real_error": outcomes["wrong_value_on_a_real_error"],
        "no_op_on_a_clean_cell": outcomes["no_op_on_a_clean_cell"],
        "corrupted_a_clean_cell": outcomes["corrupted_a_clean_cell"],
        "net_cells_improved": outcomes["net_cells_improved"],
        "by_detector": by_detector(fixes, dirty, clean, truth),
    }


def thin_to_one_determinant(
    fds: tuple[FunctionalDependency, ...],
) -> tuple[FunctionalDependency, ...]:
    """Keep the first dependency per dependent column, dropping the rest.

    Used by the mechanism probe below to strip a premise of REDUNDANCY while leaving its column
    coverage untouched.
    """
    seen: set[str] = set()
    kept: list[FunctionalDependency] = []
    for fd in fds:
        if fd.dependent not in seen:
            seen.add(fd.dependent)
            kept.append(fd)
    return tuple(kept)


def mechanism_probes(
    source: Path,
    declared_schema: Schema,
    oracle_fds: tuple[FunctionalDependency, ...],
    dirty: pd.DataFrame,
    ground_truth: Any,
    clean: pd.DataFrame,
    truth: dict[Any, str],
) -> dict[str, Any]:
    """Diagnostics, added by AMENDMENT 1 AFTER the arms above were measured.

    These are **not** hypothesis tests and were not pre-registered. They exist because the
    headline pair needs a mechanism to be actionable: the declared premise produces 399 candidate
    repairs and writes 0, while the oracle produces 397 -- FEWER -- and writes 54, and the
    declared premise's 13 dependents already include all 10 columns the oracle wrote to. So the
    zero is not explained by column coverage, and "the user declared too little" is not supported
    by the candidate counts.

    Two probes, in opposite directions, because a one-sided result could not separate the
    hypothesis from an accident of which columns happen to be involved:

    - ``oracle_thinned_to_one_determinant_per_dependent`` -- the REVERSE probe, and the load
      bearing one. A premise of 13 dependencies every one of which is admitted by ground truth,
      covering the same columns, with the redundancy removed. If this writes zero, then the
      declared arm's zero is **not** a consequence of the premise being hand-authored or
      imperfect, and cannot be fixed by declaring better.
    - ``declared_plus_oracle_redundancy_on_condition`` -- the FORWARD probe. Gives the declared
      premise the oracle's other two determinants for ``Condition``, the column the oracle
      repaired most. Tests whether per-column redundancy is *sufficient*.

    Reported as probes rather than arms, and labelled post hoc in the artifact, because a
    diagnostic promoted to a finding without a pre-registration is how a mechanism gets asserted
    instead of measured.
    """
    all_str = dict.fromkeys((str(column) for column in dirty.columns), "str")
    declared_pairs = {
        (tuple(fd.determinant), fd.dependent) for fd in declared_schema.functional_dependencies
    }
    condition_extra = tuple(
        fd
        for fd in oracle_fds
        if fd.dependent == "Condition"
        and (tuple(fd.determinant), fd.dependent) not in declared_pairs
    )
    thinned = thin_to_one_determinant(oracle_fds)

    probes: dict[str, Any] = {}
    for name, fds in (
        ("oracle_thinned_to_one_determinant_per_dependent", thinned),
        (
            "declared_plus_oracle_redundancy_on_condition",
            (*declared_schema.functional_dependencies, *condition_extra),
        ),
    ):
        fixes, counts = pipeline_fixes(
            source, repair_schema=Schema(columns=all_str, functional_dependencies=fds)
        )
        scored = score_arm(fixes, ground_truth, dirty, clean, truth, counts)
        per_dependent = Counter(fd.dependent for fd in fds)
        probes[name] = {
            "fd_count": len(fds),
            "max_determinants_per_dependent": max(per_dependent.values()) if per_dependent else 0,
            "dependent_columns": len(per_dependent),
            "writes": scored["writes"],
            "tp": scored["tp"],
            "f1": scored["f1"],
            "write_precision": scored["write_precision"],
            "candidate_repairs_proposed": counts["candidate_repairs_proposed"],
            "every_fd_holds_on_clean": all(fd_holds_on_clean(clean, fd) for fd in fds),
        }
    probes["added_by"] = (
        "AMENDMENT 1 of the pre-registration, AFTER the four arms were measured. Diagnostics, "
        "not hypothesis tests: they characterise the 0-versus-54 gap and do not explain it."
    )
    return probes


def main() -> int:
    """Measure hospital under declared, oracle and mined premises through the write path."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    # --- K3 first: an unfrozen premise voids the run before anything is measured. ---
    registered = registered_premise_sha256()
    observed = premise_sha256(PREMISE)
    if observed != registered:
        print(
            f"FAIL K3: {_display(PREMISE)} hashes {observed}, but the "
            f"pre-registration records {registered}. The premise is not the one that was frozen "
            "before ground truth was consulted, so the run is VOID. Do not edit the "
            "pre-registration to match: append an amendment explaining why the premise changed.",
            file=sys.stderr,
        )
        return 4
    print(f"  K3 premise frozen: {observed[:16]}... matches the pre-registration")

    dataset = load_real_world_dataset(CORPUS)
    ground_truth = dataset.ground_truth
    if not ground_truth:
        print(f"FAIL: {CORPUS} yielded 0 ground-truth cells; dataset misloaded.", file=sys.stderr)
        return 2
    dirty, clean = dataset.dirty_df, dataset.clean_df
    truth = _truth_by_cell(dirty, clean)

    declared_schema = load_schema(PREMISE)

    # --- K2: prove the declared premise binds, so a zero can be read as a refusal. ---
    bound, vacuity = check_vacuity(declared_schema, dirty)
    if not bound:
        print(
            f"FAIL K2: the declared premise does not bind on {CORPUS}: {vacuity}. "
            "An unbound premise and a refusing premise produce the same zero, so this run "
            "reports VACUOUS rather than a number.",
            file=sys.stderr,
        )
        return 5
    print(
        f"  K2 premise binds: {vacuity['fd_issues_raised_on_declared_dependents']:,} FD issues on "
        f"{len(vacuity['authoritative_columns'])} authoritative columns "
        f"of {vacuity['declared_columns']} declared"
    )

    # --- K1a: the instrument must reproduce the published proposal-stage anchor. ---
    published = anchor_f1()
    proposals, _detected = _repairs_from_proposed_fixes(dataset)
    proposal = score_arm(list(proposals), ground_truth, dirty, clean, truth)
    delta = abs(proposal["f1"] - published)
    k1a = delta <= K1_TOLERANCE
    print(f"  proposal stage : F1 {proposal['f1']} (published {published}, delta {delta:.6f})")
    if not k1a:
        print(
            f"FAIL K1a: this harness scores the published path at {proposal['f1']}, not "
            f"{published}. The instrument does not reproduce the number it builds on, so NOTHING "
            "may be reported from this run. Fix the harness first.",
            file=sys.stderr,
        )
        return 3

    with tempfile.TemporaryDirectory(prefix="dataforge-declared-") as raw_tmp:
        source = Path(raw_tmp) / f"{CORPUS}.csv"
        dirty.to_csv(source, index=False)

        # --- K1b: reproduce the committed mined-premise C4 arm exactly. ---
        artifact = build_constraint_review_artifact(
            infer_schema(dirty),
            source_path=source,
            source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        )
        artifact = update_constraint_review_artifact(
            artifact,
            accept_ids=[
                reviewed.candidate_id
                for reviewed in artifact.candidates
                if reviewed.candidate.kind == "functional_dependency"
            ],
        )
        mined_fixes, mined_counts = pipeline_fixes(source, constraints=artifact)
        mined_c4 = score_arm(mined_fixes, ground_truth, dirty, clean, truth, mined_counts)
        referent = stage_mined_c4()
        k1b = all(mined_c4[field] == referent[field] for field in ("writes", "tp", "fp"))
        print(
            f"  mined c4       : {mined_c4['writes']} writes / tp {mined_c4['tp']} / "
            f"fp {mined_c4['fp']} (committed {referent})"
        )
        if not k1b:
            print(
                f"FAIL K1b: this harness scores the mined C4 arm at "
                f"{ {f: mined_c4[f] for f in ('writes', 'tp', 'fp')} }, but the committed stage "
                f"artifact records {referent}. Two instruments disagree on the same "
                "configuration, so NOTHING may be reported. Fix the harness, or if the code "
                "moved, that is a finding to write up under anchor_truth -- not a number to edit.",
                file=sys.stderr,
            )
            return 3

        # --- The two arms that have never been measured. ---
        declared_fixes, declared_counts = pipeline_fixes(source, repair_schema=declared_schema)
        declared = score_arm(declared_fixes, ground_truth, dirty, clean, truth, declared_counts)
        oracle_fds = discover_oracle_fds(
            clean, columns=tuple(str(column) for column in dirty.columns)
        )
        oracle_fixes, oracle_counts = pipeline_fixes(
            source, repair_schema=schema_for(dirty, oracle_fds)
        )
        oracle = score_arm(oracle_fixes, ground_truth, dirty, clean, truth, oracle_counts)

        probes = mechanism_probes(
            source, declared_schema, oracle_fds, dirty, ground_truth, clean, truth
        )

    print(
        f"  DECLARED       : F1 {declared['f1']} | {declared['writes']} writes | "
        f"{declared['repaired_a_real_error']}R / {declared['corrupted_a_clean_cell']}C | "
        f"write_precision {declared['write_precision']}"
    )
    print(
        f"  ORACLE ceiling : F1 {oracle['f1']} | {oracle['writes']} writes | "
        f"{oracle['repaired_a_real_error']}R / {oracle['corrupted_a_clean_cell']}C | "
        f"write_precision {oracle['write_precision']}"
    )

    declared_audit = premise_audit(declared_schema.functional_dependencies, clean, PREMISE)
    oracle_audit = premise_audit(oracle_fds, clean, None)

    anchor_is_declared = declared["f1"] >= ANCHOR_MATERIALITY_F1
    verdict = {
        "k1a_instrument_reproduces_published_anchor": k1a,
        "k1b_instrument_reproduces_committed_mined_c4": k1b,
        "k2_declared_premise_binds": bound,
        "k3_premise_frozen_before_measurement": True,
        "p1_declared_writes_something": declared["writes"] > 0,
        "p2_declared_beats_both_mined_pipelines": declared["f1"] > 0.0039,
        "p3_declared_below_proposal_stage": declared["f1"] < proposal["f1"],
        "p4_declared_corrupts_at_least_one_clean_cell": declared["corrupted_a_clean_cell"] > 0,
        "p5_oracle_above_declared": oracle["f1"] > declared["f1"],
        "p6_oracle_below_proposal_stage": oracle["f1"] < proposal["f1"],
        "declared_f1_over_best_mined_pipeline_f1": (
            round(declared["f1"] / 0.0039, 1) if declared["f1"] else 0.0
        ),
        "proposal_f1_over_declared_f1": (
            round(proposal["f1"] / declared["f1"], 1) if declared["f1"] else None
        ),
        "proposal_f1_over_oracle_f1": (
            round(proposal["f1"] / oracle["f1"], 1) if oracle["f1"] else None
        ),
        "anchor_decision_declared_is_material": anchor_is_declared,
        "anchor_decision_rule": (
            "Pre-committed in the pre-registration before the result was known: if the declared "
            f"arm's F1 >= {ANCHOR_MATERIALITY_F1} it takes the capability anchor, being the only "
            "measured number both reachable through the shipped write path and premised on "
            "something a user authors, and 0.8352 is retained relabelled as proposal-stage. "
            "Otherwise the honest anchor is that there is no demonstrated end-to-end correction "
            "capability on this corpus, and the claim restates as detection plus advisory triage "
            "plus reversibility."
        ),
    }

    payload = {
        "schema_version": "dataforge_declared_premise_capability_v1",
        "preregistration": "eval/preregistration/declared_premise_capability.md",
        "dataset": CORPUS,
        "ground_truth_cells": len(ground_truth),
        "real_error_cells_dirty_vs_clean": len(truth),
        "premise_file": _display(PREMISE),
        "premise_sha256": observed,
        "premise_sha256_definition": (
            "sha256 of the file's text with line endings normalised to \\n, NOT of its bytes: "
            "core.autocrlf=true leaves CRLF in a Windows worktree while storing LF, so a byte "
            "hash would void the run in Linux CI for a reason unrelated to the premise"
        ),
        "scorer": (
            "dataforge.bench.core.score_repairs for tp/fp/fn/F1 (imported, not reimplemented), "
            "plus the four-outcome tally imported from "
            "scripts/bench/measure_premise_write_exposure.py"
        ),
        "method": (
            "Four arms over one table, one ground truth and two shared scorers. "
            "proposal_stage_anchor reproduces dataforge/bench/methods.py::"
            "_repairs_from_proposed_fixes (no verifier, no safety filter, no auto-apply gate). "
            "The three pipeline arms take run_repair_pipeline's would-apply set in dry_run mode "
            "and differ ONLY in their premise: a hand-declared schema loaded by the shipped "
            "load_schema, functional dependencies admitted by holding on the CLEAN frame, or "
            "dependencies mined from the dirty frame and accepted in review. Because the scorers "
            "are shared, a difference between arms can only be caused by the stage or the "
            "premise, never by scoring."
        ),
        "arms": {
            "proposal_stage_anchor": proposal,
            "pipeline_mined_c4": mined_c4,
            "pipeline_declared_premise": declared,
            "pipeline_oracle_premise": oracle,
        },
        "premise_audit": {
            "declared": declared_audit,
            "oracle": oracle_audit,
            "note": (
                "The oracle premise is admitted BY ground truth (fd_holds_on_clean as a filter), "
                "so its fd_set_precision is 1.0 by construction and means nothing. The declared "
                "premise was frozen by hash first and is only GRADED by ground truth, so its "
                "fd_set_precision is a real measurement of what a user's declaration costs."
            ),
        },
        "k2_vacuity_report": vacuity,
        "mechanism_probes": probes,
        "referents": {
            "published_anchor_f1": published,
            "published_anchor_source": (
                "eval/results/agent_comparison.json, gated against the code by "
                "scripts/ci/anchor_truth.py -- deliberately not a constant in the harness"
            ),
            "committed_mined_c4": referent,
            "committed_mined_c4_source": (
                "eval/results/capability_measurement_stage.json "
                "/arms/pipeline_c4_declared_authority"
            ),
        },
        "verdict": verdict,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
