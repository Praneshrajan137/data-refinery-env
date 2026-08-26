"""Measure every detector that bypasses the calibration gate, unconditionally.

Why this script exists
----------------------
``dataforge/domain/vocabulary.py``:181-184 states the rule the allowlist sets for itself:

    **This is an allowlist, and that is deliberate.** [...] A new detector is calibration-bound
    until it earns an entry here **with a committed measurement**.

A ``deterministic`` fix from a member of ``CONSTRAINT_CHECKABLE_DETECTORS`` bypasses the calibration
threshold entirely -- no threshold, no confidence, no labels, nothing downstream. Against that
standard, only ``fd_violation`` had a committed write measurement (2026-08-25,
``docs/trust/deductive-coverage-result.md``). ``missing_value`` had none. ``type_mismatch`` had none:
the 92/92 sometimes cited for it belongs to the **LLM corrector**, a different component, and was
formally retired by ``real-error-detection-result.md``:125-141.

``decimal_shift`` was *removed* from this allowlist on measurement -- precision 0.0000 on three
datasets, 263,428 false rewrites on an error-free table -- so the standard was being enforced on exit
and not on entry. This script supplies the entry evidence for all three members.

Pre-registered in ``eval/preregistration/bypass_allowlist_evidence.md`` with the method, the premise
arms and two kill criteria fixed before running.

What is measured, and why unconditionally
-----------------------------------------
The real detector produces the queue and the real repairer proposes on each flag; neither is
reimplemented. Every proposal is classified against retained ground truth into:

* ``repaired_a_real_error``    -- the cell was wrong and the proposal matches truth;
* ``wrong_value_on_a_real_error`` -- the cell was wrong and the proposal does not;
* ``corrupted_a_clean_cell``   -- **the cell was already correct** and the proposal changes it;
* ``no_op_on_a_clean_cell``    -- the cell was already correct and the proposal matches it.

Scoring only cells that were already errors reports how good repairs are on cells that needed one and
is silent on the failure that actually costs a user data. ``fd_violation`` reported write precision
1.0000 on hospital under that conditioning and corrupted 86 previously-correct cells when scored over
everything it touched.

Accounting is per **distinct cell**, never per detector flag: with a mined premise the same cell is
flagged once per dependency naming its column, which inflated one flag count to 48,599 against 10,064
distinct cells.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final, Protocol

import pandas as pd

# This script reuses the FD-discovery helpers from its sibling, so the repository root must be
# importable. Matches the pattern in ``scripts/measure_trust_ledger.py``.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dataforge.datasets.real_world import load_real_world_dataset  # noqa: E402
from dataforge.detectors.base import Issue, Schema  # noqa: E402
from dataforge.detectors.categorical_normalization import (  # noqa: E402
    CategoricalNormalizationDetector,
)
from dataforge.detectors.fd_violation import FDViolationDetector  # noqa: E402
from dataforge.detectors.format_violation import FormatViolationDetector  # noqa: E402
from dataforge.detectors.missing_value import MissingValueDetector  # noqa: E402
from dataforge.detectors.type_mismatch import TypeMismatchDetector  # noqa: E402
from dataforge.domain.vocabulary import CONSTRAINT_CHECKABLE_DETECTORS  # noqa: E402
from dataforge.repairers.base import ProposedFix  # noqa: E402
from dataforge.repairers.categorical_normalization import (  # noqa: E402
    CategoricalNormalizationRepairer,
)
from dataforge.repairers.fd_violation import FDViolationRepairer  # noqa: E402
from dataforge.repairers.format_violation import FormatViolationRepairer  # noqa: E402
from dataforge.repairers.missing_value import MissingValueRepairer  # noqa: E402
from dataforge.repairers.type_mismatch import TypeMismatchRepairer  # noqa: E402
from dataforge.table import TableLike  # noqa: E402
from scripts.bench.measure_deductive_coverage import (  # noqa: E402
    _fd_label,
    discover_oracle_fds,
    fd_holds_on_clean,
    mined_fds,
)


class _Detector(Protocol):
    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]: ...


class _Repairer(Protocol):
    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: None = None,
    ) -> ProposedFix | None: ...


#: Detectors this harness can score. A SUPERSET of the allowlist, deliberately: the two withheld
#: repairers are not allowlist members and the whole point of measuring them is to find out what
#: they would do if they were. Keyed on issue type, which is what ``detector_id`` carries.
#:
#: ``format_violation`` and ``categorical_normalization`` were added 2026-08-26. They are absent
#: from ``build_repairers``' deterministic registry, so nothing in the product can reach them --
#: this harness constructs them directly, which is the only way to measure a repairer that is
#: withheld. Pre-registered in ``eval/preregistration/withheld_repairer_coverage.md``.
MEASURABLE_DETECTORS: Final[tuple[str, ...]] = (
    "categorical_normalization",
    "fd_violation",
    "format_violation",
    "missing_value",
    "type_mismatch",
)

#: Detectors whose repairer reads no premise, so a premised arm would be identical by construction.
#: Reporting two arms for these would imply a premise sensitivity that does not exist:
#: ``format_violation`` accepts a ``schema`` and never reads it, and
#: ``categorical_normalization`` executes ``del retry_context, schema`` on its first line.
UNPREMISED_DETECTORS: Final[frozenset[str]] = frozenset(
    {"type_mismatch", "format_violation", "categorical_normalization"}
)


class _ProfileMemoRepairer:
    """``FormatViolationRepairer`` with its column profile computed once per column.

    Why this wrapper exists, dated 2026-08-26. ``FormatViolationRepairer._dominant_profile`` rescans
    the ENTIRE column and recomputes ``value_shape`` for every value, once **per flag**. Measured on
    tax (200,000 rows, 15 columns): the detector takes 5.7 seconds and emits 20,018 flags, and the
    repairer costs 632 ms per flag -- **211 minutes** to propose on one corpus. The cost is
    O(flags x rows), so it is invisible on the 1,000-row corpora and intractable on the one corpus
    that has ever caught a repairer this project removed.

    The pre-registration requires tax measured UNSAMPLED, because a head slice of tax is a biased
    view of a different population rather than a weaker view of this one. This memo is how that arm
    becomes reachable without violating it.

    **It cannot change a single proposal.** ``_dominant_profile(df, column)`` is a pure function of
    the dataframe and the column name, neither of which this harness mutates, so memoising it returns
    the identical tuple. Equivalence is not argued, it is verified: with the memo enabled, hospital
    still reports 99 writes / 99 corrupted and rayyan 109 / 109, asserted in
    ``tests/unit/test_withheld_repairer_harness.py``.

    Not fixed in the product deliberately. The repairer is unreachable -- absent from
    ``build_repairers``' registry -- and this measurement is what records it as permanently
    withheld. Optimising a component with no consumer is the error ``PRODUCT.md`` section 1.3 names:
    rigour there buys correctness of a report, not of the product.
    """

    def __init__(self) -> None:
        self._inner = FormatViolationRepairer()
        self._cache: dict[str, tuple[str | None, list[str]]] = {}

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: None = None,
    ) -> ProposedFix | None:
        """Delegate, with ``_dominant_profile`` served from a per-column cache."""
        original = FormatViolationRepairer.__dict__["_dominant_profile"]
        unwrapped = original.__func__
        cache = self._cache

        def _memoized(inner_df: TableLike, column: str) -> tuple[str | None, list[str]]:
            if column not in cache:
                cache[column] = unwrapped(inner_df, column)
            return cache[column]

        # Restored from ``__dict__`` and re-wrapped, not reassigned from the attribute. Accessing
        # ``FormatViolationRepairer._dominant_profile`` yields the underlying FUNCTION, so assigning
        # that back turns a staticmethod into an instance method and the next unmemoised call
        # receives ``self`` as its first argument. That bug was caught by the cell-for-cell
        # equivalence test and NOT by the test asserting the attribute was restored, which compared
        # function identity and passed while the descriptor was wrong.
        FormatViolationRepairer._dominant_profile = staticmethod(_memoized)  # type: ignore[method-assign]
        try:
            return self._inner.propose(issue, df, schema, retry_context)
        finally:
            FormatViolationRepairer._dominant_profile = original  # type: ignore[method-assign]


def _build(detector_id: str) -> tuple[_Detector, _Repairer]:
    """Return the shipped detector and repairer for a measurable detector id."""
    if detector_id == "fd_violation":
        return FDViolationDetector(), FDViolationRepairer(cache_dir=None, allow_llm=False)
    if detector_id == "missing_value":
        return MissingValueDetector(), MissingValueRepairer()
    if detector_id == "type_mismatch":
        return TypeMismatchDetector(), TypeMismatchRepairer()
    if detector_id == "format_violation":
        return FormatViolationDetector(), _ProfileMemoRepairer()
    if detector_id == "categorical_normalization":
        return CategoricalNormalizationDetector(), CategoricalNormalizationRepairer()
    raise ValueError(f"no harness wired for detector {detector_id!r}")


def _premise_arms(dataset: Any, detector_id: str) -> dict[str, Schema | None]:
    """Return the premise arms appropriate to each detector.

    Members of :data:`UNPREMISED_DETECTORS` get a **no-premise** arm only. That is their shipped
    configuration rather than a simplification: each either calls ``del schema`` or accepts the
    parameter and never reads it, so a premised arm would be identical by construction. For
    ``type_mismatch``, decision-table row 6 writes with no schema at all and row 7 shows a premise
    can only ever subtract writes, making the unpremised arm the widest one.

    The two FD-driven repairers get ``oracle`` and ``mined``. Neither proposes without a declared
    dependency, so a no-premise arm for them would measure nothing.
    """
    dirty, clean = dataset.dirty_df, dataset.clean_df
    columns = tuple(str(column) for column in dirty.columns)
    if detector_id in UNPREMISED_DETECTORS:
        return {"no_premise": None}
    return {
        "oracle": _schema(dirty, discover_oracle_fds(clean, columns=columns)),
        "mined": _schema(dirty, mined_fds(dirty)),
    }


def _schema(dirty: pd.DataFrame, fds: Any) -> Schema:
    """Every column typed ``str`` so the FD mechanism is isolated from type narrowing."""
    return Schema(
        columns=dict.fromkeys((str(column) for column in dirty.columns), "str"),
        functional_dependencies=tuple(fds),
    )


def classify_writes(
    dataset: Any,
    detector: _Detector,
    repairer: _Repairer,
    schema: Schema | None,
) -> dict[str, Any]:
    """Replay detector then repairer over a corpus and classify every proposal.

    Deliberately generic across detectors: the classification depends only on ground truth and the
    proposed value, so the same accounting applies to a dependency-derived fill, a majority vote and
    a sentinel erasure. That is what makes the three members comparable at all.
    """
    dirty, clean = dataset.dirty_df, dataset.clean_df
    truth_by_cell = {(c.row, c.column): c.clean_value for c in dataset.ground_truth}

    issues = detector.detect(dirty, schema)
    tally = {
        "repaired_a_real_error": 0,
        "wrong_value_on_a_real_error": 0,
        "corrupted_a_clean_cell": 0,
        "no_op_on_a_clean_cell": 0,
    }
    abstained = 0
    examples: dict[str, list[dict[str, Any]]] = {
        "corrupted_a_clean_cell": [],
        "wrong_value_on_a_real_error": [],
    }
    seen: set[tuple[int, str]] = set()

    for issue in issues:
        key = (issue.row, issue.column)
        if key in seen:
            continue
        seen.add(key)
        proposal = repairer.propose(issue, dirty, schema, None)
        if proposal is None:
            abstained += 1
            continue

        proposed = proposal.fix.new_value
        old_value = str(dirty.iat[issue.row, dirty.columns.get_loc(issue.column)])
        if key in truth_by_cell:
            bucket = (
                "repaired_a_real_error"
                if proposed == truth_by_cell[key]
                else "wrong_value_on_a_real_error"
            )
            truth = truth_by_cell[key]
        else:
            current = str(clean.iat[issue.row, clean.columns.get_loc(issue.column)])
            bucket = "no_op_on_a_clean_cell" if proposed == current else "corrupted_a_clean_cell"
            truth = current
        tally[bucket] += 1
        if bucket in examples and len(examples[bucket]) < 20:
            examples[bucket].append(
                {
                    "row": issue.row,
                    "column": issue.column,
                    "was": old_value,
                    "would_be_written": proposed,
                    "truth": truth,
                    "provenance": proposal.provenance,
                }
            )

    proposals = sum(tally.values())
    harmful = tally["wrong_value_on_a_real_error"] + tally["corrupted_a_clean_cell"]

    def _rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    return {
        "detector_flags": len(issues),
        "distinct_cells_flagged": len(seen),
        "abstained_on_flag": abstained,
        **tally,
        "proposals": proposals,
        "write_precision": _rate(tally["repaired_a_real_error"], proposals),
        "harmful_write_rate": _rate(harmful, proposals),
        "net_cells_improved": tally["repaired_a_real_error"] - harmful,
        "coverage_of_all_table_errors": _rate(
            tally["repaired_a_real_error"], len(dataset.ground_truth)
        ),
        "examples": examples,
    }


def measure(
    corpus: str, *, cache_root: Path | None, detectors: Sequence[str] | None = None
) -> dict[str, Any]:
    """Measure the requested detectors on one corpus, across their premise arms.

    ``detectors`` defaults to the allowlist, so every existing invocation is unchanged. Passing an
    explicit list is what lets a non-member be scored -- a repairer that is withheld cannot be
    reached through the product, so measuring it requires naming it.
    """
    dataset = load_real_world_dataset(corpus, cache_root=cache_root)
    selected = sorted(detectors) if detectors else sorted(CONSTRAINT_CHECKABLE_DETECTORS)
    results: dict[str, Any] = {}
    for detector_id in selected:
        detector, repairer = _build(detector_id)
        arms: dict[str, Any] = {}
        for arm_name, schema in _premise_arms(dataset, detector_id).items():
            arms[arm_name] = {
                "premise": _describe_premise(dataset, schema),
                "in_allowlist": detector_id in CONSTRAINT_CHECKABLE_DETECTORS,
                **classify_writes(dataset, detector, repairer, schema),
            }
        results[detector_id] = arms

    return {
        "schema": "dataforge_bypass_allowlist_evidence_v1",
        "corpus": corpus,
        "rows": int(dataset.dirty_df.shape[0]),
        "real_errors_in_table": len(dataset.ground_truth),
        "dirty_sha256": dataset.dirty_sha256,
        "clean_sha256": dataset.clean_sha256,
        "note": (
            "Unconditional write exposure for every detector whose deterministic fixes bypass the "
            "calibration threshold. Per distinct cell, not per flag. Pre-registered in "
            "eval/preregistration/bypass_allowlist_evidence.md."
        ),
        "detectors": results,
    }


def _describe_premise(dataset: Any, schema: Schema | None) -> dict[str, Any]:
    """Record what the premise was, including how much of it is actually true."""
    if schema is None:
        return {"kind": "none", "fd_count": 0, "note": "no schema supplied; the shipped default"}
    true_fds = [
        fd for fd in schema.functional_dependencies if fd_holds_on_clean(dataset.clean_df, fd)
    ]
    return {
        "kind": "declared_all_str_plus_fds",
        "fd_count": len(schema.functional_dependencies),
        "fd_count_holding_on_clean": len(true_fds),
        "fd_set_precision": (
            round(len(true_fds) / len(schema.functional_dependencies), 4)
            if schema.functional_dependencies
            else None
        ),
        "functional_dependencies": [_fd_label(fd) for fd in schema.functional_dependencies][:20],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default="hospital")
    parser.add_argument("--cache-root", type=Path, default=None)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument(
        "--detector",
        action="append",
        choices=MEASURABLE_DETECTORS,
        help=(
            "score this detector instead of the allowlist; repeatable. Named explicitly because a "
            "withheld repairer is not reachable through the product and must be asked for."
        ),
    )
    args = parser.parse_args()

    payload = measure(args.corpus, cache_root=args.cache_root, detectors=args.detector)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(
        f"corpus {payload['corpus']}  rows {payload['rows']}  "
        f"real errors {payload['real_errors_in_table']}"
    )
    header = (
        f"  {'detector':<26}{'arm':<11}{'flagged':>8}{'writes':>8}{'repaired':>10}"
        f"{'WRONG':>7}{'CORRUPT':>9}{'no-op':>7}{'precision':>11}{'net':>7}"
    )
    print(header)
    for detector_id, arms in payload["detectors"].items():
        for arm_name, stats in arms.items():
            print(
                f"  {detector_id:<26}{arm_name:<11}{stats['distinct_cells_flagged']:>8}"
                f"{stats['proposals']:>8}{stats['repaired_a_real_error']:>10}"
                f"{stats['wrong_value_on_a_real_error']:>7}"
                f"{stats['corrupted_a_clean_cell']:>9}{stats['no_op_on_a_clean_cell']:>7}"
                f"{str(stats['write_precision']):>11}{stats['net_cells_improved']:>7}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
