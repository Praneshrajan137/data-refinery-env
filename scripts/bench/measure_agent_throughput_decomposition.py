"""Decompose WHY an external/agent proposal is refused, bucket by bucket.

`docs/STRATEGY.md` recorded that a frontier agent proposer through the gate yields "0 fixes
pass -- every FIX rejected by SMT+safety", and that sentence has been read as evidence that
the verification layer refuses agent work. Reading the code, that attribution is very likely
wrong in a way that matters: `dataforge/agent/executor.py` evaluates the safety constitution
and RETURNS before the SMT verifier is called, and the rule that fires --
`NO_UNCONFIRMED_LLM_WRITE` -- inspects `provenance` alone. It never looks at the value, the
premise, or any constraint. The reproduction command in
`eval/results/agent_gpt56sol_hospital.json` omits `--confirm-escalations`, so every proposal
escalates before reaching a verifier.

This measures the mechanism rather than a model. An LLM arm would confound "the gate refused"
with "the model proposed badly"; a deterministic external fix isolates the gate. It runs the
shipped `verify_and_apply` -- not a local reimplementation of the write loop -- because a
shorter local loop is evidence against a measurement in this repo, not for it.

Two independent conditions must BOTH hold before an external fix is written, and the 2x2
separates them:

  * `confirm_escalations` clears `NO_UNCONFIRMED_LLM_WRITE` (a policy rule on the fix's origin)
  * a discriminating declared premise makes the fix `proven` rather than `plausibility_only`

Arm 5 is the non-vacuity control. Without it this script could only show that the gate can be
opened, never that it still closes: it proposes a value that VIOLATES the declared premise
under the most permissive settings and requires a refusal.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dataforge import load_schema
from dataforge.engine.repair import (
    ExternalFix,
    VerifyAndApplyRequest,
    verify_and_apply,
)

REPO = Path(__file__).resolve().parents[2]
FIXTURE_CSV = REPO / "dataforge" / "fixtures" / "premised_fd_10rows.csv"
FIXTURE_SCHEMA = REPO / "dataforge" / "fixtures" / "premised_fd_10rows.schema.yaml"

# The fixture's one repairable cell: row index 9 holds "bostonn" where the declared
# functional dependency `state -> city` determines "boston".
TARGET_ROW = 9
TARGET_COLUMN = "city"
CORRECT_VALUE = "boston"
DIRTY_VALUE = "bostonn"

# A value that satisfies no declared constraint: it disagrees with every other row in the
# `state=MA` group, so a premise that actually constrains must refuse it.
VIOLATING_VALUE = "worcester"


@dataclass(frozen=True)
class Arm:
    name: str
    premise: bool
    confirm: bool
    new_value: str
    expectation: str
    why: str


ARMS: tuple[Arm, ...] = (
    Arm(
        name="no_premise_unconfirmed",
        premise=False,
        confirm=False,
        new_value=CORRECT_VALUE,
        expectation="safety_escalation",
        why=(
            "The configuration the published agent arm actually ran. If this refuses at "
            "safety_escalation then the measurement attributed to 'SMT+safety' never reached "
            "a verifier, and the cited number describes a policy default rather than the "
            "verification layer."
        ),
    ),
    Arm(
        name="no_premise_confirmed",
        premise=False,
        confirm=True,
        new_value=CORRECT_VALUE,
        expectation="floor_cannot_verify",
        why=(
            "Clearing the escalation is not sufficient. With no premise the fix is "
            "plausibility_only and is held -- 'no declared premise, no write' survives "
            "confirmation, which is the invariant this arm exists to protect."
        ),
    ),
    Arm(
        name="premise_unconfirmed",
        premise=True,
        confirm=False,
        new_value=CORRECT_VALUE,
        expectation="safety_escalation",
        why=(
            "A premise is not sufficient either. Ordering matters: the escalation is checked "
            "BEFORE the prove gate, so a perfectly provable fix is still refused on its "
            "origin label alone."
        ),
    ),
    Arm(
        name="premise_confirmed",
        premise=True,
        confirm=True,
        new_value=CORRECT_VALUE,
        expectation="applied",
        why=(
            "Both conditions met. If this writes, agent throughput is not zero and never was "
            "architecturally zero -- the published figure measured a default, and the fix is "
            "a flag decision rather than a redesign of the gate."
        ),
    ),
    Arm(
        name="premise_confirmed_violating_value",
        premise=True,
        confirm=True,
        new_value=VIOLATING_VALUE,
        expectation="verifier_rejected",
        why=(
            "NON-VACUITY CONTROL. Same permissive settings as the arm above, but the proposed "
            "value contradicts the declared dependency. A refusal here is what makes the "
            "previous arm's write meaningful; if this also wrote, the gate would be open "
            "rather than working. The expectation names the exact bucket rather than a vague "
            "'refused', because a control that accepts any refusal cannot distinguish the "
            "prove gate doing its job from an unrelated rule firing first."
        ),
    ),
)


def _run_arm(arm: Arm, workdir: Path) -> dict[str, Any]:
    """Run one arm on a private copy of the fixture and report the refusal bucket."""
    source = workdir / f"{arm.name}.csv"
    shutil.copy2(FIXTURE_CSV, source)
    before = source.read_bytes()

    result = verify_and_apply(
        VerifyAndApplyRequest(
            source_path=source,
            fixes=[
                ExternalFix(
                    row=TARGET_ROW,
                    column=TARGET_COLUMN,
                    new_value=arm.new_value,
                    expected_old_value=DIRTY_VALUE,
                )
            ],
            mode="apply",
            schema=load_schema(FIXTURE_SCHEMA) if arm.premise else None,
            proposer="throughput-decomposition-probe",
            confirm_escalations=arm.confirm,
        )
    )

    receipt = result.receipt
    after = source.read_bytes()
    reasons = Counter(
        str(getattr(fix, "review_reason", "unknown")) for fix in (receipt.suggested_fixes or [])
    )

    if receipt.applied:
        observed = "applied"
    elif reasons:
        observed = reasons.most_common(1)[0][0]
    else:
        observed = "no_proposal_survived"

    return {
        "arm": arm.name,
        "premise_declared": arm.premise,
        "confirm_escalations": arm.confirm,
        "proposed_value": arm.new_value,
        "expectation": arm.expectation,
        "why": arm.why,
        "observed": observed,
        "applied": bool(receipt.applied),
        "bytes_changed": before != after,
        "fixes_written": len(receipt.applied_fixes or []),
        "review_reasons": dict(reasons),
        "txn_id": receipt.txn_id,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True, help="where to write the JSON")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="dataforge-throughput-") as raw:
        workdir = Path(raw)
        arms = [_run_arm(arm, workdir) for arm in ARMS]

    applied_arms = [arm for arm in arms if arm["applied"]]
    escalation_arms = [arm for arm in arms if arm["observed"] == "safety_escalation"]

    payload = {
        "measurement": "agent_throughput_decomposition",
        "corpus": "premised_fd_10rows",
        "note": (
            "Deterministic external proposals through the shipped verify_and_apply. Measures "
            "the gate, not a model: an LLM arm cannot separate a refusal by the gate from a "
            "bad proposal by the model."
        ),
        "arms": arms,
        "summary": {
            "arms_that_wrote": len(applied_arms),
            "arms_refused_on_origin_label_before_verification": len(escalation_arms),
            "escalation_is_reachable_without_a_premise": any(
                arm["observed"] == "safety_escalation" and not arm["premise_declared"]
                for arm in arms
            ),
            "premise_alone_is_insufficient": any(
                arm["observed"] == "safety_escalation" and arm["premise_declared"] for arm in arms
            ),
            "confirmation_alone_is_insufficient": any(
                arm["observed"] == "floor_cannot_verify" and arm["confirm_escalations"]
                for arm in arms
            ),
            "violating_value_refused_under_permissive_settings": all(
                not arm["applied"]
                for arm in arms
                if arm["arm"] == "premise_confirmed_violating_value"
            ),
        },
    }

    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"wrote {args.artifact}")
    for arm in arms:
        verdict = "MATCH" if arm["observed"] == arm["expectation"] else "DIVERGED"
        print(
            f"  {verdict:9s} {arm['arm']:38s} expected={arm['expectation']:22s} observed={arm['observed']}"
        )

    mismatches = [arm for arm in arms if arm["observed"] != arm["expectation"]]
    if mismatches:
        print(
            f"\n{len(mismatches)} arm(s) diverged from expectation -- the hypothesis is wrong somewhere."
        )
        return 1
    print("\nAll arms matched their pre-stated expectation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
