"""Verified autonomous agent controller for DataForge.

This is the production entry point that makes DataForge *truly agentic* without
weakening its moat. The control flow is:

1. **Deterministic-first seed.** Run detectors and the deterministic repairers
   (the high-accuracy floor). Their fixes are already safety+SMT verified by
   :func:`dataforge.engine.repair.propose_repairs`.
2. **Closed agent loop over the residual.** For issues the rules could not fix,
   an autonomous policy proposes actions. Every ``FIX`` is gated by the same
   safety constitution and SMT verifier; rejections are fed back so the policy
   self-corrects.
3. **Proven-only gate.** An agent value is LLM-derived, so it is *proven* only when
   an authoritative schema verified it. Otherwise it was checked solely by the
   advisory inferred guard -- where the known verifier-floor gaps live -- so it is
   ``plausibility_only`` and is HELD in ``held_fixes`` rather than written.
4. **Single verified commit.** Floor and proven agent fixes commit through the
   existing :func:`dataforge.engine.repair.apply_transaction` -- the same atomic,
   journaled, byte-for-byte reversible write path the CLI already uses.

Because the agent only *adds* proven fixes on top of the deterministic floor, its
output can never be worse than the deterministic baseline.

**What is and is not guaranteed.** No unproven value reaches disk unless the caller
sets ``allow_unproven_autoapply``, and that choice is recorded truthfully as
not-proven in the certificate. This holds for any policy, however weak or
adversarial, because the gate is enforced inside ``apply_transaction`` itself rather
than by this controller remembering to call it.

That wording is deliberately narrower than what this docstring claimed until
2026-08-09. It previously said "nothing unverified ever reaches disk -- regardless of
how weak or adversarial the policy is", which was FALSE: this controller called
``apply_transaction`` directly, skipping the proven-only partition, and a schema-less
``llm_live`` value was written after clearing only a structural check (row in bounds,
column exists). See DECISIONS.md 2026-08-09 and
``tests/property/test_no_corruption_invariant.py``.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from dataforge.agent.executor import VerifiedActionExecutor
from dataforge.agent.policy import AgentObservation, Policy, ResidualIssue, make_policy
from dataforge.agent.scratchpad import Scratchpad
from dataforge.detectors import run_all_detectors
from dataforge.detectors.base import Issue, Schema
from dataforge.engine.repair import (
    CandidateFix,
    RepairMode,
    RepairReceipt,
    VerifiedFix,
    apply_transaction,
    authoritative_columns,
    propose_repairs,
    strength_for_fix,
    verification_strength_for,
)
from dataforge.repairers.base import ProposedFix, RepairAttempt
from dataforge.safety import SafetyContext, SafetyFilter, SafetyVerdict
from dataforge.table import (
    cell_value,
    column_names,
    copy_table,
    read_csv,
    row_count,
    set_cell_value,
)
from dataforge.transactions.log import sha256_bytes, sha256_file

__all__ = [
    "AgentActionRecord",
    "AgentRepairRequest",
    "AgentRepairResult",
    "run_agent_repair",
]

_SAMPLE_WINDOW = 3


class AgentRepairRequest(BaseModel):
    """Input contract for the verified agent repair controller."""

    source_path: Path
    mode: RepairMode = "dry_run"
    repair_schema: Schema | None = Field(default=None, alias="schema")
    policy: str = "hosted"
    provider: str | None = None
    max_steps: int = Field(default=30, ge=1, le=200)
    model: str | None = None
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    allow_pii: bool = False
    confirm_pii: bool = False
    confirm_escalations: bool = False
    # Off by default: the proven-only invariant. Without an authoritative schema an
    # agent (LLM) value is ``plausibility_only`` and is HELD, not written. Setting
    # this accepts unproven writes; they stay reversible and are recorded truthfully
    # as not-proven in the certificate.
    allow_unproven_autoapply: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", populate_by_name=True)


class AgentActionRecord(BaseModel):
    """One step in the agent's audit trace."""

    step: int = Field(ge=1)
    action_type: str
    accepted: bool | None = None
    detail: str

    model_config = ConfigDict(frozen=True)


class AgentRepairResult(BaseModel):
    """Output contract for a verified agent repair run."""

    mode: RepairMode
    applied: bool
    reversible: bool = True
    source_path: str
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    post_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    txn_id: str | None = None
    revert_command: str | None = None
    policy_name: str
    steps_used: int = Field(ge=0)
    max_steps: int = Field(ge=1)
    floor_fix_count: int = Field(ge=0)
    agent_fix_count: int = Field(ge=0)
    fixes_count: int = Field(ge=0)
    residual_count: int = Field(ge=0)
    issues_count: int = Field(ge=0)
    safety_verdict: str
    fixes: list[VerifiedFix] = Field(default_factory=list)
    held_fixes: list[VerifiedFix] = Field(default_factory=list)
    trace: list[AgentActionRecord] = Field(default_factory=list)
    reason: str
    authoritative_schema_present: bool = False
    # The columns the authoritative schema actually constrains. Authority is per-column:
    # a schema that declares one column's type grants no authority over any other.
    covered_columns: tuple[str, ...] = ()

    model_config = ConfigDict(frozen=True)

    def to_receipt(self) -> RepairReceipt:
        """Project this agent result into the canonical ``repair_receipt_v1``.

        This lets the agent surface emit the SAME trust certificate that BOTH
        ``dataforge.certificate.verify_certificate`` and the deep
        ``reverify_certificate`` check for the deterministic pipeline. Each applied
        fix carries a truthful ``verification_strength`` (``proven`` when
        deterministic or backed by an authoritative schema, else
        ``plausibility_only``), derived by the same engine helper the pipeline
        uses, so the deep re-verification's truthfulness check is meaningful. A
        deterministic-only applied run verifies as proven; an applied run that
        includes an LLM write still verifies structurally but honestly reports
        ``auto_apply_is_proven_deterministic`` as False. The agent already writes
        through the shared ``apply_transaction`` path, so the receipt describes the
        same journaled, reversible transaction.
        """
        applied_fixes = (
            [
                fix.model_copy(
                    update={
                        "verification_strength": verification_strength_for(
                            fix.provenance,
                            # Per-COLUMN authority, not table-level.
                            authoritative_schema_present=fix.column in self.covered_columns,
                        )
                    }
                )
                for fix in self.fixes
            ]
            if self.applied
            else []
        )
        provenance = sorted({fix.provenance for fix in self.fixes})
        verifier_verdict = "accept" if (self.applied and self.fixes) else "not_run"
        return RepairReceipt(
            mode=self.mode,
            applied=self.applied,
            reversible=self.reversible,
            source_path=self.source_path,
            source_sha256=self.source_sha256,
            post_sha256=self.post_sha256,
            txn_id=self.txn_id,
            safety_verdict=self.safety_verdict,
            verifier_verdict=verifier_verdict,
            candidate_provenance=provenance,
            applied_fixes=applied_fixes,
            revert_command=self.revert_command,
            issues_count=self.issues_count,
            fixes_count=self.fixes_count,
            reason=self.reason,
        )


def _residual_issue(issue: Issue) -> ResidualIssue:
    """Project a detector Issue into the policy-facing residual record."""
    return ResidualIssue(
        row=issue.row,
        column=issue.column,
        issue_type=issue.issue_type,
        severity=issue.severity.value,
        expected=issue.expected,
        actual=issue.actual,
        reason=issue.reason,
    )


def _residual_issues(issues: list[Issue], attempt_groups: list[list[RepairAttempt]]) -> list[Issue]:
    """Return issues the deterministic floor did not accept a fix for."""
    residual: list[Issue] = []
    for issue, attempts in zip(issues, attempt_groups, strict=False):
        if not attempts or attempts[-1].status != "accepted":
            residual.append(issue)
    return residual


def _sample_rows(df: object, focus_row: int | None) -> tuple[dict[str, str], ...]:
    """Return a small window of rows around a focus row for the observation."""
    total = row_count(df)  # type: ignore[arg-type]
    if total == 0:
        return ()
    columns = column_names(df)  # type: ignore[arg-type]
    if focus_row is None:
        start, end = 0, min(total, _SAMPLE_WINDOW)
    else:
        start = max(0, focus_row - _SAMPLE_WINDOW)
        end = min(total, focus_row + _SAMPLE_WINDOW + 1)
    return tuple(
        {c: cell_value(df, i, c) for c in columns}  # type: ignore[arg-type]
        for i in range(start, end)
    )


def _build_observation(
    df: object,
    residual: dict[tuple[int, str], ResidualIssue],
    scratchpad: Scratchpad,
    last_result: str,
    steps_taken: int,
    max_steps: int,
    staged_count: int,
) -> AgentObservation:
    """Assemble the per-turn observation handed to the policy."""
    residual_list = tuple(residual.values())
    focus = residual_list[0].row if residual_list else None
    return AgentObservation(
        columns=tuple(column_names(df)),  # type: ignore[arg-type]
        row_count=row_count(df),  # type: ignore[arg-type]
        residual_issues=residual_list,
        sample_rows=_sample_rows(df, focus),
        scratchpad_summary=scratchpad.summary(),
        last_result=last_result,
        steps_taken=steps_taken,
        max_steps=max_steps,
        staged_fix_count=staged_count,
    )


def _verified_fix_payload(fix: ProposedFix, reason: str) -> VerifiedFix:
    """Build the public verified-fix payload from an accepted proposal."""
    return VerifiedFix(
        **CandidateFix.from_proposed(fix).model_dump(),
        verifier_reason=reason,
    )


def run_agent_repair(
    request: AgentRepairRequest,
    *,
    policy: Policy | None = None,
) -> AgentRepairResult:
    """Run the verified autonomous agent repair pipeline.

    Args:
        request: The repair request contract.
        policy: Optional pre-built policy (used by tests and callers that hold a
            backend). When omitted, a policy is constructed from
            ``request.policy`` with graceful fallback to deterministic.

    Returns:
        A frozen :class:`AgentRepairResult` describing what was verified,
        committed, and reverted-able.
    """
    source_path = request.source_path.resolve()
    source_bytes = source_path.read_bytes()
    source_sha256 = sha256_bytes(source_bytes)
    schema = request.repair_schema

    df = read_csv(source_path)
    issues = run_all_detectors(df, schema)

    # 1. Deterministic floor (already safety+SMT verified, no LLM).
    floor_fixes, attempt_groups = propose_repairs(
        issues,
        source_path,
        copy_table(df),
        schema,
        allow_llm=False,
        model=request.model,
        allow_pii=request.allow_pii,
        confirm_pii=request.confirm_pii,
        confirm_escalations=request.confirm_escalations,
        interactive=False,
    )

    # Rebuild the post-floor working table the agent reasons and verifies against.
    working_df = copy_table(df)
    for fix in floor_fixes:
        set_cell_value(working_df, fix.fix.row, fix.fix.column, fix.fix.new_value)

    residual_issues = _residual_issues(issues, attempt_groups)
    residual: dict[tuple[int, str], ResidualIssue] = {
        (issue.row, issue.column): _residual_issue(issue) for issue in residual_issues
    }

    safety_context = SafetyContext(
        allow_pii=request.allow_pii,
        confirm_pii=request.confirm_pii,
        confirm_escalations=request.confirm_escalations,
    )
    scratchpad = Scratchpad()

    active_policy = policy or make_policy(
        request.policy,
        model=request.model,
        temperature=request.temperature,
        provider=request.provider,
    )

    executor = VerifiedActionExecutor(
        working_df,
        schema,
        safety_context=safety_context,
        scratchpad=scratchpad,
        provenance=active_policy.provenance,
    )
    # Prevent the agent from re-touching cells the floor already fixed.
    for fix in floor_fixes:
        executor.mark_resolved(fix.fix.row, fix.fix.column)

    trace: list[AgentActionRecord] = []
    last_result = ""
    steps_used = 0

    initial_obs = _build_observation(
        working_df, residual, scratchpad, last_result, 0, request.max_steps, 0
    )
    active_policy.reset(initial_obs)

    # 2. Closed agent loop over the residual, with verified writes.
    for step in range(1, request.max_steps + 1):
        if not residual:
            break
        observation = _build_observation(
            working_df,
            residual,
            scratchpad,
            last_result,
            step - 1,
            request.max_steps,
            len(executor.staged_fixes),
        )
        action = active_policy.propose_action(observation)
        if action is None:
            break

        outcome = executor.execute(action)
        steps_used = step
        last_result = outcome.feedback
        trace.append(
            AgentActionRecord(
                step=step,
                action_type=outcome.action_type,
                accepted=outcome.accepted,
                detail=outcome.feedback[:500],
            )
        )
        if outcome.resolved_cell is not None:
            residual.pop(outcome.resolved_cell, None)

    agent_fixes = executor.staged_fixes
    all_fixes = [*floor_fixes, *agent_fixes]

    # 3. Proven-only gate (the invariant DECISIONS.md 2026-07-11 declared for every
    #    policy). An agent value is untrusted provenance, so without an authoritative
    #    schema it is ``plausibility_only`` -- checked only by the advisory inferred
    #    guard, where the known verifier-floor gaps live -- and is HELD, not written.
    #    This controller bypassed the gate from 2026-07-11 until 2026-08-09.
    #
    #    Only the STRENGTH dimension is enforced here, not the calibration-confidence
    #    dimension that ``partition_auto_apply`` also applies. Those are separable: a
    #    proven fix is correct by construction or schema-verified, whereas the
    #    per-class calibrated thresholds are a disabled-by-default product policy
    #    (every committed threshold is the 1.01 sentinel). Imposing them here would
    #    make agent mode apply nothing even WITH a schema, which is a different and
    #    much larger change than closing the soundness hole. Live-LLM agent writes
    #    remain separately gated by ``NO_UNCONFIRMED_LLM_WRITE``.
    covered_columns = authoritative_columns(schema)

    def _is_held(fix: ProposedFix) -> bool:
        if request.allow_unproven_autoapply:
            return False
        return strength_for_fix(fix, covered_columns) == "plausibility_only"

    # Partition by fix IDENTITY, not by (row, column). An earlier version of this gate
    # filtered by cell, which would have dropped a PROVEN floor fix whenever an unproven
    # agent fix touched the same cell. That cannot happen today -- the controller calls
    # ``executor.mark_resolved`` for every floor fix and the executor refuses a FIX on an
    # already-resolved cell -- but that is safety two invariants away from here, and a
    # change to the dedup logic would have broken it silently.
    # ``test_held_partition_is_by_identity_not_by_cell`` pins the property.
    held_fixes = [fix for fix in all_fixes if _is_held(fix)]
    all_fixes = [fix for fix in all_fixes if not _is_held(fix)]
    floor_fixes = [fix for fix in floor_fixes if not _is_held(fix)]
    agent_fixes = [fix for fix in agent_fixes if not _is_held(fix)]

    # 4. Batch safety gate (mirrors the deterministic pipeline: any non-ALLOW
    #    verdict voids the batch rather than shipping an inconsistent set).
    batch_safety = SafetyFilter().evaluate_batch(
        all_fixes, SafetyContext(confirm_escalations=request.confirm_escalations)
    )
    if batch_safety.verdict != SafetyVerdict.ALLOW:
        all_fixes = []
        agent_fixes = []
        floor_fixes = []

    applied = False
    txn_id: str | None = None
    post_sha256: str | None = None
    reason = "No accepted fixes were produced."
    held_note = (
        f" {len(held_fixes)} fix(es) were held as unproven (no authoritative schema); "
        "pass allow_unproven_autoapply to accept them."
        if held_fixes
        else ""
    )

    if batch_safety.verdict != SafetyVerdict.ALLOW:
        reason = batch_safety.reason
    elif request.mode == "apply" and all_fixes:
        txn_id = apply_transaction(
            source_path,
            all_fixes,
            source_bytes,
            covered_columns=authoritative_columns(schema),
            allow_unproven_autoapply=request.allow_unproven_autoapply,
        )
        post_sha256 = sha256_file(source_path)
        applied = True
        reason = (
            f"Applied {len(all_fixes)} verified fix(es) "
            f"({len(floor_fixes)} deterministic, {len(agent_fixes)} agent).{held_note}"
        )
    elif all_fixes:
        reason = (
            f"Dry run produced {len(all_fixes)} verified fix(es) "
            f"({len(floor_fixes)} deterministic, {len(agent_fixes)} agent); "
            f"no source data was mutated.{held_note}"
        )
    elif held_fixes:
        reason = (
            f"No fix was auto-applied: {len(held_fixes)} fix(es) are unproven "
            "(see held_fixes)." + held_note
        )

    fix_payloads = [
        _verified_fix_payload(fix, "Accepted by safety and SMT verifier.") for fix in all_fixes
    ]
    held_payloads = [
        _verified_fix_payload(fix, "Held: not proven against an authoritative schema.")
        for fix in held_fixes
    ]

    return AgentRepairResult(
        mode=request.mode,
        applied=applied,
        source_path=str(source_path),
        source_sha256=source_sha256,
        post_sha256=post_sha256,
        txn_id=txn_id,
        revert_command=f"dataforge revert {txn_id}" if txn_id is not None else None,
        policy_name=active_policy.name,
        steps_used=steps_used,
        max_steps=request.max_steps,
        floor_fix_count=len(floor_fixes),
        agent_fix_count=len(agent_fixes),
        fixes_count=len(all_fixes),
        residual_count=len(residual),
        issues_count=len(issues),
        safety_verdict=batch_safety.verdict.value,
        fixes=fix_payloads,
        held_fixes=held_payloads,
        trace=trace,
        reason=reason,
        authoritative_schema_present=schema is not None,
        covered_columns=tuple(sorted(covered_columns)),
    )
