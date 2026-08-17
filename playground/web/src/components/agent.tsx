/** Agent and guardrail surfaces: untrusted proposals and what the verifier did with them. */
import { EvidenceNote, Metric, VerificationStrengthBadge } from "../components/primitives";
import { motionSprings } from "../motion";
import { strengthOf } from "../observatory";
import type { GuardrailVerdict } from "../observatory";
import type { AgentSummary, VerifiedFix } from "../types";
import { ShieldCheck } from "lucide-react";
import { motion } from "motion/react";

export function agentTraceMotion(step: { action_type: string; accepted?: boolean | null }): string {
  const action = step.action_type.toUpperCase();
  if (action === "FIX") {
    return step.accepted === false ? "rejected" : "proven";
  }
  if (["FINALIZE", "DONE", "STOP", "FINISH", "COMPLETE"].includes(action)) {
    return "done";
  }
  if (["INSPECT_ROWS", "PATTERN_MATCH", "STAT_TEST", "HYPOTHESIS"].includes(action)) {
    return "verifying";
  }
  return "proposing";
}

export function AgentSummaryPanel({ agent }: { agent: AgentSummary }) {
  return (
    <motion.section
      className="agent-summary"
      aria-label="Verified agent run"
      data-agent-motion="done"
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <header className="agent-summary__head">
        <div>
          <p className="eyebrow">Verified agent</p>
          <h3>{agent.policy_name}</h3>
        </div>
        <div className="agent-summary__metrics" role="group" aria-label="Agent run metrics">
          <Metric label="Steps" value={`${agent.steps_used}/${agent.max_steps}`} />
          <Metric label="Floor fixes" value={String(agent.floor_fix_count)} />
          <Metric label="Agent fixes" value={String(agent.agent_fix_count)} />
          <Metric label="Residual" value={String(agent.residual_count)} />
        </div>
      </header>
      <p className="agent-summary__reason">{agent.reason}</p>
      <p className="agent-summary__note">
        Agent proposals come from the {agent.policy_name} proposer and are each safety- and
        SMT-verified before display. Nothing is applied; this is a dry run, and a stronger
        proposer does not bypass the gate.
      </p>

      {agent.trace.length > 0 ? (
        <ol className="agent-trace" aria-label="Agent action trace">
          {agent.trace.map((step) => (
            <li
              key={step.step}
              className="agent-trace__step"
              data-agent-motion={agentTraceMotion(step)}
            >
              <span className="agent-trace__index">{step.step}</span>
              <span className="agent-trace__action">{step.action_type}</span>
              {step.accepted === true ? <span className="agent-trace__verdict agent-trace__verdict--ok">verified</span> : null}
              {step.accepted === false ? <span className="agent-trace__verdict agent-trace__verdict--rejected">rejected</span> : null}
              <span className="agent-trace__detail">{step.detail}</span>
            </li>
          ))}
        </ol>
      ) : null}

      {agent.agent_fixes.length > 0 ? (
        <div className="agent-summary__fixes">
          <h4>Agent-proposed verified fixes</h4>
          <ul>
            {agent.agent_fixes.map((fix) => (
              <li key={`${fix.row}:${fix.column}`}>
                <span className="agent-fix__cell">
                  row {fix.row} · {fix.column}
                </span>
                <span className="agent-fix__change">
                  {fix.old_value || "∅"} → {fix.new_value}
                </span>
                <VerificationStrengthBadge strength={strengthOf(fix)} />
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </motion.section>
  );
}

export function GuardrailVerdictPanel({ verdict }: { verdict: GuardrailVerdict }) {
  if (verdict.level === "pending") {
    return null;
  }
  const independentAgreed = verdict.independentVerification === "agreed";
  return (
    <motion.section
      className={`trust-verdict trust-verdict--${verdict.level}`}
      aria-labelledby="guardrail-verdict-title"
      role="status"
      aria-live="polite"
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <header className="trust-verdict__head">
        <div className="trust-verdict__title">
          <ShieldCheck aria-hidden="true" />
          <div>
            <p className="eyebrow">Guardrail verdict</p>
            <h2 id="guardrail-verdict-title">{verdict.headline}</h2>
          </div>
        </div>
        <p className="trust-verdict__guarantee">{verdict.guaranteeLine}</p>
      </header>
      <dl className="trust-verdict__metrics">
        {verdict.metrics.map((metric) => (
          <div className={`trust-metric trust-metric--${metric.tone}`} key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>
              <span className="trust-metric__value">{metric.value}</span>
              <span className="trust-metric__hint">{metric.hint}</span>
            </dd>
          </div>
        ))}
      </dl>
      <p className="trust-verdict__foot">
        Independent verifier: <strong>{independentAgreed ? "agreed" : "not run"}</strong>. The
        hosted playground never mutates uploads; applying proven fixes is a local CLI workflow.
      </p>
    </motion.section>
  );
}

export function WouldApplyList({ fixes }: { fixes: VerifiedFix[] }) {
  if (fixes.length === 0) {
    return (
      <EvidenceNote
        title="Nothing proven to apply"
        body="No proposal was verified against an authoritative schema, so none would auto-apply. Refusing to write unproven values is the correct behavior."
      />
    );
  }
  return (
    <section className="would-apply-list" aria-labelledby="would-apply-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Proven, would apply</p>
          <h2 id="would-apply-title">Verified external fixes</h2>
        </div>
        <span className="quiet-chip quiet-chip--ok">{fixes.length} proven</span>
      </div>
      {fixes.map((fix) => (
        <article className="would-apply-row" key={`${fix.row}:${fix.column}:${fix.new_value}`}>
          <div className="would-apply-row__head">
            <strong>
              Row {fix.row}, <code>{fix.column}</code>
            </strong>
            <VerificationStrengthBadge strength={strengthOf(fix)} />
          </div>
          <span className="would-apply-row__change">
            {fix.old_value || "(empty)"} &rarr; {fix.new_value || "(empty)"}
          </span>
          {fix.verifier_reason ? <p className="verifier-note">{fix.verifier_reason}</p> : null}
        </article>
      ))}
    </section>
  );
}
