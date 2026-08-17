/** The trust surface: verdict, portable certificate, held-for-review, and proof asymmetry. */
import { EvidenceNote, VerificationStrengthBadge } from "../components/primitives";
import { motionSprings, panelVariants } from "../motion";
import { PROOF_ATTRIBUTION_ASYMMETRY, formatLabel, humanizeReviewReason, parseUnsatCore, strengthOf } from "../observatory";
import type { SelectedEvidence, TrustVerdict } from "../observatory";
import type { CandidateRepair, Certificate, RepairFailure } from "../types";
import { failureKey } from "../ui/helpers";
import { AlertTriangle, BadgeCheck, CheckCircle2, Download, ShieldCheck } from "lucide-react";
import { motion } from "motion/react";

export function TrustVerdictPanel({ verdict, stale = false }: { verdict: TrustVerdict; stale?: boolean }) {
  if (verdict.level === "pending") {
    return null;
  }
  const independentAgreed = verdict.independentVerification === "agreed";
  return (
    <motion.section
      className={`trust-verdict trust-verdict--${stale ? "stale" : verdict.level}`}
      aria-labelledby="trust-verdict-title"
      role="status"
      aria-live="polite"
      data-stale={stale ? "true" : undefined}
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <header className="trust-verdict__head">
        <div className="trust-verdict__title">
          <ShieldCheck aria-hidden="true" />
          <div>
            <p className="eyebrow">{stale ? "Previous run" : "Trust verdict"}</p>
            <h2 id="trust-verdict-title">{verdict.headline}</h2>
          </div>
        </div>
        <p className="trust-verdict__guarantee">{verdict.guaranteeLine}</p>
      </header>
      {stale ? (
        // A verdict is a claim about a specific run. Rendering last run's claim beside a
        // current error made the product assert something it had not established, which is
        // precisely the failure its verification layer exists to prevent. The result is kept
        // -- it is real, and the user may still need it -- but it is no longer presented as
        // describing what just happened.
        <p className="trust-verdict__stale-note">
          This describes an earlier run, not the attempt that just failed or was cancelled.
          Nothing new was analysed and nothing was applied.
        </p>
      ) : null}
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
        {independentAgreed ? (
          <span className="corroborated-chip">
            <BadgeCheck aria-hidden="true" /> Independently verified
          </span>
        ) : (
          <span className="single-verifier-note">Single verifier</span>
        )}{" "}
        {independentAgreed
          ? "— two independently written verifiers agreed on the applied set."
          : "— the deterministic gate proved every applied change; a second cross-check was not required for this run."}
      </p>
    </motion.section>
  );
}

export function CertificatePanel({
  certificate,
  independentVerification,
  auditCommand,
  onDownload,
}: {
  certificate: Certificate;
  independentVerification: string;
  auditCommand: string;
  onDownload: () => void;
}) {
  const passed = certificate.checks.filter((check) => check.ok).length;
  return (
    <motion.section
      className={`certificate-panel certificate-panel--${certificate.ok ? "ok" : "attention"}`}
      aria-labelledby="certificate-title"
      variants={panelVariants}
      initial="initial"
      animate="animate"
      exit="exit"
    >
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Portable trust certificate</p>
          <h2 id="certificate-title">
            {certificate.ok
              ? `Re-verified ${passed}/${certificate.checks.length} checks`
              : `Certificate reports ${certificate.checks.length - passed} unmet check(s)`}
          </h2>
        </div>
        <span className={`quiet-chip quiet-chip--${certificate.ok ? "ok" : "attention"}`}>
          {certificate.ok ? "self-verifies" : "review"}
        </span>
      </div>
      <p className="certificate-panel__lede">
        The receipt is self-contained: anyone holding your data and this certificate can re-check
        its trust invariants without re-running or trusting DataForge. This was re-verified
        server-side against your exact uploaded bytes.
      </p>
      <ul className="certificate-checks" aria-label="Certificate checks">
        {certificate.checks.map((check) => (
          <li
            className={`certificate-check certificate-check--${check.ok ? "ok" : "fail"}`}
            key={check.name}
          >
            {check.ok ? (
              <CheckCircle2 aria-hidden="true" />
            ) : (
              <AlertTriangle aria-hidden="true" />
            )}
            <div>
              <strong>{formatLabel(check.name)}</strong>
              <p>{check.detail}</p>
            </div>
          </li>
        ))}
      </ul>
      <div className="certificate-panel__actions">
        <button type="button" className="certificate-download" onClick={onDownload}>
          <Download aria-hidden="true" /> Download portable certificate
        </button>
        <p className="certificate-panel__reverify">
          {independentVerification === "agreed" ? (
            <span className="corroborated-chip">
              <BadgeCheck aria-hidden="true" /> Independently verified
            </span>
          ) : (
            <span className="single-verifier-note">Single verifier</span>
          )}{" "}
          Re-verify off this machine with <code>{auditCommand}</code>.
        </p>
      </div>
    </motion.section>
  );
}

export function HeldForReviewList({ items }: { items: CandidateRepair[] }) {
  if (items.length === 0) {
    return null;
  }
  return (
    <section className="held-list" aria-labelledby="held-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Held for review</p>
          <h2 id="held-title">Proposals not proven safe to auto-apply</h2>
        </div>
        <span className="quiet-chip">{items.length} held</span>
      </div>
      <p className="held-list__lede">
        These were not written. Refusing to guess when a value cannot be proven from the data is a
        first-class, honest outcome — not a failure.
      </p>
      {items.map((item) => (
        <article
          className="held-row"
          key={`${item.row}:${item.column}:${item.new_value}:${item.review_reason ?? "held"}`}
        >
          <div className="held-row__head">
            <strong>
              Row {item.row}, <code>{item.column}</code>
            </strong>
            <VerificationStrengthBadge strength={strengthOf(item)} />
          </div>
          <span className="held-row__change">
            {item.old_value || "(empty)"} → {item.new_value || "(empty)"}
          </span>
          <p>{humanizeReviewReason(item.review_reason)}</p>
        </article>
      ))}
    </section>
  );
}

export function ProofAttribution({ labels }: { labels: string[] }) {
  const attributions = parseUnsatCore(labels);
  if (attributions.length === 0) {
    // No core means no constraint was violated. Saying so is an L3 absence state,
    // not an empty element.
    return labels.length > 0 ? (
      <p className="proof-attribution proof-attribution--opaque">
        The verifier reported a reason this build cannot decode. Raw core:{" "}
        <code>{labels.join(", ")}</code>
      </p>
    ) : null;
  }
  return (
    <ul className="proof-attribution">
      {attributions.map((attribution) => (
        <li key={attribution.raw}>
          <span className="proof-attribution__kind">{attribution.kindLabel}</span>
          <span className="proof-attribution__sentence">{attribution.sentence}</span>
        </li>
      ))}
    </ul>
  );
}

export function FailureList({
  failures,
  onSelect,
  titleId = "failures-title",
}: {
  failures: RepairFailure[];
  onSelect: (selection: SelectedEvidence) => void;
  titleId?: string;
}) {
  if (failures.length === 0) {
    return <EvidenceNote title="No repair abstentions" body="Every attempted repair either verified or no issue required a fix." />;
  }
  return (
    <section className="failure-list" aria-labelledby={titleId}>
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Abstentions</p>
          <h2 id={titleId}>Attempted but not fixed</h2>
        </div>
        <span className="quiet-chip">{failures.length} failures</span>
      </div>
      <p className="proof-asymmetry">{PROOF_ATTRIBUTION_ASYMMETRY}</p>
      {failures.map((failure) => (
        <article className="failure-row" key={failureKey(failure)}>
          <button type="button" onClick={() => onSelect({ kind: "failure", id: failureKey(failure) })}>
            <strong>
              Row {failure.row}, <code>{failure.column}</code>
            </strong>
            <span>{failure.issue_type} - {failure.status} - attempts {failure.attempt_count}</span>
          </button>
          <p>{failure.reason}</p>
          <ProofAttribution labels={failure.unsat_core} />
        </article>
      ))}
    </section>
  );
}
