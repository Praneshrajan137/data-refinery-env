/** Small shared display components: badges, metrics, and the undesigned-state surfaces. */
import { problemToMessage } from "../csv";
import { motionDurations, motionSprings } from "../motion";
import { formatLabel, formatPercent } from "../observatory";
import type { DatasetInput, ProblemDetail, RepairReadiness, RiskLevel, Severity, VerificationStrength } from "../types";
import { AlertTriangle, BadgeCheck, RefreshCw, ShieldCheck } from "lucide-react";
import { motion } from "motion/react";
import type { ReactNode } from "react";

export function CommandRow({ label, command, delay = 0 }: { label: string; command: string; delay?: number }) {
  return (
    <motion.div
      className="command-row"
      initial={{ opacity: 0, y: 3 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, duration: motionDurations.fast }}
    >
      <span>{label}</span>
      <code>{command}</code>
    </motion.div>
  );
}

export function EvidenceNote({ title, body }: { title: string; body: string }) {
  return (
    <div className="evidence-note">
      <ShieldCheck aria-hidden="true" />
      <div>
        <strong>{title}</strong>
        <p>{body}</p>
      </div>
    </div>
  );
}

export function CopyFallback({ evidenceText }: { evidenceText: string }) {
  return (
    <div className="copy-fallback" role="status" aria-live="polite">
      <strong>Clipboard permission was blocked</strong>
      <p>Export still works. You can also select this evidence payload directly.</p>
      <textarea aria-label="Copyable repair evidence" readOnly value={evidenceText} />
    </div>
  );
}

export function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function SeverityBadge({ severity }: { severity: Severity }) {
  return <span className={`severity severity--${severity}`}>{severity}</span>;
}

export function ConfidenceBadge({ value }: { value: number }) {
  const bucket = value >= 0.85 ? "high" : value >= 0.65 ? "medium" : "low";
  return <span className={`confidence confidence--${bucket}`}>{formatPercent(value)}</span>;
}

export function VerificationStrengthBadge({ strength }: { strength: VerificationStrength }) {
  const proven = strength === "proven";
  const explanation = proven
    ? "Proven: deterministic or verified against an authoritative schema. Safe to auto-apply."
    : "Plausibility-only: a model-proposed value with no authoritative schema. Never silently written.";
  return (
    <span
      className={`strength-badge strength-badge--${proven ? "proven" : "plausibility"}${proven ? " df-motion-settle" : ""}`}
      // The title is kept for a mouse user's convenience but is no longer the only route to
      // the explanation. A tooltip is unavailable on touch, unavailable to keyboard users, and
      // unreliable with a screen reader -- and this is the product's central distinction:
      // proven means safe to write, plausibility-only means a model guessed. Conflating them
      // authorises a bad write, so the explanation cannot be optional.
      title={explanation}
    >
      {proven ? <BadgeCheck aria-hidden="true" /> : <AlertTriangle aria-hidden="true" />}
      {/* The visible label is its own element so that the added screen-reader explanation does
          not merge into the badge's text content. Without this, exact text matching on
          "proven" -- which the guardrail e2e relies on -- silently stopped matching. */}
      <span className="strength-badge__label">
        {proven ? "proven" : "plausible \u00b7 not written"}
      </span>
      <span className="visually-hidden">. {explanation}</span>
    </span>
  );
}

/**
 * The one visible explanation of the rung vocabulary, for everyone.
 *
 * Rendered once per surface that shows strength badges, rather than per badge: a disclosure on
 * every row would be dozens of duplicate controls, and the fact being explained is the same
 * every time. This is what makes the distinction available to a touch user, who can never see
 * a title tooltip.
 */
export function VerificationStrengthLegend() {
  return (
    <dl className="strength-legend" aria-label="What proven and plausible mean">
      <div>
        <dt>
          <BadgeCheck aria-hidden="true" /> proven
        </dt>
        <dd>Derived by rule, or checked against a schema you accepted. Safe to write.</dd>
      </div>
      <div>
        <dt>
          <AlertTriangle aria-hidden="true" /> plausible &middot; not written
        </dt>
        <dd>A model proposed it and nothing proved it. Shown for review, never applied.</dd>
      </div>
    </dl>
  );
}

export function ProblemBanner({
  problem,
  onRetry,
}: {
  problem: ProblemDetail;
  onRetry?: () => void;
}) {
  return (
    <motion.div
      className="problem-banner"
      role="alert"
      initial={{ opacity: 0, y: 3 }}
      animate={{ opacity: 1, y: 0 }}
      transition={motionSprings.snap}
    >
      <AlertTriangle aria-hidden="true" />
      <div>
        <strong>{problem.title}</strong>
        <p>{problemToMessage(problem)}</p>
        {/*
          Retry was previously unavailable on every failure path. The only retry affordance in
          the product was the backend chip, whose handler was window.location.reload() -- it
          discarded the loaded CSV and any completed receipt to recover from a transient error.
          A timeout or a sleeping backend is the common case on free hosting, so the recovery
          for it should not cost the user their work.
        */}
        {onRetry ? (
          <button type="button" className="problem-banner__retry" onClick={onRetry}>
            <RefreshCw aria-hidden="true" />
            Try again
          </button>
        ) : null}
      </div>
    </motion.div>
  );
}

export function LoadingState({ label }: { label: string }) {
  return (
    <motion.div
      className="loading-state"
      role="status"
      aria-live="polite"
      data-agent-motion="verifying"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: motionDurations.fast }}
    >
      <RefreshCw aria-hidden="true" />
      <span>{label}</span>
      <span className="loading-state__track" aria-hidden="true">
        <span className="loading-state__sweep df-motion-resolve" />
      </span>
    </motion.div>
  );
}

export function EmptyState({ icon, title, body }: { icon: ReactNode; title: string; body: string }) {
  return (
    <motion.div
      className="empty-state"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: motionDurations.fast }}
    >
      {icon}
      <strong>{title}</strong>
      <p>{body}</p>
    </motion.div>
  );
}

export function StageCounts({ counts }: { counts: Record<string, number | string | boolean> }) {
  const entries = Object.entries(counts);
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className="stage-counts">
      {entries.slice(0, 4).map(([key, value]) => (
        <span key={key}>
          {formatLabel(key)} {String(value)}
        </span>
      ))}
    </div>
  );
}

export function DatasetBadge({ dataset }: { dataset: DatasetInput | null }) {
  if (!dataset) {
    return <span className="quiet-chip">Waiting</span>;
  }
  return (
    <span className="quiet-chip">
      {dataset.preview.rows.length} preview rows, {dataset.preview.columns.length} columns
    </span>
  );
}

export function RiskBadge({ label, value }: { label: string; value: RiskLevel | RepairReadiness }) {
  return (
    <span className={`risk-badge risk-badge--${value}`}>
      <strong>{label}</strong>
      {formatLabel(value)}
    </span>
  );
}

/**
 * Offline notice.
 *
 * The product had no offline treatment of any kind: no code read navigator.onLine and no
 * listener existed, so a lost connection surfaced only as a failed request minutes later,
 * titled as though the user's CSV were malformed.
 *
 * It states what is still true as well as what is not, because the important fact for a user
 * mid-run is that their loaded CSV and any completed receipt have NOT been lost -- the
 * playground holds them in memory and never needed the network to keep them.
 */
export function OfflineBanner() {
  return (
    <div className="offline-banner" role="status" aria-live="polite">
      <AlertTriangle aria-hidden="true" />
      <div>
        <strong>You are offline</strong>
        <p>
          New analysis needs the backend, so Analyze is paused. Your loaded CSV and any receipt
          already returned are still here, and nothing was applied.
        </p>
      </div>
    </div>
  );
}
