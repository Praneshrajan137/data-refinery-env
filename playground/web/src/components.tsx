import React, { ReactNode } from 'react';

/* ===== BUTTON COMPONENTS ===== */
export function PrimaryButton({
  children,
  onClick,
  disabled,
  className = '',
  icon,
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  icon?: ReactNode;
}) {
  return (
    <button
      className={`primary-action ${className}`}
      onClick={onClick}
      disabled={disabled}
    >
      {icon}
      {children}
    </button>
  );
}

export function SecondaryButton({
  children,
  onClick,
  disabled,
  className = '',
  icon,
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  icon?: ReactNode;
}) {
  return (
    <button
      className={`secondary-action ${className}`}
      onClick={onClick}
      disabled={disabled}
    >
      {icon}
      {children}
    </button>
  );
}

export function IconButton({
  onClick,
  disabled,
  children,
  title,
}: {
  onClick?: () => void;
  disabled?: boolean;
  children: ReactNode;
  title?: string;
}) {
  return (
    <button
      className="icon-button"
      onClick={onClick}
      disabled={disabled}
      title={title}
    >
      {children}
    </button>
  );
}

/* ===== BADGE COMPONENTS ===== */
export function Badge({
  children,
  variant = 'default',
}: {
  children: ReactNode;
  variant?: 'default' | 'cyan' | 'magenta' | 'success' | 'warning' | 'error';
}) {
  const classMap = {
    default: 'bg-color-bg-tertiary text-color-text-secondary border-color-border-default',
    cyan: 'bg-cyan-500/10 text-color-accent-cyan border-cyan-500/30',
    magenta: 'bg-magenta-500/10 text-color-accent-magenta border-magenta-500/30',
    success: 'bg-color-success/10 text-color-success border-color-success/30',
    warning: 'bg-color-warning/10 text-color-warning border-color-warning/30',
    error: 'bg-color-error/10 text-color-error border-color-error/30',
  };

  return (
    <span className={`severity ${classMap[variant]}`}>
      {children}
    </span>
  );
}

/* ===== CARD COMPONENTS ===== */
export function Card({
  children,
  className = '',
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={`panel ${className}`}>
      {children}
    </div>
  );
}

export function CardHeader({
  title,
  subtitle,
  action,
}: {
  title: string;
  subtitle?: string;
  action?: ReactNode;
}) {
  return (
    <div className="panel-heading">
      <div>
        {subtitle && <p className="eyebrow">{subtitle}</p>}
        <h2>{title}</h2>
      </div>
      {action && <div className="evidence-actions">{action}</div>}
    </div>
  );
}

/* ===== STAT DISPLAY ===== */
export function StatCard({
  label,
  value,
  compact = false,
}: {
  label: string;
  value: string | number;
  compact?: boolean;
}) {
  return (
    <div className={`metric ${compact ? 'metric--compact' : ''}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

/* ===== STATUS PILL ===== */
export function StatusPill({
  status,
  icon,
  text,
}: {
  status: 'ready' | 'loading' | 'error';
  icon: ReactNode;
  text: string;
}) {
  const classMap = {
    ready: 'status-pill--ready',
    loading: 'status-pill--loading',
    error: 'status-pill--error',
  };

  return (
    <div className={`status-pill ${classMap[status]}`} role="status" aria-live="polite">
      {icon}
      {text}
    </div>
  );
}

/* ===== EMPTY STATE ===== */
export function EmptyState({
  icon,
  title,
  body,
}: {
  icon: ReactNode;
  title: string;
  body: string;
}) {
  return (
    <div className="empty-state">
      {icon}
      <h3>{title}</h3>
      <p>{body}</p>
    </div>
  );
}

/* ===== LOADING STATE ===== */
export function LoadingState({ label }: { label: string }) {
  return (
    <div className="loading-state" role="status" aria-label={label}>
      <svg className="animate-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <circle cx="12" cy="12" r="10" />
        <path d="M12 2a10 10 0 0110 10" />
      </svg>
      <p>{label}</p>
    </div>
  );
}

/* ===== ALERT/BANNER ===== */
export function AlertBanner({
  icon,
  message,
  type = 'error',
}: {
  icon: ReactNode;
  message: string;
  type?: 'error' | 'warning' | 'info' | 'success';
}) {
  const classMap = {
    error: 'problem-banner',
    warning: 'evidence-note',
    info: 'evidence-note',
    success: 'evidence-note',
  };

  return (
    <div className={classMap[type]}>
      {icon}
      <p>{message}</p>
    </div>
  );
}

/* ===== METRIC STRIP ===== */
export function MetricStrip({
  metrics,
}: {
  metrics: Array<{ label: string; value: string | number; compact?: boolean }>;
}) {
  return (
    <div className="metric-strip" aria-label="Statistics summary">
      {metrics.map((metric, idx) => (
        <StatCard key={idx} label={metric.label} value={metric.value} compact={metric.compact} />
      ))}
    </div>
  );
}

/* ===== SECTION DIVIDER ===== */
export function SectionDivider() {
  return <div style={{ borderBottom: '1px solid var(--color-border-default)', margin: '16px 0' }} />;
}
