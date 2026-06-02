import React from 'react';
import { BarChart3, Zap, Shield, TrendingUp } from 'lucide-react';
import { Card, CardHeader, MetricStrip, Badge, PrimaryButton, SecondaryButton } from './components';

export function Dashboard() {
  const recentActivity = [
    { id: 1, file: 'hospital_data.csv', status: 'completed', issues: 12, time: '2 hours ago' },
    { id: 2, file: 'flights_manifest.csv', status: 'completed', issues: 8, time: '5 hours ago' },
    { id: 3, file: 'transactions.csv', status: 'completed', issues: 24, time: '1 day ago' },
  ];

  const metrics = [
    { label: 'Files Processed', value: '1,247', compact: false },
    { label: 'Issues Detected', value: '8,943', compact: false },
    { label: 'Repairs Verified', value: '7,821', compact: false },
    { label: 'Success Rate', value: '97.2%', compact: false },
  ];

  const features = [
    {
      icon: <Zap className="w-5 h-5" />,
      title: 'Lightning Fast',
      description: 'Process CSV files in milliseconds with advanced detection algorithms',
      color: 'cyan',
    },
    {
      icon: <Shield className="w-5 h-5" />,
      title: 'Safe & Verified',
      description: 'Every repair is verified and auditable with transaction history',
      color: 'success',
    },
    {
      icon: <TrendingUp className="w-5 h-5" />,
      title: 'Comprehensive',
      description: 'Detect type mismatches, decimal shifts, and constraint violations',
      color: 'magenta',
    },
    {
      icon: <BarChart3 className="w-5 h-5" />,
      title: 'Detailed Reports',
      description: 'Export evidence and repair proposals for compliance documentation',
      color: 'warning',
    },
  ];

  return (
    <div className="dashboard-container">
      {/* Hero Section */}
      <section className="hero-section">
        <div className="hero-content">
          <h1 className="hero-title">Data Quality Repair Platform</h1>
          <p className="hero-subtitle">
            Automatically detect and fix CSV data quality issues with verified, auditable repairs
          </p>
          <div className="hero-actions">
            <PrimaryButton>Get Started</PrimaryButton>
            <SecondaryButton>View Demo</SecondaryButton>
          </div>
        </div>
        <div className="hero-visual">
          <div className="gradient-orb" />
        </div>
      </section>

      {/* Metrics Section */}
      <section className="metrics-section">
        <Card>
          <CardHeader
            title="Platform Statistics"
            subtitle="Last 30 Days"
          />
          <MetricStrip metrics={metrics} />
        </Card>
      </section>

      {/* Features Section */}
      <section className="features-section">
        <div className="section-header">
          <h2>Powerful Capabilities</h2>
          <p>Everything you need for data quality assurance</p>
        </div>
        <div className="features-grid">
          {features.map((feature, idx) => (
            <FeatureCard key={idx} {...feature} />
          ))}
        </div>
      </section>

      {/* Recent Activity */}
      <section className="activity-section">
        <Card>
          <CardHeader title="Recent Activity" subtitle="Processing History" />
          <div className="activity-list">
            {recentActivity.map((item) => (
              <ActivityRow key={item.id} {...item} />
            ))}
          </div>
        </Card>
      </section>
    </div>
  );
}

function FeatureCard({
  icon,
  title,
  description,
  color,
}: {
  icon: React.ReactNode;
  title: string;
  description: string;
  color: 'cyan' | 'success' | 'magenta' | 'warning';
}) {
  return (
    <div className="feature-card">
      <div className={`feature-icon feature-icon--${color}`}>
        {icon}
      </div>
      <h3 className="feature-title">{title}</h3>
      <p className="feature-description">{description}</p>
    </div>
  );
}

function ActivityRow({
  file,
  status,
  issues,
  time,
}: {
  file: string;
  status: string;
  issues: number;
  time: string;
}) {
  return (
    <div className="activity-row">
      <div className="activity-info">
        <p className="activity-file">{file}</p>
        <p className="activity-time">{time}</p>
      </div>
      <div className="activity-stats">
        <Badge variant={issues > 15 ? 'warning' : 'success'}>
          {issues} issues
        </Badge>
        <Badge variant="cyan">✓ {status}</Badge>
      </div>
    </div>
  );
}

// Add to styles.css
const dashboardStyles = `
/* ===== DASHBOARD LAYOUT ===== */
.dashboard-container {
  max-width: 1600px;
  margin: 0 auto;
  display: grid;
  gap: 32px;
}

/* ===== HERO SECTION ===== */
.hero-section {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 48px;
  align-items: center;
  padding: 60px 0;
}

.hero-content {
  display: grid;
  gap: 20px;
}

.hero-title {
  font-size: 3rem;
  line-height: 1.1;
  font-weight: 800;
  background: linear-gradient(135deg, #00d9ff, #e500ff, #3366ff);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  letter-spacing: -1px;
}

.hero-subtitle {
  font-size: 1.1rem;
  color: var(--color-text-secondary);
  line-height: 1.7;
  max-width: 500px;
}

.hero-actions {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  margin-top: 12px;
}

.hero-visual {
  position: relative;
  height: 400px;
}

.gradient-orb {
  position: absolute;
  width: 300px;
  height: 300px;
  background: radial-gradient(circle, rgba(0, 217, 255, 0.3), rgba(229, 0, 255, 0.2), transparent);
  border-radius: 50%;
  filter: blur(80px);
  animation: float 6s ease-in-out infinite;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}

@keyframes float {
  0%, 100% { transform: translate(-50%, -50%) translateY(0); }
  50% { transform: translate(-50%, -50%) translateY(-20px); }
}

/* ===== METRICS SECTION ===== */
.metrics-section {
  width: 100%;
}

/* ===== FEATURES SECTION ===== */
.features-section {
  width: 100%;
}

.section-header {
  text-align: center;
  margin-bottom: 40px;
}

.section-header h2 {
  font-size: 2rem;
  margin-bottom: 12px;
  color: var(--color-text-primary);
}

.section-header p {
  font-size: 1.05rem;
  color: var(--color-text-secondary);
}

.features-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 20px;
}

.feature-card {
  padding: 28px;
  border: 1px solid var(--color-border-default);
  border-radius: 12px;
  background: var(--color-surface-elevated);
  transition: var(--transition-default);
  display: grid;
  gap: 16px;
}

.feature-card:hover {
  border-color: var(--color-accent-cyan);
  box-shadow: 0 0 32px rgba(0, 217, 255, 0.1);
  transform: translateY(-4px);
}

.feature-icon {
  width: 48px;
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 10px;
  font-weight: 700;
}

.feature-icon--cyan {
  background: rgba(0, 217, 255, 0.1);
  color: var(--color-accent-cyan);
}

.feature-icon--success {
  background: rgba(0, 217, 102, 0.1);
  color: var(--color-success);
}

.feature-icon--magenta {
  background: rgba(229, 0, 255, 0.1);
  color: var(--color-accent-magenta);
}

.feature-icon--warning {
  background: rgba(255, 181, 0, 0.1);
  color: var(--color-warning);
}

.feature-title {
  font-size: 1.1rem;
  font-weight: 700;
  color: var(--color-text-primary);
}

.feature-description {
  color: var(--color-text-secondary);
  font-size: 0.95rem;
  line-height: 1.6;
}

/* ===== ACTIVITY SECTION ===== */
.activity-section {
  width: 100%;
  padding-bottom: 40px;
}

.activity-list {
  display: grid;
  gap: 12px;
}

.activity-row {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 16px;
  align-items: center;
  padding: 16px;
  border: 1px solid var(--color-border-default);
  border-radius: 10px;
  background: var(--color-bg-tertiary);
  transition: var(--transition-default);
}

.activity-row:hover {
  border-color: var(--color-accent-cyan);
  background: rgba(0, 217, 255, 0.02);
}

.activity-info {
  display: grid;
  gap: 6px;
}

.activity-file {
  color: var(--color-text-primary);
  font-weight: 600;
  font-size: 0.95rem;
}

.activity-time {
  color: var(--color-text-tertiary);
  font-size: 0.8rem;
}

.activity-stats {
  display: flex;
  gap: 10px;
}

/* ===== RESPONSIVE ===== */
@media (max-width: 1024px) {
  .hero-section {
    grid-template-columns: 1fr;
    gap: 32px;
  }

  .hero-title {
    font-size: 2.2rem;
  }

  .features-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 768px) {
  .dashboard-container {
    gap: 24px;
  }

  .hero-section {
    padding: 40px 0;
  }

  .hero-title {
    font-size: 1.8rem;
  }

  .hero-visual {
    height: 300px;
  }

  .section-header h2 {
    font-size: 1.6rem;
  }

  .features-grid {
    grid-template-columns: 1fr;
  }

  .activity-row {
    grid-template-columns: 1fr;
  }

  .activity-stats {
    flex-wrap: wrap;
  }
}
`;
