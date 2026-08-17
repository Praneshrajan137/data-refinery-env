import { Metric, VerificationStrengthLegend } from "../components/primitives";
import type { AnalyzeResponse, BackendCapability } from "../types";
import type { WorkState } from "../ui/helpers";
import { Upload } from "lucide-react";

export function SystemPage({
  capability,
  backendState,
  streamingEnabled,
  maxUploadBytes,
  analysis,
}: {
  capability: BackendCapability | null;
  backendState: WorkState;
  streamingEnabled: boolean;
  maxUploadBytes: number;
  analysis: AnalyzeResponse | null;
}) {
  return (
    <main className="route-page system-page">
      <section className="system-grid">
        <Metric label="Backend" value={backendState} />
        <Metric label="Streaming" value={streamingEnabled ? "workflow_event_v1" : "JSON fallback"} />
        <Metric label="Upload cap" value={`${Math.floor(maxUploadBytes / 1024)} KiB`} />
        <Metric label="Advanced" value={capability?.advanced_available ? "available" : "unavailable"} />
        <Metric label="API version" value={capability?.api_version ?? analysis?.meta.api_version ?? "pending"} />
        <Metric label="Contract" value={analysis?.meta.contract_version ?? capability?.contract_version ?? "pending"} />
      </section>
      <section className="state-legend" aria-labelledby="state-legend-title">
        <div>
          <p className="eyebrow">Semantic State</p>
          <h2 id="state-legend-title">What each state means</h2>
          {/*
            Seven of these labels used to name pigments: "Vermilion active", "Viridian proof",
            "Brass review", "Hematite danger", "Ultraviolet agent". Those are the palette's
            internal names. A legend exists to tell a reader what a colour MEANS, so naming the
            paint answered a question nobody asked and left the actual one unanswered.
          */}
          <p>
            Colour answers &ldquo;what kind of thing is this?&rdquo;. How strongly something is
            marked answers &ldquo;how well is it proven?&rdquo;.
          </p>
        </div>
        <div className="legend-grid">
          <span className="legend-item legend-item--command">Primary command</span>
          <span className="legend-item legend-item--active">In progress now</span>
          <span className="legend-item legend-item--info">Evidence, no claim</span>
          <span className="legend-item legend-item--verified">Proven</span>
          <span className="legend-item legend-item--review">Waiting for a person</span>
          <span className="legend-item legend-item--danger">Refused or unsafe</span>
          <span className="legend-item legend-item--agent">Proposed by a model</span>
          <span className="legend-item legend-item--selection">Selected evidence</span>
          <span className="legend-item legend-item--loading">Loading progress</span>
          <span className="legend-item legend-item--disabled">Unavailable here</span>
          <span className="legend-item legend-item--verifying">Being verified</span>
          <span className="legend-item legend-item--proposing">Proposed, not proven</span>
          <span className="legend-item legend-item--proven">Proven, applied</span>
          <span className="legend-item legend-item--held">Held for review</span>
          <span className="legend-item legend-item--rejected">Rejected</span>
          <span className="legend-item legend-item--asking">Needs a human</span>
          <span className="legend-item legend-item--done">Done</span>
          <span className="legend-item legend-item--idle">Idle</span>
        </div>
        <VerificationStrengthLegend />
      </section>
      <section className="handoff-panel">
        <p className="eyebrow">Hosted Safety</p>
        <h2>Stateless dry-run contract</h2>
        <p>No browser storage, no frontend keys, no hosted apply/revert mutation, and no silent verifier bypass.</p>
      </section>
    </main>
  );
}
