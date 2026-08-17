import { EvidenceDock } from "../components/evidence";
import { CopyFallback } from "../components/primitives";
import { RawEvidenceLens, ReceiptLens } from "../lenses";
import type { SelectedEvidence } from "../observatory";
import type { ProductRouteId } from "../routes";
import { EmptyPagePrompt } from "../shell";
import type { AnalyzeResponse, IssueGroup, ProblemDetail } from "../types";
import type { WorkflowStageView } from "../workflow";
import { ClipboardCopy, Download, Link2 } from "lucide-react";

export function ReceiptPage({
  analysis,
  evidenceText,
  copyState,
  selectedEvidence,
  stages,
  issues,
  problem,
  shareHref,
  shareState,
  onCopy,
  onExport,
  onShare,
  onSelect,
  onNavigate,
}: {
  analysis: AnalyzeResponse | null;
  evidenceText: string;
  copyState: "idle" | "copied" | "failed";
  selectedEvidence: SelectedEvidence | null;
  stages: WorkflowStageView[];
  issues: IssueGroup[];
  problem: ProblemDetail | null;
  shareHref: string | null;
  shareState: "idle" | "copied" | "failed";
  onCopy: () => void;
  onExport: () => void;
  onShare: () => void;
  onSelect: (selection: SelectedEvidence) => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page split-page">
      <section className="workbench-plane">
        <div className="receipt-toolbar">
          <button className="icon-button" type="button" disabled={!evidenceText} onClick={onCopy}>
            <ClipboardCopy aria-hidden="true" />
            {copyState === "copied" ? "Copied" : copyState === "failed" ? "Copy failed" : "Copy"}
          </button>
          <button className="icon-button" type="button" disabled={!evidenceText} onClick={onExport}>
            <Download aria-hidden="true" />
            Export
          </button>
          {/*
            A shareable link is only useful if the user knows it exists. This is offered only
            for a sample-backed run, because an uploaded CSV's bytes never leave the browser --
            a link naming it would open to an empty prompt for the recipient, which would be a
            worse lie than offering nothing. For uploads, Export remains the portable artifact.
          */}
          {shareHref ? (
            <button className="icon-button" type="button" onClick={onShare}>
              <Link2 aria-hidden="true" />
              {shareState === "copied"
                ? "Link copied"
                : shareState === "failed"
                  ? "Link copy blocked"
                  : "Share link"}
            </button>
          ) : null}
        </div>
        <ReceiptLens analysis={analysis} onSelect={onSelect} />
        <RawEvidenceLens analysis={analysis} evidenceText={evidenceText} />
        {copyState === "failed" && evidenceText ? <CopyFallback evidenceText={evidenceText} /> : null}
        {!analysis ? <EmptyPagePrompt title="Run analysis to unlock receipt" onNavigate={onNavigate} /> : null}
      </section>
      <EvidenceDock selectedEvidence={selectedEvidence} stages={stages} analysis={analysis} issues={issues} problem={problem} />
    </main>
  );
}
