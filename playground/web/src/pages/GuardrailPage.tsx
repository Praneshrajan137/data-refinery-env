import { DataForgeClient } from "../api";
import { GuardrailVerdictPanel, WouldApplyList } from "../components/agent";
import { BackendStatus } from "../components/mission";
import { EvidenceNote, LoadingState, ProblemBanner } from "../components/primitives";
import { CertificatePanel, HeldForReviewList } from "../components/trust";
import { parseCsvPreview, validateCsvFile } from "../csv";
import { buildGuardrailVerdict } from "../observatory";
import type { ProductRouteId } from "../routes";
import type { BackendCapability, CsvPreview, ExternalFix, ProblemDetail, VerifyFixesResponse } from "../types";
import { SAMPLE_OPTIONS, downloadCertificate, localProblem, problemFromUnknown } from "../ui/helpers";
import type { WorkState } from "../ui/helpers";
import { ShieldCheck } from "lucide-react";
import { useRef, useState } from "react";
import type { ChangeEvent } from "react";

export function GuardrailPage({
  client,
  capability,
  backendState,
  maxUploadBytes,
  onBackendRetry,
  onNavigate,
}: {
  client: DataForgeClient;
  capability: BackendCapability | null;
  backendState: WorkState;
  maxUploadBytes: number;
  onBackendRetry: () => void;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [sampleName, setSampleName] = useState<string | null>(null);
  const [preview, setPreview] = useState<CsvPreview | null>(null);
  const [fixes, setFixes] = useState<ExternalFix[]>([]);
  const [proposer, setProposer] = useState("external-agent");
  const [acceptedConstraintIds, setAcceptedConstraintIds] = useState<string[]>([]);
  // Only a DECLARED premise confers write authority since C4; an accepted mined constraint does
  // not. Carried separately so the surface can prove something instead of only ever refusing.
  const [declaredSchema, setDeclaredSchema] = useState<string | null>(null);
  const [confirmEscalations, setConfirmEscalations] = useState(true);
  const [allowUnproven, setAllowUnproven] = useState(false);
  const [scenarioNote, setScenarioNote] = useState<string | null>(null);
  const [result, setResult] = useState<VerifyFixesResponse | null>(null);
  const [state, setState] = useState<WorkState>("idle");
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const guardrailFileInput = useRef<HTMLInputElement | null>(null);

  const busy = state === "loading";
  const backendReady = backendState === "ready";
  const canVerify = backendReady && !busy && file !== null && fixes.length > 0;

  function resetOutput() {
    setResult(null);
    setProblem(null);
  }

  async function chooseSample(value: string) {
    if (busy) {
      return;
    }
    resetOutput();
    setScenarioNote(null);
    setFixes([]);
    setAcceptedConstraintIds([]);
    setDeclaredSchema(null);
    try {
      const sampleFile = await client.sample(value);
      setFile(sampleFile);
      setSampleName(value);
      setPreview(parseCsvPreview(await sampleFile.text()));
    } catch (error) {
      setProblem(problemFromUnknown(error));
    }
  }

  async function onFileChange(event: ChangeEvent<HTMLInputElement>) {
    const chosen = event.target.files?.[0];
    if (!chosen) {
      return;
    }
    resetOutput();
    setScenarioNote(null);
    setFixes([]);
    setAcceptedConstraintIds([]);
    setDeclaredSchema(null);
    const validation = validateCsvFile(chosen, maxUploadBytes);
    if (!validation.ok) {
      setProblem(localProblem(validation.message ?? "The CSV file could not be accepted."));
      return;
    }
    try {
      setFile(chosen);
      setSampleName(null);
      setPreview(parseCsvPreview(await chosen.text()));
    } catch (error) {
      setProblem(problemFromUnknown(error));
    }
  }

  async function loadScenario() {
    if (!sampleName || busy) {
      return;
    }
    resetOutput();
    try {
      const scenario = await client.verifyScenario(sampleName);
      setFixes(scenario.fixes);
      setAcceptedConstraintIds(scenario.accepted_constraint_ids);
      setDeclaredSchema(scenario.declared_schema);
      setProposer(scenario.proposer);
      setScenarioNote(scenario.note);
    } catch (error) {
      setProblem(problemFromUnknown(error));
    }
  }

  function updateFix(index: number, patch: Partial<ExternalFix>) {
    setFixes((current) => current.map((fix, i) => (i === index ? { ...fix, ...patch } : fix)));
  }

  function addFix() {
    setFixes((current) => [...current, { row: 0, column: "", new_value: "" }]);
  }

  function removeFix(index: number) {
    setFixes((current) => current.filter((_, i) => i !== index));
  }

  async function verify() {
    if (!file || fixes.length === 0) {
      return;
    }
    setState("loading");
    setProblem(null);
    try {
      const response = await client.verifyFixes(file, fixes, {
        acceptedConstraintIds,
        proposer,
        confirmEscalations,
        allowUnproven,
        declaredSchema,
      });
      setResult(response);
      setState("ready");
    } catch (error) {
      setResult(null);
      setState("error");
      setProblem(problemFromUnknown(error));
    }
  }

  const verdict = buildGuardrailVerdict(result);

  return (
    <main className="route-page guardrail-page">
      <header className="guardrail-hero">
        <p className="eyebrow">Agent guardrail</p>
        <h1>Verify an untrusted actor&apos;s proposed fixes</h1>
        <p>
          Propose cell edits as an agent, tool, or human would. DataForge proves the correct ones
          against an authoritative schema, holds or rejects the rest with an honest reason, and
          emits a certificate you can re-verify. Nothing is applied &mdash; this is a stateless dry
          run.
        </p>
      </header>

      {!backendReady ? (
        <BackendStatus state={backendState} capability={capability} onRetry={onBackendRetry} />
      ) : null}
      {problem ? <ProblemBanner problem={problem} /> : null}

      <section className="guardrail-setup" aria-label="Guardrail inputs">
        <div className="guardrail-intake">
          <p className="eyebrow">1. Choose a dataset</p>
          <div className="sample-chip-row" role="group" aria-label="Sample datasets">
            {SAMPLE_OPTIONS.map((sample) => (
              <button
                type="button"
                key={sample.value}
                className={
                  sampleName === sample.value ? "sample-chip sample-chip--active" : "sample-chip"
                }
                onClick={() => void chooseSample(sample.value)}
                disabled={busy}
              >
                <strong>{sample.label}</strong>
                <small>{sample.detail}</small>
              </button>
            ))}
          </div>
          <label className="guardrail-upload">
            <span>or upload a CSV</span>
            <input
              ref={guardrailFileInput}
              id="guardrail-csv-upload"
              type="file"
              accept=".csv,text/csv"
              onChange={(event) => void onFileChange(event)}
              disabled={busy}
            />
          </label>
          {preview ? (
            <p className="guardrail-dataset-note" role="status">
              Verifying against <code>{file?.name}</code> &mdash; {preview.columns.length} columns.
            </p>
          ) : (
            <EvidenceNote
              title="No dataset selected"
              body="Pick a sample or upload a CSV to verify proposed fixes against."
            />
          )}
        </div>

        <div className="guardrail-proposals">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">2. Propose fixes</p>
              <h2>Untrusted proposals</h2>
            </div>
            {sampleName ? (
              <button
                type="button"
                className="ghost-button"
                onClick={() => void loadScenario()}
                disabled={busy}
              >
                Load scripted agent batch
              </button>
            ) : null}
          </div>
          {scenarioNote ? <p className="guardrail-scenario-note">{scenarioNote}</p> : null}
          {declaredSchema ? (
            <p className="guardrail-schema-chip" role="status">
              <ShieldCheck aria-hidden="true" /> Declared schema &mdash; correctly-typed edits can be
              proven. This verifies fixes someone else proposed; it is not DataForge finding
              repairs.
            </p>
          ) : (
            <p className="guardrail-schema-chip guardrail-schema-chip--none">
              No declared schema &mdash; proposals can only be held, never proven.{" "}
              {acceptedConstraintIds.length > 0
                ? `${acceptedConstraintIds.length} mined constraint${
                    acceptedConstraintIds.length === 1 ? "" : "s"
                  } accepted in review, which is not evidence enough to authorise a write.`
                : null}
            </p>
          )}

          {fixes.length > 0 ? (
            <ul className="fix-editor" aria-label="Proposed fixes">
              {fixes.map((fix, index) => (
                <li className="fix-editor__row" key={index}>
                  <label>
                    <span>Row</span>
                    <input
                      type="number"
                      min={0}
                      value={fix.row}
                      onChange={(event) =>
                        updateFix(index, { row: Number(event.target.value) || 0 })
                      }
                    />
                  </label>
                  <label>
                    <span>Column</span>
                    <input
                      type="text"
                      value={fix.column}
                      onChange={(event) => updateFix(index, { column: event.target.value })}
                    />
                  </label>
                  <label>
                    <span>New value</span>
                    <input
                      type="text"
                      value={fix.new_value}
                      onChange={(event) => updateFix(index, { new_value: event.target.value })}
                    />
                  </label>
                  <label>
                    <span>Expected old (optional)</span>
                    <input
                      type="text"
                      value={fix.expected_old_value ?? ""}
                      onChange={(event) =>
                        updateFix(index, { expected_old_value: event.target.value || null })
                      }
                    />
                  </label>
                  <button
                    type="button"
                    className="fix-editor__remove"
                    aria-label={`Remove fix ${index + 1}`}
                    onClick={() => removeFix(index)}
                  >
                    Remove
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <EvidenceNote
              title="No proposals yet"
              body="Add a fix or load the scripted agent batch to see the guardrail in action."
            />
          )}

          <div className="guardrail-controls">
            <button type="button" className="ghost-button" onClick={addFix} disabled={busy}>
              Add fix
            </button>
            <label className="guardrail-field">
              <span>Proposer</span>
              <input
                type="text"
                value={proposer}
                onChange={(event) => setProposer(event.target.value)}
              />
            </label>
            <label className="guardrail-toggle">
              <input
                type="checkbox"
                checked={confirmEscalations}
                onChange={(event) => setConfirmEscalations(event.target.checked)}
              />
              <span>Confirm escalations (dry run)</span>
            </label>
            <label className="guardrail-toggle">
              <input
                type="checkbox"
                checked={allowUnproven}
                onChange={(event) => setAllowUnproven(event.target.checked)}
              />
              <span>Allow unproven</span>
            </label>
          </div>

          <button
            type="button"
            className="primary-button guardrail-verify"
            onClick={() => void verify()}
            disabled={!canVerify}
          >
            <ShieldCheck aria-hidden="true" /> Verify proposed fixes
          </button>
        </div>
      </section>

      {state === "loading" ? <LoadingState label="Verifying proposed fixes" /> : null}

      {result ? (
        <>
          <GuardrailVerdictPanel verdict={verdict} />
          <WouldApplyList fixes={result.would_apply} />
          <HeldForReviewList items={result.receipt.suggested_fixes ?? []} />
          <CertificatePanel
            certificate={result.certificate}
            independentVerification={result.receipt.independent_verification ?? "not_run"}
            auditCommand={result.apply_handoff.audit_command}
            onDownload={() => downloadCertificate(result)}
          />
          <section className="route-actions" aria-label="Next pages">
            <button type="button" onClick={() => onNavigate("run")}>
              Back to the repair loop
            </button>
          </section>
        </>
      ) : null}
    </main>
  );
}
