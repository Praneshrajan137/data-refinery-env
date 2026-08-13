import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { AnalyzeResponse } from "../types";
import { parseUnsatCore, PROOF_ATTRIBUTION_ASYMMETRY } from "../observatory";
import { buildEvidenceModel } from "./model";
import { buildClaimSet, claimSetViolations, type Claim, type ClaimSelection } from "./claims";
import { addressableMinHeightPx, rungSpecs } from "./grammar";

/**
 * Details on demand: one addressable object per claim.
 *
 * This is the only surface allowed to carry epistemic strength, because each mark
 * here stands for exactly one claim (L2). Three consequences:
 *
 * - **Each claim is a real DOM button**, so it is focusable, labelled and operable.
 *   The redundancy law is satisfied by the markup itself rather than by a duplicate
 *   table alongside a canvas.
 * - **Earned depth finally renders.** A row is at least `addressableMinHeightPx`
 *   tall, so a contact shadow has a ground to fall on. Proven claims sit on the
 *   plane and cast one; plausibility-only claims float and cast none. The offset is
 *   a CSS transform and the shadow a static `box-shadow`, so no shader exists to go
 *   unverified and `audit_motion`'s keyframe restriction is untouched.
 * - **No aggregation, therefore no aggregation lie.** The set is bounded by
 *   selection, so every mark is exactly one claim.
 */

interface Props {
  analysis: AnalyzeResponse | null;
  selection: ClaimSelection | null;
  onClear: () => void;
}

export function ClaimDetail({ analysis, selection, onClear }: Props) {
  const listRef = useRef<HTMLUListElement | null>(null);
  const [activeIndex, setActiveIndex] = useState(0);
  const [expanded, setExpanded] = useState<string | null>(null);

  const model = useMemo(() => (analysis ? buildEvidenceModel(analysis) : null), [analysis]);
  const set = useMemo(
    () => (model ? buildClaimSet(model, selection) : null),
    [model, selection],
  );

  useEffect(() => setActiveIndex(0), [selection]);

  const violations = useMemo(() => (set ? claimSetViolations(set) : []), [set]);

  // Roving tabindex: one tab stop for the whole list, arrows to move within it.
  // With up to 200 claims, native per-button tab stops would make the page
  // untraversable by keyboard.
  const onKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLUListElement>) => {
      if (set === null || set.claims.length === 0) {
        return;
      }
      const last = set.claims.length - 1;
      let next = activeIndex;
      if (event.key === "ArrowDown") {
        next = Math.min(activeIndex + 1, last);
      } else if (event.key === "ArrowUp") {
        next = Math.max(activeIndex - 1, 0);
      } else if (event.key === "Home") {
        next = 0;
      } else if (event.key === "End") {
        next = last;
      } else {
        return;
      }
      event.preventDefault();
      setActiveIndex(next);
      const buttons = listRef.current?.querySelectorAll<HTMLButtonElement>(".claim__button");
      buttons?.[next]?.focus();
    },
    [activeIndex, set],
  );

  if (analysis === null || set === null || selection === null) {
    return null;
  }

  const heading =
    selection.column !== undefined
      ? `Claims in ${selection.column}`
      : "Claims in this selection";

  return (
    <section className="loop-panel claim-detail" aria-label="Individual claims">
      <header className="claim-detail__head">
        <div>
          <h3>{heading}</h3>
          <p className="claim-detail__scope">
            One row per claim. Each is a single cell, so the verdict shown belongs to that
            cell alone.
          </p>
        </div>
        <button type="button" className="evidence-surface__jump" onClick={onClear}>
          Clear selection
        </button>
      </header>

      {violations.length > 0 ? (
        <p className="evidence-surface__absence evidence-surface__absence--withheld" role="alert">
          These claims were withheld because their rendering would have misstated how much was
          proven. Detail: {violations.join("; ")}
        </p>
      ) : set.absence !== null ? (
        <p className={`evidence-surface__absence evidence-surface__absence--${set.absence}`}>
          {set.absenceText}
        </p>
      ) : (
        <>
          {set.truncated ? (
            <p className="claim-detail__truncated">
              Showing {set.claims.length} of {set.matched} claims in this selection. Narrow the
              selection to see the rest; the remainder are counted, not hidden.
            </p>
          ) : null}
          <ul
            className="claim-list"
            ref={listRef}
            onKeyDown={onKeyDown}
            aria-label={`${set.matched} claims, strongest verdict first`}
          >
            {set.claims.map((claim, index) => (
              <ClaimRow
                key={claim.key}
                claim={claim}
                tabbable={index === activeIndex}
                expanded={expanded === claim.key}
                onToggle={() => {
                  setActiveIndex(index);
                  setExpanded(expanded === claim.key ? null : claim.key);
                }}
              />
            ))}
          </ul>
          <p className="proof-asymmetry">{PROOF_ATTRIBUTION_ASYMMETRY}</p>
        </>
      )}
    </section>
  );
}

function ClaimRow({
  claim,
  tabbable,
  expanded,
  onToggle,
}: {
  claim: Claim;
  tabbable: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const spec = rungSpecs[claim.rung];
  const attributions = parseUnsatCore(claim.unsatCore);

  return (
    <li className="claim" data-rung={claim.rung}>
      <button
        type="button"
        className="claim__button"
        tabIndex={tabbable ? 0 : -1}
        aria-expanded={expanded}
        onClick={onToggle}
        style={{ minHeight: `${addressableMinHeightPx + 8}px` }}
      >
        <span className="claim__where">
          Row {claim.row}, <code>{claim.column}</code>
        </span>
        {/* Text is mandatory on every state sentence: the rung must survive the
            removal of colour, form and depth. */}
        <span className="claim__verdict">{spec.text}</span>
        <span className="claim__type">{claim.issueType.replace(/_/g, " ")}</span>
      </button>
      {expanded ? (
        <div className="claim__body">
          {claim.oldValue !== null && claim.newValue !== null ? (
            <p className="claim__change">
              <code>{claim.oldValue}</code> &rarr; <code>{claim.newValue}</code>
            </p>
          ) : null}
          <p className="claim__detail">{claim.detail}</p>
          {claim.reviewReason !== null ? (
            <p className="claim__reason">{claim.reviewReason.replace(/_/g, " ")}</p>
          ) : null}
          {attributions.length > 0 ? (
            <ul className="proof-attribution">
              {attributions.map((attribution) => (
                <li key={attribution.raw}>
                  <span className="proof-attribution__kind">{attribution.kindLabel}</span>
                  <span className="proof-attribution__sentence">{attribution.sentence}</span>
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
    </li>
  );
}
