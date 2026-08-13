import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { AnalyzeResponse } from "../types";
import { buildEvidenceModel, type EvidenceModel } from "./model";
import {
  buildDensityField,
  densityFieldViolations,
  summariseColumns,
  type DensityField,
} from "./density";
import { createDensityPainter, type DensityPainter } from "./paintDensity";
import { onTokenChange, readVizTokens } from "./tokens";

/**
 * The overview: where cells are flagged. Deliberately says nothing about proof.
 *
 * Stage one of overview -> zoom and filter -> details-on-demand (Shneiderman, IEEE
 * Visual Languages, 1996). Collapsing those three stages into one artifact is what
 * forced the previous version to pick a lying representative rung for every band.
 * Here the marks carry no rung at all, so there is nothing to overstate: the map
 * answers "where", the detail view answers "what was proven".
 *
 * The canvas is a projection of the DOM, not the reverse. The per-column table below
 * is always rendered and carries the exact counts, because a canvas is opaque to
 * assistive technology and axe runs with zero exclusions.
 */

const MIN_HEIGHT = 180;
const MAX_HEIGHT = 420;
const ASPECT = 0.42;

export interface OverviewSelection {
  column: string;
  rowStart: number;
  rowEnd: number;
}

interface Props {
  analysis: AnalyzeResponse | null;
  onZoom: (selection: OverviewSelection) => void;
  /** Test seam: skip the canvas entirely and render text only. */
  textOnly?: boolean;
}

export function EvidenceOverview({ analysis, onZoom, textOnly }: Props) {
  const frameRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const painterRef = useRef<DensityPainter | null>(null);
  const [size, setSize] = useState({ widthPx: 0, heightPx: 0 });
  const [tokenEpoch, setTokenEpoch] = useState(0);
  const [violation, setViolation] = useState<string | null>(null);

  const model: EvidenceModel | null = useMemo(
    () => (analysis ? buildEvidenceModel(analysis) : null),
    [analysis],
  );

  const field: DensityField | null = useMemo(() => {
    if (model === null || size.widthPx <= 0) {
      return null;
    }
    return buildDensityField(model, size);
  }, [model, size]);

  // Size from the CONTAINER, not from the row count. The previous version derived
  // height as clamp(rows, 180, 420), which coupled the surface's height to the
  // dataset and was the root of the 3px band that made depth unrenderable.
  useEffect(() => {
    const frame = frameRef.current;
    if (frame === null || typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) {
        return;
      }
      const widthPx = Math.floor(entry.contentRect.width);
      const heightPx = Math.max(MIN_HEIGHT, Math.min(MAX_HEIGHT, Math.round(widthPx * ASPECT)));
      setSize((prev) =>
        prev.widthPx === widthPx && prev.heightPx === heightPx ? prev : { widthPx, heightPx },
      );
    });
    observer.observe(frame);
    return () => observer.disconnect();
  }, []);

  useEffect(() => onTokenChange(() => setTokenEpoch((n) => n + 1)), []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas === null || textOnly === true) {
      return;
    }
    const painter = createDensityPainter(canvas);
    painterRef.current = painter;
    return () => {
      painter?.destroy();
      painterRef.current = null;
    };
  }, [textOnly]);

  useEffect(() => {
    const painter = painterRef.current;
    if (painter === null || field === null || size.widthPx <= 0) {
      return;
    }
    const problems = densityFieldViolations(field);
    setViolation(problems.length > 0 ? problems.join("; ") : null);
    if (problems.length > 0) {
      return;
    }
    painter.draw(field, readVizTokens(), size.widthPx, size.heightPx);
  }, [field, size, tokenEpoch]);

  const columns = useMemo(
    () => (field && model ? summariseColumns(field, model) : []),
    [field, model],
  );

  const handleZoom = useCallback(
    (column: string) => {
      if (model === null) {
        return;
      }
      onZoom({ column, rowStart: 0, rowEnd: Math.max(model.rows - 1, 0) });
    },
    [model, onZoom],
  );

  if (model === null) {
    return null;
  }

  const label = describeField(field, model);

  return (
    <section className="loop-panel evidence-overview" aria-label="Flagged cell overview">
      <header className="evidence-overview__head">
        <h3>Where cells are flagged</h3>
        <p className="evidence-overview__scope">
          This map shows <strong>where</strong> the detectors flagged cells. It does not show
          what was proven &mdash; a band covers many rows, so it cannot speak for any single
          one. Select a column to see the individual claims.
        </p>
        <p className="evidence-overview__coverage">{model.coverage.note}</p>
      </header>

      {model.absence === "not_measured" || model.absence === "zero" ? (
        <p className={`evidence-surface__absence evidence-surface__absence--${model.absence}`}>
          {model.absenceText}
        </p>
      ) : violation !== null ? (
        <p className="evidence-surface__absence evidence-surface__absence--withheld" role="alert">
          The map was withheld because the encoder produced an inconsistent field. The counts
          below are unaffected. Detail: {violation}
        </p>
      ) : (
        <>
          <div className="evidence-overview__frame" ref={frameRef}>
            {textOnly === true ? null : (
              <canvas
                className="evidence-overview__canvas"
                ref={canvasRef}
                role="img"
                aria-label={label}
                style={{ height: `${size.heightPx}px` }}
              />
            )}
          </div>
          <p className="evidence-overview__axis">
            <span>Row 0</span>
            <span>
              {model.columns.length} columns
              {field && field.rowsPerBand > 1 ? `, ${field.rowsPerBand} rows per band` : ""}
              {field && field.maxCount > 0 ? `, up to ${field.maxCount} cells per band` : ""}
            </span>
            <span>Row {Math.max(model.rows - 1, 0)}</span>
          </p>
        </>
      )}

      <div className="table-frame" tabIndex={0}>
        <table>
          <caption className="visually-hidden">
            Flagged cells by column, with the exact count and how much of the column is affected
          </caption>
          <thead>
            <tr>
              <th scope="col">Column</th>
              <th scope="col">Flagged cells</th>
              <th scope="col">Bands affected</th>
              <th scope="col">Claims</th>
            </tr>
          </thead>
          <tbody>
            {columns.length === 0 ? (
              <tr>
                <td colSpan={4}>{model.absenceText || model.coverage.note}</td>
              </tr>
            ) : (
              columns.map((row) => (
                <tr key={row.column}>
                  <th scope="row">{row.column}</th>
                  <td>{row.cellCount}</td>
                  <td>
                    {row.bandsPresent} of {row.bandsTotal}
                  </td>
                  <td>
                    <button
                      type="button"
                      className="evidence-surface__jump"
                      onClick={() => handleZoom(row.column)}
                    >
                      Inspect {row.column}
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

/**
 * The accessible label, derived from the same field the canvas paints, so the two
 * cannot drift. Notice it reports position and counts only: there is no rung to
 * report, by law.
 */
export function describeField(field: DensityField | null, model: EvidenceModel): string {
  const shape = `${model.rows} rows by ${model.columns.length} columns`;
  if (field === null || field.marks.length === 0) {
    return `Flagged cell overview: ${shape}. ${model.absenceText || model.coverage.note}`;
  }
  return (
    `Flagged cell overview: ${shape}. ${field.cellCount} flagged cells in ` +
    `${field.marks.length} bands of up to ${field.rowsPerBand} rows. ` +
    `This map shows where cells are flagged, not what was proven. ${model.coverage.note}`
  );
}
