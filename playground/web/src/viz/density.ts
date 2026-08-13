import { minMarkHeightPx } from "./grammar";
import type { EvidenceModel } from "./model";
import type { AbsenceState } from "./grammar";

/**
 * The density encoder: where cells are flagged, and nothing else.
 *
 * This module deliberately contains no notion of epistemic strength. Under the
 * addressability law (L2), a mark standing for a whole band of rows is not
 * individually addressable and therefore may not carry one. The gate in
 * audit_quantitative.mjs enforces that by inspecting this exact file, named by the
 * `evidence-overview` registry entry.
 *
 * Two consequences worth stating, because both replace earlier mistakes:
 *
 * 1. There is NO aggregation rule. Minimum-rung and maximum-rung were both tried;
 *    both tried to pick a representative of a set, and every representative of a
 *    set misreports it. Removing the strength dimension removes the problem.
 *
 * 2. Marks are BINARY presence, not a count ramp. Encoding count as intensity
 *    would put a magnitude on shading -- Cleveland & McGill's 7th and weakest
 *    channel -- which L1 forbids. Concentration is still legible, because it reads
 *    from the PROPORTION of bands lit in a column, which is positional. Exact
 *    counts travel as data for the label and the detail view, never as ink.
 */

export interface Viewport {
  widthPx: number;
  heightPx: number;
}

export interface DensityMark {
  xPx: number;
  yPx: number;
  wPx: number;
  hPx: number;
  /** Cells in this band. Carried for the label; never encoded as intensity. */
  count: number;
  column: string;
  columnIndex: number;
  rowStart: number;
  rowEnd: number;
}

export interface DensityField {
  marks: DensityMark[];
  bandCount: number;
  rowsPerBand: number;
  /** Cells represented by these marks. */
  cellCount: number;
  /** The largest per-band count, reported in text so the map stays interpretable. */
  maxCount: number;
  absence: AbsenceState | null;
  absenceText: string;
  coverageNote: string;
}

export function buildDensityField(model: EvidenceModel, viewport: Viewport): DensityField {
  const columnCount = model.columns.length;

  if (columnCount === 0 || model.rows === 0 || viewport.widthPx <= 0 || viewport.heightPx <= 0) {
    return {
      marks: [],
      bandCount: 0,
      rowsPerBand: 0,
      cellCount: 0,
      maxCount: 0,
      absence: model.absence ?? "not_measured",
      absenceText: model.absenceText,
      coverageNote: model.coverage.note,
    };
  }

  const maxBands = Math.max(1, Math.floor(viewport.heightPx / minMarkHeightPx));
  const bandCount = Math.min(model.rows, maxBands);
  const rowsPerBand = Math.ceil(model.rows / bandCount);

  const columnIndexByName = new Map<string, number>();
  model.columns.forEach((name, index) => columnIndexByName.set(name, index));

  const counts = new Map<string, number>();
  for (const cell of model.cells) {
    const columnIndex = columnIndexByName.get(cell.column);
    if (columnIndex === undefined) {
      continue;
    }
    const bandIndex = Math.floor(cell.row / rowsPerBand);
    const key = `${columnIndex}\u0000${bandIndex}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  const columnWidth = viewport.widthPx / columnCount;
  const bandHeight = viewport.heightPx / bandCount;
  const marks: DensityMark[] = [];
  let cellCount = 0;
  let maxCount = 0;

  for (const [key, count] of counts) {
    const [columnRaw, bandRaw] = key.split("\u0000");
    const columnIndex = Number(columnRaw);
    const bandIndex = Number(bandRaw);
    const rowStart = bandIndex * rowsPerBand;
    cellCount += count;
    maxCount = Math.max(maxCount, count);
    marks.push({
      xPx: columnIndex * columnWidth,
      yPx: bandIndex * bandHeight,
      wPx: columnWidth,
      hPx: bandHeight,
      count,
      column: model.columns[columnIndex],
      columnIndex,
      rowStart,
      rowEnd: Math.min(rowStart + rowsPerBand - 1, model.rows - 1),
    });
  }

  // Deterministic order: identical input must give identical output, and a stable
  // order keeps the accessible summary stable too.
  marks.sort((a, b) => a.columnIndex - b.columnIndex || a.rowStart - b.rowStart);

  return {
    marks,
    bandCount,
    rowsPerBand,
    cellCount,
    maxCount,
    absence: model.absence,
    absenceText: model.absenceText,
    coverageNote: model.coverage.note,
  };
}

/** Runtime check that the encoder honoured its own constraints. */
export function densityFieldViolations(field: DensityField): string[] {
  const problems: string[] = [];
  const occupied = new Set<string>();
  for (const mark of field.marks) {
    const key = `${mark.columnIndex}\u0000${mark.rowStart}`;
    if (occupied.has(key)) {
      problems.push(`overlapping marks at column ${mark.columnIndex}, row ${mark.rowStart}`);
    }
    occupied.add(key);
    if (mark.count < 1) {
      problems.push(`mark reports a count of ${mark.count}`);
    }
    if (mark.wPx <= 0 || mark.hPx <= 0) {
      problems.push("mark has a non-positive size");
    }
  }
  return problems;
}

/**
 * Per-column presence summary, for the accessible twin. Reports counts as text,
 * which is the channel allowed to carry magnitude.
 */
export interface ColumnPresence {
  column: string;
  columnIndex: number;
  cellCount: number;
  bandsPresent: number;
  bandsTotal: number;
}

export function summariseColumns(field: DensityField, model: EvidenceModel): ColumnPresence[] {
  const byColumn = new Map<number, ColumnPresence>();
  for (const mark of field.marks) {
    const existing = byColumn.get(mark.columnIndex);
    if (existing === undefined) {
      byColumn.set(mark.columnIndex, {
        column: mark.column,
        columnIndex: mark.columnIndex,
        cellCount: mark.count,
        bandsPresent: 1,
        bandsTotal: field.bandCount,
      });
      continue;
    }
    existing.cellCount += mark.count;
    existing.bandsPresent += 1;
  }
  return model.columns
    .map((_, index) => byColumn.get(index))
    .filter((entry): entry is ColumnPresence => entry !== undefined);
}
