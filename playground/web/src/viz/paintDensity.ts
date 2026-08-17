import type { DensityField, DensityMark } from "./density";
import type { RgbaColor, VizTokens } from "./tokens";

/**
 * The density painter: pixels, because these marks have no individual identity.
 *
 * A 2D canvas, not WebGL. The requirement that once justified a GPU path was
 * "instanced quads at scale", but binning bounds the mark count at `bandCount x columns`, where
 * `bandCount = min(rows, floor(heightPx / minMarkHeightPx))`. This comment used to claim
 * `140 x 128 = 17,920`; that was arithmetic on an assumed 420px canvas. Measured, the canvas
 * renders about 240px tall, so banding caps near 80 and the real ceiling is `80 x 128 = 10,240`.
 * A full-width field of 800 marks measures 0.4 ms best / 2.1 ms worst, drawn once and never
 * animated -- see e2e/density-throughput.spec.ts, which reads the measurement this painter
 * records rather than timing a loop of its own. Two further reasons the 2D path is not merely
 * adequate but better here:
 *
 * - A 2D canvas supports `getImageData`, so the output can be pixel-verified. WebGL
 *   cannot be read back without `preserveDrawingBuffer`. The GPU renderer was the
 *   one that could not be tested, which is how ~130 lines of untested GLSL shipped.
 * - Device-pixel-aligned `fillRect` gives crisp 1px band edges without any of the
 *   shader-side work that a padded-quad approach needs.
 *
 * `globalCompositeOperation` stays "source-over". Accumulating modes are forbidden
 * everywhere by L2.
 */

function css(colour: RgbaColor, alpha: number): string {
  const to255 = (n: number): number => Math.round(Math.min(1, Math.max(0, n)) * 255);
  const a = Math.min(1, Math.max(0, alpha * colour.a));
  return `rgba(${to255(colour.r)}, ${to255(colour.g)}, ${to255(colour.b)}, ${a})`;
}

/**
 * Presence alpha. A single constant, not a ramp over `count`.
 *
 * Encoding count as intensity would place a magnitude on shading, Cleveland &
 * McGill's weakest channel, which L1 forbids. Concentration stays legible because it
 * reads from the PROPORTION of bands lit within a column, which is positional. Exact
 * counts travel as text.
 */
const PRESENCE_ALPHA = 0.62;

export interface DensityPainter {
  draw(field: DensityField, tokens: VizTokens, widthPx: number, heightPx: number): void;
  destroy(): void;
}

/**
 * The name the painter records its cost under.
 *
 * Real instrumentation, not test scaffolding: it appears in a browser profile and in field
 * telemetry, so the paint cost is observable where users actually are. It also lets the frame
 * budget measure THIS code. The previous throughput gate built its own `fillRect` loop inside
 * `page.evaluate` and asserted on that, which verified that Chromium can draw 17,920
 * rectangles -- a fact about the browser, not about the painter. The loops even differ: this
 * one hoists `fillStyle` out of the loop and rounds per mark.
 */
export const DENSITY_PAINT_MEASURE = "df-density-paint";

export function createDensityPainter(canvas: HTMLCanvasElement): DensityPainter | null {
  const ctx = canvas.getContext("2d");
  if (ctx === null) {
    return null;
  }

  return {
    draw(field: DensityField, tokens: VizTokens, widthPx: number, heightPx: number): void {
      const timed = typeof performance !== "undefined" && typeof performance.mark === "function";
      if (timed) {
        performance.mark(`${DENSITY_PAINT_MEASURE}-start`);
      }
      const dpr =
        typeof window === "undefined" ? 1 : Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.max(1, Math.round(widthPx * dpr));
      canvas.height = Math.max(1, Math.round(heightPx * dpr));
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.globalCompositeOperation = "source-over";
      ctx.clearRect(0, 0, widthPx, heightPx);

      const ink = tokens.densityInk;
      ctx.fillStyle = css(ink, PRESENCE_ALPHA);
      for (const mark of field.marks) {
        paintMark(ctx, mark);
      }
      if (timed) {
        // The mark count travels with the measurement: a duration without the work it covers
        // cannot be compared against a budget.
        performance.measure(DENSITY_PAINT_MEASURE, {
          start: `${DENSITY_PAINT_MEASURE}-start`,
          detail: { marks: field.marks.length },
        });
        performance.clearMarks(`${DENSITY_PAINT_MEASURE}-start`);
      }
    },
    destroy(): void {
      ctx.setTransform(1, 0, 0, 1, 0, 0);
    },
  };
}

function paintMark(ctx: CanvasRenderingContext2D, mark: DensityMark): void {
  // Snap to device pixels so a 3px band does not smear across two rows.
  const x = Math.round(mark.xPx);
  const y = Math.round(mark.yPx);
  const w = Math.max(1, Math.round(mark.wPx) - 1);
  const h = Math.max(1, Math.round(mark.hPx));
  ctx.fillRect(x, y, w, h);
}
