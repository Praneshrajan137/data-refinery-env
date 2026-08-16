import { rungOrder, rungSpecs, type Rung } from "./grammar";

/**
 * The token bridge: the renderer is a token CONSUMER, never a colour author.
 *
 * No colour may be authored in frontend source -- a Python contract test greps
 * every .ts/.tsx/.css under src/ for colour literals and fails the build. So
 * colours are read at runtime from the audited CSS custom properties and uploaded
 * as uniforms.
 *
 * That constraint is a benefit, not a tax. Because every colour arrives from the
 * generated token system, the visualisation inherits light mode, dark mode,
 * prefers-contrast: more, and the P3 gamut automatically, and it cannot drift
 * from the palette that audit_colors.mjs validates for contrast and earned
 * salience.
 */

export interface RgbaColor {
  r: number;
  g: number;
  b: number;
  a: number;
}

export interface RungColors {
  bg: RgbaColor;
  text: RgbaColor;
  line: RgbaColor;
}

export interface VizTokens {
  rungs: Record<Rung, RungColors>;
  surface: RgbaColor;
  gridLine: RgbaColor;
  /** Neutral magnitude colour. Never a verdict colour. */
  countText: RgbaColor;
  /**
   * The density field's ink. A NEUTRAL token by law: the overview carries no rung,
   * so it may not borrow a verdict colour (L2).
   */
  densityInk: RgbaColor;
  /** Proof glow. Reserved for proven rungs; see audit_colors P3 restriction. */
  proofGlow: RgbaColor;
  /** Tokens that could not be resolved, reported rather than silently defaulted. */
  unresolved: string[];
}

const TRANSPARENT: RgbaColor = { r: 0, g: 0, b: 0, a: 0 };

/**
 * Parse the CSS colour forms the token system actually emits: hex (the palette),
 * rgb()/rgba() (what the browser returns for color-mix), and colour keywords we
 * care about. Returns null rather than guessing -- an invented colour would be a
 * claim the token system never made.
 *
 * Deliberately pure so it is testable under jsdom, where no canvas exists to
 * normalise colours for us.
 */
export function parseCssColor(input: string): RgbaColor | null {
  const value = input.trim().toLowerCase();
  if (value === "" || value === "transparent") {
    return value === "transparent" ? { ...TRANSPARENT } : null;
  }

  const HASH = "#";
  if (value.startsWith(HASH)) {
    const digits = value.slice(1);
    if (!/^[0-9a-f]+$/.test(digits)) {
      return null;
    }
    const expand = (pair: string): number => parseInt(pair, 16) / 255;
    if (digits.length === 3 || digits.length === 4) {
      const parts = digits.split("").map((ch) => expand(ch + ch));
      return { r: parts[0], g: parts[1], b: parts[2], a: digits.length === 4 ? parts[3] : 1 };
    }
    if (digits.length === 6 || digits.length === 8) {
      const parts: number[] = [];
      for (let i = 0; i < digits.length; i += 2) {
        parts.push(expand(digits.slice(i, i + 2)));
      }
      return { r: parts[0], g: parts[1], b: parts[2], a: digits.length === 8 ? parts[3] : 1 };
    }
    return null;
  }

  const functional = /^rgba?\(([^)]+)\)$/.exec(value);
  if (functional) {
    const parts = functional[1]
      .split(/[,/\s]+/)
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
    if (parts.length < 3) {
      return null;
    }
    const channel = (raw: string): number => {
      if (raw.endsWith("%")) {
        return Number.parseFloat(raw) / 100;
      }
      return Number.parseFloat(raw) / 255;
    };
    const alpha = (raw: string | undefined): number => {
      if (raw === undefined) {
        return 1;
      }
      return raw.endsWith("%") ? Number.parseFloat(raw) / 100 : Number.parseFloat(raw);
    };
    const r = channel(parts[0]);
    const g = channel(parts[1]);
    const b = channel(parts[2]);
    const a = alpha(parts[3]);
    if ([r, g, b, a].some((n) => Number.isNaN(n))) {
      return null;
    }
    return { r, g, b, a };
  }

  // color(display-p3 r g b / a) -- the P3 glow override. Read the components
  // directly; treating them as sRGB slightly desaturates but never brightens,
  // so it cannot manufacture salience.
  const p3 = /^color\(display-p3([^)]+)\)$/.exec(value);
  if (p3) {
    const parts = p3[1]
      .split(/[,/\s]+/)
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
    if (parts.length < 3) {
      return null;
    }
    const nums = parts.map((part) => Number.parseFloat(part));
    if (nums.slice(0, 3).some((n) => Number.isNaN(n))) {
      return null;
    }
    return {
      r: Math.min(1, Math.max(0, nums[0])),
      g: Math.min(1, Math.max(0, nums[1])),
      b: Math.min(1, Math.max(0, nums[2])),
      a: nums.length > 3 && !Number.isNaN(nums[3]) ? nums[3] : 1,
    };
  }

  return null;
}

type Reader = (name: string) => string;

function makeReader(root: HTMLElement): Reader {
  const computed = getComputedStyle(root);
  return (name: string) => computed.getPropertyValue(name);
}

/**
 * Read the design tokens the visualisation needs.
 *
 * `fallback` is used only when a token is missing or unparseable, and every such
 * case is recorded in `unresolved` so a missing token surfaces as a reported
 * defect rather than a plausible-looking colour.
 */
export function readVizTokens(root?: HTMLElement | null, reader?: Reader): VizTokens {
  const target = root ?? (typeof document !== "undefined" ? document.documentElement : null);
  const read: Reader = reader ?? (target ? makeReader(target) : () => "");
  const unresolved: string[] = [];

  const colour = (name: string, fallback: RgbaColor): RgbaColor => {
    const parsed = parseCssColor(read(name));
    if (parsed === null) {
      unresolved.push(name);
      return fallback;
    }
    return parsed;
  };

  // Neutral mid-grey fallbacks. Chosen so a failure is legible and unalarming
  // rather than looking like a verdict colour.
  const neutralInk: RgbaColor = { r: 0.35, g: 0.35, b: 0.34, a: 1 };
  const neutralSurface: RgbaColor = { r: 0.98, g: 0.97, b: 0.96, a: 1 };

  const rungs = {} as Record<Rung, RungColors>;
  for (const rung of rungOrder) {
    const family = rungSpecs[rung].tokenFamily;
    // status-* families expose only -bg and -text (no -line); fall back to text so
    // the stroke still carries the rung rather than vanishing.
    const lineName = `--df-${family}-line`;
    const lineRaw = read(lineName);
    const line =
      parseCssColor(lineRaw) ?? colour(`--df-${family}-text`, neutralInk);
    rungs[rung] = {
      bg: colour(`--df-${family}-bg`, neutralSurface),
      text: colour(`--df-${family}-text`, neutralInk),
      line,
    };
  }

  return {
    rungs,
    surface: colour("--df-surface-1", neutralSurface),
    gridLine: colour("--df-line-subtle", neutralInk),
    countText: colour("--df-confidence-medium-text", neutralInk),
    densityInk: forcedColoursInk(target) ?? colour("--df-confidence-medium-text", neutralInk),
    proofGlow: colour("--df-proof-glow", { ...TRANSPARENT }),
    unresolved,
  };
}

/**
 * The density map's ink under Windows High Contrast, or null when not in forced-colours mode.
 *
 * This is the one place forced colours cannot be handled in CSS. `forced-colors: active`
 * overrides CSS-painted colour, but it cannot reach a canvas `fillStyle`, so the density map
 * would keep painting `--df-confidence-medium-text` onto an OS-supplied background -- a
 * neutral grey on a forced black or white, with no guarantee of contrast and no repaint when
 * the mode changes. `onTokenChange` now watches the query; this supplies the value.
 *
 * A custom property cannot carry the answer: `--df-ink: CanvasText` reads back as the literal
 * string "CanvasText", because custom properties are not resolved against the system palette.
 * The document's own `color` IS overridden to CanvasText in forced-colours mode, so reading
 * the resolved `color` off the root gets the real value the OS chose.
 */
function forcedColoursInk(target: HTMLElement | null): RgbaColor | null {
  if (target === null || typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return null;
  }
  if (!window.matchMedia("(forced-colors: active)").matches) {
    return null;
  }
  return parseCssColor(getComputedStyle(target).color);
}

/**
 * Subscribe to the media conditions that change the token values.
 *
 * There is no manual theme toggle in this app -- light/dark, contrast and forced colours all
 * come from the OS.
 *
 * `forced-colors` was missing, and the previous version of this comment claimed the other
 * three were "the complete set of triggers". They were not. Forced-colors mode (Windows High
 * Contrast) overrides CSS-painted colour but CANNOT reach a canvas `fillStyle`, so the
 * density map went on painting its own ink onto an OS-forced background with no
 * notification and no repaint. The canvas is the one surface where forced colours have to be
 * handled in code rather than in CSS.
 */
export function onTokenChange(callback: () => void): () => void {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return () => {};
  }
  const queries = [
    "(prefers-color-scheme: dark)",
    "(prefers-contrast: more)",
    "(color-gamut: p3)",
    "(forced-colors: active)",
  ].map((query) => window.matchMedia(query));

  const handler = (): void => callback();
  for (const query of queries) {
    query.addEventListener("change", handler);
  }
  return () => {
    for (const query of queries) {
      query.removeEventListener("change", handler);
    }
  };
}
