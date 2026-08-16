/**
 * Type declarations for the perceptual measurement kernel.
 *
 * The kernel is plain ESM JavaScript in `scripts/` rather than TypeScript in `src/` because
 * `culori` is a devDependency and SPEC_color_system.md requires that no colour engine ship
 * in the browser bundle. Keeping it outside `src/` makes that structural, and
 * `auditNoColourEngineInSource` enforces it.
 *
 * These declarations exist so the `src/design/*.test.ts` measurements typecheck under
 * `tsc -b` without relaxing `noImplicitAny` for the whole project.
 */

/** A colour accepted by culori: a CSS string or a culori colour object. */
export type ColourInput = string | { mode: string; [channel: string]: unknown };

export type VisionCondition =
        | "normal"
        | "deuteranopia"
        | "protanopia"
        | "tritanopia"
        | "achromatopsia";

/** OKLab distance below which two rungs count as colour-collapsed. */
export const COLLAPSE_FLOOR: number;

export const VISION_CONDITIONS: readonly VisionCondition[];

export interface OklchMeasure {
        l: number;
        c: number;
        /** Null for achromatic colours, where hue is undefined rather than zero. */
        h: number | null;
}

export function measureOklch(colour: ColourInput): OklchMeasure;

export function oklabDistance(a: ColourInput, b: ColourInput): number;

export function simulate(colour: ColourInput, condition: VisionCondition): string;

/** Separability metric. Includes lightness, because lightness genuinely separates. */
export function distanceUnder(
        a: ColourInput,
        b: ColourInput,
        condition: VisionCondition,
): number;

/** Hue-collapse diagnostic. Excludes lightness, so it answers a different question. */
export function chromaticDistanceUnder(
        a: ColourInput,
        b: ColourInput,
        condition: VisionCondition,
): number;

export function contrastRatio(foreground: ColourInput, background: ColourInput): number;

export function lightnessOf(colour: ColourInput): number;

export interface MonotonicityViolation {
        index: number;
        kind: "falls" | "flat";
        from: number;
        to: number;
}

export function monotonicityViolations(values: readonly number[]): MonotonicityViolation[];

export function unorderedPairs<T>(items: readonly T[]): [T, T][];
