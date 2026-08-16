/**
 * The perceptual measurement kernel.
 *
 * The design laws are stated in perceptual terms and were verified in symbolic terms:
 * every gate checked token names, set membership and counts, and not one parsed a colour
 * value or simulated an eye. That is why the one law was violated on its own first-named
 * channel, in both themes, while `auditEarnedSalience` reported success.
 *
 * This module is the shared measurement engine the gates use. It is deliberately a
 * build-time script rather than a `src/` module: `culori` is a devDependency and
 * SPEC_color_system.md requires that no colour engine ship in the browser bundle. Keeping
 * the engine outside `src/` makes that guarantee structural instead of relying on
 * tree-shaking, and `auditNoColourEngineInSource` in audit_colors.mjs enforces it.
 *
 * Normative definitions live in specs/SPEC_perceptual_verification.md. Where this file and
 * that spec disagree, the spec wins and this file is the bug.
 */

import {
        converter,
        differenceEuclidean,
        filterDeficiencyDeuter,
        filterDeficiencyProt,
        filterDeficiencyTrit,
        filterGrayscale,
        formatHex,
        wcagContrast,
} from "culori";

const toOklch = converter("oklch");
const toOklab = converter("oklab");
const oklabDifference = differenceEuclidean("oklab");

/**
 * Two rungs are colour-collapsed below this OKLab distance.
 *
 * Derived in SPEC_perceptual_verification.md 4.3 from the palette's own tone stops: the
 * smallest step the system authors deliberately is 5 tones (`-90` to `-95`), and
 * `colorForTone` sets `l = tone / 100`, so that step is `dL = 0.05`. OKLab distance with
 * only L differing equals |dL|, so the floor is dimensionally consistent with a difference
 * the design system already relies on being visible.
 *
 * Fixed BEFORE measuring the current palette, on purpose. A threshold set to whatever the
 * palette happens to pass is a description, not a threshold.
 */
export const COLLAPSE_FLOOR = 0.05;

/**
 * Vision conditions under test.
 *
 * Dichromacy uses culori 4.0.2's `filterDeficiency*`, which implements a published model:
 *
 *   G. M. Machado, M. M. Oliveira and L. A. F. Fernandes, "A Physiologically-based Model
 *   for Simulation of Color Vision Deficiency", IEEE Transactions on Visualization and
 *   Computer Graphics, vol. 15, no. 6, pp. 1291-1298, Nov.-Dec. 2009,
 *   doi: 10.1109/TVCG.2009.113.
 *
 * Severity is pinned at exactly 1 (full dichromacy) so the model's precomputed lookup table
 * is indexed exactly and no interpolation between severity steps occurs. Anomalous
 * trichromacy at partial severity is a wider spectrum this kernel does not claim to cover.
 *
 * `achromatopsia` is culori's luminance-preserving greyscale, standing in for total colour
 * loss. It also covers the "survives grayscale" claim that two source comments make and
 * nothing executed.
 */
export const VISION_CONDITIONS = Object.freeze([
        "normal",
        "deuteranopia",
        "protanopia",
        "tritanopia",
        "achromatopsia",
]);

const SIMULATORS = {
        normal: (colour) => colour,
        deuteranopia: filterDeficiencyDeuter(1),
        protanopia: filterDeficiencyProt(1),
        tritanopia: filterDeficiencyTrit(1),
        achromatopsia: filterGrayscale(1),
};

/** Measure a colour in OKLCh. Chroma of an achromatic colour is reported as 0, not NaN. */
export function measureOklch(colour) {
        const measured = toOklch(colour);
        if (measured === undefined) {
                throw new Error(`Not a parseable colour: ${JSON.stringify(colour)}`);
        }
        return {
                l: measured.l ?? 0,
                c: measured.c ?? 0,
                h: Number.isFinite(measured.h) ? measured.h : null,
        };
}

/** Euclidean distance in OKLab. The space the system already authors in. */
export function oklabDistance(a, b) {
        return oklabDifference(a, b);
}

/** Simulate one vision condition. Returns a hex string so callers can print evidence. */
export function simulate(colour, condition) {
        const simulator = SIMULATORS[condition];
        if (simulator === undefined) {
                throw new Error(`Unknown vision condition: ${condition}`);
        }
        return formatHex(simulator(colour));
}

/**
 * Distance between two colours as a viewer with the given condition would see them.
 *
 * Both colours are simulated before comparison. Simulating only one would measure the
 * deficiency rather than the pair's separability under it.
 *
 * This is the SEPARABILITY metric, and it deliberately includes lightness. A pair whose hue
 * distinction collapses entirely can still be told apart if one is much darker, and a
 * criterion that ignored lightness would demand chromatic differences that lightness
 * already supplies.
 */
export function distanceUnder(a, b, condition) {
        return oklabDistance(simulate(a, condition), simulate(b, condition));
}

/**
 * Chromatic-only distance: the OKLab a-b plane, ignoring lightness.
 *
 * A DIAGNOSTIC, not the criterion. It answers "did the hue distinction survive?" separately
 * from "can these be told apart at all?", and the two genuinely differ. Measured on pure
 * red against pure green:
 *
 *   normal        red h=29  green h=142  total 0.5198
 *   deuteranopia  red h=87  green h=88   total 0.3597  -- hue collapsed, total fell
 *   protanopia    red h=91  green h=89   total 0.6362  -- hue collapsed, total ROSE
 *
 * Protanopia collapses the hue and simultaneously drives red from L=0.628 to L=0.236,
 * so total distance increases. That is the long-wavelength luminance loss the W3C names as
 * WCAG 2.x's own blind spot, measured here rather than assumed. Reporting only the total
 * would hide a complete hue collapse; reporting only the chromatic distance would
 * manufacture failures for pairs that lightness separates perfectly well.
 */
export function chromaticDistanceUnder(a, b, condition) {
        const first = toOklab(simulate(a, condition));
        const second = toOklab(simulate(b, condition));
        const deltaA = (first.a ?? 0) - (second.a ?? 0);
        const deltaB = (first.b ?? 0) - (second.b ?? 0);
        return Math.sqrt(deltaA * deltaA + deltaB * deltaB);
}

/** WCAG 2.x contrast, re-exported so audits have exactly one contrast implementation. */
export function contrastRatio(foreground, background) {
        return wcagContrast(foreground, background);
}

/** Lightness distance only, for reporting alongside chroma. */
export function lightnessOf(colour) {
        return measureOklch(colour).l;
}

/**
 * Walk an ordered ladder of measured values and report every adjacent pair that fails
 * STRICT monotonic increase.
 *
 * Flat counts as a violation because the law says "strictly". Two rungs rendering the
 * identical value means one of them is not carrying the channel at all, which is worth
 * surfacing rather than tolerating.
 */
export function monotonicityViolations(values) {
        const violations = [];
        for (let index = 1; index < values.length; index += 1) {
                const previous = values[index - 1];
                const current = values[index];
                if (current < previous - 1e-9) {
                        violations.push({ index, kind: "falls", from: previous, to: current });
                } else if (Math.abs(current - previous) <= 1e-9) {
                        violations.push({ index, kind: "flat", from: previous, to: current });
                }
        }
        return violations;
}

/** Every unordered pair of a list, as [a, b] with a before b in the input order. */
export function unorderedPairs(items) {
        const pairs = [];
        for (let i = 0; i < items.length; i += 1) {
                for (let j = i + 1; j < items.length; j += 1) {
                        pairs.push([items[i], items[j]]);
                }
        }
        return pairs;
}
