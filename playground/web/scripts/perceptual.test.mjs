/**
 * Unit tests for the perceptual measurement kernel.
 *
 * The kernel is about to be used to judge the palette, so it must be verified against
 * values that can be checked by hand or reasoned about from first principles -- never
 * against the palette itself. A kernel calibrated on the thing it measures proves nothing,
 * which is the tautological-guard mistake this repo has already shipped twice.
 */

import { describe, expect, it } from "vitest";

import {
        COLLAPSE_FLOOR,
        VISION_CONDITIONS,
        chromaticDistanceUnder,
        contrastRatio,
        distanceUnder,
        measureOklch,
        monotonicityViolations,
        oklabDistance,
        simulate,
        unorderedPairs,
} from "./perceptual.mjs";

describe("measureOklch", () => {
        it("reads pure black and pure white at the ends of the lightness range", () => {
                expect(measureOklch("#000000").l).toBeCloseTo(0, 5);
                expect(measureOklch("#ffffff").l).toBeCloseTo(1, 5);
        });

        it("reports achromatic colours as zero chroma, never NaN", () => {
                for (const grey of ["#000000", "#808080", "#ffffff"]) {
                        const measured = measureOklch(grey);
                        expect(measured.c).toBeCloseTo(0, 4);
                        expect(Number.isNaN(measured.c)).toBe(false);
                }
        });

        it("gives a mid grey a lightness between the extremes", () => {
                const mid = measureOklch("#808080").l;
                expect(mid).toBeGreaterThan(0.4);
                expect(mid).toBeLessThan(0.7);
        });

        it("throws on an unparseable value rather than returning a silent zero", () => {
                expect(() => measureOklch("not-a-colour")).toThrow(/parseable/);
        });

        it("agrees with the oklch value committed by the generator", () => {
                // The generator writes oklch(0.3 0.038 152) for success-30 and its hex
                // alongside. Reading the hex back must land on the authored value, or the
                // gate and the design tool disagree about what shipped.
                //
                // Hue tolerance is 1 degree, not tighter: the shipped value is an 8-bit
                // hex after chroma-reduction gamut mapping, and at C=0.0375 a 0.5 degree
                // hue shift is a displacement of ~0.0003 in OKLab a-b, well inside one
                // sRGB step at this darkness. Measured drift is 0.52 degrees.
                const measured = measureOklch("#1f3324");
                expect(measured.l).toBeCloseTo(0.299, 2);
                expect(measured.c).toBeCloseTo(0.0375, 3);
                expect(measured.h).toBeCloseTo(152, -0.3);
                expect(Math.abs(measured.h - 152)).toBeLessThan(1);
        });
});

describe("oklabDistance", () => {
        it("is zero for a colour against itself", () => {
                expect(oklabDistance("#1f3324", "#1f3324")).toBeCloseTo(0, 6);
        });

        it("is symmetric", () => {
                const forward = oklabDistance("#000000", "#ffffff");
                const backward = oklabDistance("#ffffff", "#000000");
                expect(forward).toBeCloseTo(backward, 9);
        });

        it("equals the lightness difference when only lightness differs", () => {
                // The derivation COLLAPSE_FLOOR rests on: OKLab distance between two
                // achromatic colours is |dL|. If this fails, the floor is not
                // dimensionally consistent with a tone step and the spec's derivation is
                // wrong.
                const black = measureOklch("#000000").l;
                const white = measureOklch("#ffffff").l;
                expect(oklabDistance("#000000", "#ffffff")).toBeCloseTo(white - black, 4);
        });

        it("separates black from white by very much more than the collapse floor", () => {
                expect(oklabDistance("#000000", "#ffffff")).toBeGreaterThan(COLLAPSE_FLOOR * 10);
        });
});

describe("simulate", () => {
        it("returns the input unchanged under normal vision", () => {
                expect(simulate("#1f3324", "normal")).toBe("#1f3324");
        });

        it("leaves greys untouched under every condition", () => {
                // A dichromacy model must be identity on the achromatic axis. This is the
                // strongest hand-checkable property of the Machado et al. matrices: with
                // r = g = b, any row that sums to 1 returns the same value.
                for (const condition of VISION_CONDITIONS) {
                        expect(simulate("#808080", condition)).toBe("#808080");
                        expect(simulate("#ffffff", condition)).toBe("#ffffff");
                        expect(simulate("#000000", condition)).toBe("#000000");
                }
        });

        it("collapses the red-green HUE distinction under both red-green dichromacies", () => {
                // The defining behaviour, measured on the chromatic plane. Pure red and
                // pure green sit 113 degrees apart under normal vision and land within a
                // couple of degrees of each other under both conditions.
                const normalGap = chromaticDistanceUnder("#ff0000", "#00ff00", "normal");
                expect(chromaticDistanceUnder("#ff0000", "#00ff00", "deuteranopia")).toBeLessThan(
                        normalGap * 0.5,
                );
                expect(chromaticDistanceUnder("#ff0000", "#00ff00", "protanopia")).toBeLessThan(
                        normalGap * 0.5,
                );
        });

        it("drives long-wavelength red much darker under protanopia", () => {
                // The W3C names this as WCAG 2.x's own blind spot: "predominantly long
                // wavelength colors against darker colors ... for those who have
                // protanopia". Pinned as a measurement because this product renders
                // `rejected` in danger at hue 18 on a dark background.
                const normalRed = measureOklch(simulate("#ff0000", "normal")).l;
                const protanRed = measureOklch(simulate("#ff0000", "protanopia")).l;
                expect(protanRed).toBeLessThan(normalRed * 0.5);
        });

        it("can INCREASE total distance while collapsing hue, so the two measures differ", () => {
                // Counter-intuitive and load-bearing: protanopia collapses the red-green
                // hue difference to ~2 degrees yet raises total OKLab distance above
                // normal vision, because red loses so much lightness. A separability
                // criterion must therefore include lightness, and a hue-collapse
                // diagnostic must exclude it.
                const normalTotal = distanceUnder("#ff0000", "#00ff00", "normal");
                expect(distanceUnder("#ff0000", "#00ff00", "protanopia")).toBeGreaterThan(
                        normalTotal,
                );
                expect(distanceUnder("#ff0000", "#00ff00", "deuteranopia")).toBeLessThan(
                        normalTotal,
                );
        });

        it("preserves a blue-yellow difference better under deuteranopia than red-green", () => {
                const redGreen = chromaticDistanceUnder("#ff0000", "#00ff00", "deuteranopia");
                const blueYellow = chromaticDistanceUnder("#0000ff", "#ffff00", "deuteranopia");
                expect(blueYellow).toBeGreaterThan(redGreen);
        });

        it("removes all chroma under achromatopsia", () => {
                for (const vivid of ["#ff0000", "#00ff00", "#0000ff", "#1f3324"]) {
                        expect(measureOklch(simulate(vivid, "achromatopsia")).c).toBeLessThan(0.01);
                }
        });

        it("rejects an unknown condition instead of silently passing it through", () => {
                expect(() => simulate("#ffffff", "supervision")).toThrow(/Unknown vision condition/);
        });
});

describe("distanceUnder", () => {
        it("simulates both colours, not just one", () => {
                // Simulating one side would measure the deficiency rather than the pair's
                // separability under it, and would report a large distance for two colours
                // that a dichromat sees as identical.
                const red = "#ff0000";
                const green = "#00ff00";
                const bothSimulated = distanceUnder(red, green, "deuteranopia");
                const oneSimulated = oklabDistance(simulate(red, "deuteranopia"), green);
                expect(bothSimulated).not.toBeCloseTo(oneSimulated, 3);
        });
});

describe("monotonicityViolations", () => {
        it("finds nothing in a strictly increasing ladder", () => {
                expect(monotonicityViolations([0.1, 0.2, 0.3, 0.4])).toEqual([]);
        });

        it("reports a fall", () => {
                const violations = monotonicityViolations([0.1, 0.3, 0.2]);
                expect(violations).toHaveLength(1);
                expect(violations[0]).toMatchObject({ index: 2, kind: "falls" });
        });

        it("treats flat as a violation because the law says strictly", () => {
                const violations = monotonicityViolations([0.1, 0.2, 0.2]);
                expect(violations).toHaveLength(1);
                expect(violations[0]).toMatchObject({ index: 2, kind: "flat" });
        });

        it("counts every failing pair, not just the first", () => {
                // The measured light-theme chroma ladder shape: rises once, then falls,
                // falls, flat, falls, flat.
                const violations = monotonicityViolations([
                        0.00637, 0.08522, 0.05769, 0.04153, 0.04153, 0.03751, 0.03751,
                ]);
                expect(violations).toHaveLength(5);
                expect(violations.map((entry) => entry.kind)).toEqual([
                        "falls",
                        "falls",
                        "flat",
                        "falls",
                        "flat",
                ]);
        });

        it("says nothing about a ladder too short to have a pair", () => {
                expect(monotonicityViolations([])).toEqual([]);
                expect(monotonicityViolations([0.5])).toEqual([]);
        });
});

describe("contrastRatio", () => {
        it("gives the known extreme for black on white", () => {
                // WCAG 2.x: (1.0 + 0.05) / (0.0 + 0.05) = 21.
                expect(contrastRatio("#000000", "#ffffff")).toBeCloseTo(21, 4);
        });

        it("gives 1 for a colour against itself", () => {
                expect(contrastRatio("#123456", "#123456")).toBeCloseTo(1, 6);
        });
});

describe("unorderedPairs", () => {
        it("produces n(n-1)/2 pairs", () => {
                expect(unorderedPairs([1, 2, 3, 4, 5, 6, 7])).toHaveLength(21);
        });

        it("never pairs an item with itself and never repeats a pair", () => {
                const pairs = unorderedPairs(["a", "b", "c"]);
                expect(pairs).toEqual([
                        ["a", "b"],
                        ["a", "c"],
                        ["b", "c"],
                ]);
        });
});
