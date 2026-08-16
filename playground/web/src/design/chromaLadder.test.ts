/**
 * The chroma ladder, measured.
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * `perceptual-language.md` used to define the one law as "perceptual intensity is a
 * strictly monotonic function of epistemic strength", with intensity defined as "the sum of
 * a signal's CHROMA, motion amplitude, glow, weight, and form-completeness", and section
 * 7.2 required monotonicity "on every channel (color-chroma, glow, motion amplitude,
 * weight)".
 *
 * That requirement was violated by the shipped palette in both themes, for the whole life
 * of the design system, while `auditEarnedSalience` reported success -- because that gate
 * is five `startsWith()` calls on token names and never reads a colour value.
 *
 * These tests pin the measurement. They ran RED against the old law, which is the evidence
 * that the law needed changing, and they now encode the resolved law: chroma is an
 * IDENTITY/URGENCY channel and carries no warrant, so it is not required to be monotonic --
 * and warrant computations are forbidden from reading it at all.
 *
 * The numbers are asserted as ranges, not exact values, so a deliberate palette change
 * fails loudly here rather than silently redefining the finding.
 */

import { describe, expect, it } from "vitest";

import {
        measureOklch,
        monotonicityViolations,
} from "../../scripts/perceptual.mjs";
import colorSystem from "./color-system.generated.json";
import quantitativeTokens from "./quantitative-tokens.json";

type Theme = "light" | "dark";

/**
 * The semantic TEXT token each rung renders its verdict in.
 *
 * Not derivable from `tokenFamily`: `idle` uses the confidence-low family and `rejected`
 * uses the status-danger family, neither of which is named after its rung.
 */
const RUNG_TEXT_TOKEN: Record<string, string> = {
        idle: "--df-confidence-low-text",
        rejected: "--df-status-danger-text",
        plausibility_only: "--df-plausibility-text",
        downgraded: "--df-downgraded-text",
        held: "--df-held-text",
        proven: "--df-proven-text",
        corroborated: "--df-corroborated-text",
};

const rungOrder = quantitativeTokens.rungOrder as readonly string[];

function hexFor(rung: string, theme: Theme): string {
        const token = RUNG_TEXT_TOKEN[rung];
        const semantic = (colorSystem.semantic as Record<Theme, Record<string, { hex: string; palette: string }>>)[
                theme
        ];
        const entry = semantic[token];
        if (entry === undefined) {
                throw new Error(`No ${theme} value for ${token} (rung ${rung})`);
        }
        return entry.hex;
}

function chromaLadder(theme: Theme): number[] {
        return rungOrder.map((rung) => measureOklch(hexFor(rung, theme)).c);
}

describe("the chroma ladder as shipped", () => {
        it("covers every rung, so the measurement cannot be vacuous", () => {
                expect(rungOrder.length).toBe(7);
                for (const rung of rungOrder) {
                        expect(RUNG_TEXT_TOKEN[rung], `no text token mapped for ${rung}`).toBeDefined();
                        expect(hexFor(rung, "light")).toMatch(/^#[0-9a-f]{6}$/);
                        expect(hexFor(rung, "dark")).toMatch(/^#[0-9a-f]{6}$/);
                }
        });

        it("is NOT monotonic in the light theme: 5 of 6 adjacent pairs fail", () => {
                const violations = monotonicityViolations(chromaLadder("light"));
                expect(violations).toHaveLength(5);
                expect(violations.map((entry) => entry.kind)).toEqual([
                        "falls",
                        "falls",
                        "flat",
                        "falls",
                        "flat",
                ]);
        });

        it("is NOT monotonic in the dark theme: 4 of 6 adjacent pairs fail", () => {
                const violations = monotonicityViolations(chromaLadder("dark"));
                expect(violations).toHaveLength(4);
        });

        it("is close to INVERTED in the light theme once idle is excluded", () => {
                // idle is rank 0, never drawn, and near-achromatic at C=0.006, so it is the
                // one rung whose low chroma agrees with the old law. Excluding it shows the
                // real shape: chroma falls monotonically as warrant rises.
                const withoutIdle = chromaLadder("light").slice(1);
                for (let index = 1; index < withoutIdle.length; index += 1) {
                        expect(
                                withoutIdle[index],
                                `chroma rose from rung ${index} to ${index + 1}, which the old law required and the palette does not do`,
                        ).toBeLessThanOrEqual(withoutIdle[index - 1] + 1e-9);
                }
        });

        it("gives the weakest real rung far more chroma than the strongest", () => {
                const light = chromaLadder("light");
                const rejected = light[rungOrder.indexOf("rejected")];
                const proven = light[rungOrder.indexOf("proven")];
                expect(rejected / proven).toBeGreaterThan(2);
                expect(rejected / proven).toBeLessThan(2.6);
        });

        it("measures the SHIPPED value, not the authored seed", () => {
                // The seeds declare warning.c = 0.066, but `warning-20` gamut-maps down to
                // about 0.042. A gate that audited seed values would report a ladder users
                // never see, so the shipped hex is the only admissible input.
                const seedWarningChroma = colorSystem.seeds.warning.c;
                const shippedHeldChroma = measureOklch(hexFor("held", "light")).c;
                expect(seedWarningChroma).toBeCloseTo(0.066, 3);
                expect(shippedHeldChroma).toBeLessThan(seedWarningChroma * 0.8);
        });
});

describe("chroma is not a warrant channel", () => {
        it("is absent from the salience weights that measure warrant", () => {
                // The structural guarantee behind the resolved law. If chroma were ever
                // added here, `measureRungSalience` would start reading a colour value and
                // the inversion above would become a monotonicity failure again.
                // `$`-prefixed keys are the token file's documentation convention.
                const channels = Object.keys(quantitativeTokens.salienceWeights).filter(
                        (key) => !key.startsWith("$"),
                );
                expect(channels).toEqual(["fill", "stroke", "contact", "glow", "accent"]);
                for (const forbidden of ["chroma", "hue", "saturation", "colour", "color"]) {
                        expect(channels).not.toContain(forbidden);
                }
        });

        it("keeps hue and saturation out of the permitted magnitude channels", () => {
                const permitted = quantitativeTokens.magnitudeChannels as readonly string[];
                const forbidden = quantitativeTokens.forbiddenMagnitudeChannels as readonly string[];
                expect(permitted).not.toContain("hue");
                expect(permitted).not.toContain("saturation");
                expect(forbidden).toContain("hue");
                expect(forbidden).toContain("saturation");
        });
});
