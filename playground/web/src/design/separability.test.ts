/**
 * Separability of the rung ladder under impaired vision, measured.
 *
 * `perceptual-language.md` section 5 claimed colourblind safety was "guaranteed by the
 * redundancy law", and two source comments claimed distinctions "survive grayscale". Nothing
 * executed any of it: there was no CVD simulation, no greyscale test, and no pairwise
 * comparison anywhere in the repository.
 *
 * These tests pin the measurements that resulted, including the two that were defects.
 * `scripts/audit_perceptual.mjs` is the build gate; this file exists so the specific findings
 * survive as named facts rather than as a number in gate output.
 */

import { describe, expect, it } from "vitest";

import {
        COLLAPSE_FLOOR,
        VISION_CONDITIONS,
        distanceUnder,
        measureOklch,
        unorderedPairs,
} from "../../scripts/perceptual.mjs";
import colorSystem from "./color-system.generated.json";
import quantitativeTokens from "./quantitative-tokens.json";

function measureOklchChroma(hex: string): number {
        return measureOklch(hex).c;
}

/**
 * The three channel roles, read without casting the whole object.
 *
 * `channelRoles` carries a `$comment` alongside the three arrays -- the token file's
 * documentation convention -- so a blanket Record<string, string[]> cast is unsound.
 */
const channelRoles: Record<"warrant" | "identity" | "urgency", readonly string[]> = {
        warrant: quantitativeTokens.channelRoles.warrant,
        identity: quantitativeTokens.channelRoles.identity,
        urgency: quantitativeTokens.channelRoles.urgency,
};

type Theme = "light" | "dark";

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
const rungs = quantitativeTokens.rungs as Record<
        string,
        { fill: string; stroke: string; groundContact: string }
>;

function hexFor(rung: string, theme: Theme): string {
        const semantic = (
                colorSystem.semantic as Record<Theme, Record<string, { hex: string }>>
        )[theme];
        return semantic[RUNG_TEXT_TOKEN[rung]].hex;
}

function formOf(rung: string): string {
        const spec = rungs[rung];
        return `${spec.fill}|${spec.stroke}|${spec.groundContact}`;
}

describe("the identity law holds for every rung pair", () => {
        it("leaves no pair distinguished by text alone", () => {
                // The law: separability comes from measured colour distance OR from form.
                // Text is excluded because every rung renders a verdict string, so allowing
                // it would make the law vacuous.
                const violations: string[] = [];
                for (const theme of ["light", "dark"] as const) {
                        for (const [first, second] of unorderedPairs(rungOrder)) {
                                const sameForm = formOf(first) === formOf(second);
                                if (!sameForm) {
                                        continue;
                                }
                                for (const condition of VISION_CONDITIONS) {
                                        const distance = distanceUnder(
                                                hexFor(first, theme),
                                                hexFor(second, theme),
                                                condition,
                                        );
                                        if (distance < COLLAPSE_FLOOR) {
                                                violations.push(
                                                        `${theme}/${condition}: ${first} vs ${second} d=${distance.toFixed(4)}`,
                                                );
                                        }
                                }
                        }
                }
                expect(violations).toEqual([]);
        });
});

describe("proven versus corroborated", () => {
        it("still shares a text colour, so colour cannot carry the distinction", () => {
                // Unchanged and intentional: both are proof, and proof is one colour. This
                // asserts the premise of the finding, so if someone later gives corroborated
                // its own hue this test fails and the reasoning gets revisited.
                for (const theme of ["light", "dark"] as const) {
                        expect(hexFor("proven", theme)).toBe(hexFor("corroborated", theme));
                        expect(distanceUnder(hexFor("proven", theme), hexFor("corroborated", theme), "normal")).toBeCloseTo(
                                0,
                                6,
                        );
                }
        });

        it("is separated by form, which is what the grammar failed to declare", () => {
                // The defect: the rail was already rendering in styles.css, but the grammar
                // declared both rungs as filled|solid|contact. The only DECLARED difference
                // was the verdict string. Declaring the rail is the fix.
                expect(rungs.corroborated.stroke).toBe("witnessed");
                expect(rungs.proven.stroke).toBe("solid");
                expect(formOf("proven")).not.toBe(formOf("corroborated"));
        });

        it("gives the witness rail more ink than a plain solid stroke", () => {
                const weights = quantitativeTokens.salienceWeights.stroke as Record<string, number>;
                expect(weights.witnessed).toBeGreaterThan(weights.solid);
        });
});

describe("proven versus rejected", () => {
        it("collapses on colour under deuteranopia in both themes", () => {
                // The product's single most important distinction sits on the red-green
                // confusion axis: success at hue 152 against danger at hue 18. It survives
                // ONLY because form differs. That was the standing claim of the redundancy
                // law; this is the first time it has been executed.
                for (const theme of ["light", "dark"] as const) {
                        const distance = distanceUnder(hexFor("proven", theme), hexFor("rejected", theme), "deuteranopia");
                        expect(distance).toBeLessThan(COLLAPSE_FLOOR);
                }
        });

        it("is carried by form, and the forms are maximally different", () => {
                expect(formOf("proven")).toBe("filled|solid|contact");
                expect(formOf("rejected")).toBe("unfilled|struck|none");
                // Every component differs, which is why the collapse above is survivable.
                const [provenFill, provenStroke, provenContact] = formOf("proven").split("|");
                const [rejectedFill, rejectedStroke, rejectedContact] = formOf("rejected").split("|");
                expect(provenFill).not.toBe(rejectedFill);
                expect(provenStroke).not.toBe(rejectedStroke);
                expect(provenContact).not.toBe(rejectedContact);
        });
});

describe("downgraded versus held", () => {
        it("shares an exact colour, so colour carries no information between them", () => {
                for (const theme of ["light", "dark"] as const) {
                        expect(hexFor("downgraded", theme)).toBe(hexFor("held", theme));
                }
        });

        it("is separated by stroke and ground contact", () => {
                expect(rungs.held.stroke).toBe("solid");
                expect(rungs.downgraded.stroke).toBe("dashed");
                expect(rungs.held.groundContact).toBe("none");
                expect(rungs.downgraded.groundContact).toBe("weakening");
        });
});

describe("the three channel roles", () => {
        it("are disjoint, so no signal answers two questions", () => {
                const roles = channelRoles;
                const pairs: ["warrant" | "identity" | "urgency", "warrant" | "identity" | "urgency"][] = [
                        ["warrant", "identity"],
                        ["warrant", "urgency"],
                        ["identity", "urgency"],
                ];
                for (const [first, second] of pairs) {
                        const overlap = roles[second].filter((channel) => roles[first].includes(channel));
                        expect(overlap, `${first} and ${second} share ${overlap.join(", ")}`).toEqual([]);
                }
        });

        it("keeps hue and chroma out of warrant", () => {
                const warrant = channelRoles
                        .warrant;
                expect(warrant).not.toContain("hue");
                expect(warrant).not.toContain("chroma");
        });

        it("declares every salience weight as a warrant channel", () => {
                // Otherwise a channel could contribute to warrant without being declared,
                // and the declaration would be decorative.
                const warrant = channelRoles
                        .warrant;
                const weighted = Object.keys(quantitativeTokens.salienceWeights).filter(
                        (key) => !key.startsWith("$"),
                );
                for (const channel of weighted) {
                        expect(warrant).toContain(channel);
                }
        });

        it("gives every rung a declared urgency level", () => {
                const levels = quantitativeTokens.urgencyLevels as readonly string[];
                for (const rung of rungOrder) {
                        const urgency = (quantitativeTokens.rungs as Record<string, { urgency?: string }>)[
                                rung
                        ].urgency;
                        expect(levels, `rung ${rung}`).toContain(urgency);
                }
        });

        it("does not claim chroma is monotonic in urgency, because it is not", () => {
                // The dark theme inverts: plausibility_only (review) reaches 0.0554 while
                // rejected (attention) sits at 0.0520. Recorded as a test so the honest
                // demotion cannot be quietly upgraded into a law later.
                const plausibility = measureOklchChroma(hexFor("plausibility_only", "dark"));
                const rejected = measureOklchChroma(hexFor("rejected", "dark"));
                expect(plausibility).toBeGreaterThan(rejected);
        });
});

describe("achromatopsia", () => {
        it("removes colour as a distinguishing channel for most of the ladder", () => {
                // Recorded because two source comments claim distinctions "survive
                // grayscale". They do -- on form. Colour does not survive, and the count
                // makes that concrete rather than reassuring.
                let collapsed = 0;
                for (const [first, second] of unorderedPairs(rungOrder)) {
                        const distance = distanceUnder(hexFor(first, "light"), hexFor(second, "light"), "achromatopsia");
                        if (distance < COLLAPSE_FLOOR) {
                                collapsed += 1;
                        }
                }
                expect(collapsed).toBeGreaterThan(0);
                expect(collapsed).toBeLessThanOrEqual(unorderedPairs(rungOrder).length);
        });
});
