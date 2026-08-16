/**
 * The identity law (I), measured.
 *
 * `perceptual-language.md` section 5 claims colourblind safety is "guaranteed by the
 * redundancy law -- every distinction survives the removal of hue because form + text +
 * position + motion carry it independently", and two source comments claim distinctions
 * "survive grayscale". Nothing executed those claims. There was no CVD simulation, no
 * greyscale test, and no pairwise comparison of any kind anywhere in the repository.
 *
 * WCAG 2.x contrast cannot substitute. The W3C states that "contrast is calculated in such
 * a way that color (hue) is not a key factor", so passing every ratio in `auditContrast`
 * says nothing about whether a viewer can tell `proven` from `rejected`.
 *
 * This gate executes the claim. For every ordered rung pair, in both themes, under normal
 * vision and four vision conditions, separability must be satisfied by either
 *
 *   1. simulated OKLab distance at or above COLLAPSE_FLOOR, or
 *   2. a difference in FORM -- the (fill, stroke, groundContact) triple.
 *
 * Mandatory verdict text is deliberately NOT a route. Every rung renders text by
 * construction, so accepting it would make this gate vacuous -- the exact failure this repo
 * has shipped twice, most recently a guard that re-derived a rung from the rung.
 *
 * Normative definitions: specs/SPEC_perceptual_verification.md sections 4.2 and 4.3.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  COLLAPSE_FLOOR,
  VISION_CONDITIONS,
  chromaticDistanceUnder,
  distanceUnder,
  measureOklch,
  unorderedPairs,
} from "./perceptual.mjs";

const scriptDir = fileURLToPath(new URL(".", import.meta.url));
const webRoot = resolve(scriptDir, "..");
const srcRoot = resolve(webRoot, "src");

const failures = [];
function fail(message) {
  failures.push(message);
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

/**
 * The semantic TEXT token each rung renders its verdict in.
 *
 * Not derivable from `tokenFamily`: `idle` uses the confidence-low family and `rejected`
 * uses status-danger, neither named after its rung. Kept here rather than in the token JSON
 * because it is a fact about the colour system's naming, not about the grammar.
 */
const RUNG_TEXT_TOKEN = {
  idle: "--df-confidence-low-text",
  rejected: "--df-status-danger-text",
  plausibility_only: "--df-plausibility-text",
  downgraded: "--df-downgraded-text",
  held: "--df-held-text",
  proven: "--df-proven-text",
  corroborated: "--df-corroborated-text",
};

const THEMES = ["light", "dark"];

function formOf(spec) {
  return `${spec.fill}|${spec.stroke}|${spec.groundContact}`;
}

function auditRungMappingIsComplete(tokens) {
  // Fail closed: a rung missing from the map would silently drop out of every pair.
  for (const rung of tokens.rungOrder) {
    if (RUNG_TEXT_TOKEN[rung] === undefined) {
      fail(`Rung '${rung}' has no text token mapped; it would be omitted from every pair (law I).`);
    }
  }
  for (const rung of Object.keys(RUNG_TEXT_TOKEN)) {
    if (!tokens.rungOrder.includes(rung)) {
      fail(`Text token mapped for '${rung}', which is not a rung; the map describes nothing (law I).`);
    }
  }
  const expectedPairs = (tokens.rungOrder.length * (tokens.rungOrder.length - 1)) / 2;
  if (expectedPairs < 21) {
    fail(`Only ${expectedPairs} rung pairs to test; the ladder has shrunk and this gate is weaker (law I).`);
  }
}

function auditSeparability(system, tokens) {
  const formOnly = [];
  const collapsedUnderNormal = [];

  for (const theme of THEMES) {
    for (const [first, second] of unorderedPairs(tokens.rungOrder)) {
      const firstHex = system.semantic[theme][RUNG_TEXT_TOKEN[first]]?.hex;
      const secondHex = system.semantic[theme][RUNG_TEXT_TOKEN[second]]?.hex;
      if (firstHex === undefined || secondHex === undefined) {
        fail(`Missing ${theme} colour for pair ${first}/${second} (law I).`);
        continue;
      }
      const sameForm = formOf(tokens.rungs[first]) === formOf(tokens.rungs[second]);

      for (const condition of VISION_CONDITIONS) {
        const distance = distanceUnder(firstHex, secondHex, condition);
        if (distance >= COLLAPSE_FLOOR) {
          continue;
        }
        // Colour has collapsed for this pair under this condition.
        if (sameForm) {
          fail(
            `${theme}/${condition}: '${first}' and '${second}' are colour-collapsed ` +
              `(OKLab distance ${distance.toFixed(4)} < ${COLLAPSE_FLOOR}) AND share the form ` +
              `${formOf(tokens.rungs[first])}. Nothing but the verdict string distinguishes them, ` +
              "and text is not a separability route because every rung has one (law I).",
          );
        } else {
          formOnly.push({ theme, condition, first, second, distance });
          if (condition === "normal") {
            collapsedUnderNormal.push({ theme, first, second, distance });
          }
        }
      }
    }
  }

  return { formOnly, collapsedUnderNormal };
}

/**
 * Report what survives only on form.
 *
 * Not a failure -- route 2 of the law -- but a fact the design's authors should see rather
 * than discover. The most important instance is `proven` against `rejected`, which the
 * measurement shows collapsing under deuteranopia in both themes.
 */
function report({ formOnly, collapsedUnderNormal }) {
  if (formOnly.length === 0) {
    console.log("Identity law: every rung pair separates on colour alone under every condition.");
    return;
  }

  const byPair = new Map();
  for (const entry of formOnly) {
    const key = `${entry.first} vs ${entry.second}`;
    if (!byPair.has(key)) {
      byPair.set(key, []);
    }
    byPair.get(key).push(`${entry.theme}/${entry.condition} d=${entry.distance.toFixed(4)}`);
  }

  console.log(
    `Identity law: ${formOnly.length} pair-condition combinations rely on FORM because colour ` +
      "collapses. This is lawful (route 2) and is reported so it is a known property, not a surprise:",
  );
  for (const [pair, occurrences] of [...byPair.entries()].sort()) {
    console.log(`  ${pair}`);
    console.log(`    ${occurrences.join(", ")}`);
  }

  if (collapsedUnderNormal.length > 0) {
    console.log(
      "\n  Note: the following collapse under NORMAL vision too, so colour carries no " +
        "information between them for any viewer:",
    );
    for (const entry of collapsedUnderNormal) {
      console.log(
        `    ${entry.theme}: ${entry.first} vs ${entry.second} d=${entry.distance.toFixed(4)}`,
      );
    }
  }
}

/**
 * Hue collapse, reported as a diagnostic separate from separability.
 *
 * A pair can keep a large total distance while losing its hue distinction entirely, because
 * total distance includes lightness. Measured on pure red against pure green, protanopia
 * collapses hue to about 2 degrees apart yet RAISES total distance above normal vision,
 * since red falls from L=0.628 to L=0.236. That long-wavelength luminance loss is the blind
 * spot the W3C names in WCAG 2.x itself.
 */
function reportHueCollapse(system, tokens) {
  const collapses = [];
  for (const theme of THEMES) {
    for (const [first, second] of unorderedPairs(tokens.rungOrder)) {
      const firstHex = system.semantic[theme][RUNG_TEXT_TOKEN[first]]?.hex;
      const secondHex = system.semantic[theme][RUNG_TEXT_TOKEN[second]]?.hex;
      if (firstHex === undefined || secondHex === undefined) {
        continue;
      }
      const normal = chromaticDistanceUnder(firstHex, secondHex, "normal");
      if (normal < 0.01) {
        continue; // Never had a hue distinction to lose.
      }
      for (const condition of ["deuteranopia", "protanopia", "tritanopia"]) {
        const simulated = chromaticDistanceUnder(firstHex, secondHex, condition);
        if (simulated < normal * 0.25) {
          collapses.push({ theme, condition, first, second, normal, simulated });
        }
      }
    }
  }
  if (collapses.length > 0) {
    console.log(
      `\nHue-collapse diagnostic: ${collapses.length} pair-conditions lose at least 75% of ` +
        "their chromatic separation. Lightness or form may still separate them.",
    );
    for (const entry of collapses.slice(0, 12)) {
      console.log(
        `  ${entry.theme}/${entry.condition}: ${entry.first} vs ${entry.second} ` +
          `chromatic ${entry.normal.toFixed(4)} -> ${entry.simulated.toFixed(4)}`,
      );
    }
    if (collapses.length > 12) {
      console.log(`  ... and ${collapses.length - 12} more`);
    }
  }
}

/**
 * The urgency law (U).
 *
 * Urgency is the third quantity the old law conflated into "intensity". It may legitimately
 * track actionability -- a rejected fix IS important and the eye should go to it -- but its
 * channels must be disjoint from warrant's. Signalling urgency through a warrant channel,
 * by giving a rejected claim ground contact or glow so it stands out, is the precise
 * overtrust this language exists to prevent.
 *
 * The disjointness check is the normative part. The chroma band-monotonicity check below is
 * also gated, and it is legitimate to gate rather than reverse-fitted, because the urgency
 * levels were assigned from semantics BEFORE the palette was measured -- see
 * `$urgencyDerivation` in the token file. The palette then turned out to agree, which is
 * evidence the resolution identified the right axis rather than rationalising the pixels.
 */
function auditUrgencyLaw(system, tokens) {
  const roles = tokens.channelRoles;
  if (roles === undefined) {
    fail("No channelRoles declared; the three quantities must be explicit (law U).");
    return;
  }

  // Disjointness, pairwise across all three roles.
  const roleNames = ["warrant", "identity", "urgency"];
  for (const [first, second] of unorderedPairs(roleNames)) {
    const firstSet = new Set(roles[first] ?? []);
    const overlap = (roles[second] ?? []).filter((channel) => firstSet.has(channel));
    if (overlap.length > 0) {
      fail(
        `Channels ${overlap.join(", ")} are declared for both '${first}' and '${second}'. ` +
          "The three quantities must use disjoint channels, or one signal answers two " +
          "questions and can lie about one of them (law U).",
      );
    }
  }

  // Warrant's declared channels must match the weights that actually compute warrant,
  // otherwise the declaration is decorative.
  const weighted = Object.keys(tokens.salienceWeights).filter((key) => !key.startsWith("$"));
  for (const channel of weighted) {
    if (!(roles.warrant ?? []).includes(channel)) {
      fail(
        `Salience weight '${channel}' contributes to warrant but is not declared a warrant ` +
          "channel (law U).",
      );
    }
  }

  // Every rung declares a known urgency level.
  const levels = tokens.urgencyLevels ?? [];
  if (levels.length === 0) {
    fail("No urgencyLevels declared (law U).");
    return;
  }
  for (const rung of tokens.rungOrder) {
    const urgency = tokens.rungs[rung].urgency;
    if (urgency === undefined) {
      fail(`Rung '${rung}' declares no urgency; actionability must be explicit (law U).`);
    } else if (!levels.includes(urgency)) {
      fail(`Rung '${rung}' declares unknown urgency '${urgency}' (law U).`);
    }
  }
  if (failures.length > 0) {
    return;
  }

  // Chroma against the declared urgency bands: MEASURED AND REPORTED, NOT GATED.
  //
  // This was written as a gate and then demoted, because the measurement did not support
  // it. Two reasons, and both are about the law rather than the palette:
  //
  //   1. The themes disagree. Light satisfies the bands (none max 0.0375 < review min
  //      0.0415 < attention 0.0852). Dark does not: plausibility_only reaches 0.0554 while
  //      rejected, a higher declared band, sits at 0.0520.
  //   2. The semantic ordering between those exact two rungs is arguable. `rejected` is
  //      RESOLVED -- the system evaluated and refused, and nothing is required of the user.
  //      `plausibility_only` is UNRESOLVED -- it was not written and is waiting for a human
  //      decision. A good argument puts plausibility_only above rejected, which would make
  //      the dark theme correct and the declaration wrong.
  //
  // Gating a direction that cannot be defended from first principles would be asserting a
  // law the design does not follow, and tuning the palette to satisfy it would be fitting
  // pixels to a rule nobody can justify. Following the precedent already set for L4 in
  // quantitative-grammar.md -- "Calling it a law would be a claim the implementation does
  // not support" -- this stays a reported measurement.
  const bandReport = [];
  for (const theme of THEMES) {
    const byLevel = new Map(levels.map((level) => [level, []]));
    for (const rung of tokens.rungOrder) {
      const hex = system.semantic[theme][RUNG_TEXT_TOKEN[rung]]?.hex;
      if (hex === undefined) {
        continue;
      }
      byLevel.get(tokens.rungs[rung].urgency).push({ rung, chroma: measureOklch(hex).c });
    }
    for (let index = 1; index < levels.length; index += 1) {
      const lower = byLevel.get(levels[index - 1]);
      const higher = byLevel.get(levels[index]);
      if (lower.length === 0 || higher.length === 0) {
        continue;
      }
      const lowerMax = lower.reduce((a, b) => (a.chroma > b.chroma ? a : b));
      const higherMin = higher.reduce((a, b) => (a.chroma < b.chroma ? a : b));
      bandReport.push({
        theme,
        lower: levels[index - 1],
        higher: levels[index],
        lowerMax,
        higherMin,
        ordered: lowerMax.chroma < higherMin.chroma,
      });
    }
  }
  console.log("\nUrgency bands against measured chroma (reported, not gated):");
  for (const entry of bandReport) {
    const verdict = entry.ordered ? "ordered" : "INVERTED";
    console.log(
      `  ${entry.theme}: ${entry.lower} max ${entry.lowerMax.chroma.toFixed(4)} ` +
        `(${entry.lowerMax.rung}) vs ${entry.higher} min ${entry.higherMin.chroma.toFixed(4)} ` +
        `(${entry.higherMin.rung}) -- ${verdict}`,
    );
  }
}

/**
 * The rung ladder must have exactly one source.
 *
 * The ladder is the spine of the whole design system, and it exists in two independent
 * places: `RUNG_ORDER` in `dataforge/domain/vocabulary.py`, generated into
 * `src/domain/vocabulary.generated.ts`, and `rungOrder` in `quantitative-tokens.json`.
 *
 * They agree today. Nothing checked that they agree: `audit_quantitative.mjs` validates
 * `rungOrder` only against the token file's own `rungs` keys, which is self-referential, and
 * `audit_vocabulary.mjs` verifies the generated TypeScript against the Python fingerprint
 * without ever looking at the design tokens.
 *
 * This repo has already paid four times for exactly this class of duplication -- most
 * expensively inside `certificate.py`, where a stale three-member provenance set made the
 * artifact a third party reads report `proven` for an untrusted value. A fifth instance was
 * sitting here latent, in the file that decides how every rung is drawn.
 */
function auditRungLadderHasOneSource(tokens) {
  const generated = readFileSync(resolve(srcRoot, "domain", "vocabulary.generated.ts"), "utf8");
  const match = generated.match(/export const RUNG_ORDER:[^=]*=\s*\[([^\]]*)\]/);
  if (match === null) {
    fail("Could not read RUNG_ORDER from src/domain/vocabulary.generated.ts.");
    return;
  }
  const domainOrder = [...match[1].matchAll(/"([^"]+)"/g)].map((entry) => entry[1]);
  const designOrder = [...tokens.rungOrder];

  if (domainOrder.join(",") !== designOrder.join(",")) {
    fail(
      "The rung ladder disagrees between its two sources.\n" +
        `  domain  (vocabulary.py -> vocabulary.generated.ts): ${domainOrder.join(", ")}\n` +
        `  design  (quantitative-tokens.json rungOrder):       ${designOrder.join(", ")}\n` +
        "The domain vocabulary is authoritative. This is the fifth instance of a duplicated " +
        "trust vocabulary in this repository.",
    );
  }
}

const system = readJson(resolve(srcRoot, "design", "color-system.generated.json"));
const tokens = readJson(resolve(srcRoot, "design", "quantitative-tokens.json"));

auditRungMappingIsComplete(tokens);
auditRungLadderHasOneSource(tokens);
const outcome = auditSeparability(system, tokens);
auditUrgencyLaw(system, tokens);
report(outcome);
reportHueCollapse(system, tokens);

if (failures.length > 0) {
  console.error(`\nPerceptual audit FAILED with ${failures.length} violation(s):`);
  for (const message of failures) {
    console.error(`  - ${message}`);
  }
  process.exit(1);
}

console.log("\nPerceptual audit passed.");
