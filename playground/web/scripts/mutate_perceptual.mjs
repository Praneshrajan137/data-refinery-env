/**
 * Mutation harness for the perceptual gates.
 *
 * A gate whose mutants survive is not a gate. This breaks each new law in the most plausible
 * way and requires the corresponding gate to fail.
 *
 * The harness earns its keep by construction: the gates added here replace checks that were
 * `String.prototype.startsWith` on token names, and it was exactly that kind of check which
 * let the one law be violated on its own first-named channel, in both themes, for the entire
 * life of the design system while the build stayed green.
 *
 * Mutants target DISTINCT laws. If several can only be killed by the same gate, then there is
 * one gate under test rather than several.
 *
 * Usage:
 *   node scripts/mutate_perceptual.mjs
 */

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = fileURLToPath(new URL(".", import.meta.url));
const webRoot = resolve(scriptDir, "..");
const srcRoot = resolve(webRoot, "src");

const QUANTITATIVE_TOKENS = resolve(srcRoot, "design", "quantitative-tokens.json");
const MOTION_TOKENS = resolve(srcRoot, "design", "motion-tokens.json");
const COLOUR_GENERATOR = resolve(webRoot, "scripts", "generate_color_system.mjs");
const STYLES = resolve(srcRoot, "styles.css");
const VIZ_TOKENS = resolve(srcRoot, "viz", "tokens.ts");
const GRAMMAR = resolve(srcRoot, "viz", "grammar.ts");
const CONFIDENCE = resolve(srcRoot, "viz", "confidence.ts");

/** Each runner is a gate. A mutant names the runner that must catch it.
 *
 * `colours` runs the generator's `--check` before the audit, because that is what
 * `npm run colors:check` does and what the build does. Without it, a mutant that edits the
 * generator survives: `audit_colors.mjs` reads the GENERATED artifact, so a generator change
 * alone is invisible to it. That was a real survivor on the first run of this harness.
 */
const RUNNERS = {
  perceptual: [["scripts/audit_perceptual.mjs"]],
  colours: [
    ["scripts/generate_color_system.mjs", "--check"],
    ["scripts/audit_colors.mjs"],
  ],
  motion: [
    ["scripts/generate_motion_system.mjs", "--check"],
    ["scripts/audit_motion.mjs"],
  ],
  quantitative: [["scripts/audit_quantitative.mjs"]],
};

/**
 * JSON mutants operate on the PARSED object, not on source text.
 *
 * String replacement produced five NO-OP mutants on the first run of this harness -- silent
 * passes, counted as survivors -- because the token files are pretty-printed across multiple
 * lines and the patterns assumed single-line JSON. A NO-OP mutant is worse than a failing one:
 * it proves nothing while looking like a test.
 */
const MUTANTS = [
  {
    name: "chroma is smuggled into the warrant channels",
    law: "W",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.channelRoles.warrant.push("chroma");
    },
  },
  {
    name: "a colour source is imported into the warrant computation",
    law: "W",
    runner: "colours",
    path: GRAMMAR,
    apply: (source) => `${source}\n// mutated\nconst _leak = "oklch";\n`,
  },
  {
    name: "the warrant channel set drifts from the salience weights",
    law: "U",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.channelRoles.warrant = tokens.channelRoles.warrant.filter(
        (channel) => channel !== "accent",
      );
    },
  },
  {
    name: "urgency borrows a warrant channel",
    law: "U",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.channelRoles.urgency.push("glow");
    },
  },
  {
    name: "a rung stops declaring its urgency",
    law: "U",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      delete tokens.rungs.rejected.urgency;
    },
  },
  {
    name: "a rung declares an urgency level that does not exist",
    law: "U",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.rungs.held.urgency = "screaming";
    },
  },
  {
    name: "corroborated loses the witness rail and collapses onto proven",
    law: "I",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.rungs.corroborated.stroke = "solid";
    },
  },
  {
    name: "two rungs share both a colour and a form",
    law: "I",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    // held and downgraded already share an exact hex; making their forms identical removes
    // the only route by which that pair is separable at all.
    json: (tokens) => {
      tokens.rungs.downgraded.stroke = tokens.rungs.held.stroke;
      tokens.rungs.downgraded.groundContact = tokens.rungs.held.groundContact;
      tokens.rungs.downgraded.fill = tokens.rungs.held.fill;
    },
  },
  {
    name: "the design rung order drifts from the domain vocabulary",
    law: "one source",
    runner: "perceptual",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      const order = tokens.rungOrder;
      [order[1], order[2]] = [order[2], order[1]];
    },
  },
  {
    name: "the high-contrast action border is downgraded below its own background",
    law: "high contrast",
    runner: "colours",
    path: COLOUR_GENERATOR,
    // The exact defect this gate was written to catch: 1.46:1 against brand-30.
    apply: (source) =>
      source.replace('"--df-action-border": "brand-80",', '"--df-action-border": "brand-40",'),
  },
  {
    name: "a literal animation duration is reintroduced",
    law: "motion ceiling",
    runner: "motion",
    path: STYLES,
    apply: (source) =>
      source.replace(
        "animation: spin var(--df-motion-cycle) linear infinite;",
        "animation: spin 1.15s linear infinite;",
      ),
  },
  {
    name: "an undeclared keyframe loops forever",
    law: "loop honesty",
    runner: "motion",
    path: STYLES,
    apply: (source) =>
      `${source}\n@keyframes df-mutant-pulse {\n  from { opacity: 0; }\n  to { opacity: 1; }\n}\n.mutant { animation: df-mutant-pulse var(--df-motion-cycle) linear infinite; }\n`,
  },
  {
    name: "a keyframe animates a layout property",
    law: "GPU safety",
    runner: "motion",
    path: STYLES,
    apply: (source) =>
      `${source}\n@keyframes df-mutant-grow {\n  from { width: 0; }\n  to { width: 100%; }\n}\n`,
  },
  {
    name: "a cyclic animation's declared duration stops matching the CSS it uses",
    law: "motion ceiling",
    runner: "motion",
    path: MOTION_TOKENS,
    json: (tokens) => {
      tokens.cyclicAnimations.spin.duration = "max";
    },
  },
  {
    name: "forced colours support is removed from the canvas bridge",
    law: "forced colours",
    runner: "colours",
    path: VIZ_TOKENS,
    apply: (source) => source.replace(/forcedColoursInk/g, "unusedInk"),
  },
  {
    name: "a magnitude component stops rendering a scale",
    law: "L1a",
    runner: "quantitative",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.components["confidence-distribution"].rendersScale = false;
    },
  },
  {
    name: "a proportion is drawn as a point with no interval and no exemption",
    law: "L6",
    runner: "quantitative",
    path: QUANTITATIVE_TOKENS,
    json: (tokens) => {
      tokens.components["confidence-distribution"].rendersInterval = false;
    },
  },
  {
    name: "the histogram stops using the shared interval implementation",
    law: "L6",
    runner: "quantitative",
    path: CONFIDENCE,
    apply: (source) =>
      source
        .replace('import { proportionInterval } from "./interval";', "")
        .replace(
          /proportionInterval\(bin\.count, entry\.count\)/,
          "{ estimate: 0, lower: 0, upper: 0 }",
        ),
  },
];

function runGate(runner) {
  for (const argv of RUNNERS[runner]) {
    try {
      execFileSync(process.execPath, argv, { cwd: webRoot, stdio: "pipe" });
    } catch (error) {
      return error.status ?? 1;
    }
  }
  return 0;
}

function baselineGreen() {
  for (const runner of Object.keys(RUNNERS)) {
    if (runGate(runner) !== 0) {
      console.error(`BASELINE FAILS for ${runner}; fix the gates before mutating.`);
      return false;
    }
  }
  return true;
}

function mutate(mutant, original) {
  if (mutant.json !== undefined) {
    const parsed = JSON.parse(original);
    mutant.json(parsed);
    return `${JSON.stringify(parsed, null, 2)}\n`;
  }
  return mutant.apply(original);
}

function main() {
  if (!baselineGreen()) {
    return 1;
  }
  console.log(`baseline: all ${Object.keys(RUNNERS).length} perceptual gates pass on clean source`);

  const survivors = [];
  for (const mutant of MUTANTS) {
    const original = readFileSync(mutant.path, "utf8");
    const mutated = mutate(mutant, original);
    if (mutated === original) {
      console.log(`NO-OP     ${mutant.name}`);
      survivors.push(mutant.name);
      continue;
    }
    writeFileSync(mutant.path, mutated, { encoding: "utf8" });
    try {
      const status = runGate(mutant.runner);
      if (status === 0) {
        console.log(`SURVIVED  ${mutant.name} [law ${mutant.law}]`);
        survivors.push(mutant.name);
      } else {
        console.log(`killed    ${mutant.name} [law ${mutant.law}]`);
      }
    } finally {
      writeFileSync(mutant.path, original, { encoding: "utf8" });
    }
  }

  if (!baselineGreen()) {
    console.error("RESTORE FAILED: the gates do not pass after restoring the source.");
    return 1;
  }

  const killed = MUTANTS.length - survivors.length;
  console.log(`\n${killed}/${MUTANTS.length} mutants killed`);
  if (survivors.length > 0) {
    console.log("survivors mean the gate does not actually verify its law:");
    for (const name of survivors) {
      console.log(`  - ${name}`);
    }
    return 1;
  }
  console.log("source restored, gates green");
  return 0;
}

process.exit(main());
