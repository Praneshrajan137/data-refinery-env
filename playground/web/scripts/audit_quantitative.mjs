import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = fileURLToPath(new URL(".", import.meta.url));
const webRoot = resolve(scriptDir, "..");
const srcRoot = resolve(webRoot, "src");
const vizRoot = resolve(srcRoot, "viz");
const tokensPath = resolve(srcRoot, "design", "quantitative-tokens.json");

const tokens = JSON.parse(readFileSync(tokensPath, "utf8"));

function fail(message) {
  throw new Error(message);
}

function walkFiles(dir) {
  const files = [];
  let entries;
  try {
    entries = readdirSync(dir);
  } catch {
    return files;
  }
  for (const item of entries) {
    const path = join(dir, item);
    if (statSync(path).isDirectory()) {
      files.push(...walkFiles(path));
    } else {
      files.push(path);
    }
  }
  return files;
}

const vizFiles = walkFiles(vizRoot).filter((file) => /\.(ts|tsx)$/.test(file));
const vizSources = vizFiles
  .filter((file) => !/\.test\.tsx?$/.test(file))
  .map((file) => ({ file, body: readFileSync(file, "utf8") }));

/**
 * Strip COMMENTS ONLY before checking for forbidden behaviour.
 *
 * The behavioural audits below (hue ramps, accumulating blend, physics layouts,
 * perspective/orbit/parallax) are about what the code DOES. Matching them against
 * comments produces false positives on exactly the files that document why a
 * technique was rejected -- and a gate that punishes explaining itself is a gate
 * someone will eventually weaken instead of satisfy.
 *
 * String literals are deliberately PRESERVED. An earlier version of this helper
 * stripped them too, which silently defanged the blend check: a forbidden blend
 * mode is expressed as a string value (`globalCompositeOperation = "lighter"`),
 * so removing strings removed the very thing being audited. Caught by mutation
 * testing, which is the only reason it was caught at all.
 *
 * The colour-literal audit does not use this: it mirrors the Python contract test,
 * which bans colour literals everywhere including comments.
 */
function codeOnly(body) {
  return body.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/(^|[^:])\/\/[^\n]*/g, "$1 ");
}

const vizCode = vizSources.map(({ file, body }) => ({ file, body: codeOnly(body) }));

// L2/L3/L5 depend on a registry that actually describes shipped components. A gate
// that passes on an empty registry is not a gate -- it is the "14 advisories, 0
// reachable" failure this repo has already paid for once. Fail closed on vacuity.
function auditRegistryIsNotVacuous() {
  const ids = Object.keys(tokens.components ?? {});
  if (ids.length === 0) {
    fail("Quantitative component registry is empty; this gate would pass vacuously.");
  }
  if (vizSources.length === 0) {
    fail(
      `Registry declares ${ids.length} component(s) but src/viz contains no source files; ` +
        "the registry describes nothing.",
    );
  }
}

// L1: magnitude is position or length only. Volume, area, hue and saturation are
// Cleveland & McGill channels 5-7 and are forbidden as magnitude encodings.
function auditQuantityLaw() {
  for (const channel of tokens.forbiddenMagnitudeChannels) {
    if (tokens.magnitudeChannels.includes(channel)) {
      fail(`Channel '${channel}' is both permitted and forbidden as a magnitude encoding.`);
    }
  }
  for (const [id, spec] of Object.entries(tokens.components)) {
    if (!tokens.magnitudeChannels.includes(spec.magnitudeChannel)) {
      fail(
        `Component '${id}' encodes magnitude with '${spec.magnitudeChannel}', ` +
          `which is not one of: ${tokens.magnitudeChannels.join(", ")}.`,
      );
    }
  }
  // A hue ramp in source is a magnitude-by-hue encoding regardless of intent.
  const rampPattern =
    /(interpolate(?:Rgb|Hsl|Lab|Hcl|Viridis|Inferno|Magma|Plasma|Turbo|Rainbow)|scaleSequential|scaleLinear\s*\(\s*\)\s*\.range\s*\(\s*\[\s*["'`]#|hueFor|hueRamp|rainbow|heatmapColor)/;
  for (const { file, body } of vizCode) {
    if (rampPattern.test(body)) {
      fail(`${file} appears to map a magnitude onto hue; hue is semantics, never a ramp (L1).`);
    }
  }
}

// L2: a mark that is not individually addressable may not carry a rung. There is no
// aggregation rule to check, because aggregated marks carry no rung at all.
function auditAddressabilityLaw() {
  if (tokens.aggregation.rungOnAggregatedMarks !== false) {
    fail(
      "Aggregated marks must not carry a rung. Minimum-rung and maximum-rung were both " +
        "tried and rejected; see quantitative-grammar.md section 3.2 (L2).",
    );
  }
  if (tokens.aggregation.neutralDensityChannel !== "count") {
    fail("The only permitted aggregate channel is a neutral count (L2).");
  }
  const forbidden = tokens.aggregation.forbiddenBlendModes;
  if (!forbidden.includes("additive") || !forbidden.includes("lighten")) {
    fail("Additive and lighten blending must remain forbidden everywhere (L2).");
  }

  for (const [id, spec] of Object.entries(tokens.components)) {
    if (typeof spec.addressable !== "boolean") {
      fail(`Component '${id}' must declare whether its marks are addressable (L2).`);
    }
    if (typeof spec.renderer !== "string") {
      fail(`Component '${id}' must declare its renderer (L2).`);
    }
    // The renderer follows the nature of the mark: pixels for density fields with no
    // individual identity, DOM/SVG objects for claims that have identity.
    if (spec.addressable === false && spec.renderer !== "canvas2d") {
      fail(
        `Component '${id}' is non-addressable but renders with '${spec.renderer}'; ` +
          "aggregated density fields are drawn as pixels (L2).",
      );
    }
    if (spec.addressable === true && spec.renderer === "canvas2d") {
      fail(
        `Component '${id}' is addressable but renders to a canvas; a claim with identity ` +
          "must be a DOM object so it is focusable and labelled (L2).",
      );
    }
  }

  // A rung must never be reachable from the encoder of a NON-ADDRESSABLE component.
  // Each component names its encoder explicitly rather than being matched by a
  // filename regex: an earlier version of this check globbed for "density" and
  // matched only the painter, which is rung-free because it consumes rung-DERIVED
  // fields. It therefore passed while the encoder emitted 23 rung references. A gate
  // that inspects the wrong file is not a gate.
  const rungFamilies = Object.values(tokens.rungs).map((spec) => spec.tokenFamily);
  for (const [id, spec] of Object.entries(tokens.components)) {
    const encoderPath = resolve(vizRoot, spec.encoder);
    const encoder = vizSources.find((source) => source.file === encoderPath);
    if (encoder === undefined) {
      fail(
        `Component '${id}' declares encoder '${spec.encoder}', which does not exist under ` +
          "src/viz. The registry must describe real modules (L2).",
      );
    }
    if (spec.addressable === true) {
      continue;
    }
    const body = codeOnly(encoder.body);
    if (/\brung\b/i.test(body)) {
      fail(
        `${spec.encoder} encodes the non-addressable component '${id}' but references a rung; ` +
          "aggregated marks carry no rung (L2).",
      );
    }
    // The word "rung" is not the only way to carry one. An encoder could assign rung
    // NAMES as bare strings and never use the word -- which is exactly the mutant
    // that survived the first run of scripts/mutate_quantitative.mjs. Check the ids
    // themselves, not just the vocabulary that usually accompanies them.
    for (const rungId of Object.keys(tokens.rungs)) {
      if (rungId === "idle") {
        continue; // never drawn anywhere, so its name carries no claim
      }
      if (new RegExp(`\\b${rungId}\\b`).test(body)) {
        fail(
          `${spec.encoder} encodes the non-addressable component '${id}' but names the rung ` +
            `'${rungId}'; aggregated marks carry no rung (L2).`,
        );
      }
    }
    for (const family of rungFamilies) {
      if (family === "confidence-low") {
        continue; // the neutral family, legal in a density ramp
      }
      if (body.includes(`--df-${family}-`)) {
        fail(
          `${spec.encoder} encodes the non-addressable component '${id}' but references the ` +
            `rung token family '--df-${family}-*' (L2).`,
        );
      }
    }
  }

  // An accumulating blend in a renderer converts overplot density into salience.
  const blendPattern =
    /(ONE_MINUS_SRC_ALPHA\s*,\s*gl\.ONE\b|blendFunc\s*\([^)]*gl\.ONE\s*,\s*gl\.ONE\s*\)|globalCompositeOperation\s*=\s*["'`](lighter|screen|color-dodge|lighten)["'`]|blending\s*:\s*["'`]?(additive|Additive))/;
  for (const { file, body } of vizCode) {
    if (blendPattern.test(body)) {
      fail(
        `${file} uses an accumulating blend mode; overplot density would become epistemic ` +
          "intensity (L2).",
      );
    }
  }
}

// L3: zero, not-measured and truncated are three different claims. Every component
// must declare which of them it can be in, and must render them distinguishably.
function auditAbsenceLaw() {
  for (const [id, spec] of Object.entries(tokens.components)) {
    if (!Array.isArray(spec.absenceStates) || spec.absenceStates.length === 0) {
      fail(`Component '${id}' declares no absence state; empty is a claim too (L3).`);
    }
    for (const state of spec.absenceStates) {
      if (!tokens.absenceStates.includes(state)) {
        fail(`Component '${id}' declares unknown absence state '${state}' (L3).`);
      }
    }
  }
  // Every declared absence state must be reachable from source, not just declared.
  const declared = new Set(
    Object.values(tokens.components).flatMap((spec) => spec.absenceStates),
  );
  const joined = vizSources.map(({ body }) => body).join("\n");
  for (const state of declared) {
    if (!joined.includes(state)) {
      fail(`Absence state '${state}' is declared in the registry but never used in src/viz (L3).`);
    }
  }
}

// L4: stillness dominates. At most one looping progress indicator per view.
function auditAttentionBudget() {
  for (const [id, spec] of Object.entries(tokens.components)) {
    if (typeof spec.loopingPrimitives !== "number") {
      fail(`Component '${id}' must declare loopingPrimitives (L4).`);
    }
    if (spec.loopingPrimitives > 1) {
      fail(
        `Component '${id}' declares ${spec.loopingPrimitives} looping primitives; ` +
          "at most one resolve may be in motion per view (L4).",
      );
    }
  }
  // Physics-settling layouts are motion with no referent event.
  const simulationPattern = /(forceSimulation|d3-force|forceManyBody|forceLink|\.alphaDecay|springLayout)/;
  for (const { file, body } of vizCode) {
    if (simulationPattern.test(body)) {
      fail(`${file} uses a force/physics layout; layouts must arrive composed (L4).`);
    }
  }
}

// L5: depth is ordinal epistemic strength, orthographic, bounded, no camera -- AND
// only on marks that are individually addressable.
function auditDepthLaw() {
  const { rungs, rungOrder, maxDepthPx, addressableMinHeightPx } = tokens;

  if (typeof addressableMinHeightPx !== "number" || addressableMinHeightPx < 8) {
    fail(
      "addressableMinHeightPx must be a number of at least 8px: below that a mark cannot " +
        "be perceived or operated as an object (L5 constraint 1).",
    );
  }

  // Constraint 1, the check whose ABSENCE made this law dead code. Any component
  // that may render depth must declare addressable marks. The first version of this
  // grammar suppressed depth on 3px aggregated marks instead, which meant depth was
  // unreachable on every real dataset.
  const depthBearingRungs = Object.entries(rungs).filter(([, spec]) => spec.depthPx !== 0);
  if (depthBearingRungs.length > 0) {
    const addressableComponents = Object.entries(tokens.components).filter(
      ([, spec]) => spec.addressable === true,
    );
    if (addressableComponents.length === 0) {
      fail(
        `${depthBearingRungs.length} rung(s) declare a depth offset but no component declares ` +
          "addressable marks, so depth could never render (L5 constraint 1).",
      );
    }
  }

  const orderNames = [...rungOrder];
  if (orderNames.length !== Object.keys(rungs).length) {
    fail("rungOrder must cover exactly the declared rungs (L5).");
  }
  let previousStrength = -1;
  for (const name of orderNames) {
    const spec = rungs[name];
    if (!spec) {
      fail(`rungOrder names '${name}', which has no rung spec (L5).`);
    }
    if (spec.strength <= previousStrength) {
      fail(`rungOrder is not strictly increasing in strength at '${name}' (L5).`);
    }
    previousStrength = spec.strength;
    if (Math.abs(spec.depthPx) > maxDepthPx) {
      fail(
        `Rung '${name}' offsets ${spec.depthPx}px, beyond the ${maxDepthPx}px depth bound; ` +
          "depth is a categorical cue, not a spatial experience (L5).",
      );
    }
  }
  // Ground contact is the proven cue: only the two proven rungs may claim contact,
  // and only they may glow. A glowing unproven mark is the overtrust lie.
  for (const [name, spec] of Object.entries(rungs)) {
    const isProven = name === "proven" || name === "corroborated";
    if (spec.groundContact === "contact" && !isProven) {
      fail(`Rung '${name}' claims ground contact, which is reserved for proof (L5).`);
    }
    if (isProven && spec.groundContact !== "contact") {
      fail(`Rung '${name}' is proven and must make ground contact (L5).`);
    }
    if (spec.glowEligible && !isProven) {
      fail(`Rung '${name}' is glow-eligible but unproven; glow is earned salience only.`);
    }
  }
  // Perspective, orbit and scroll parallax are forbidden outright (WCAG 2.3.3).
  const cameraPattern =
    /(PerspectiveCamera|perspective\s*\(|OrbitControls|TrackballControls|enableRotate|autoRotate|parallax)/i;
  for (const { file, body } of vizCode) {
    if (cameraPattern.test(body)) {
      fail(
        `${file} references perspective/orbit/parallax; projection is orthographic with no ` +
          "camera control (L5, WCAG 2.3.3).",
      );
    }
  }
}

// Colour discipline: the renderer is a token consumer, never a colour author.
function auditNoAuthoredColour() {
  for (const { file, body } of vizSources) {
    if (/#[0-9a-fA-F]{3,8}\b/.test(body)) {
      fail(`${file} contains a colour literal; read colours from the audited tokens instead.`);
    }
    if (/0x[0-9a-fA-F]{6}\b/.test(body)) {
      fail(`${file} contains a packed colour literal; read colours from the audited tokens.`);
    }
  }
}

auditRegistryIsNotVacuous();
auditQuantityLaw();
auditAddressabilityLaw();
auditAbsenceLaw();
auditAttentionBudget();
auditDepthLaw();
auditNoAuthoredColour();

console.log(
  `Quantitative grammar audit passed (${Object.keys(tokens.components).length} components, ` +
    `${vizSources.length} source files).`,
);
