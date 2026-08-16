import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { wcagContrast } from "culori";

const scriptDir = fileURLToPath(new URL(".", import.meta.url));
const webRoot = resolve(scriptDir, "..");
const srcRoot = resolve(webRoot, "src");
const packagePath = resolve(webRoot, "package.json");
const cssPath = resolve(srcRoot, "design", "color-system.generated.css");
const jsonPath = resolve(srcRoot, "design", "color-system.generated.json");

const agentStates = [
  "verifying",
  "proposing",
  "proven",
  "held",
  "rejected",
  "asking",
  "done",
  "idle",
];

const requiredTokens = [
  "--df-bg",
  "--df-surface-1",
  "--df-surface-2",
  "--df-surface-3",
  "--df-text-1",
  "--df-text-2",
  "--df-line-subtle",
  "--df-line",
  "--df-line-strong",
  "--df-action-bg",
  "--df-action-bg-hover",
  "--df-action-text",
  "--df-action-border",
  "--df-action-soft",
  "--df-action-soft-text",
  "--df-focus-ring",
  "--df-focus-halo",
  "--df-info-bg",
  "--df-info-text",
  "--df-info-line",
  "--df-selection-bg",
  "--df-selection-text",
  "--df-selection-line",
  "--df-hover-bg",
  "--df-disabled-bg",
  "--df-disabled-text",
  "--df-disabled-line",
  "--df-loading-bg",
  "--df-loading-text",
  "--df-loading-line",
  "--df-status-safe-bg",
  "--df-status-safe-text",
  "--df-status-review-bg",
  "--df-status-review-text",
  "--df-status-danger-bg",
  "--df-status-danger-text",
  "--df-agent-bg",
  "--df-agent-text",
  "--df-autonomy-bg",
  "--df-autonomy-text",
  "--df-autonomy-line",
  "--df-stage-idle-bg",
  "--df-stage-idle-text",
  "--df-stage-idle-line",
  "--df-stage-active-bg",
  "--df-stage-active-text",
  "--df-stage-active-line",
  "--df-stage-complete-bg",
  "--df-stage-complete-text",
  "--df-stage-complete-line",
  "--df-stage-blocked-bg",
  "--df-stage-blocked-text",
  "--df-stage-blocked-line",
  "--df-stage-failed-bg",
  "--df-stage-failed-text",
  "--df-stage-failed-line",
  "--df-confidence-high-bg",
  "--df-confidence-high-text",
  "--df-confidence-high-line",
  "--df-confidence-medium-bg",
  "--df-confidence-medium-text",
  "--df-confidence-medium-line",
  "--df-confidence-low-bg",
  "--df-confidence-low-text",
  "--df-confidence-low-line",
  "--df-proof-bg",
  "--df-proof-text",
  "--df-proof-line",
  "--df-proven-bg",
  "--df-proven-text",
  "--df-proven-line",
  "--df-plausibility-bg",
  "--df-plausibility-text",
  "--df-plausibility-line",
  "--df-held-bg",
  "--df-held-text",
  "--df-held-line",
  "--df-corroborated-bg",
  "--df-corroborated-text",
  "--df-corroborated-line",
  "--df-downgraded-bg",
  "--df-downgraded-text",
  "--df-downgraded-line",
  "--df-diff-old-bg",
  "--df-diff-old-text",
  "--df-diff-new-bg",
  "--df-diff-new-text",
  ...agentStates.flatMap((state) => [
    `--df-agent-${state}-bg`,
    `--df-agent-${state}-text`,
    `--df-agent-${state}-line`,
  ]),
];

const expectedSeeds = {
  neutral: { c: 0.006, h: 92 },
  brand: { c: 0.096, h: 34 },
  data: { c: 0.044, h: 188 },
  agent: { c: 0.058, h: 298 },
  success: { c: 0.038, h: 152 },
  warning: { c: 0.066, h: 78 },
  danger: { c: 0.086, h: 18 },
};

function fail(message) {
  throw new Error(message);
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function walkFiles(dir) {
  const files = [];
  for (const item of readdirSync(dir)) {
    const path = join(dir, item);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      files.push(...walkFiles(path));
    } else {
      files.push(path);
    }
  }
  return files;
}

function contrast(system, theme, foreground, background) {
  const fg = system.semantic[theme][foreground]?.hex;
  const bg = system.semantic[theme][background]?.hex;
  if (!fg || !bg) {
    fail(`Missing contrast token pair ${theme} ${foreground} on ${background}.`);
  }
  return wcagContrast(fg, bg);
}

function assertContrast(system, theme, foreground, background, minimum) {
  const ratio = contrast(system, theme, foreground, background);
  if (ratio < minimum) {
    fail(
      `${theme} ${foreground} on ${background} is ${ratio.toFixed(2)}:1, below ${minimum}:1.`,
    );
  }
}

function auditGeneratedFiles(system, css) {
  for (const token of requiredTokens) {
    if (!css.includes(`${token}:`)) {
      fail(`Missing generated CSS token ${token}.`);
    }
    for (const theme of ["light", "dark"]) {
      if (!system.semantic[theme][token]) {
        fail(`Missing generated JSON token ${theme}.${token}.`);
      }
    }
  }
  if (!css.includes("@media (prefers-color-scheme: dark)")) {
    fail("Generated CSS must include dark-mode token overrides.");
  }
  if (!css.includes("@media (color-gamut: p3)")) {
    fail("Generated CSS must include P3-only non-text accent tokens.");
  }
  if (!css.includes("@media (prefers-contrast: more)")) {
    fail("Generated CSS must include high-contrast token overrides.");
  }
  if (!system.highContrast?.light || !system.highContrast?.dark) {
    fail("Generated JSON must include highContrast light and dark references.");
  }
  for (const line of css.split("\n")) {
    if (line.includes("color(display-p3") && !/--df-(data|action|proof)-glow:/.test(line)) {
      fail(`P3 output is only allowed for proven/evidence/command glow tokens: ${line.trim()}`);
    }
  }
}

function auditContrast(system) {
  for (const theme of ["light", "dark"]) {
    for (const surface of ["--df-bg", "--df-surface-1", "--df-surface-2", "--df-surface-3"]) {
      assertContrast(system, theme, "--df-text-1", surface, 7);
      assertContrast(system, theme, "--df-text-2", surface, 4.5);
    }
    assertContrast(system, theme, "--df-action-text", "--df-action-bg", 4.5);
    assertContrast(system, theme, "--df-info-text", "--df-info-bg", 4.5);
    assertContrast(system, theme, "--df-selection-text", "--df-selection-bg", 4.5);
    assertContrast(system, theme, "--df-disabled-text", "--df-disabled-bg", 3);
    assertContrast(system, theme, "--df-loading-text", "--df-loading-bg", 4.5);
    assertContrast(system, theme, "--df-status-safe-text", "--df-status-safe-bg", 4.5);
    assertContrast(system, theme, "--df-status-review-text", "--df-status-review-bg", 4.5);
    assertContrast(system, theme, "--df-status-danger-text", "--df-status-danger-bg", 4.5);
    assertContrast(system, theme, "--df-autonomy-text", "--df-autonomy-bg", 4.5);
    assertContrast(system, theme, "--df-stage-idle-text", "--df-stage-idle-bg", 4.5);
    assertContrast(system, theme, "--df-stage-active-text", "--df-stage-active-bg", 4.5);
    assertContrast(system, theme, "--df-stage-complete-text", "--df-stage-complete-bg", 4.5);
    assertContrast(system, theme, "--df-stage-blocked-text", "--df-stage-blocked-bg", 4.5);
    assertContrast(system, theme, "--df-stage-failed-text", "--df-stage-failed-bg", 4.5);
    assertContrast(system, theme, "--df-confidence-high-text", "--df-confidence-high-bg", 4.5);
    assertContrast(system, theme, "--df-confidence-medium-text", "--df-confidence-medium-bg", 4.5);
    assertContrast(system, theme, "--df-confidence-low-text", "--df-confidence-low-bg", 4.5);
    assertContrast(system, theme, "--df-proof-text", "--df-proof-bg", 4.5);
    assertContrast(system, theme, "--df-proven-text", "--df-proven-bg", 4.5);
    assertContrast(system, theme, "--df-plausibility-text", "--df-plausibility-bg", 4.5);
    assertContrast(system, theme, "--df-held-text", "--df-held-bg", 4.5);
    assertContrast(system, theme, "--df-corroborated-text", "--df-corroborated-bg", 4.5);
    assertContrast(system, theme, "--df-downgraded-text", "--df-downgraded-bg", 4.5);
    assertContrast(system, theme, "--df-diff-old-text", "--df-diff-old-bg", 4.5);
    assertContrast(system, theme, "--df-diff-new-text", "--df-diff-new-bg", 4.5);
    assertContrast(system, theme, "--df-focus-ring", "--df-bg", 3);
    assertContrast(system, theme, "--df-line-strong", "--df-bg", 3);
    for (const state of agentStates) {
      assertContrast(system, theme, `--df-agent-${state}-text`, `--df-agent-${state}-bg`, 4.5);
    }
  }
}

function auditAurelianProofPalette(system) {
  for (const [name, expected] of Object.entries(expectedSeeds)) {
    const seed = system.seeds[name];
    if (!seed) {
      fail(`Missing Aurelian Proof Intelligence seed ${name}.`);
    }
    if (seed.c !== expected.c || seed.h !== expected.h) {
      fail(`${name} seed must be c ${expected.c} / h ${expected.h}, got c ${seed.c} / h ${seed.h}.`);
    }
  }

  const lightActionBg = system.semantic.light["--df-action-bg"]?.palette ?? "";
  const lightActionHover = system.semantic.light["--df-action-bg-hover"]?.palette ?? "";
  if (lightActionBg !== "brand-30" || lightActionHover !== "brand-40") {
    fail(`Light command must be aurelian cinnabar, got ${lightActionBg} / ${lightActionHover}.`);
  }

  for (const theme of ["light", "dark"]) {
    for (const tokenName of ["--df-action-bg", "--df-action-bg-hover"]) {
      const palette = system.semantic[theme][tokenName]?.palette ?? "";
      if (!palette.startsWith("brand-") && !(theme === "dark" && palette.startsWith("neutral-"))) {
        fail(`${theme} ${tokenName} must use aurelian command materials, not ${palette}.`);
      }
      if (/^(data|success|safe|forge)-/.test(palette)) {
        fail(`${theme} ${tokenName} must not use blue/teal/green evidence or success palettes.`);
      }
    }
    const borderPalette = system.semantic[theme]["--df-action-border"]?.palette ?? "";
    if (!borderPalette.startsWith("brand-")) {
      fail(`${theme} --df-action-border must use aurelian cinnabar signal, not ${borderPalette}.`);
    }
    const successBg = system.semantic[theme]["--df-status-safe-bg"]?.palette ?? "";
    const successText = system.semantic[theme]["--df-status-safe-text"]?.palette ?? "";
    if (!successBg.startsWith("neutral-") || !successText.startsWith("success-")) {
      fail(`${theme} success status must use neutral background and verdigris text.`);
    }
  }

  if ("forge" in system.seeds || "safe" in system.seeds || "review" in system.seeds) {
    fail("Legacy forge/safe/review seed names are not allowed in the Aurelian Proof Intelligence palette.");
  }
  if (system.seeds.success.c > 0.04) {
    fail("Proof viridian must remain low-chroma and reserved for verified outcomes.");
  }
  if (system.seeds.brand.h >= 190 && system.seeds.brand.h <= 270) {
    fail("Primary brand hue must not be blue-led.");
  }
  if (system.seeds.brand.h < 20 || system.seeds.brand.h > 55) {
    fail("Primary brand hue must stay in the restrained vermilion executive range.");
  }
}

function auditHighContrast(system) {
  const expectedHighContrast = {
    light: {
      "--df-text-2": "neutral-20",
      "--df-line": "neutral-60",
      "--df-line-strong": "neutral-40",
      "--df-focus-ring": "agent-30",
      // Swapped with dark: see generate_color_system.mjs. brand-40 gave 1.46:1 against the
      // light theme's brand-30 action background, a downgrade from the 3.43:1 it overrode.
      "--df-action-border": "brand-80",
    },
    dark: {
      "--df-text-2": "neutral-95",
      "--df-line": "neutral-70",
      "--df-line-strong": "neutral-80",
      "--df-focus-ring": "agent-90",
      "--df-action-border": "brand-40",
    },
  };

  for (const [theme, refs] of Object.entries(expectedHighContrast)) {
    for (const [tokenName, palette] of Object.entries(refs)) {
      const actual = system.highContrast?.[theme]?.[tokenName]?.palette ?? "";
      if (actual !== palette) {
        fail(`High-contrast ${theme} ${tokenName} must use ${palette}, not ${actual}.`);
      }
    }
  }
}

function auditApexBackgroundDiscipline(system) {
  const forbiddenLightStateBackgrounds = new Set([
    "brand-95",
    "data-95",
    "agent-95",
    "success-95",
    "warning-95",
    "danger-95",
  ]);
  const largeStateBackgroundTokens = [
    "--df-info-bg",
    "--df-selection-bg",
    "--df-disabled-bg",
    "--df-loading-bg",
    "--df-data-bg",
    "--df-agent-bg",
    "--df-autonomy-bg",
    "--df-stage-active-bg",
    "--df-stage-complete-bg",
    "--df-stage-blocked-bg",
    "--df-stage-failed-bg",
    "--df-confidence-high-bg",
    "--df-confidence-medium-bg",
    "--df-confidence-low-bg",
    "--df-proof-bg",
    "--df-proven-bg",
    "--df-plausibility-bg",
    "--df-held-bg",
    "--df-corroborated-bg",
    "--df-downgraded-bg",
    "--df-status-safe-bg",
    "--df-status-review-bg",
    "--df-status-danger-bg",
    "--df-diff-old-bg",
    "--df-diff-new-bg",
    ...agentStates.map((state) => `--df-agent-${state}-bg`),
  ];

  for (const tokenName of largeStateBackgroundTokens) {
    const palette = system.semantic.light[tokenName]?.palette ?? "";
    if (!palette.startsWith("neutral-")) {
      fail(`light ${tokenName} must use a neutral/platinum surface, not ${palette}.`);
    }
    if (forbiddenLightStateBackgrounds.has(palette)) {
      fail(`light ${tokenName} must not use pastel state fill ${palette}.`);
    }
  }
}

function auditPackage() {
  const packageJson = readJson(packagePath);
  if (packageJson.dependencies?.culori) {
    fail("culori must remain a devDependency and must not ship in runtime dependencies.");
  }
  if (packageJson.devDependencies?.culori !== "4.0.2") {
    fail("culori must be pinned to devDependency version 4.0.2.");
  }
  for (const scriptName of ["colors", "colors:check", "audit:colors"]) {
    if (!packageJson.scripts?.[scriptName]) {
      fail(`Missing package script ${scriptName}.`);
    }
  }
  if (!packageJson.scripts.build.includes("colors:check")) {
    fail("npm run build must run colors:check before compiling.");
  }
}

function auditRawHexUsage() {
  const generated = new Set([cssPath, jsonPath]);
  const offenders = [];
  for (const file of walkFiles(srcRoot)) {
    if (generated.has(file) || !/\.(css|tsx?|jsx?)$/.test(file)) {
      continue;
    }
    const body = readFileSync(file, "utf8");
    const matches = body.match(/#[0-9a-fA-F]{3,8}\b/g);
    if (matches) {
      offenders.push(`${file}: ${matches.join(", ")}`);
    }
  }
  if (offenders.length > 0) {
    fail(`Raw hex colors are only allowed in generated artifacts.\n${offenders.join("\n")}`);
  }
}

function auditEarnedSalience(system, css) {
  // The core correctness property: perceptual intensity must track epistemic
  // strength, so overtrust is unrenderable. These are BUILD GATES, not advice.
  for (const theme of ["light", "dark"]) {
    const provenText = system.semantic[theme]["--df-proven-text"]?.palette ?? "";
    if (!provenText.startsWith("success-")) {
      fail(`${theme} --df-proven-text must be viridian proof (success-*), not ${provenText}.`);
    }
    const plausibilityText = system.semantic[theme]["--df-plausibility-text"]?.palette ?? "";
    if (!plausibilityText.startsWith("agent-")) {
      fail(`${theme} --df-plausibility-text must be ultraviolet (agent-*), not ${plausibilityText}.`);
    }
    // Confidence is a magnitude, never a verdict: it may never borrow proof-green,
    // review-brass, or danger-carmine. A confident-green chip on an unproven value
    // is precisely the overtrust lie this language forbids.
    for (const level of ["high", "medium", "low"]) {
      const palette = system.semantic[theme][`--df-confidence-${level}-text`]?.palette ?? "";
      if (!palette.startsWith("neutral-")) {
        fail(
          `${theme} --df-confidence-${level}-text must be a neutral magnitude, not the verdict color ${palette}.`,
        );
      }
    }
    // Corroboration intensifies proof; downgrade is a relaxed proof (held/brass).
    const corroborated = system.semantic[theme]["--df-corroborated-text"]?.palette ?? "";
    if (!corroborated.startsWith("success-")) {
      fail(`${theme} --df-corroborated-text must build on proof (success-*), not ${corroborated}.`);
    }
    const downgraded = system.semantic[theme]["--df-downgraded-text"]?.palette ?? "";
    if (!downgraded.startsWith("warning-")) {
      fail(`${theme} --df-downgraded-text must read as held/review (warning-*), not ${downgraded}.`);
    }
  }
  // Glow is the strongest salience and is reserved for proof/evidence/command.
  // Plausibility and failure must never glow.
  for (const forbidden of ["--df-agent-glow", "--df-danger-glow"]) {
    if (css.includes(`${forbidden}:`)) {
      fail(`Glow on ${forbidden} would give unearned salience to unproven/failed claims.`);
    }
  }
}

/**
 * The warrant law (W), measured rather than name-matched.
 *
 * `auditEarnedSalience` above verifies that each rung's text token belongs to the right
 * hue FAMILY, by string prefix. It never reads a colour value, which is why the one law was
 * violated on its own first-named channel in both themes without failing a build: measured
 * chroma falls as warrant rises, on 5 of 6 adjacent pairs in light and 4 of 6 in dark. See
 * SPEC_perceptual_verification.md section 2 for the tables.
 *
 * The resolution moves chroma out of the warrant set entirely, so this gate enforces the
 * boundary instead of a monotonic ordering that would have made failures quieter than
 * proofs:
 *
 *   1. warrant is computed only from the declared warrant channels, and
 *   2. no hue or chroma value may enter that computation.
 *
 * Making the exclusion structural is what prevents a regression. A rule that says "do not
 * put chroma in the warrant sum" is a rule to remember; a gate that fails when a colour
 * channel appears among the salience weights is not.
 */
function auditWarrantChannelPurity() {
  const tokens = readJson(resolve(srcRoot, "design", "quantitative-tokens.json"));

  const declared = Object.keys(tokens.salienceWeights).filter((key) => !key.startsWith("$"));
  const expected = ["fill", "stroke", "contact", "glow", "accent"];
  if (declared.join(",") !== expected.join(",")) {
    fail(
      `Warrant channels are ${declared.join(", ")}; expected exactly ${expected.join(", ")}. ` +
        "Warrant is carried by form, not colour (SPEC_perceptual_verification.md, law W).",
    );
  }

  const colourChannels = ["chroma", "hue", "saturation", "color", "colour", "lightness", "oklch"];
  for (const channel of declared) {
    for (const forbidden of colourChannels) {
      if (channel.toLowerCase().includes(forbidden)) {
        fail(
          `Warrant channel '${channel}' names a colour quantity. Hue and chroma carry ` +
            "identity and urgency, never warrant (law W).",
        );
      }
    }
  }

  // The DECLARED warrant role must also be free of colour. Found by mutation: adding "chroma"
  // to channelRoles.warrant survived, because this function only inspected salienceWeights.
  for (const channel of tokens.channelRoles?.warrant ?? []) {
    for (const forbidden of colourChannels) {
      if (channel.toLowerCase().includes(forbidden)) {
        fail(
          `channelRoles.warrant declares '${channel}', a colour quantity. Warrant is carried ` +
            "by form; hue carries identity and chroma carries urgency (law W).",
        );
      }
    }
  }

  // The salience weights must be pure numbers. A weight expressed as a token reference or a
  // colour string would smuggle colour into the warrant sum past the name check above.
  for (const [channel, weights] of Object.entries(tokens.salienceWeights)) {
    if (channel.startsWith("$")) {
      continue;
    }
    for (const [variant, weight] of Object.entries(weights)) {
      if (typeof weight !== "number" || !Number.isFinite(weight)) {
        fail(
          `Warrant weight ${channel}.${variant} is ${JSON.stringify(weight)}; warrant weights ` +
            "must be finite numbers so warrant can never read a colour (law W).",
        );
      }
    }
  }

  // `measureRungSalience` is the one function that computes warrant. It must not import or
  // reference a colour source.
  const grammarSource = readFileSync(resolve(srcRoot, "viz", "grammar.ts"), "utf8");
  for (const forbidden of ["color-system", "oklch", "chroma", "culori", "readVizTokens"]) {
    if (grammarSource.includes(forbidden)) {
      fail(
        `src/viz/grammar.ts references '${forbidden}'. The warrant computation must not be ` +
          "able to read a colour value (law W).",
      );
    }
  }
}

/**
 * The colour engine must never reach the browser bundle.
 *
 * SPEC_color_system.md requires that "no color engine ships in the browser runtime bundle",
 * and `auditPackage` checks only that culori is a devDependency pinned to 4.0.2 -- which
 * says nothing about whether `src/` imports it. The perceptual measurement kernel added for
 * laws W and I lives in `scripts/` for exactly this reason, and this gate keeps it there.
 */
function auditNoColourEngineInSource() {
  const offenders = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const child = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(child);
        continue;
      }
      if (!/\.(ts|tsx|js|jsx|mjs)$/.test(entry.name)) {
        continue;
      }
      // Test files are excluded from the production bundle by Vite, and the perceptual
      // measurements have to be assertable somewhere.
      if (/\.test\.(ts|tsx|mjs)$/.test(entry.name)) {
        continue;
      }
      const source = readFileSync(child, "utf8");
      if (/from\s+["']culori["']|require\(["']culori["']\)/.test(source)) {
        offenders.push(child);
      }
    }
  };
  walk(srcRoot);
  if (offenders.length > 0) {
    fail(
      `culori is imported from src/ in: ${offenders.join(", ")}. The colour engine is ` +
        "build-time only and must not enter the browser bundle (SPEC_color_system.md).",
    );
  }
}

/**
 * The high-contrast overrides must actually meet their contrast ratios.
 *
 * `auditHighContrast` checks these five tokens for IDENTITY against hard-coded palette
 * strings, and `auditContrast` reads `system.semantic[theme]` only -- so the ratios of the
 * `prefers-contrast: more` overrides were never computed. The feature that exists to help
 * low-vision users was the one feature with no contrast verification, and an override could
 * have been strictly worse than the value it replaces without failing a build.
 *
 * Each override is checked at the same threshold its semantic counterpart uses, and is also
 * required to be no worse than the value it replaces -- because an override that reduces
 * contrast is not a high-contrast mode.
 */
function auditHighContrastRatios(system) {
  const thresholds = {
    "--df-text-2": { background: "--df-bg", minimum: 4.5 },
    "--df-line": { background: "--df-bg", minimum: 3 },
    "--df-line-strong": { background: "--df-bg", minimum: 3 },
    "--df-focus-ring": { background: "--df-bg", minimum: 3 },
    "--df-action-border": { background: "--df-action-bg", minimum: 3 },
  };

  for (const theme of ["light", "dark"]) {
    const overrides = system.highContrast[theme];
    if (overrides === undefined) {
      fail(`No high-contrast overrides for the ${theme} theme.`);
      continue;
    }
    for (const [token, override] of Object.entries(overrides)) {
      const rule = thresholds[token];
      if (rule === undefined) {
        fail(
          `High-contrast override ${token} (${theme}) has no declared contrast threshold. ` +
            "An override with no verified ratio is an accessibility claim nobody checked.",
        );
        continue;
      }
      // The background may itself be overridden in high-contrast mode; prefer the override.
      const backgroundHex =
        overrides[rule.background]?.hex ?? system.semantic[theme][rule.background]?.hex;
      if (backgroundHex === undefined) {
        fail(`Cannot resolve ${rule.background} for ${theme} high contrast.`);
        continue;
      }
      const ratio = wcagContrast(override.hex, backgroundHex);
      if (ratio < rule.minimum) {
        fail(
          `${theme} high-contrast ${token} on ${rule.background} is ${ratio.toFixed(2)}:1, ` +
            `below ${rule.minimum}:1.`,
        );
      }
      // And it must not be a downgrade of the value it replaces.
      const semanticHex = system.semantic[theme][token]?.hex;
      if (semanticHex !== undefined) {
        const semanticRatio = wcagContrast(semanticHex, backgroundHex);
        if (ratio < semanticRatio - 1e-6) {
          fail(
            `${theme} high-contrast ${token} is ${ratio.toFixed(2)}:1, WORSE than the ` +
              `standard ${semanticRatio.toFixed(2)}:1 it overrides. An override that lowers ` +
              "contrast is not a high-contrast mode.",
          );
        }
      }
    }
  }
}

/**
 * Forced colours must be handled, and the canvas must be handled in script.
 *
 * There was no `forced-colors` support at all: no media query, no system colours, and
 * `viz/tokens.ts` claimed its three media queries were "the complete set of triggers". The
 * canvas is the specific hazard, because `forced-colors: active` overrides CSS-painted colour
 * but cannot reach a `fillStyle`, so the density map painted its own neutral ink onto an
 * OS-supplied background with no repaint when the mode changed.
 */
function auditForcedColours() {
  const stylesPath = resolve(srcRoot, "styles.css");
  const styles = readFileSync(stylesPath, "utf8");
  if (!styles.includes("@media (forced-colors: active)")) {
    fail(
      "styles.css must handle @media (forced-colors: active). Forced-colours mode drops " +
        "box-shadow, which carries ground contact, earned depth and the corroborated witness " +
        "rail, so every rung distinction those channels made would vanish.",
    );
  }

  const vizTokens = readFileSync(resolve(srcRoot, "viz", "tokens.ts"), "utf8");
  if (!vizTokens.includes("(forced-colors: active)")) {
    fail(
      "viz/tokens.ts must subscribe to (forced-colors: active). The canvas is painted in " +
        "script, so forced colours cannot reach it and a mode change would not repaint.",
    );
  }
  if (!/forcedColoursInk/.test(vizTokens)) {
    fail(
      "viz/tokens.ts must resolve a forced-colours ink for the canvas. A custom property " +
        "cannot carry it: `--df-ink: CanvasText` reads back as the literal string.",
    );
  }
}

const system = readJson(jsonPath);
const css = readFileSync(cssPath, "utf8");

auditGeneratedFiles(system, css);
auditContrast(system);
auditAurelianProofPalette(system);
auditHighContrast(system);
auditHighContrastRatios(system);
auditForcedColours();
auditApexBackgroundDiscipline(system);
auditEarnedSalience(system, css);
auditWarrantChannelPurity();
auditNoColourEngineInSource();
auditPackage();
auditRawHexUsage();

console.log("Color audit passed.");
