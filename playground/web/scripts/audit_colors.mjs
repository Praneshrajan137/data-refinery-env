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
  "--df-focus-ring",
  "--df-focus-halo",
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
  "--df-diff-old-bg",
  "--df-diff-old-text",
  "--df-diff-new-bg",
  "--df-diff-new-text",
];

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
}

function auditContrast(system) {
  for (const theme of ["light", "dark"]) {
    for (const surface of ["--df-bg", "--df-surface-1", "--df-surface-2", "--df-surface-3"]) {
      assertContrast(system, theme, "--df-text-1", surface, 7);
      assertContrast(system, theme, "--df-text-2", surface, 4.5);
    }
    assertContrast(system, theme, "--df-action-text", "--df-action-bg", 4.5);
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
    assertContrast(system, theme, "--df-diff-old-text", "--df-diff-old-bg", 4.5);
    assertContrast(system, theme, "--df-diff-new-text", "--df-diff-new-bg", 4.5);
    assertContrast(system, theme, "--df-focus-ring", "--df-bg", 3);
    assertContrast(system, theme, "--df-line-strong", "--df-bg", 3);
  }
}

function auditMineralPalette(system) {
  for (const theme of ["light", "dark"]) {
    for (const tokenName of ["--df-action-bg", "--df-action-bg-hover"]) {
      const palette = system.semantic[theme][tokenName]?.palette ?? "";
      if (!palette.startsWith("neutral-") && !palette.startsWith("brand-")) {
        fail(`${theme} ${tokenName} must use graphite or vermilion command materials, not ${palette}.`);
      }
      if (/^(data|success|safe|forge)-/.test(palette)) {
        fail(`${theme} ${tokenName} must not use blue/teal/green evidence or success palettes.`);
      }
    }
    const borderPalette = system.semantic[theme]["--df-action-border"]?.palette ?? "";
    if (!borderPalette.startsWith("brand-")) {
      fail(`${theme} --df-action-border must use restrained vermilion signal, not ${borderPalette}.`);
    }
    const successBg = system.semantic[theme]["--df-status-safe-bg"]?.palette ?? "";
    const successText = system.semantic[theme]["--df-status-safe-text"]?.palette ?? "";
    if (!successBg.startsWith("neutral-") || !successText.startsWith("success-")) {
      fail(`${theme} success status must use neutral background and verdigris text.`);
    }
  }

  if ("forge" in system.seeds || "safe" in system.seeds || "review" in system.seeds) {
    fail("Legacy forge/safe/review seed names are not allowed in the Mineral Intelligence palette.");
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
    "--df-status-safe-bg",
    "--df-status-review-bg",
    "--df-status-danger-bg",
    "--df-diff-old-bg",
    "--df-diff-new-bg",
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

const system = readJson(jsonPath);
const css = readFileSync(cssPath, "utf8");

auditGeneratedFiles(system, css);
auditContrast(system);
auditMineralPalette(system);
auditApexBackgroundDiscipline(system);
auditPackage();
auditRawHexUsage();

console.log("Color audit passed.");
