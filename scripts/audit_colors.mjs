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
    assertContrast(system, theme, "--df-diff-old-text", "--df-diff-old-bg", 4.5);
    assertContrast(system, theme, "--df-diff-new-text", "--df-diff-new-bg", 4.5);
    assertContrast(system, theme, "--df-focus-ring", "--df-bg", 3);
    assertContrast(system, theme, "--df-line-strong", "--df-bg", 3);
  }
}

function auditInstitutionalPalette(system) {
  const actionPalettes = new Set(
    Object.values(system.semantic.light)
      .concat(Object.values(system.semantic.dark))
      .filter((token) => token.palette.startsWith("brand-"))
      .map((token) => token.palette.split("-")[0]),
  );
  if (!actionPalettes.has("brand")) {
    fail("Institutional action tokens must map to the cobalt brand palette.");
  }

  for (const theme of ["light", "dark"]) {
    for (const tokenName of ["--df-action-bg", "--df-action-bg-hover", "--df-action-border"]) {
      const palette = system.semantic[theme][tokenName]?.palette ?? "";
      if (!palette.startsWith("brand-")) {
        fail(`${theme} ${tokenName} must use the brand palette, not ${palette}.`);
      }
      if (/^(success|safe|forge)-/.test(palette)) {
        fail(`${theme} ${tokenName} must not use green/teal success or forge palettes.`);
      }
    }
    const successBg = system.semantic[theme]["--df-status-safe-bg"]?.palette ?? "";
    const successText = system.semantic[theme]["--df-status-safe-text"]?.palette ?? "";
    if (!successBg.startsWith("success-") || !successText.startsWith("success-")) {
      fail(`${theme} success status tokens must use the subdued success palette.`);
    }
  }

  if ("forge" in system.seeds || "safe" in system.seeds || "review" in system.seeds) {
    fail("Legacy forge/safe/review seed names are not allowed in the institutional palette.");
  }
  if (system.seeds.success.c > 0.075) {
    fail("Success green must remain low-chroma and reserved for verified outcomes.");
  }
  if (system.seeds.brand.h < 240 || system.seeds.brand.h > 275) {
    fail("Primary action brand hue must stay in the cobalt range.");
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
auditInstitutionalPalette(system);
auditPackage();
auditRawHexUsage();

console.log("Color audit passed.");
