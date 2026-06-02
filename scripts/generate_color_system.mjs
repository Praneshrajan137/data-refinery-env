import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { converter, formatHex } from "culori";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const webRoot = resolve(scriptDir, "..");
const cssPath = resolve(webRoot, "src", "design", "color-system.generated.css");
const jsonPath = resolve(webRoot, "src", "design", "color-system.generated.json");
const checkMode = process.argv.includes("--check");

const toneStops = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 98, 100];
const seeds = {
  neutral: { l: 0.64, c: 0.018, h: 255, meaning: "cool financial graphite structure" },
  brand: { l: 0.56, c: 0.145, h: 260, meaning: "cobalt primary action and command" },
  data: { l: 0.58, c: 0.08, h: 235, meaning: "steel-blue dataset evidence" },
  agent: { l: 0.55, c: 0.095, h: 292, meaning: "restrained advanced agentic mode" },
  success: { l: 0.56, c: 0.06, h: 155, meaning: "subdued verified success only" },
  warning: { l: 0.68, c: 0.105, h: 78, meaning: "ochre caution and human review" },
  danger: { l: 0.55, c: 0.12, h: 28, meaning: "sober unsafe or failed state" },
};

const semanticRefs = {
  light: {
    "--df-bg": "neutral-98",
    "--df-surface-1": "neutral-100",
    "--df-surface-2": "neutral-98",
    "--df-surface-3": "neutral-95",
    "--df-surface-strong": "neutral-90",
    "--df-text-1": "neutral-10",
    "--df-text-2": "neutral-30",
    "--df-text-inverse": "neutral-100",
    "--df-line-subtle": "neutral-90",
    "--df-line": "neutral-80",
    "--df-line-strong": "neutral-60",
    "--df-action-bg": "brand-40",
    "--df-action-bg-hover": "brand-30",
    "--df-action-text": "neutral-100",
    "--df-action-border": "brand-50",
    "--df-action-soft": "brand-95",
    "--df-action-soft-text": "brand-30",
    "--df-focus-ring": "agent-50",
    "--df-focus-halo": "agent-95",
    "--df-data-bg": "data-95",
    "--df-data-text": "data-30",
    "--df-agent-bg": "agent-95",
    "--df-agent-text": "agent-30",
    "--df-agent-line": "agent-70",
    "--df-status-safe-bg": "success-95",
    "--df-status-safe-text": "success-30",
    "--df-status-safe-line": "success-70",
    "--df-status-review-bg": "warning-95",
    "--df-status-review-text": "warning-20",
    "--df-status-review-line": "warning-70",
    "--df-status-danger-bg": "danger-95",
    "--df-status-danger-text": "danger-30",
    "--df-status-danger-line": "danger-70",
    "--df-diff-old-bg": "danger-95",
    "--df-diff-old-text": "danger-30",
    "--df-diff-new-bg": "success-95",
    "--df-diff-new-text": "success-30",
    "--df-code-bg": "neutral-10",
    "--df-code-text": "neutral-95",
  },
  dark: {
    "--df-bg": "neutral-10",
    "--df-surface-1": "neutral-20",
    "--df-surface-2": "neutral-30",
    "--df-surface-3": "neutral-40",
    "--df-surface-strong": "neutral-30",
    "--df-text-1": "neutral-98",
    "--df-text-2": "neutral-90",
    "--df-text-inverse": "neutral-10",
    "--df-line-subtle": "neutral-40",
    "--df-line": "neutral-50",
    "--df-line-strong": "neutral-60",
    "--df-action-bg": "brand-70",
    "--df-action-bg-hover": "brand-80",
    "--df-action-text": "neutral-10",
    "--df-action-border": "brand-60",
    "--df-action-soft": "brand-20",
    "--df-action-soft-text": "brand-90",
    "--df-focus-ring": "agent-80",
    "--df-focus-halo": "agent-20",
    "--df-data-bg": "data-20",
    "--df-data-text": "data-90",
    "--df-agent-bg": "agent-20",
    "--df-agent-text": "agent-90",
    "--df-agent-line": "agent-60",
    "--df-status-safe-bg": "success-20",
    "--df-status-safe-text": "success-90",
    "--df-status-safe-line": "success-60",
    "--df-status-review-bg": "warning-20",
    "--df-status-review-text": "warning-95",
    "--df-status-review-line": "warning-60",
    "--df-status-danger-bg": "danger-20",
    "--df-status-danger-text": "danger-90",
    "--df-status-danger-line": "danger-60",
    "--df-diff-old-bg": "danger-20",
    "--df-diff-old-text": "danger-90",
    "--df-diff-new-bg": "success-20",
    "--df-diff-new-text": "success-90",
    "--df-code-bg": "neutral-0",
    "--df-code-text": "neutral-95",
  },
};

const glowRefs = {
  "--df-data-glow": "data-60",
  "--df-action-glow": "brand-60",
  "--df-agent-glow": "agent-60",
  "--df-safe-glow": "success-60",
  "--df-danger-glow": "danger-60",
};

const toRgb = converter("rgb");
const toP3 = converter("p3");
const epsilon = 0.000001;

function colorForTone(seed, tone) {
  return {
    mode: "oklch",
    l: tone / 100,
    c: tone === 0 || tone === 100 ? 0 : seed.c,
    h: seed.h,
  };
}

function isInGamut(color, space) {
  const converted = converter(space)(color);
  if (!converted) {
    return false;
  }
  return ["r", "g", "b"].every(
    (channel) => converted[channel] >= -epsilon && converted[channel] <= 1 + epsilon,
  );
}

function mapToGamut(color, space) {
  if (isInGamut(color, space)) {
    return color;
  }

  let low = 0;
  let high = color.c;
  for (let index = 0; index < 30; index += 1) {
    const chroma = (low + high) / 2;
    const candidate = { ...color, c: chroma };
    if (isInGamut(candidate, space)) {
      low = chroma;
    } else {
      high = chroma;
    }
  }

  return { ...color, c: low };
}

function fixed(value) {
  return Number(value).toFixed(5).replace(/\.?0+$/, "");
}

function clamp01(value) {
  return Math.min(1, Math.max(0, value));
}

function toHex(color) {
  return formatHex(toRgb(mapToGamut(color, "rgb"))).toLowerCase();
}

function toP3Css(color, alpha = 1) {
  const converted = toP3(mapToGamut(color, "p3"));
  const channels = [converted.r, converted.g, converted.b].map((value) => fixed(clamp01(value)));
  const suffix = alpha < 1 ? ` / ${fixed(alpha)}` : "";
  return `color(display-p3 ${channels.join(" ")}${suffix})`;
}

function paletteKey(name, tone) {
  return `${name}-${tone}`;
}

function paletteVar(name, tone) {
  return `--df-palette-${name}-${tone}`;
}

function semanticToken(ref, palettes) {
  const separator = ref.lastIndexOf("-");
  const palette = ref.slice(0, separator);
  const tone = ref.slice(separator + 1);
  return {
    palette: ref,
    hex: palettes[palette][tone].hex,
  };
}

function buildSystem() {
  const palettes = {};
  for (const [name, seed] of Object.entries(seeds)) {
    palettes[name] = {};
    for (const tone of toneStops) {
      const color = colorForTone(seed, tone);
      palettes[name][tone] = {
        oklch: `oklch(${fixed(color.l)} ${fixed(color.c)} ${fixed(color.h)})`,
        hex: toHex(color),
        p3: toP3Css(color),
      };
    }
  }

  const semantic = {};
  for (const [theme, refs] of Object.entries(semanticRefs)) {
    semantic[theme] = Object.fromEntries(
      Object.entries(refs).map(([token, ref]) => [token, semanticToken(ref, palettes)]),
    );
  }

  return {
    version: "1.0.0",
    generatedBy: "playground/web/scripts/generate_color_system.mjs",
    colorSpace: "OKLCH seeds, constant-hue/chroma-reduced sRGB output, P3 non-text accents",
    toneStops,
    seeds,
    palettes,
    semantic,
  };
}

function buildCss(system) {
  const lines = [
    "/* This file is generated by scripts/generate_color_system.mjs. Do not edit by hand. */",
    ":root {",
    "  color-scheme: light;",
  ];

  for (const [name, palette] of Object.entries(system.palettes)) {
    for (const tone of toneStops) {
      lines.push(`  ${paletteVar(name, tone)}: ${palette[tone].hex};`);
    }
  }
  for (const [token, value] of Object.entries(system.semantic.light)) {
    lines.push(`  ${token}: var(--df-palette-${value.palette});`);
  }
  for (const [token, ref] of Object.entries(glowRefs)) {
    lines.push(`  ${token}: color-mix(in srgb, var(--df-palette-${ref}) 24%, transparent);`);
  }
  lines.push("}");
  lines.push("");
  lines.push("@media (prefers-color-scheme: dark) {");
  lines.push("  :root {");
  lines.push("    color-scheme: dark;");
  for (const [token, value] of Object.entries(system.semantic.dark)) {
    lines.push(`    ${token}: var(--df-palette-${value.palette});`);
  }
  lines.push("  }");
  lines.push("}");
  lines.push("");
  lines.push("@media (color-gamut: p3) {");
  lines.push("  :root {");
  for (const [token, ref] of Object.entries(glowRefs)) {
    const separator = ref.lastIndexOf("-");
    const palette = ref.slice(0, separator);
    const tone = ref.slice(separator + 1);
    lines.push(`    ${token}: ${toP3Css(colorForTone(seeds[palette], Number(tone)), 0.28)};`);
  }
  lines.push("  }");
  lines.push("}");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

function buildJson(system) {
  return `${JSON.stringify(system, null, 2)}\n`;
}

function assertCurrent(filePath, nextBody) {
  if (!existsSync(filePath)) {
    throw new Error(`${filePath} is missing. Run npm run colors.`);
  }
  const current = readFileSync(filePath, "utf8");
  if (current !== nextBody) {
    throw new Error(`${filePath} is out of date. Run npm run colors.`);
  }
}

const system = buildSystem();
const css = buildCss(system);
const json = buildJson(system);

if (checkMode) {
  assertCurrent(cssPath, css);
  assertCurrent(jsonPath, json);
  console.log("Color system artifacts are up to date.");
} else {
  mkdirSync(dirname(cssPath), { recursive: true });
  writeFileSync(cssPath, css);
  writeFileSync(jsonPath, json);
  console.log("Generated DataForge color system artifacts.");
}
