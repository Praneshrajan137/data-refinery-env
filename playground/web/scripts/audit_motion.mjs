import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = fileURLToPath(new URL(".", import.meta.url));
const webRoot = resolve(scriptDir, "..");
const srcRoot = resolve(webRoot, "src");
const tokensPath = resolve(srcRoot, "design", "motion-tokens.json");
const cssPath = resolve(srcRoot, "design", "motion-system.generated.css");

const tokens = JSON.parse(readFileSync(tokensPath, "utf8"));
const css = readFileSync(cssPath, "utf8");

function fail(message) {
  throw new Error(message);
}

// Honesty rule: a loop is a claim of ongoing activity. Only the active primitives
// (hover = uncommitted proposal, resolve = verification in progress) may loop.
// Anything else looping would animate an idle or finished element -- a lie.
const allowedLoopPrimitives = new Set(["hover", "resolve"]);

function auditLoopHonesty() {
  for (const [name, spec] of Object.entries(tokens.primitives)) {
    if (spec.loop && !allowedLoopPrimitives.has(name)) {
      fail(`Primitive ${name} declares loop:true but only active primitives may loop.`);
    }
    if (!spec.loop && allowedLoopPrimitives.has(name)) {
      fail(`Active primitive ${name} must declare loop:true.`);
    }
  }
  // The generated CSS must only bind `infinite` to the allowed loop primitives.
  const infiniteClasses = [...css.matchAll(/\.df-motion-([a-z]+)\s*\{[^}]*infinite/g)].map(
    (match) => match[1],
  );
  for (const name of infiniteClasses) {
    if (!allowedLoopPrimitives.has(name)) {
      fail(`Generated CSS loops .df-motion-${name}, but only active primitives may loop.`);
    }
  }
}

function auditReducedMotionTwin() {
  if (!css.includes("@media (prefers-reduced-motion: reduce)")) {
    fail("Motion system must ship a reduced-motion twin.");
  }
  for (const name of Object.keys(tokens.primitives)) {
    if (name === "still") {
      continue;
    }
    if (!css.includes(`.df-motion-${name}`)) {
      fail(`Missing utility class .df-motion-${name}.`);
    }
  }
}

function auditGpuSafe() {
  // Keyframes must only animate transform/opacity (GPU-safe, no layout thrash,
  // vestibular-safe: small amplitudes, no strobing).
  const keyframeBlocks = [...css.matchAll(/@keyframes df-[a-z]+\s*\{([\s\S]*?)\n\}/g)];
  for (const [, body] of keyframeBlocks) {
    const props = [...body.matchAll(/([a-z-]+)\s*:/g)].map((match) => match[1]);
    for (const prop of props) {
      if (prop !== "transform" && prop !== "opacity") {
        fail(`Keyframe animates '${prop}'; only transform/opacity are allowed (GPU/vestibular-safe).`);
      }
    }
  }
}

function auditNoLegacyDrift() {
  // No file may hand-declare a --df-motion-* or --df-ease-* custom property
  // outside the generated motion CSS -- that is the drift we eliminated.
  const generated = new Set([cssPath]);
  const offenders = [];
  for (const file of walkFiles(srcRoot)) {
    if (generated.has(file) || !/\.(css)$/.test(file)) {
      continue;
    }
    const body = readFileSync(file, "utf8");
    if (/--df-(motion|ease)-[a-z]+\s*:/.test(body)) {
      offenders.push(file);
    }
  }
  if (offenders.length > 0) {
    fail(`Motion/easing custom properties may only be defined in the generated file:\n${offenders.join("\n")}`);
  }
}

function walkFiles(dir) {
  const files = [];
  for (const item of readdirSync(dir)) {
    const path = join(dir, item);
    if (statSync(path).isDirectory()) {
      files.push(...walkFiles(path));
    } else {
      files.push(path);
    }
  }
  return files;
}

auditLoopHonesty();
auditReducedMotionTwin();
auditGpuSafe();
auditNoLegacyDrift();

console.log("Motion audit passed.");
