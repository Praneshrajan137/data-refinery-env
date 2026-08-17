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

/**
 * `import { readdirSync, readFileSync, statSync } from "node:fs";
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

-prefixed keys are metadata, not primitives. `$comment` and `$wcag` are the file's
 * established convention for recording an argument next to the thing it justifies.
 */
function declaredPrimitives() {
  return Object.entries(tokens.primitives).filter(([name]) => !name.startsWith("$"));
}
function auditLoopHonesty() {  for (const [name, spec] of declaredPrimitives()) {
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
  for (const [name] of declaredPrimitives()) {
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

/**
 * Every keyframe in the codebase, not only the generated ones.
 *
 * `auditGpuSafe` above scans `@keyframes df-*` in the generated file. Three keyframes live
 * in styles.css -- `spin`, `df-rail-sweep`, `df-agent-breathe` -- and were never audited, so
 * the transform/opacity rule that the design system describes as vestibular safety covered
 * 6 of 9 keyframes.
 */
function auditAllKeyframesAreGpuSafe() {
  for (const file of walkFiles(srcRoot)) {
    if (!/\.css$/.test(file)) {
      continue;
    }
    const body = readFileSync(file, "utf8");
    for (const [, name, block] of body.matchAll(/@keyframes\s+([\w-]+)\s*\{([\s\S]*?)\n\}/g)) {
      // Strip the percentage/from/to selectors so only declarations remain.
      const declarations = block.replace(/[\d.]+%|\bfrom\b|\bto\b/g, "");
      for (const [, property] of declarations.matchAll(/([a-z-]+)\s*:/g)) {
        if (property !== "transform" && property !== "opacity") {
          fail(
            `@keyframes ${name} in ${file} animates '${property}'; only transform and opacity ` +
              "are allowed (GPU-safe, no layout thrash, vestibular-safe).",
          );
        }
      }
    }
  }
}

/**
 * Every looping animation anywhere must be declared.
 *
 * A loop is a claim of ongoing activity. The rule was enforced only against
 * `.df-motion-*` classes in the generated CSS, so a hand-written `animation: ... infinite`
 * in styles.css bypassed it entirely -- which is exactly what `spin` and `df-agent-breathe`
 * did.
 */
function auditEveryLoopIsDeclared() {
  const declaredLoops = new Set(allowedLoopPrimitives);
  for (const name of Object.keys(tokens.cyclicAnimations ?? {})) {
    if (!name.startsWith("$")) {
      declaredLoops.add(name);
    }
  }

  for (const file of walkFiles(srcRoot)) {
    if (!/\.css$/.test(file)) {
      continue;
    }
    const body = readFileSync(file, "utf8");
    for (const [, shorthand] of body.matchAll(/animation:\s*([^;]+);/g)) {
      if (!/\binfinite\b/.test(shorthand)) {
        continue;
      }
      // The animation name is the first identifier that is not a keyword or a var().
      const candidates = shorthand
        .replace(/var\([^)]*\)/g, " ")
        .split(/\s+/)
        .filter((token) => /^[A-Za-z][\w-]*$/.test(token))
        .filter((token) => !["infinite", "linear", "both", "none", "alternate", "forwards", "backwards", "normal", "reverse", "ease", "ease-in", "ease-out", "ease-in-out", "running", "paused"].includes(token));
      const name = candidates[0];
      if (name === undefined) {
        fail(`Could not identify the animation name in '${shorthand.trim()}' (${file}).`);
      }
      // The keyframe name may be the primitive itself (`hover`), the generated keyframe
      // (`df-hover`), or the utility class form (`df-motion-hover`). A declared cyclic
      // animation is matched on its literal name (`df-agent-breathe`).
      const aliases = new Set([name]);
      if (name.startsWith("df-motion-")) {
        aliases.add(name.slice("df-motion-".length));
      }
      if (name.startsWith("df-")) {
        aliases.add(name.slice("df-".length));
      }
      const declared = [...aliases].some((alias) => declaredLoops.has(alias));
      if (!declared) {
        fail(
          `${file} loops '${name}' forever, but it is not a declared loop. Add it to ` +
            "cyclicAnimations in motion-tokens.json with the rung whose claim it makes, or " +
            "stop looping it: an undeclared loop animates an element with no event behind it.",
        );
      }
    }
  }
}

/**
 * Durations must be token references, never literals.
 *
 * This is what makes the declared ceiling real. `max: 480` was a name that the codebase
 * exceeded by 3.75x, because a literal `1.8s` in styles.css is invisible to a gate that
 * reads the token file. Requiring `var(--df-motion-*)` makes an out-of-band duration
 * unrepresentable rather than merely discouraged.
 */
function auditNoLiteralDurations() {
  for (const file of walkFiles(srcRoot)) {
    if (!/\.css$/.test(file) || file === cssPath) {
      continue;
    }
    const body = readFileSync(file, "utf8");
    for (const [match, shorthand] of body.matchAll(/(?:^|[\s;{])animation:\s*([^;]+);/gm)) {
      if (/[\d.]+m?s\b/.test(shorthand)) {
        fail(
          `${file} sets a literal animation duration in '${shorthand.trim()}'. Durations must ` +
            "reference var(--df-motion-*) so the declared maximum is a ceiling and not a name.",
        );
      }
      void match;
    }
    for (const [, shorthand] of body.matchAll(/transition:\s*([^;]+);/g)) {
      if (/[\d.]+m?s\b/.test(shorthand)) {
        fail(
          `${file} sets a literal transition duration in '${shorthand.trim()}'. Durations must ` +
            "reference var(--df-motion-*).",
        );
      }
    }
  }
}

/**
 * A cyclic animation's declared duration must be the one it actually uses.
 *
 * Found by mutation: changing `spin`'s declared duration from `cycle` to `max` in the token
 * file left every gate green, because nothing cross-checked the declaration against the CSS.
 * The declaration would have been decorative -- and the whole point of declaring these two
 * animations was to bring them under the honesty rules they had been bypassing.
 */
function auditCyclicDurationsMatchTheirCss() {
  const cyclic = Object.entries(tokens.cyclicAnimations ?? {}).filter(
    ([name]) => !name.startsWith("$"),
  );
  if (cyclic.length === 0) {
    return;
  }
  const styles = readFileSync(resolve(srcRoot, "styles.css"), "utf8");

  for (const [name, spec] of cyclic) {
    if (tokens.durationsMs[spec.duration] === undefined) {
      fail(`Cyclic animation ${name} declares duration '${spec.duration}', which is not a token.`);
      continue;
    }
    const expected = `var(--df-motion-${spec.duration})`;
    // Find every shorthand that runs this animation and confirm it uses the declared token.
    const uses = [...styles.matchAll(/animation:\s*([^;]+);/g)]
      .map(([, shorthand]) => shorthand)
      .filter((shorthand) => new RegExp(`\\b${name}\\b`).test(shorthand));
    if (uses.length === 0) {
      fail(
        `Cyclic animation ${name} is declared in motion-tokens.json but never used in CSS; a ` +
          "declaration for an animation that does not run describes nothing.",
      );
      continue;
    }
    for (const shorthand of uses) {
      if (!shorthand.includes(expected)) {
        fail(
          `${name} declares duration '${spec.duration}' but its CSS uses ` +
            `'${shorthand.trim()}', which does not reference ${expected}. The declaration and ` +
            "the rendering must agree, or the token file is decorative.",
        );
      }
    }
  }
}

auditLoopHonesty();
auditLoopsAreArgued();
auditReducedMotionTwin();
auditGpuSafe();
auditAllKeyframesAreGpuSafe();
auditEveryLoopIsDeclared();
auditNoLiteralDurations();
auditCyclicDurationsMatchTheirCss();
auditNoLegacyDrift();

console.log("Motion audit passed.");

/**
 * Every group that contains a forever-loop must carry a WCAG argument.
 *
 * `cyclicAnimations` had a `$wcag` argument; `primitives` did not, even though `hover` and
 * `resolve` both loop infinitely -- the two motions a user cannot escape by waiting. The
 * exemption under 2.2.2 was relied upon and never written down, which is the same failure the
 * cyclicAnimations argument was added to fix. Requiring it means the next looping primitive
 * cannot ship unargued either.
 */
function auditLoopsAreArgued() {
  const groups = [
    ["primitives", tokens.primitives, declaredPrimitives().some(([, spec]) => spec.loop)],
    [
      "cyclicAnimations",
      tokens.cyclicAnimations ?? {},
      Object.keys(tokens.cyclicAnimations ?? {}).some((name) => !name.startsWith("$")),
    ],
  ];

  for (const [groupName, group, hasLoop] of groups) {
    if (!hasLoop) {
      continue;
    }
    const argument = group.$wcag;
    if (typeof argument !== "string" || argument.trim().length === 0) {
      fail(
        `${groupName} contains a forever-loop but carries no $wcag argument. A loop that cannot ` +
          "be outwaited needs its 2.2.2 position stated where the loop is declared.",
      );
    }
    if (!argument.includes("2.2.2")) {
      fail(
        `${groupName}.$wcag does not address WCAG 2.2.2 (Pause, Stop, Hide), which is the ` +
          "criterion a forever-loop has to answer.",
      );
    }
  }
}
