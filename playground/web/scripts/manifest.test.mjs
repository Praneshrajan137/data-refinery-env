/**
 * Manifest integrity tests.
 *
 * Two failures motivated these, both of the same shape: a declaration that looked live
 * but was not.
 *
 *   1. `package.json` declared "test" TWICE. JSON.parse silently keeps the last one, so
 *      the first script -- which omitted the perceptual mutation harness -- was dead text
 *      that no tool would ever report. Duplicate keys are invisible to every JSON parser,
 *      so they must be detected in the RAW source.
 *
 *   2. `test:a11y` and `perf:density` existed as scripts but were referenced by no
 *      workflow. The 35-scan accessibility sweep and the 16ms frame budget could not fail
 *      a pull request. Declaring a gate and invoking a gate are different things, so the
 *      binding between CI and the manifest is asserted here rather than assumed.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

// Resolved from cwd rather than import.meta.url: vitest does not hand a test file a
// file:// specifier, so fileURLToPath(import.meta.url) throws "The URL must be of scheme
// file" here (it works fine inside imported .mjs modules, which is why the budget test
// can use it). Vitest runs with cwd at the package root; the manifest name is asserted
// below so a wrong cwd fails loudly instead of silently skipping.
const webRoot = process.cwd();
const repoRoot = resolve(webRoot, "..", "..");
const manifestPath = resolve(webRoot, "package.json");
const workflowPath = resolve(repoRoot, ".github", "workflows", "ci.yml");

const manifestSource = readFileSync(manifestPath, "utf8");
const manifest = JSON.parse(manifestSource);

/**
 * Finds duplicate keys by re-walking the raw text, because a parsed object cannot show
 * them. Scoped per object depth so that the same key legitimately appearing in two
 * different objects is not reported.
 */
function duplicateKeys(source) {
  const duplicates = [];
  const seenByDepth = new Map();
  let depth = 0;
  let inString = false;
  let escaped = false;
  let current = "";
  let pendingKey = null;

  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === '"') {
        inString = false;
        pendingKey = current;
        current = "";
      } else {
        current += char;
      }
      continue;
    }

    if (char === '"') {
      inString = true;
      current = "";
      continue;
    }
    if (char === "{" || char === "[") {
      depth += 1;
      seenByDepth.set(depth, new Set());
      pendingKey = null;
      continue;
    }
    if (char === "}" || char === "]") {
      seenByDepth.delete(depth);
      depth -= 1;
      pendingKey = null;
      continue;
    }
    if (char === ":" && pendingKey !== null) {
      const seen = seenByDepth.get(depth) ?? new Set();
      if (seen.has(pendingKey)) {
        duplicates.push(pendingKey);
      }
      seen.add(pendingKey);
      seenByDepth.set(depth, seen);
      pendingKey = null;
      continue;
    }
    if (char === ",") {
      pendingKey = null;
    }
  }

  return duplicates;
}

describe("duplicateKeys", () => {
  it("detects the exact defect that made one test script dead", () => {
    expect(duplicateKeys('{"scripts":{"test":"a","test":"b"}}')).toEqual(["test"]);
  });

  it("does not report the same key in two sibling objects", () => {
    expect(duplicateKeys('{"a":{"test":1},"b":{"test":2}}')).toEqual([]);
  });

  it("ignores braces, colons and commas inside string values", () => {
    expect(duplicateKeys('{"a":"{\\"test\\": 1, \\"test\\": 2}","b":2}')).toEqual([]);
  });

  it("handles nesting without leaking outer keys into inner scopes", () => {
    expect(duplicateKeys('{"x":1,"y":{"x":1,"x":2}}')).toEqual(["x"]);
  });
});

describe("package.json", () => {
  it("was resolved from the playground web root", () => {
    expect(manifest.name).toBe("dataforge-playground");
  });

  it("declares no key twice", () => {
    expect(duplicateKeys(manifestSource)).toEqual([]);
  });

  it("still runs both mutation harnesses in the default test script", () => {
    expect(manifest.scripts.test).toContain("audit:quantitative:mutants");
    expect(manifest.scripts.test).toContain("audit:perceptual:mutants");
  });
});

describe("CI invokes the gates the manifest declares", () => {  const workflow = readFileSync(workflowPath, "utf8");
  const invoked = new Set(
    [...workflow.matchAll(/npm --prefix playground\/web run ([\w:-]+)/g)].map(
      (match) => match[1],
    ),
  );

  it("references only scripts that exist, so a rename cannot silently drop a gate", () => {
    for (const script of invoked) {
      expect(manifest.scripts, `ci.yml runs "${script}"`).toHaveProperty(script);
    }
  });

  it.each(["test", "test:a11y", "perf:density"])(
    "invokes %s, which was previously declared but never run",
    (script) => {
      expect(invoked.has(script)).toBe(true);
    },
  );
});

/**
 * Playwright configs must not share a preview port.
 *
 * CI runs `test`, `test:a11y` and `perf:density` as three sequential steps, and
 * `reuseExistingServer` is false under CI, so each starts its own preview server. While all
 * three declared port 4173, a lagging teardown from one step would make the next fail with
 * "port already in use". Nothing compared the configs, which is why the collision was
 * introduced unnoticed when the last two steps were wired into CI.
 */
describe("playwright preview ports", () => {
  const configs = ["playwright.config.ts", "playwright.a11y.config.ts", "playwright.perf.config.ts"];

  const portsOf = (source) => [...source.matchAll(/127\.0\.0\.1:(\d+)|--port (\d+)/g)].map((m) => m[1] ?? m[2]);

  it.each(configs)("%s declares exactly one port consistently", (name) => {
    const ports = new Set(portsOf(readFileSync(resolve(webRoot, name), "utf8")));
    expect(ports.size, `${name} mixes ports: ${[...ports].join(", ")}`).toBe(1);
  });

  it("gives every config a distinct port", () => {
    const chosen = configs.map((name) => portsOf(readFileSync(resolve(webRoot, name), "utf8"))[0]);
    expect(new Set(chosen).size, `ports: ${chosen.join(", ")}`).toBe(configs.length);
  });
});
