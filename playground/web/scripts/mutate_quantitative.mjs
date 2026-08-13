/**
 * Mutation harness for the quantitative grammar laws.
 *
 * A gate whose mutants survive is not a gate. Every assertion added for the
 * addressability law is exercised here by breaking the source deliberately, running
 * the verifier that is supposed to catch it, and requiring a non-zero exit. Files are
 * always restored.
 *
 * This exists because two of this gate's own checks were previously found dead: one
 * was defanged by stripping string literals before scanning (which killed the
 * additive-blend check, since the blend mode is a string), and one inspected the
 * wrong file entirely. A third hole -- a rung NAME assigned as a bare string, with
 * the word "rung" never appearing -- was found by the first run of this harness.
 * None were visible without mutation.
 *
 * Mutants are routed to the verifier that can see them. A static audit cannot detect
 * a change in runtime arithmetic, so `depthForAddressable` is checked by the unit
 * suite instead. Claiming the gate covers it would be the same category of error the
 * harness exists to prevent.
 */
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

const TOKENS = "src/design/quantitative-tokens.json";

/** Line endings are CRLF in this checkout, so patterns must not assume "\n". */
const NL = String.raw`\r?\n`;

/** @type {{name: string, file: string, runner?: "gate" | "unit", apply: (source: string) => string}[]} */
const MUTANTS = [
  {
    name: "rung leaks into the aggregated density encoder",
    file: "src/viz/density.ts",
    apply: (source) =>
      source.replace(
        "export interface DensityMark {",
        "export interface DensityMark {\n  rung: string;",
      ),
  },
  {
    name: "rung token family leaks into the density encoder",
    file: "src/viz/density.ts",
    apply: (source) => `${source}\nconst _leak = "var(--df-plausibility-line)";\n`,
  },
  {
    name: "a rung name is assigned as a bare string in the density encoder",
    file: "src/viz/density.ts",
    apply: (source) => `${source}\nconst _verdict = "proven";\n`,
  },
  {
    name: "aggregated marks are permitted to carry a rung",
    file: TOKENS,
    apply: (source) =>
      source.replace('"rungOnAggregatedMarks": false', '"rungOnAggregatedMarks": true'),
  },
  {
    name: "an accumulating blend mode is used by the density painter",
    file: "src/viz/paintDensity.ts",
    apply: (source) =>
      source.replace(
        'ctx.globalCompositeOperation = "source-over";',
        'ctx.globalCompositeOperation = "lighter";',
      ),
  },
  {
    name: "a non-addressable component is declared as DOM",
    file: TOKENS,
    apply: (source) =>
      source.replace(
        new RegExp(`("addressable": false,${NL}\\s*"renderer": )"canvas2d"`),
        '$1"dom"',
      ),
  },
  {
    name: "an addressable component is declared as canvas2d",
    file: TOKENS,
    apply: (source) =>
      source.replace(
        new RegExp(`("renderer": )"dom"(,${NL}\\s*"encoder": "claims\\.ts")`),
        '$1"canvas2d"$2',
      ),
  },
  {
    name: "a component declares an encoder that does not exist",
    file: TOKENS,
    apply: (source) => source.replace('"encoder": "density.ts"', '"encoder": "nowhere.ts"'),
  },
  {
    name: "an unproven rung is given ground contact",
    file: TOKENS,
    apply: (source) =>
      source.replace(
        new RegExp(`("plausibility_only": \\{[\\s\\S]{0,120}?"groundContact": )"[a-z]+"`),
        '$1"contact"',
      ),
  },
  {
    name: "a depth offset exceeds the vestibular bound",
    file: TOKENS,
    apply: (source) => source.replace('"depthPx": 6,', '"depthPx": 60,'),
  },
  {
    // Runtime arithmetic: invisible to a static audit, so the unit suite must own it.
    name: "depth is granted below the addressability floor",
    file: "src/viz/grammar.ts",
    runner: "unit",
    apply: (source) =>
      source.replace(
        "return markHeightPx >= addressableMinHeightPx ? rungSpecs[rung].depthPx : 0;",
        "return rungSpecs[rung].depthPx;",
      ),
  },
  {
    // The non-tautological overtrust guard: re-derives strength from provenance.
    name: "an untrusted provenance is allowed to be proven",
    file: "src/viz/claims.ts",
    runner: "unit",
    apply: (source) =>
      source.replace(
        "if (proven && claim.provenance !== null && UNTRUSTED_PROVENANCE.has(claim.provenance)) {",
        "if (false) {",
      ),
  },
  {
    // The distribution must come from the population, not the severity-ordered prefix.
    name: "the confidence distribution is computed from the truncated detail set",
    file: "src/viz/confidence.ts",
    runner: "unit",
    apply: (source) =>
      source.replace(
        "totalCells: classes.reduce((sum, entry) => sum + entry.count, 0),",
        "totalCells: flagged.cells.length,",
      ),
  },
];

const RUNNERS = {
  gate: () => [process.execPath, ["scripts/audit_quantitative.mjs"]],
  // Invoke vitest's entry module with node directly: execFileSync cannot run a .cmd
  // shim without a shell, and enabling a shell would make the command line
  // quoting-sensitive on Windows.
  unit: () => [process.execPath, ["node_modules/vitest/vitest.mjs", "run", "src/viz", "--silent"]],
};

function run(kind) {
  const [command, args] = RUNNERS[kind]();
  try {
    execFileSync(command, args, { stdio: "pipe" });
    return { exit: 0, output: "" };
  } catch (error) {
    return { exit: error.status ?? 1, output: `${error.stdout ?? ""}${error.stderr ?? ""}` };
  }
}

for (const kind of Object.keys(RUNNERS)) {
  const baseline = run(kind);
  if (baseline.exit !== 0) {
    console.error(`BASELINE FAILS for ${kind}:\n${baseline.output}`);
    process.exit(1);
  }
  console.log(`baseline: ${kind} passes on clean source`);
}

let survivors = 0;
for (const mutant of MUTANTS) {
  const kind = mutant.runner ?? "gate";
  const original = readFileSync(mutant.file, "utf8");
  const mutated = mutant.apply(original);
  if (mutated === original) {
    console.error(`NO-OP     ${mutant.name} (${mutant.file} unchanged)`);
    survivors += 1;
    continue;
  }
  writeFileSync(mutant.file, mutated);
  try {
    const result = run(kind);
    if (result.exit === 0) {
      console.error(`SURVIVED  ${mutant.name} [${kind}]`);
      survivors += 1;
    } else {
      console.log(`killed    ${mutant.name} [${kind}]`);
    }
  } finally {
    writeFileSync(mutant.file, original);
  }
}

for (const kind of Object.keys(RUNNERS)) {
  if (run(kind).exit !== 0) {
    console.error(`RESTORE FAILED: ${kind} does not pass after restore`);
    process.exit(1);
  }
}

console.log(`\n${MUTANTS.length - survivors}/${MUTANTS.length} mutants killed`);
if (survivors > 0) {
  console.error(`${survivors} survivor(s): a claimed guarantee is not enforced`);
  process.exit(1);
}
console.log("source restored, all verifiers green");
