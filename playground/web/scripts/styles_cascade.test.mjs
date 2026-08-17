/**
 * Cascade-order gate for styles.css.
 *
 * WHAT THIS CATCHES, AND WHY IT IS WORTH A GATE
 *
 * A media query carries no additional specificity. A responsive override therefore only
 * applies if it appears AFTER the base declaration it means to override, for the same
 * selector and property. When it appears before, it is dead text that reads exactly like a
 * working responsive rule.
 *
 * Three components in this stylesheet had that defect, undetected:
 *
 *   .product-loop-rail        collapse at 1320px and 980px, base declared ~360 lines later
 *   .product-loop-grid        collapse at 1320px and 980px, base declared ~430 lines later
 *   .safety-revert-explainer  collapse at 980px, base declared ~500 lines later
 *
 * None of the three ever collapsed, at any viewport, since they were written. The visible
 * consequence was a WCAG 1.4.10 reflow failure: .safety-revert-explainer keeps 220px and
 * 180px track minimums, so at a 320px viewport its third column sat at x=288 and forced the
 * document to 358px. The loop grid's four columns overflowed their panels by 16-24px.
 *
 * This was invisible to every existing check. The colour, motion, perceptual and
 * quantitative audits all read tokens or generated artifacts; nothing read the cascade. It
 * was also invisible to axe, which analyses the DOM as rendered at one viewport and has no
 * opinion about whether a stylesheet says what its author meant.
 *
 * Only IDENTICAL selector strings are compared, which makes the check conservative and
 * exact: identical selectors have identical specificity, so a later unconditional
 * declaration always wins and the finding is never a judgement call.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

const stylesPath = resolve(process.cwd(), "src", "styles.css");
const styles = readFileSync(stylesPath, "utf8");

/**
 * Parses rule blocks, recording for each declaration its selector, property, source order,
 * and whether it sits inside an at-rule. Deliberately small: this is not a CSS engine, and
 * the only structure it needs is "which properties does this selector set, and where".
 */
export function declarations(css) {
  const stripped = css.replace(/\/\*[\s\S]*?\*\//g, "");
  const results = [];
  let index = 0;
  let atRuleDepth = 0;
  let buffer = "";

  while (index < stripped.length) {
    const char = stripped[index];
    if (char === "{") {
      const prelude = buffer.trim();
      buffer = "";
      if (prelude.startsWith("@")) {
        atRuleDepth += 1;
        results.push({ kind: "at-rule-open", prelude, offset: index });
        index += 1;
        continue;
      }
      // Rule block: consume to its matching close brace.
      let depth = 1;
      let body = "";
      index += 1;
      while (index < stripped.length && depth > 0) {
        const inner = stripped[index];
        if (inner === "{") depth += 1;
        else if (inner === "}") {
          depth -= 1;
          if (depth === 0) break;
        }
        body += inner;
        index += 1;
      }
      index += 1;
      const selectors = prelude
        .split(",")
        .map((selector) => selector.trim().replace(/\s+/g, " "))
        .filter((selector) => selector.length > 0);
      for (const declaration of body.split(";")) {
        const colon = declaration.indexOf(":");
        if (colon === -1) continue;
        const property = declaration.slice(0, colon).trim().toLowerCase();
        if (property === "" || property.startsWith("--") || property.includes("{")) continue;
        for (const selector of selectors) {
          results.push({
            kind: "declaration",
            selector,
            property,
            conditional: atRuleDepth > 0,
            offset: index,
          });
        }
      }
      continue;
    }
    if (char === "}") {
      if (atRuleDepth > 0) atRuleDepth -= 1;
      buffer = "";
      index += 1;
      continue;
    }
    buffer += char;
    index += 1;
  }

  return results.filter((entry) => entry.kind === "declaration");
}

/**
 * Returns every conditional declaration that a LATER unconditional declaration of the same
 * selector and property defeats.
 */
export function deadConditionalDeclarations(css) {
  const all = declarations(css);
  const unconditional = all.filter((entry) => !entry.conditional);
  const dead = [];
  for (const entry of all) {
    if (!entry.conditional) continue;
    const defeatedBy = unconditional.filter(
      (other) =>
        other.selector === entry.selector &&
        other.property === entry.property &&
        other.offset > entry.offset,
    );
    if (defeatedBy.length > 0) {
      dead.push(`${entry.selector} { ${entry.property} } is overridden by a later base rule`);
    }
  }
  return [...new Set(dead)];
}

describe("deadConditionalDeclarations", () => {
  it("detects the exact defect three components shipped with", () => {
    const css = `
      @media (max-width: 980px) {
        .panel { grid-template-columns: minmax(0, 1fr); }
      }
      .panel { display: grid; grid-template-columns: 220px 1fr 180px; }
    `;
    expect(deadConditionalDeclarations(css)).toEqual([
      ".panel { grid-template-columns } is overridden by a later base rule",
    ]);
  });

  it("accepts the correct order, base first then the override", () => {
    const css = `
      .panel { display: grid; grid-template-columns: 220px 1fr; }
      @media (max-width: 980px) {
        .panel { grid-template-columns: minmax(0, 1fr); }
      }
    `;
    expect(deadConditionalDeclarations(css)).toEqual([]);
  });

  it("reports a grouped selector only for the member that is actually defeated", () => {
    const css = `
      @media (max-width: 980px) {
        .a, .b { grid-template-columns: 1fr; }
      }
      .a { grid-template-columns: 2fr; }
    `;
    expect(deadConditionalDeclarations(css)).toEqual([
      ".a { grid-template-columns } is overridden by a later base rule",
    ]);
  });

  it("does not compare different selectors, whose specificity may legitimately differ", () => {
    const css = `
      @media (max-width: 980px) {
        .a { color: red; }
      }
      .wrapper .a { color: blue; }
    `;
    expect(deadConditionalDeclarations(css)).toEqual([]);
  });

  it("ignores declarations inside comments", () => {
    const css = `
      @media (max-width: 980px) {
        .a { color: red; }
      }
      /* .a { color: blue; } */
    `;
    expect(deadConditionalDeclarations(css)).toEqual([]);
  });

  it("treats a property set twice inside media queries as fine", () => {
    const css = `
      .a { color: green; }
      @media (max-width: 1320px) { .a { color: red; } }
      @media (max-width: 980px) { .a { color: blue; } }
    `;
    expect(deadConditionalDeclarations(css)).toEqual([]);
  });
});

describe("styles.css", () => {
  it("has no responsive rule defeated by a later unconditional rule", () => {
    const dead = deadConditionalDeclarations(styles);
    expect(dead, `dead responsive declarations:\n  ${dead.join("\n  ")}`).toEqual([]);
  });
});
