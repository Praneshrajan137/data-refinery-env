import { expect, test } from "@playwright/test";

/**
 * The accessibility checks axe cannot make.
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * axe is a static-DOM analyser. It reports roughly a third of real accessibility defects
 * and, by design, none of the ones that require operating the page or measuring rendered
 * geometry. Before this file, the entire e2e suite contained zero `keyboard.press("Tab")`
 * calls -- the three Enter presses were each preceded by an explicit `.focus()`, so
 * activation was verified and focus ORDER was not. There was no reflow test at any width,
 * and no measurement of any control's rendered size, while `styles.css` declared a 44px
 * floor for exactly two selectors below 720px and heights of 24-38px everywhere else.
 *
 * WHICH THRESHOLDS ARE GATED, AND WHY THOSE
 * -----------------------------------------
 * Target size is gated at 24x24 CSS px, which is WCAG 2.2 Success Criterion 2.5.8 (Target
 * Size (Minimum), Level AA) -- normative for AA conformance. It is NOT gated at 44x44:
 * that is SC 2.5.5 (Target Size (Enhanced), Level AAA), and gating a AAA criterion would
 * assert a conformance level this product does not claim. The 44px gap is measured and
 * REPORTED instead, following the precedent set when the chroma band-monotonicity check
 * was demoted from gate to report rather than asserting a direction the design does not
 * actually follow.
 *
 * Reflow is gated at 320 CSS px width, which is SC 1.4.10 (Reflow, Level AA): content must
 * not require two-dimensional scrolling at 320px, that width being the stated equivalent
 * of a 1280px viewport at 400% zoom.
 */

const EVERY_PATH = ["", "run", "atlas", "evidence", "repairs", "guardrail", "receipt", "system"];

/** SC 2.5.8, Level AA. */
const TARGET_MINIMUM_PX = 24;
/** SC 2.5.5, Level AAA -- reported, not gated. */
const TARGET_ENHANCED_PX = 44;
/** SC 1.4.10, Level AA. */
const REFLOW_WIDTH_PX = 320;

const INTERACTIVE = "a[href], button, input, select, textarea, [tabindex]:not([tabindex='-1'])";

test.describe("keyboard operability", () => {
  test("every route can be traversed by Tab alone, in document order, with no focus trap", async ({
    page,
  }) => {
    for (const routeName of EVERY_PATH) {
      await page.goto(`/playground/${routeName}`);
      await expect(page.locator("main")).toBeVisible();

      // A positive tabindex overrides document order and is the classic way focus order
      // stops matching reading order. Nothing should need one.
      const positiveTabindex = await page.locator("[tabindex]").evaluateAll((nodes) =>
        nodes
          .map((node) => Number.parseInt(node.getAttribute("tabindex") ?? "0", 10))
          .filter((value) => value > 0),
      );
      expect(positiveTabindex, `${routeName || "index"} uses a positive tabindex`).toEqual([]);

      const reachable = await page.evaluate(async (selector) => {
        const visible = (element: Element) => {
          const rect = element.getBoundingClientRect();
          const style = window.getComputedStyle(element);
          return (
            rect.width > 0 &&
            rect.height > 0 &&
            style.visibility !== "hidden" &&
            style.display !== "none" &&
            !(element as HTMLElement).hasAttribute("disabled")
          );
        };
        const candidates = [...document.querySelectorAll(selector)].filter(visible);
        return candidates.length;
      }, INTERACTIVE);

      if (reachable === 0) {
        continue;
      }

      // Walk forward with real Tab presses and record the DOM position of each stop. A
      // trap shows up as the sequence never advancing; an order defect shows up as the
      // recorded positions not increasing.
      const seen: number[] = [];
      await page.locator("body").click({ position: { x: 2, y: 2 } });
      for (let step = 0; step < Math.min(reachable, 40); step += 1) {
        await page.keyboard.press("Tab");
        const position = await page.evaluate(() => {
          const active = document.activeElement;
          if (active === null || active === document.body) {
            return -1;
          }
          return [...document.querySelectorAll("*")].indexOf(active);
        });
        if (position === -1) {
          break;
        }
        seen.push(position);
      }

      expect(seen.length, `${routeName || "index"} reached no focusable element by Tab`).toBeGreaterThan(0);

      const distinct = new Set(seen);
      expect(
        distinct.size,
        `${routeName || "index"} focus did not advance past ${distinct.size} element(s) in ${seen.length} presses, which is a focus trap`,
      ).toBeGreaterThan(Math.min(3, seen.length - 1));

      const ascending = seen.every((position, index) => index === 0 || position > seen[index - 1]);
      expect(
        ascending,
        `${routeName || "index"} tab order does not follow document order: ${seen.join(", ")}`,
      ).toBe(true);
    }
  });

  test("the focused control is always visibly focused", async ({ page }) => {
    await page.goto("/playground/run");
    await expect(page.locator("main")).toBeVisible();

    await page.locator("body").click({ position: { x: 2, y: 2 } });
    for (let step = 0; step < 12; step += 1) {
      await page.keyboard.press("Tab");
      const indication = await page.evaluate(() => {
        const active = document.activeElement as HTMLElement | null;
        if (active === null || active === document.body) {
          return null;
        }
        const style = window.getComputedStyle(active);
        return {
          label: active.getAttribute("aria-label") ?? active.textContent?.trim().slice(0, 40) ?? "",
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          boxShadow: style.boxShadow,
        };
      });
      if (indication === null) {
        break;
      }
      const hasOutline =
        indication.outlineStyle !== "none" && Number.parseFloat(indication.outlineWidth) > 0;
      const hasRing = indication.boxShadow !== "none" && indication.boxShadow !== "";
      expect(
        hasOutline || hasRing,
        `"${indication.label}" receives focus with no visible indication`,
      ).toBe(true);
    }
  });
});

test.describe("reflow", () => {
  test(`every route reflows at ${REFLOW_WIDTH_PX}px without two-dimensional scrolling`, async ({
    page,
  }) => {
    await page.setViewportSize({ width: REFLOW_WIDTH_PX, height: 800 });

    for (const routeName of EVERY_PATH) {
      await page.goto(`/playground/${routeName}`);
      await expect(page.locator("main")).toBeVisible();

      const overflow = await page.evaluate(() => {
        const doc = document.documentElement;
        // Any element wider than the viewport that is not inside an element which
        // legitimately scrolls horizontally is a reflow failure. Tables are allowed to
        // scroll inside their own container; the PAGE is not.
        //
        // Reported deepest-first: an ancestor is wide BECAUSE a descendant is, so the
        // shallowest offenders are symptoms and the deepest one is the cause. Sorting the
        // other way round produced a diagnostic that named only wrappers.
        const offenders: {
          tag: string;
          className: string;
          width: number;
          depth: number;
          text: string;
        }[] = [];
        for (const element of document.querySelectorAll("*")) {
          const rect = element.getBoundingClientRect();
          // Both conditions matter. Width alone misses an element that FITS but is
          // positioned past the right edge (absolute offset, negative margin, transform),
          // which is how a 358px document reported zero offenders. Right-edge alone misses
          // nothing, but width is kept in the report because it distinguishes "too big" from
          // "pushed out".
          const tooWide = rect.width > doc.clientWidth + 1;
          const pushedOut = rect.right > doc.clientWidth + 1 && rect.width > 0;
          if (!tooWide && !pushedOut) {
            continue;
          }
          let ancestor: Element | null = element.parentElement;
          let scrollable = false;
          let depth = 0;
          while (ancestor !== null) {
            depth += 1;
            const style = window.getComputedStyle(ancestor);
            if (style.overflowX === "auto" || style.overflowX === "scroll") {
              scrollable = true;
              break;
            }
            ancestor = ancestor.parentElement;
          }
          if (!scrollable) {
            offenders.push({
              tag: element.tagName.toLowerCase(),
              className: typeof element.className === "string" ? element.className : "",
              width: Math.round(rect.width),
              depth,
              text: `${Math.round(rect.left)}..${Math.round(rect.right)} ${(element.textContent ?? "").trim().slice(0, 22)}`,
            });
          }
        }
        // Reported from BOTH ends of the overflow chain. Deepest-first alone is
        // misleading: a block child fills its parent, so a chain of equal widths is
        // ambiguous, and slicing to the deepest few hid the ancestor that actually
        // established the width. The outermost offender is where the constraint is lost;
        // the innermost is what could not shrink.
        // A second, independent detector. The rect-based pass above cannot see
        // ::before/::after content, and a 358px document with zero rect offenders is
        // exactly what that blind spot looks like. This asks each box whether ITS OWN
        // content overflows while being neither clipped nor scrollable, which catches
        // pseudo-element and inline-content overflow that has no element of its own.
        const leaking: { tag: string; className: string; over: number; depth: number }[] = [];
        for (const element of document.querySelectorAll("*")) {
          const style = window.getComputedStyle(element);
          if (style.overflowX !== "visible") {
            continue;
          }
          const over = element.scrollWidth - element.clientWidth;
          if (over <= 1 || element.clientWidth === 0) {
            continue;
          }
          let depth = 0;
          let ancestor: Element | null = element.parentElement;
          while (ancestor !== null) {
            depth += 1;
            ancestor = ancestor.parentElement;
          }
          leaking.push({
            tag: element.tagName.toLowerCase(),
            className: typeof element.className === "string" ? element.className : "",
            over,
            depth,
          });
        }
        leaking.sort((left, right) => right.depth - left.depth);

        offenders.sort((left, right) => left.depth - right.depth);
        const outermost = offenders.slice(0, 3);
        const innermost = offenders.slice(-3).reverse();

        // Recorded even when no element exceeds the viewport, because the document can
        // scroll without any single rect overflowing -- the root boxes' own min-width plus
        // margins do it, and a report of "0 offenders, 358px wide" is a dead end.
        const body = document.body;
        const roots =
          `html.scrollWidth=${doc.scrollWidth} html.clientWidth=${doc.clientWidth} ` +
          `body.scrollWidth=${body.scrollWidth} body.offsetWidth=${body.offsetWidth} ` +
          `body.minWidth=${window.getComputedStyle(body).minWidth} ` +
          `html.minWidth=${window.getComputedStyle(doc).minWidth}`;

        return {
          documentScrolls: doc.scrollWidth > doc.clientWidth + 1,
          scrollWidth: doc.scrollWidth,
          clientWidth: doc.clientWidth,
          offenders: [...outermost, ...innermost],
          roots,
          leaking: leaking.slice(0, 4),
        };
      });

      expect(
        overflow.documentScrolls,
        `${routeName || "index"} scrolls horizontally at ${REFLOW_WIDTH_PX}px ` +
          `(${overflow.scrollWidth} > ${overflow.clientWidth}); outermost then innermost: ` +
          overflow.offenders
            .map((o) => `${o.tag}.${o.className}@${o.width}px(d${o.depth})"${o.text}"`)
            .join(" | ") +
          ` leaking: ` +
          overflow.leaking
            .map((l) => `${l.tag}.${l.className}+${l.over}px(d${l.depth})`)
            .join(" | ") +
          ` [${overflow.roots}]`,
      ).toBe(false);
    }
  });
});

test.describe("target size", () => {
  test(`every control meets the ${TARGET_MINIMUM_PX}px AA minimum, and the AAA gap is reported`, async ({
    page,
  }) => {
    await page.setViewportSize({ width: 412, height: 915 });

    const belowMinimum: string[] = [];
    const belowEnhanced: string[] = [];

    for (const routeName of EVERY_PATH) {
      await page.goto(`/playground/${routeName}`);
      await expect(page.locator("main")).toBeVisible();

      const measured = await page.evaluate((selector) => {
        const results: {
          label: string;
          selector: string;
          width: number;
          height: number;
          exempt: string | null;
        }[] = [];
        for (const element of document.querySelectorAll(selector)) {
          const rect = element.getBoundingClientRect();
          if (rect.width === 0 || rect.height === 0) {
            continue;
          }
          const style = window.getComputedStyle(element);
          if (style.visibility === "hidden" || style.display === "none") {
            continue;
          }

          const tag = element.tagName.toLowerCase();
          const type = element.getAttribute("type");
          const describe = `${tag}${type ? `[type=${type}]` : ""}.${
            typeof element.className === "string" ? element.className : ""
          }`;

          // A visually hidden input whose LABEL is the real target is not itself a target.
          // This is the standard accessible file-input pattern -- the 1x1 input at
          // styles.css .file-intake input -- and measuring the input rather than the label
          // reports a defect that no user can encounter. The label is measured on its own.
          const hiddenButLabelled =
            (style.pointerEvents === "none" || style.opacity === "0") &&
            (element.closest("label") !== null ||
              (element.id !== "" && document.querySelector(`label[for="${element.id}"]`) !== null));
          if (hiddenButLabelled) {
            results.push({ label: "", selector: describe, width: 0, height: 0, exempt: "labelled" });
            continue;
          }

          // WCAG 2.5.8 measures the TARGET, and for a checkbox or radio with an associated
          // label the clickable target is the union of the two. Measuring the 13x13 native
          // box alone understates every checkbox in the product.
          let { width, height } = rect;
          if (type === "checkbox" || type === "radio") {
            const wrapper =
              element.closest("label") ??
              (element.id !== "" ? document.querySelector(`label[for="${element.id}"]`) : null);
            if (wrapper !== null) {
              const union = wrapper.getBoundingClientRect();
              width = Math.max(width, union.width);
              height = Math.max(height, union.height);
            }
          }

          // SC 2.5.8 exempts targets in a sentence or block of text.
          const parentTag = element.parentElement?.tagName.toLowerCase() ?? "";
          const inline = tag === "a" && ["p", "li", "span", "td"].includes(parentTag);

          results.push({
            label:
              element.getAttribute("aria-label") ??
              element.textContent?.trim().slice(0, 32) ??
              tag,
            selector: describe,
            width,
            height,
            exempt: inline ? "inline-text" : null,
          });
        }
        return results;
      }, INTERACTIVE);

      for (const control of measured) {
        if (control.exempt !== null) {
          continue;
        }
        const where = `${routeName || "index"}: ${control.selector} "${control.label}" ${Math.round(control.width)}x${Math.round(control.height)}`;
        if (control.width < TARGET_MINIMUM_PX || control.height < TARGET_MINIMUM_PX) {
          belowMinimum.push(where);
        } else if (control.width < TARGET_ENHANCED_PX || control.height < TARGET_ENHANCED_PX) {
          belowEnhanced.push(where);
        }
      }
    }

    // Reported, not gated: SC 2.5.5 is Level AAA.
    console.log(
      `TARGET_SIZE below ${TARGET_ENHANCED_PX}px (AAA 2.5.5, reported): ${belowEnhanced.length} control(s)`,
    );

    expect(
      belowMinimum,
      `controls below the ${TARGET_MINIMUM_PX}px WCAG 2.5.8 AA minimum:\n  ${belowMinimum.join("\n  ")}`,
    ).toEqual([]);
  });
});
