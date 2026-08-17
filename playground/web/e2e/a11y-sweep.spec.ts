import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

/**
 * Exhaustive accessibility sweep: every route, every rendering mode the OS can impose.
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * The main suite had five axe scans covering three routes -- /evidence, /receipt and
 * /guardrail. `/atlas`, `/repairs` and `/system` were never scanned at all. Four of the five
 * ran at the Playwright default colour scheme, so dark mode was covered by exactly one flow,
 * and NO scan ran under `prefers-contrast: more`.
 *
 * That last gap mattered: the high-contrast overrides, whose palette values
 * `auditHighContrast` pins by exact string, had no accessibility verification of any kind. A
 * real defect lived there -- the light theme's high-contrast action border sat at 1.46:1
 * against its own background, below the 3:1 that WCAG 1.4.11 requires for a control boundary,
 * and a downgrade from the 3.43:1 of the standard border it replaced. The mode that exists to
 * raise contrast was lowering it, in both themes, because the two themes' values had been
 * swapped.
 *
 * Isolated in its own config because 28 axe analyses is CPU-bound enough to starve the
 * functional suite. Run with:
 *   npm run test:a11y
 */

const EVERY_ROUTE = [
  "run",
  "atlas",
  "evidence",
  "repairs",
  "guardrail",
  "receipt",
  "system",
] as const;

/**
 * The index is scanned separately from the named routes because it is not one of them: it
 * is whatever `routeFromPathname` falls back to. It appeared in the navigation smoke test
 * but in no accessibility scan, so the first URL any visitor loads was the one route never
 * checked.
 */
const EVERY_PATH = ["", ...EVERY_ROUTE] as const;

async function scanEveryPath(page: import("@playwright/test").Page, label: string) {
  for (const routeName of EVERY_PATH) {
    await page.goto(`/playground/${routeName}`);
    await expect(page.locator("main")).toBeVisible();

    const scan = await new AxeBuilder({ page }).analyze();
    expect(
      scan.violations,
      `${routeName || "index"} in ${label}: ` +
        scan.violations.map((violation) => violation.id).join(", "),
    ).toEqual([]);
  }
}

for (const contrast of ["no-preference", "more"] as const) {
  for (const colorScheme of ["light", "dark"] as const) {
    test(`every route passes axe in ${colorScheme} at contrast:${contrast}`, async ({ page }) => {
      await page.emulateMedia({ colorScheme, contrast });
      await scanEveryPath(page, `${colorScheme}/${contrast}`);
    });
  }
}

/**
 * Reduced motion was emulated in the functional suite but never scanned.
 *
 * The reduced-motion twin collapses every duration to 0.001ms and removes transforms. That
 * is a different rendered DOM, not merely a faster one -- state that motion was carrying
 * has to be carried by something else, and a rung that depended on a transform for its
 * form would lose it here. One pass over every route, in light only: motion does not
 * interact with the colour axis, so multiplying this by scheme and contrast would quadruple
 * the runtime to re-verify the same thing.
 */
test("every route passes axe under reduced motion", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await scanEveryPath(page, "reduced-motion");
});

/**
 * A mobile viewport is a different DOM here, not a narrower one: three width breakpoints
 * restack the layout, and the 720px block is the only place a touch-target floor is
 * declared at all. The sweep's own header claimed the checks were "viewport-independent",
 * which is true of token contrast and false of landmark structure once columns collapse.
 */
test("every route passes axe at a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 412, height: 915 });
  await scanEveryPath(page, "mobile 412px");
});

/**
 * Forced colours, checked structurally.
 *
 * Windows High Contrast is not `prefers-contrast: more`: it hands the palette to the OS and
 * DROPS box-shadow, which in this design carries ground contact, earned depth, and the
 * corroborated witness rail's inner line. The identity law guarantees that form rather than
 * hue distinguishes every rung pair, so the ladder should survive -- this asserts that it
 * does rather than trusting the argument.
 */
test("forced colours keeps every route accessible", async ({ page }) => {
  await page.emulateMedia({ forcedColors: "active" });
  for (const routeName of EVERY_PATH) {
    await page.goto(`/playground/${routeName}`);
    await expect(page.locator("main")).toBeVisible();

    const scan = await new AxeBuilder({ page }).analyze();
    expect(
      scan.violations,
      `${routeName || "index"} under forced colours: ` +
        scan.violations.map((violation) => violation.id).join(", "),
    ).toEqual([]);
  }
});

/**
 * The canvas under forced colours is verified at unit level, not here.
 *
 * A browser test for it was written and removed on evidence. The API route mocks live in
 * `playground.spec.ts`'s `beforeEach`, so without them the sample dataset never loads and
 * Analyze never enables; importing that file to reuse the mocks would re-register all of its
 * tests inside this config. `forcedColoursInk` is a pure function of `matchMedia` and a
 * computed `color`, so `src/viz/tokens.test.ts` tests exactly the thing that does the work,
 * faster and more precisely than driving a whole browser to reach it.
 *
 * The CSS side of forced colours is covered by the sweep above, which loads every route with
 * `forcedColors: "active"`.
 */
