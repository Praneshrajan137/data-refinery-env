/**
 * The CSS-custom-property to canvas bridge.
 *
 * The forced-colours case is the reason this file exists. `forced-colors: active` overrides
 * CSS-painted colour but CANNOT reach a canvas `fillStyle`, so the density map is the one
 * surface where Windows High Contrast has to be handled in code. Before `forcedColoursInk`
 * the map painted `--df-confidence-medium-text` -- a neutral grey -- onto whatever background
 * the OS chose, with no contrast guarantee and no repaint on mode change, because
 * `onTokenChange` did not subscribe to the query. Its comment claimed the three queries it
 * did watch were "the complete set of triggers".
 *
 * Tested here rather than in a browser: it is a pure function of `matchMedia` and a computed
 * `color`. An e2e version was written and removed, because reaching the canvas required the
 * API route mocks that live in another spec file's `beforeEach`.
 */

import { afterEach, describe, expect, it, vi } from "vitest";

import { onTokenChange, readVizTokens } from "./tokens";

type MediaListener = () => void;

/**
 * Stub `matchMedia` so a specific query reports as matching.
 *
 * jsdom does not implement `matchMedia` at all, so without this every branch that consults it
 * silently takes the "not matching" path -- which would make a forced-colours test pass for
 * the wrong reason.
 */
function stubMatchMedia(matching: string[]): { listeners: Map<string, MediaListener[]> } {
  const listeners = new Map<string, MediaListener[]>();
  vi.stubGlobal("matchMedia", (query: string) => ({
    matches: matching.includes(query),
    media: query,
    addEventListener: (_event: string, handler: MediaListener) => {
      const existing = listeners.get(query) ?? [];
      existing.push(handler);
      listeners.set(query, existing);
    },
    removeEventListener: (_event: string, handler: MediaListener) => {
      const existing = listeners.get(query) ?? [];
      listeners.set(
        query,
        existing.filter((candidate) => candidate !== handler),
      );
    },
  }));
  return { listeners };
}

afterEach(() => {
  vi.unstubAllGlobals();
  document.documentElement.style.removeProperty("color");
});

describe("forced colours", () => {
  it("paints the density map with the OS text colour", () => {
    // In forced-colours mode the document's own `color` is overridden to CanvasText, so
    // reading the resolved value off the root gets whatever the OS chose. A custom property
    // cannot carry it: `--df-ink: CanvasText` reads back as the literal string.
    stubMatchMedia(["(forced-colors: active)"]);
    document.documentElement.style.setProperty("color", "rgb(0, 255, 0)");

    const tokens = readVizTokens();

    expect(tokens.densityInk.r).toBeCloseTo(0, 2);
    expect(tokens.densityInk.g).toBeCloseTo(1, 2);
    expect(tokens.densityInk.b).toBeCloseTo(0, 2);
  });

  it("leaves the density ink alone when forced colours is not active", () => {
    stubMatchMedia([]);
    document.documentElement.style.setProperty("color", "rgb(0, 255, 0)");

    const tokens = readVizTokens();

    // Falls back to the neutral confidence token (or its neutral fallback), never to the
    // document colour. If this ever equalled pure green the override would be leaking.
    expect(tokens.densityInk.g).not.toBeCloseTo(1, 2);
  });

  it("subscribes to the forced-colours query so a mode change repaints", () => {
    const { listeners } = stubMatchMedia([]);

    const unsubscribe = onTokenChange(() => {});

    expect(listeners.get("(forced-colors: active)")?.length ?? 0).toBeGreaterThan(0);
    unsubscribe();
    expect(listeners.get("(forced-colors: active)")?.length ?? 0).toBe(0);
  });

  it("subscribes to every mode the OS can impose", () => {
    const { listeners } = stubMatchMedia([]);

    const unsubscribe = onTokenChange(() => {});

    for (const query of [
      "(prefers-color-scheme: dark)",
      "(prefers-contrast: more)",
      "(color-gamut: p3)",
      "(forced-colors: active)",
    ]) {
      expect(listeners.get(query)?.length ?? 0, `not subscribed to ${query}`).toBeGreaterThan(0);
    }
    unsubscribe();
  });

  it("does nothing when matchMedia is unavailable", () => {
    // Server-side rendering and older environments. Must not throw.
    vi.stubGlobal("matchMedia", undefined);
    expect(() => onTokenChange(() => {})()).not.toThrow();
    expect(() => readVizTokens()).not.toThrow();
  });
});
