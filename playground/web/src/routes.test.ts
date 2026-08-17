import { describe, expect, it } from "vitest";
import {
  DEFAULT_ROUTE_ID,
  PRODUCT_ROUTES,
  buildRunQuery,
  isKnownRoutePath,
  hrefWithRunQuery,
  normalizeRoutePath,
  parseRunQuery,
  routeById,
  routeFromPathname,
} from "./routes";

describe("product routes", () => {
  it("parses playground paths into stable product routes", () => {
    // The bare base resolves to the DECLARED front door, not to whichever route happens to be
    // listed first. Asserted against DEFAULT_ROUTE_ID rather than a literal so that changing the
    // front door stays a one-line decision instead of a test edit in two places.
    expect(routeFromPathname("/playground/").id).toBe(DEFAULT_ROUTE_ID);
    expect(routeFromPathname("/playground/run").id).toBe("run");
    expect(routeFromPathname("/playground/atlas/").id).toBe("atlas");
    expect(routeFromPathname("/playground/evidence?x=1").id).toBe("evidence");
    expect(routeFromPathname("/playground/guardrail").id).toBe("guardrail");
  });

  it("falls back to the declared front door for unknown paths", () => {
    expect(routeFromPathname("/playground/unknown").id).toBe(DEFAULT_ROUTE_ID);
    expect(routeById("receipt").href).toBe("/playground/receipt");
  });

  it("declares a front door that is a real route", () => {
    // A default pointing at a non-existent id would silently fall through to PRODUCT_ROUTES[0],
    // reinstating the accident the named constant exists to remove.
    expect(PRODUCT_ROUTES.map((route) => route.id)).toContain(DEFAULT_ROUTE_ID);
  });

  it("normalizes empty, hash, query, and trailing slash paths", () => {
    expect(normalizeRoutePath("")).toBe("/");
    expect(normalizeRoutePath("/run/")).toBe("/run");
    expect(normalizeRoutePath("/run?sample=hospital")).toBe("/run");
    expect(normalizeRoutePath("/system#colors")).toBe("/system");
  });
});

describe("shareable run state in the query string", () => {
  it("round-trips a full run description", () => {
    const state = {
      sample: "hospital_10rows",
      advanced: true,
      agent: true,
      consensus: true,
      constraints: ["cnd-1", "cnd-2"],
    };
    expect(parseRunQuery(buildRunQuery(state))).toEqual(state);
  });

  it("keeps a default run URL clean rather than emitting empty parameters", () => {
    expect(buildRunQuery({})).toBe("");
    expect(buildRunQuery({ advanced: false, agent: false, consensus: false })).toBe("");
  });

  it("ignores a sample name that is not a plausible identifier", () => {
    // A shared link is untrusted input; the sample name is used to hit a backend path.
    expect(parseRunQuery("?sample=../../etc/passwd").sample).toBeUndefined();
    expect(parseRunQuery("?sample=" + "a".repeat(65)).sample).toBeUndefined();
    expect(parseRunQuery("?sample=hospital_10rows").sample).toBe("hospital_10rows");
  });

  it("treats only explicit truthy values as enabled", () => {
    expect(parseRunQuery("?advanced=1").advanced).toBe(true);
    expect(parseRunQuery("?advanced=true").advanced).toBe(true);
    expect(parseRunQuery("?advanced=0").advanced).toBeUndefined();
    expect(parseRunQuery("?advanced=yes").advanced).toBeUndefined();
  });

  it("de-duplicates and trims accepted constraint ids", () => {
    expect(parseRunQuery("?constraints=cnd-1, cnd-1 ,cnd-2").constraints).toEqual([
      "cnd-1",
      "cnd-2",
    ]);
  });

  it("drops an empty constraint list rather than carrying an empty array", () => {
    expect(parseRunQuery("?constraints=").constraints).toBeUndefined();
    expect(parseRunQuery("?constraints=,,").constraints).toBeUndefined();
  });

  it("tolerates a leading question mark or none", () => {
    expect(parseRunQuery("?sample=beers_10rows").sample).toBe("beers_10rows");
    expect(parseRunQuery("sample=beers_10rows").sample).toBe("beers_10rows");
  });

  it("ignores parameters it does not know", () => {
    expect(parseRunQuery("?unknown=1&sample=flights_10rows")).toEqual({
      sample: "flights_10rows",
    });
  });

  it("builds a shareable href against a route", () => {
    expect(hrefWithRunQuery(routeById("receipt"), { sample: "hospital_10rows" })).toBe(
      "/playground/receipt?sample=hospital_10rows",
    );
  });

  it("still resolves the route when a query is present, which it previously discarded", () => {
    expect(routeFromPathname("/playground/receipt").id).toBe("receipt");
    expect(normalizeRoutePath("/receipt?sample=x#frag")).toBe("/receipt");
  });
});

describe("unknown addresses", () => {
  it("recognises every real route", () => {
    for (const route of ["", "run", "atlas", "evidence", "repairs", "guardrail", "receipt", "system"]) {
      expect(isKnownRoutePath(`/playground/${route}`), route || "index").toBe(true);
    }
  });

  it("rejects an address that names no page", () => {
    expect(isKnownRoutePath("/playground/nonsense")).toBe(false);
    expect(isKnownRoutePath("/playground/run/extra")).toBe(false);
  });

  it("still recognises a real route carrying run state", () => {
    expect(isKnownRoutePath("/playground/receipt?sample=hospital_10rows")).toBe(true);
  });

  it("does not disagree with routeFromPathname about real routes", () => {
    // routeFromPathname keeps its silent fallback on purpose; these two answer different
    // questions and must not drift on the paths that DO exist.
    expect(isKnownRoutePath("/playground/atlas")).toBe(true);
    expect(routeFromPathname("/playground/atlas").id).toBe("atlas");
  });
});
