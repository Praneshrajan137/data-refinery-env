import { describe, expect, it } from "vitest";
import { normalizeRoutePath, routeById, routeFromPathname } from "./routes";

describe("product routes", () => {
  it("parses playground paths into stable product routes", () => {
    expect(routeFromPathname("/playground/").id).toBe("run");
    expect(routeFromPathname("/playground/run").id).toBe("run");
    expect(routeFromPathname("/playground/atlas/").id).toBe("atlas");
    expect(routeFromPathname("/playground/evidence?x=1").id).toBe("evidence");
    expect(routeFromPathname("/playground/guardrail").id).toBe("guardrail");
  });

  it("falls back to the product loop for unknown paths", () => {
    expect(routeFromPathname("/playground/unknown").id).toBe("run");
    expect(routeById("receipt").href).toBe("/playground/receipt");
  });

  it("normalizes empty, hash, query, and trailing slash paths", () => {
    expect(normalizeRoutePath("")).toBe("/");
    expect(normalizeRoutePath("/run/")).toBe("/run");
    expect(normalizeRoutePath("/run?sample=hospital")).toBe("/run");
    expect(normalizeRoutePath("/system#colors")).toBe("/system");
  });
});
