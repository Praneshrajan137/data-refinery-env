import { describe, expect, it } from "vitest";
import { backendPath, getRuntimeConfig, normalizeBackendUrl } from "./config";

describe("runtime config", () => {
  it("normalizes backend URLs", () => {
    expect(normalizeBackendUrl(" https://example.hf.space/// ")).toBe(
      "https://example.hf.space",
    );
    expect(normalizeBackendUrl(undefined)).toBe("");
  });

  it("reads the public config contract", () => {
    window.__DATAFORGE_CONFIG__ = { BACKEND_URL: "https://backend.example/" };

    expect(getRuntimeConfig()).toEqual({ BACKEND_URL: "https://backend.example" });
  });

  it("builds relative paths when no backend is configured", () => {
    expect(backendPath("", "/api/health")).toBe("/api/health");
    expect(backendPath("https://backend.example", "/api/health")).toBe(
      "https://backend.example/api/health",
    );
  });
});
