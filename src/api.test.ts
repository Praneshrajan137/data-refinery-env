import { describe, expect, it } from "vitest";
import { problemFromResponse } from "./api";

describe("problem detail handling", () => {
  it("preserves RFC 9457 problem extension members", async () => {
    const response = new Response(
      JSON.stringify({
        type: "https://dataforge.local/problems/advanced_mode_unavailable",
        title: "Advanced Mode Unavailable",
        status: 400,
        detail: "Provider key is missing.",
        error: "advanced_mode_unavailable",
      }),
      {
        status: 400,
        headers: { "content-type": "application/problem+json" },
      },
    );

    await expect(problemFromResponse(response)).resolves.toMatchObject({
      status: 400,
      error: "advanced_mode_unavailable",
      detail: "Provider key is missing.",
    });
  });

  it("extracts legacy FastAPI detail payloads without exposing wrappers to the UI", async () => {
    const response = new Response(
      JSON.stringify({
        detail: { error: "file_too_large", message: "Too large." },
      }),
      {
        status: 413,
        statusText: "Payload Too Large",
        headers: { "content-type": "application/json" },
      },
    );

    await expect(problemFromResponse(response)).resolves.toMatchObject({
      status: 413,
      error: "file_too_large",
      detail: "Too large.",
    });
  });
});
