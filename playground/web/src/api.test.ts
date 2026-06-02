import { afterEach, describe, expect, it, vi } from "vitest";
import { DataForgeClient, problemFromResponse } from "./api";
import type { AnalyzeResponse, WorkflowEvent } from "./types";

afterEach(() => {
  vi.unstubAllGlobals();
});

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

  it("posts accepted constraint ids to the analyze workflow", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        calls.push({ input, init });
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );

    const client = new DataForgeClient("https://api.example.test");
    await client.analyze(
      new File(["id\n1"], "sample.csv", { type: "text/csv" }),
      true,
      ["cnd-1"],
    );

    expect(String(calls[0].input)).toContain("/api/analyze?advanced=true");
    const body = calls[0].init?.body;
    expect(body).toBeInstanceOf(FormData);
    expect((body as FormData).get("accepted_constraint_ids")).toBe("[\"cnd-1\"]");
  });

  it("parses NDJSON workflow events and returns the final stream analysis", async () => {
    const finalAnalysis = {
      source: { name: "sample.csv", sha256: "a".repeat(64), rows: 1, columns: 1, column_names: ["id"] },
      meta: { api_version: "0.1.0", contract_version: "repair_contract_v2" },
    } as AnalyzeResponse;
    const firstEvent: WorkflowEvent = {
      schema_version: "workflow_event_v1",
      run_id: "run-1",
      sequence: 0,
      stage_id: "intake",
      status: "running",
      summary: "Reading CSV.",
      started_at: "2026-06-02T00:00:00Z",
      counts: { bytes: 4 },
      requires_human: false,
    };
    const receiptEvent: WorkflowEvent = {
      ...firstEvent,
      sequence: 1,
      stage_id: "receipt",
      status: "completed",
      summary: "Done.",
      completed_at: "2026-06-02T00:00:01Z",
      analysis: finalAnalysis,
    };
    const body = `${JSON.stringify(firstEvent)}\n${JSON.stringify(receiptEvent)}\n`;
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        calls.push({ input, init });
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(body));
              controller.close();
            },
          }),
          { status: 200, headers: { "content-type": "application/x-ndjson" } },
        );
      }),
    );

    const events: WorkflowEvent[] = [];
    const client = new DataForgeClient("https://api.example.test");
    const analysis = await client.analyzeStream(
      new File(["id\n1"], "sample.csv", { type: "text/csv" }),
      false,
      ["cnd-1"],
      { onEvent: (event) => events.push(event) },
    );

    expect(String(calls[0].input)).toContain("/api/analyze/stream");
    const requestBody = calls[0].init?.body;
    expect(requestBody).toBeInstanceOf(FormData);
    expect((requestBody as FormData).get("accepted_constraint_ids")).toBe("[\"cnd-1\"]");
    expect(events.map((event) => event.stage_id)).toEqual(["intake", "receipt"]);
    expect(analysis).toEqual(finalAnalysis);
  });

  it("surfaces stream problem events as API errors", async () => {
    const problem = {
      type: "https://dataforge.local/problems/request_timeout",
      title: "Request Timeout",
      status: 504,
      detail: "Timed out.",
      error: "request_timeout",
    };
    const failedEvent: WorkflowEvent = {
      schema_version: "workflow_event_v1",
      run_id: "run-2",
      sequence: 99,
      stage_id: "receipt",
      status: "failed",
      summary: "Stopped.",
      started_at: "2026-06-02T00:00:00Z",
      completed_at: "2026-06-02T00:00:01Z",
      counts: {},
      requires_human: true,
      problem,
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(`${JSON.stringify(failedEvent)}\n`));
              controller.close();
            },
          }),
          { status: 200, headers: { "content-type": "application/x-ndjson" } },
        ),
      ),
    );

    const client = new DataForgeClient("https://api.example.test");
    await expect(
      client.analyzeStream(new File(["id\n1"], "sample.csv", { type: "text/csv" }), false, [], {
        onEvent: vi.fn(),
      }),
    ).rejects.toMatchObject({ problem });
  });
});
