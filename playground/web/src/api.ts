import { backendPath, normalizeBackendUrl } from "./config";
import type {
  AnalyzeResponse,
  AnalyzeStreamOptions,
  BackendCapability,
  ProblemDetail,
  ProfileResponse,
  RepairMode,
  RepairResponse,
  VerifyFixesOptions,
  VerifyFixesResponse,
  VerifyScenario,
  WorkflowEvent,
} from "./types";

const REQUEST_TIMEOUT_MS = 20_000;

/**
 * A stream has no single response to time out, so it is bounded by IDLE time instead: the
 * clock resets on every chunk. Before this, `analyzeStream` called bare `fetch` with no
 * timeout at all, so a hung stream hung forever and the Cancel button was the only way out.
 * Generous relative to the 20s request timeout because a long analysis legitimately goes
 * quiet between stages.
 */
const STREAM_IDLE_TIMEOUT_MS = 45_000;

export class ApiProblemError extends Error {
  problem: ProblemDetail;

  constructor(problem: ProblemDetail) {
    super(problem.detail || problem.title || `Request failed with status ${problem.status}`);
    this.name = "ApiProblemError";
    this.problem = problem;
  }
}

function timeoutProblem(detail: string): ProblemDetail {
  return {
    type: "https://dataforge.local/problems/request_timeout",
    title: "Request timed out",
    status: 504,
    detail,
    error: "request_timeout",
  };
}

/**
 * A failed `fetch` rejects with a bare TypeError whose message ("Failed to fetch") is a
 * browser string, not an explanation. Left unmapped it reached the UI under the hardcoded
 * title "Dataset validation failed", which told the user their CSV was bad when their network
 * was down.
 */
function networkProblem(): ProblemDetail {
  const offline = typeof navigator !== "undefined" && navigator.onLine === false;
  return {
    type: "https://dataforge.local/problems/network_unavailable",
    title: offline ? "You are offline" : "Cannot reach the backend",
    status: 0,
    detail: offline
      ? "This device reports no network connection, so the request was never sent."
      : "The backend could not be reached. It may be asleep, restarting, or blocked by the network.",
    error: offline ? "offline" : "network_unavailable",
  };
}

export class DataForgeClient {
  private readonly backendUrl: string;

  constructor(backendUrl: string) {
    this.backendUrl = normalizeBackendUrl(backendUrl);
  }

  async health(): Promise<BackendCapability> {
    return this.requestJson<BackendCapability>("/api/health", { method: "GET" }, 4_000);
  }

  async sample(name: string): Promise<File> {
    const response = await fetchWithTimeout(
      backendPath(this.backendUrl, `/api/samples/${encodeURIComponent(name)}`),
      { method: "GET" },
      REQUEST_TIMEOUT_MS,
    );
    if (!response.ok) {
      throw new ApiProblemError(await problemFromResponse(response));
    }
    const blob = await response.blob();
    return new File([blob], `${name}.csv`, { type: "text/csv" });
  }

  async profile(file: File, advanced: boolean): Promise<ProfileResponse> {
    const params = advanced ? "?advanced=true" : "";
    const formData = new FormData();
    formData.append("file", file);
    return this.requestJson<ProfileResponse>(`/api/profile${params}`, {
      method: "POST",
      body: formData,
    });
  }

  async analyze(
    file: File,
    advanced: boolean,
    acceptedConstraintIds: string[] = [],
    repairMode: RepairMode = "deterministic",
    allowEntityConsensus = false,
    // Accepting a signal is what makes the Cancel button real on this path. It renders
    // whenever a run is in flight, but in the non-streaming fallback the request had no way
    // to be aborted, so pressing it did nothing to the work already underway.
    signal?: AbortSignal,
  ): Promise<AnalyzeResponse> {
    const params = advanced ? "?advanced=true" : "";
    const formData = new FormData();
    formData.append("file", file);
    if (acceptedConstraintIds.length > 0) {
      formData.append("accepted_constraint_ids", JSON.stringify(acceptedConstraintIds));
    }
    if (repairMode !== "deterministic") {
      formData.append("repair_mode", repairMode);
    }
    if (allowEntityConsensus) {
      formData.append("allow_entity_consensus", "true");
    }
    return this.requestJson<AnalyzeResponse>(
      `/api/analyze${params}`,
      { method: "POST", body: formData },
      REQUEST_TIMEOUT_MS,
      signal,
    );
  }

  async analyzeStream(
    file: File,
    advanced: boolean,
    acceptedConstraintIds: string[] = [],
    options: AnalyzeStreamOptions,
    repairMode: RepairMode = "deterministic",
    allowEntityConsensus = false,
  ): Promise<AnalyzeResponse> {
    const params = advanced ? "?advanced=true" : "";
    const formData = new FormData();
    formData.append("file", file);
    if (acceptedConstraintIds.length > 0) {
      formData.append("accepted_constraint_ids", JSON.stringify(acceptedConstraintIds));
    }
    if (repairMode !== "deterministic") {
      formData.append("repair_mode", repairMode);
    }
    if (allowEntityConsensus) {
      formData.append("allow_entity_consensus", "true");
    }

    // An idle-bounded controller wraps the caller's signal, so the stream is cancellable by
    // the user AND cannot hang forever. Previously this was a bare `fetch`.
    const idle = new AbortController();
    let idleTimedOut = false;
    let idleTimer = 0;
    const resetIdleTimer = () => {
      window.clearTimeout(idleTimer);
      idleTimer = window.setTimeout(() => {
        idleTimedOut = true;
        idle.abort();
      }, STREAM_IDLE_TIMEOUT_MS);
    };
    const forwardAbort = () => idle.abort();
    options.signal?.addEventListener("abort", forwardAbort);
    resetIdleTimer();

    const failIfIdleTimedOut = (error: unknown): never => {
      if (idleTimedOut && !(options.signal?.aborted ?? false)) {
        throw new ApiProblemError(
          timeoutProblem(
            `The workflow stream went quiet for more than ${Math.round(STREAM_IDLE_TIMEOUT_MS / 1000)} seconds. Nothing was applied.`,
          ),
        );
      }
      throw error;
    };

    /**
     * A stream can be cut mid-line, leaving a partial JSON object. An unguarded JSON.parse
     * threw a raw SyntaxError ("Unexpected end of JSON input") that escaped as a non-problem
     * error and reached the user under the wrong title.
     */
    const parseEvent = (raw: string): WorkflowEvent => {
      try {
        return JSON.parse(raw) as WorkflowEvent;
      } catch {
        throw new ApiProblemError({
          type: "https://dataforge.local/problems/stream_malformed",
          title: "Workflow stream was cut short",
          status: 502,
          detail:
            "The connection ended part-way through a workflow event, so this run has no receipt. Nothing was applied.",
          error: "stream_malformed",
        });
      }
    };

    try {
      const response = await fetchWithTimeout(
        backendPath(this.backendUrl, `/api/analyze/stream${params}`),
        { method: "POST", body: formData },
        STREAM_IDLE_TIMEOUT_MS,
        idle.signal,
      );
      if (!response.ok) {
        throw new ApiProblemError(await problemFromResponse(response));
      }
      if (!response.body) {
        throw new ApiProblemError({
          type: "https://dataforge.local/problems/stream_unavailable",
          title: "Stream Unavailable",
          status: 502,
          detail: "The backend did not return a readable workflow stream.",
          error: "stream_unavailable",
        });
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let finalAnalysis: AnalyzeResponse | null = null;

      while (true) {
        let chunk: ReadableStreamReadResult<Uint8Array>;
        try {
          chunk = await reader.read();
        } catch (error) {
          return failIfIdleTimedOut(error);
        }
        const { done, value } = chunk;
        resetIdleTimer();
        buffer += decoder.decode(value, { stream: !done });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) {
            continue;
          }
          const event = parseEvent(trimmed);
          options.onEvent(event);
          if (event.analysis) {
            finalAnalysis = event.analysis;
          }
          if (event.problem && event.status === "failed") {
            throw new ApiProblemError(event.problem);
          }
        }
        if (done) {
          break;
        }
      }

      const trailing = buffer.trim();
      if (trailing) {
        const event = parseEvent(trailing);
        options.onEvent(event);
        if (event.analysis) {
          finalAnalysis = event.analysis;
        }
        if (event.problem && event.status === "failed") {
          throw new ApiProblemError(event.problem);
        }
      }

      if (!finalAnalysis) {
        throw new ApiProblemError({
          type: "https://dataforge.local/problems/stream_missing_receipt",
          title: "Stream Missing Receipt",
          status: 502,
          detail: "The workflow stream ended before returning a repair receipt.",
          error: "stream_missing_receipt",
        });
      }
      return finalAnalysis;
    } finally {
      window.clearTimeout(idleTimer);
      options.signal?.removeEventListener("abort", forwardAbort);
    }
  }

  async verifyScenario(name: string): Promise<VerifyScenario> {
    return this.requestJson<VerifyScenario>(
      `/api/verify-scenarios/${encodeURIComponent(name)}`,
      { method: "GET" },
    );
  }

  async verifyFixes(
    file: File,
    fixes: unknown[],
    options: VerifyFixesOptions = {},
  ): Promise<VerifyFixesResponse> {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("fixes", JSON.stringify(fixes));
    if (options.acceptedConstraintIds && options.acceptedConstraintIds.length > 0) {
      formData.append("accepted_constraint_ids", JSON.stringify(options.acceptedConstraintIds));
    }
    if (options.proposer) {
      formData.append("proposer", options.proposer);
    }
    if (options.confirmEscalations !== undefined) {
      formData.append("confirm_escalations", String(options.confirmEscalations));
    }
    if (options.allowUnproven !== undefined) {
      formData.append("allow_unproven", String(options.allowUnproven));
    }
    return this.requestJson<VerifyFixesResponse>("/api/verify-fixes", {
      method: "POST",
      body: formData,
    });
  }

  async repair(file: File, advanced: boolean): Promise<RepairResponse> {
    const params = new URLSearchParams({ dry_run: "true" });
    if (advanced) {
      params.set("advanced", "true");
    }
    const formData = new FormData();
    formData.append("file", file);
    return this.requestJson<RepairResponse>(`/api/repair?${params.toString()}`, {
      method: "POST",
      body: formData,
    });
  }

  private async requestJson<T>(
    path: string,
    init: RequestInit,
    timeoutMs = REQUEST_TIMEOUT_MS,
    signal?: AbortSignal,
  ) {
    const response = await fetchWithTimeout(
      backendPath(this.backendUrl, path),
      init,
      timeoutMs,
      signal,
    );
    if (!response.ok) {
      throw new ApiProblemError(await problemFromResponse(response));
    }
    return (await response.json()) as T;
  }
}

export async function problemFromResponse(response: Response): Promise<ProblemDetail> {
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/problem+json") || contentType.includes("application/json")) {
    try {
      const payload = (await response.json()) as Partial<ProblemDetail> & {
        detail?: unknown;
      };
      const nestedDetail =
        payload.detail && typeof payload.detail === "object"
          ? (payload.detail as Record<string, unknown>)
          : undefined;
      return {
        ...payload,
        type: String(payload.type ?? `https://dataforge.local/problems/http_${response.status}`),
        title: String(payload.title ?? (response.statusText || "Request failed")),
        status: Number(payload.status ?? response.status),
        detail:
          typeof payload.detail === "string"
            ? payload.detail
            : String(nestedDetail?.message ?? nestedDetail?.error ?? (response.statusText || "Request failed")),
        instance: typeof payload.instance === "string" ? payload.instance : undefined,
        error: String(payload.error ?? nestedDetail?.error ?? `http_${response.status}`),
      };
    } catch {
      // Fall through to a generic problem below.
    }
  }

  return {
    type: `https://dataforge.local/problems/http_${response.status}`,
    title: response.statusText || "Request failed",
    status: response.status,
    detail: `Request failed with status ${response.status}.`,
    error: `http_${response.status}`,
  };
}

async function fetchWithTimeout(
  input: RequestInfo | URL,
  init: RequestInit,
  timeoutMs: number,
  externalSignal?: AbortSignal,
): Promise<Response> {
  const controller = new AbortController();
  // WHY THE FLAG: both the timeout and the caller's Cancel button abort a controller, and
  // both surface as an indistinguishable DOMException named "AbortError". The caller treats
  // an AbortError as "the user meant it" and shows nothing, so a 20-second timeout used to
  // render as a run that quietly stopped. Recording WHICH cause fired is the only way to tell
  // them apart.
  let timedOut = false;
  const timeout = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);
  const forwardAbort = () => controller.abort();
  externalSignal?.addEventListener("abort", forwardAbort);

  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } catch (error) {
    if (timedOut && !(externalSignal?.aborted ?? false)) {
      throw new ApiProblemError(
        timeoutProblem(
          `The backend did not respond within ${Math.round(timeoutMs / 1000)} seconds. Nothing was applied.`,
        ),
      );
    }
    if (error instanceof TypeError) {
      throw new ApiProblemError(networkProblem());
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
    externalSignal?.removeEventListener("abort", forwardAbort);
  }
}
