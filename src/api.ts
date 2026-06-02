import { backendPath, normalizeBackendUrl } from "./config";
import type { BackendCapability, ProblemDetail, ProfileResponse, RepairResponse } from "./types";

const REQUEST_TIMEOUT_MS = 20_000;

export class ApiProblemError extends Error {
  problem: ProblemDetail;

  constructor(problem: ProblemDetail) {
    super(problem.detail || problem.title || `Request failed with status ${problem.status}`);
    this.name = "ApiProblemError";
    this.problem = problem;
  }
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

  private async requestJson<T>(path: string, init: RequestInit, timeoutMs = REQUEST_TIMEOUT_MS) {
    const response = await fetchWithTimeout(backendPath(this.backendUrl, path), init, timeoutMs);
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
): Promise<Response> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } finally {
    window.clearTimeout(timeout);
  }
}
