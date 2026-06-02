import type { RuntimeConfig } from "./types";

declare global {
  interface Window {
    __DATAFORGE_CONFIG__?: RuntimeConfig;
  }
}

export function normalizeBackendUrl(rawUrl: string | undefined): string {
  return String(rawUrl ?? "").trim().replace(/\/+$/, "");
}

export function getRuntimeConfig(): RuntimeConfig {
  return {
    BACKEND_URL: normalizeBackendUrl(window.__DATAFORGE_CONFIG__?.BACKEND_URL),
  };
}

export function backendPath(backendUrl: string, path: string): string {
  return backendUrl ? `${backendUrl}${path}` : path;
}
