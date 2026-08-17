export const PRODUCT_ROUTES = [
  {
    id: "run",
    path: "/run",
    href: "/playground/run",
    label: "Loop",
    title: "CSV repair loop",
    description: "Upload a CSV, profile it, review issues, inspect verified repairs, export a receipt, and understand safe revert.",
  },
  {
    id: "atlas",
    path: "/atlas",
    href: "/playground/atlas",
    label: "Atlas",
    title: "Proof atlas",
    description: "Inspect the full live workflow and verifier progression.",
  },
  {
    id: "evidence",
    path: "/evidence",
    href: "/playground/evidence",
    label: "Evidence",
    title: "Evidence review",
    description: "Read source facts, risks, issues, constraints, and the evidence map.",
  },
  {
    id: "repairs",
    path: "/repairs",
    href: "/playground/repairs",
    label: "Repairs",
    title: "Repair comparison",
    description: "Compare verified fixes, candidate repairs, failures, and verifier notes.",
  },
  {
    id: "guardrail",
    path: "/guardrail",
    href: "/playground/guardrail",
    // "Guardrail" and "Agent guardrail" named the MECHANISM. As the front door this has to name
    // the promise instead, because it is the first and possibly only sentence a visitor reads.
    label: "Proof gate",
    title: "Unproven fixes are refused, not applied",
    description:
      "Let an untrusted agent propose fixes to a CSV. DataForge proves the correct ones, holds or rejects the rest, and emits a certificate anyone can re-verify. Nothing unproven is written.",
  },
  {
    id: "receipt",
    path: "/receipt",
    href: "/playground/receipt",
    label: "Receipt",
    title: "Receipt handoff",
    description: "Review hashes, local commands, transaction journal, and limitations.",
  },
  {
    id: "system",
    path: "/system",
    href: "/playground/system",
    label: "System",
    title: "System contract",
    description: "Inspect capabilities, limits, safety guarantees, and semantic state legend.",
  },
] as const;

export type ProductRouteId = (typeof PRODUCT_ROUTES)[number]["id"];

export interface ProductRoute {
  id: ProductRouteId;
  path: string;
  href: string;
  label: string;
  title: string;
  description: string;
}

/**
 * The front door: an explicit decision, not an accident of array position.
 *
 * It used to be `PRODUCT_ROUTES[0]`, so "what a first-time visitor meets" was decided by
 * whichever route happened to be listed first, and could be changed by a reorder that looked
 * purely cosmetic. Naming it separates two things that were conflated: the NAV ORDER, which is
 * the workflow sequence (loop, atlas, evidence, repairs, guardrail, receipt, system), and the
 * LANDING ROUTE, which is a claim about what this product is for.
 *
 * It is `guardrail`, not `run`. The product's promise is that it refuses to write values it
 * cannot prove. `/run` is a CSV repair loop, and a visitor landing there reasonably concludes
 * they are looking at another data-cleaning tool -- the differentiator is invisible until they
 * have already done work. `/guardrail` is the one page that DEMONSTRATES the promise: an
 * untrusted agent proposes fixes, and DataForge proves the correct ones, holds or rejects the
 * rest, and emits a re-verifiable certificate. It is self-contained (its own sample, its own
 * one-click scenario, no dependency on the shared run state), so it is reachable cold.
 *
 * Reversible in one line if that judgement is wrong.
 */
export const DEFAULT_ROUTE_ID: ProductRouteId = "guardrail";

const ROUTE_BY_PATH: Map<string, ProductRoute> = new Map(
  [
    ["/", PRODUCT_ROUTES.find((route) => route.id === DEFAULT_ROUTE_ID) ?? PRODUCT_ROUTES[0]],
    ...PRODUCT_ROUTES.map((route) => [route.path, route] as const),
  ],
);
const ROUTE_BY_ID: Map<ProductRouteId, ProductRoute> = new Map(
  PRODUCT_ROUTES.map((route) => [route.id, route]),
);

export function routeFromPathname(pathname: string): ProductRoute {
  const withoutBase = pathname.startsWith("/playground")
    ? pathname.slice("/playground".length)
    : pathname;
  const normalized = normalizeRoutePath(withoutBase);
  return ROUTE_BY_PATH.get(normalized) ?? routeById(DEFAULT_ROUTE_ID);
}

/**
 * Whether a path names a real route.
 *
 * `routeFromPathname` falls back to the run page for anything it does not recognise, and never
 * corrects the address bar, so /playground/nonsense rendered the run page while the URL kept
 * claiming to be something else. A silent fallback is indistinguishable from a working link,
 * which is how a typo or a stale bookmark becomes a confusing bug report rather than an
 * obvious one. Exposed separately so the fallback behaviour is preserved for callers that want
 * it, while the shell can say plainly that the address is wrong.
 */
export function isKnownRoutePath(pathname: string): boolean {
  const withoutBase = pathname.startsWith("/playground")
    ? pathname.slice("/playground".length)
    : pathname;
  return ROUTE_BY_PATH.has(normalizeRoutePath(withoutBase));
}

export function routeById(id: ProductRouteId): ProductRoute {
  const route = ROUTE_BY_ID.get(id);
  if (!route) {
    return PRODUCT_ROUTES[0];
  }
  return route;
}

export function normalizeRoutePath(path: string): string {
  const trimmed = path.split("?")[0].split("#")[0].replace(/\/+$/, "");
  return trimmed.length === 0 ? "/" : trimmed;
}

/**
 * Shareable run state, carried in the query string.
 *
 * WHY THE QUERY STRING AND NOT STORAGE
 *
 * Nothing about a run was addressable: `normalizeRoutePath` discards `?` and `#`, no state was
 * persisted, and the URL held only the page id. Sending someone `/playground/receipt` gave
 * them the empty prompt, and a refresh destroyed the run.
 *
 * The obvious fix -- caching the run in a browser storage API -- is forbidden here, and
 * correctly so. CI greps playground/web for those two API names and fails the build on ANY
 * occurrence, and the System page advertises "No browser storage, no frontend keys". A hosted
 * playground that quietly retained uploaded CSVs would be breaking a promise it makes on screen.
 *
 * (Deliberately paraphrased rather than naming the two APIs: the guard matches text, so it
 * cannot tell a use from a mention, and this comment failed CI when it spelled them out. That
 * bluntness is a feature -- a guard that parsed context could be bypassed by writing something
 * that looks like a comment -- so the prose bends and the guard stays literal.)
 *
 * So what travels is the REPRODUCIBLE INPUT, not the result: which sample, which toggles,
 * which constraints were accepted. Opening a shared link re-runs that analysis and reaches the
 * same answer, because the pipeline is deterministic for a given sample and constraint set.
 *
 * That last sentence was false when first written. The hydration path loaded the sample and
 * applied the toggles but never triggered a run, so a recipient landed on the same empty prompt
 * this feature exists to remove -- a comment asserting a guarantee the code did not provide.
 * The run is now fired by a second effect in App.tsx once React reports the dataset ready, and
 * asserted end to end in playground.spec.ts ("a shared sample link reproduces the run").
 *
 * An uploaded file cannot be shared this way -- the bytes never leave the user's machine -- and
 * for that case the exportable receipt remains the portable artifact.
 */
export interface RunQueryState {
  sample?: string;
  advanced?: boolean;
  agent?: boolean;
  consensus?: boolean;
  constraints?: string[];
}

const QUERY_KEYS = {
  sample: "sample",
  advanced: "advanced",
  agent: "agent",
  consensus: "consensus",
  constraints: "constraints",
} as const;

export function parseRunQuery(search: string): RunQueryState {
  const params = new URLSearchParams(search.startsWith("?") ? search.slice(1) : search);
  const state: RunQueryState = {};

  const sample = params.get(QUERY_KEYS.sample);
  // Sample names are backend identifiers, so only a conservative shape is accepted. A shared
  // link is untrusted input like any other.
  if (sample !== null && /^[a-z0-9_]{1,64}$/i.test(sample)) {
    state.sample = sample;
  }
  for (const key of ["advanced", "agent", "consensus"] as const) {
    const raw = params.get(QUERY_KEYS[key]);
    if (raw === "1" || raw === "true") {
      state[key] = true;
    }
  }
  const constraints = params.get(QUERY_KEYS.constraints);
  if (constraints !== null && constraints.length > 0) {
    const ids = constraints
      .split(",")
      .map((id) => id.trim())
      .filter((id) => id.length > 0 && id.length <= 128);
    if (ids.length > 0) {
      state.constraints = [...new Set(ids)];
    }
  }
  return state;
}

/** Serialises only what is set, so a default run keeps a clean shareable URL. */
export function buildRunQuery(state: RunQueryState): string {
  const params = new URLSearchParams();
  if (state.sample) {
    params.set(QUERY_KEYS.sample, state.sample);
  }
  for (const key of ["advanced", "agent", "consensus"] as const) {
    if (state[key] === true) {
      params.set(QUERY_KEYS[key], "1");
    }
  }
  if (state.constraints !== undefined && state.constraints.length > 0) {
    params.set(QUERY_KEYS.constraints, state.constraints.join(","));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

/** Full shareable href for a route plus run state. */
export function hrefWithRunQuery(route: ProductRoute, state: RunQueryState): string {
  return `${route.href}${buildRunQuery(state)}`;
}
