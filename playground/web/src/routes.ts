export const PRODUCT_ROUTES = [
  {
    id: "home",
    path: "/",
    href: "/playground/",
    label: "Home",
    title: "Operations home",
    description: "Product posture, backend state, dry-run boundary, and quick start.",
  },
  {
    id: "run",
    path: "/run",
    href: "/playground/run",
    label: "Run",
    title: "Analyze command center",
    description: "Load a CSV, start or cancel analysis, and rerun accepted constraints.",
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

const ROUTE_BY_PATH: Map<string, ProductRoute> = new Map(
  PRODUCT_ROUTES.map((route) => [route.path, route]),
);
const ROUTE_BY_ID: Map<ProductRouteId, ProductRoute> = new Map(
  PRODUCT_ROUTES.map((route) => [route.id, route]),
);

export function routeFromPathname(pathname: string): ProductRoute {
  const withoutBase = pathname.startsWith("/playground")
    ? pathname.slice("/playground".length)
    : pathname;
  const normalized = normalizeRoutePath(withoutBase);
  return ROUTE_BY_PATH.get(normalized) ?? routeById("home");
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
