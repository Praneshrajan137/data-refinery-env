/** Persistent navigation shell, page header, and the pre-run prompt. */
import { motionSprings } from "./motion";
import { formatLabel } from "./observatory";
import { DEFAULT_ROUTE_ID, PRODUCT_ROUTES } from "./routes";
import type { ProductRoute, ProductRouteId } from "./routes";
import type { AnalyzeResponse, DatasetInput } from "./types";
import { FileText } from "lucide-react";
import { motion } from "motion/react";
import type { ReactNode } from "react";

export function ProductShell({
  route,
  onNavigate,
  children,
}: {
  route: ProductRoute;
  onNavigate: (routeId: ProductRouteId) => void;
  children: ReactNode;
}) {
  return (
    <div className="product-shell">
      <aside className="product-nav" aria-label="DataForge product navigation">
        <div className="nav-lockup">
          <span className="product-mark" aria-hidden="true">DF</span>
          <div>
            <p className="eyebrow">DataForge</p>
            <strong>CSV Repair Loop</strong>
          </div>
        </div>
        <nav>
          {PRODUCT_ROUTES.map((item) => (
            <a
              key={item.id}
              href={item.href}
              aria-current={route.id === item.id ? "page" : undefined}
              className={route.id === item.id ? "product-nav-link product-nav-link--current" : "product-nav-link"}
              onClick={(event) => {
                event.preventDefault();
                onNavigate(item.id);
              }}
            >
              {route.id === item.id ? (
                <motion.i
                  className="nav-active-marker"
                  layoutId="nav-active-marker"
                  transition={motionSprings.layout}
                  aria-hidden="true"
                />
              ) : null}
              <span>{item.label}</span>
              <small>{item.title}</small>
            </a>
          ))}
        </nav>
      </aside>
      <div className="product-main">{children}</div>
    </div>
  );
}

export function ProductPageHeader({
  route,
  dataset,
  analysis,
  workflowStatus,
  stale = false,
}: {
  route: ProductRoute;
  dataset: DatasetInput | null;
  analysis: AnalyzeResponse | null;
  workflowStatus: string;
  stale?: boolean;
}) {
  return (
    <header className="page-hero" aria-label={`${route.title} page`}>
      <div>
        <p className="eyebrow">DataForge Playground</p>
        <h1>{route.title}</h1>
        <p>{route.description}</p>
      </div>
      <div className="page-signals" aria-label="Current run posture">
        <span>{dataset ? dataset.file.name : "No dataset"}</span>
        <span>{analysis ? analysis.meta.contract_version : "No receipt"}</span>
        <span>{formatLabel(workflowStatus)}</span>
        {/*
          The staleness marker lives in the shared header rather than being re-plumbed into
          each page, because every route reads the same `latestAnalysis`. The first version of
          this fix reached ONE of five surfaces: the receipt, evidence, repairs and atlas pages
          all went on rendering a superseded run's data with no indication, which is worse on
          the receipt page than on the run page, because the receipt is what people export.
        */}
        {stale ? (
          <span className="page-signals__stale" role="status">
            Superseded &mdash; shown result predates the last attempt
          </span>
        ) : null}
      </div>
    </header>
  );
}

export function EmptyPagePrompt({
  title,
  onNavigate,
}: {
  title: string;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <section className="empty-route">
      <FileText aria-hidden="true" />
      <strong>{title}</strong>
      <p>This page is part of the in-memory playground session. Load a dataset and run analysis from the command center.</p>
      <button type="button" className="primary-action" onClick={() => onNavigate("run")}>
        Open Run Page
      </button>
    </section>
  );
}

/**
 * Unknown-address state.
 *
 * There was no 404 of any kind: `routeFromPathname` silently returns the run page for anything
 * it does not recognise, and nothing rewrites the address bar, so a typo or a stale bookmark
 * rendered a working-looking page at a URL that does not exist. Saying so is cheap and stops a
 * mistyped link being mistaken for a broken product.
 */
export function UnknownRoute({
  pathname,
  onNavigate,
}: {
  pathname: string;
  onNavigate: (routeId: ProductRouteId) => void;
}) {
  return (
    <main className="route-page">
      <section className="empty-route" aria-labelledby="unknown-route-title">
        <FileText aria-hidden="true" />
        <strong id="unknown-route-title">No page at this address</strong>
        <p>
          <code>{pathname}</code> is not one of this playground's pages. Nothing was lost --
          pick a page from the navigation, or start a run.
        </p>
        <button
          type="button"
          className="primary-action"
          onClick={() => onNavigate(DEFAULT_ROUTE_ID)}
        >
          Go to the front page
        </button>
      </section>
    </main>
  );
}
