/**
 * Component tests for the shared display primitives.
 *
 * These are the first *.test.tsx files in the repo. Until now no React component had a unit
 * test: rendering was verified only through three Playwright specs, so the states a user
 * meets when something goes WRONG -- empty, loading, error, clipboard denied, overflow --
 * were the least covered part of the product, because driving a browser into each of them
 * costs a full page load and a mocked backend.
 *
 * The focus here is deliberately those states rather than the happy path the e2e suite
 * already walks end to end.
 */

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import {
  ConfidenceBadge,
  CopyFallback,
  DatasetBadge,
  EmptyState,
  EvidenceNote,
  LoadingState,
  Metric,
  OfflineBanner,
  ProblemBanner,
  SeverityBadge,
  StageCounts,
} from "./primitives";
import type { ProblemDetail } from "../types";

const problem = (overrides: Partial<ProblemDetail> = {}): ProblemDetail => ({
  type: "https://dataforge.local/problems/frontend_validation",
  title: "Dataset validation failed",
  status: 400,
  detail: "something went wrong",
  error: "frontend_validation",
  ...overrides,
});

describe("ProblemBanner", () => {
  it("announces itself as an alert so a failure is not silent to a screen reader", () => {
    render(<ProblemBanner problem={problem()} />);
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });

  it("renders the humanised message for a known error code, not the raw code", () => {
    render(
      <ProblemBanner
        problem={problem({ error: "request_timeout", title: "Request Timeout", detail: undefined })}
      />,
    );
    expect(screen.getByText(/backend timed out/i)).toBeInTheDocument();
    expect(screen.queryByText("request_timeout")).not.toBeInTheDocument();
  });

  it("falls back to the server detail when the error code is unrecognised", () => {
    render(
      <ProblemBanner problem={problem({ error: "some_new_code", detail: "Upstream said no." })} />,
    );
    expect(screen.getByText("Upstream said no.")).toBeInTheDocument();
  });

  it("still renders a message when the problem carries neither a code nor a detail", () => {
    render(<ProblemBanner problem={problem({ error: undefined, detail: undefined })} />);
    expect(screen.getByRole("alert").textContent).not.toBe("");
  });

  it("offers no retry button when the caller gives no way to retry", () => {
    render(<ProblemBanner problem={problem()} />);
    expect(screen.queryByRole("button", { name: /try again/i })).not.toBeInTheDocument();
  });

  it("offers a retry that does not cost the user their loaded work", async () => {
    // The only retry that existed before was the backend chip, wired to
    // window.location.reload(), which discarded the dataset and any completed receipt.
    const onRetry = vi.fn();
    render(<ProblemBanner problem={problem({ error: "request_timeout" })} onRetry={onRetry} />);
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});

describe("LoadingState", () => {
  it("is a polite live region, so completion is announced without stealing focus", () => {
    render(<LoadingState label="Analyzing CSV" />);
    const status = screen.getByRole("status");
    expect(status).toHaveAttribute("aria-live", "polite");
    expect(status).toHaveTextContent("Analyzing CSV");
  });

  it("declares the agent state that drives its motion", () => {
    render(<LoadingState label="Verifying" />);
    expect(screen.getByRole("status")).toHaveAttribute("data-agent-motion", "verifying");
  });
});

describe("CopyFallback", () => {
  it("offers the payload for manual selection when the clipboard is denied", () => {
    render(<CopyFallback evidenceText='{"receipt":1}' />);
    expect(screen.getByRole("status")).toHaveTextContent(/clipboard permission was blocked/i);
    expect(screen.getByLabelText("Copyable repair evidence")).toHaveValue('{"receipt":1}');
  });

  it("keeps the textarea read-only, since this is evidence rather than an input", () => {
    render(<CopyFallback evidenceText="x" />);
    expect(screen.getByLabelText("Copyable repair evidence")).toHaveAttribute("readonly");
  });
});

describe("EmptyState and EvidenceNote", () => {
  it("states what is absent and what to do about it", () => {
    render(
      <EmptyState
        icon={<span data-testid="icon" />}
        title="No dataset loaded"
        body="Choose a sample or upload a CSV."
      />,
    );
    expect(screen.getByText("No dataset loaded")).toBeInTheDocument();
    expect(screen.getByText("Choose a sample or upload a CSV.")).toBeInTheDocument();
    expect(screen.getByTestId("icon")).toBeInTheDocument();
  });

  it("renders an evidence note as a titled explanation", () => {
    render(<EvidenceNote title="No proposals yet" body="Add a fix to see the guardrail." />);
    expect(screen.getByText("No proposals yet")).toBeInTheDocument();
  });
});

describe("StageCounts", () => {
  it("renders nothing at all when there is nothing to count", () => {
    const { container } = render(<StageCounts counts={{}} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("caps the display at four entries, so a wide stage cannot flood the row", () => {
    render(<StageCounts counts={{ a: 1, b: 2, c: 3, d: 4, e: 5, f: 6 }} />);
    expect(screen.getByText("a 1")).toBeInTheDocument();
    expect(screen.getByText("d 4")).toBeInTheDocument();
    expect(screen.queryByText("e 5")).not.toBeInTheDocument();
  });

  it("humanises the count key rather than printing the raw field name", () => {
    render(<StageCounts counts={{ issues_count: 3 }} />);
    expect(screen.getByText("issues count 3")).toBeInTheDocument();
  });

  it("renders boolean and string counts without crashing", () => {
    render(<StageCounts counts={{ applied: false, mode: "dry_run" }} />);
    expect(screen.getByText("applied false")).toBeInTheDocument();
    expect(screen.getByText("mode dry_run")).toBeInTheDocument();
  });
});

describe("ConfidenceBadge", () => {
  // The bucket boundaries are load-bearing: they are the only place the UI turns a
  // continuous confidence into a categorical claim, and the corrector's confidence is the
  // number the whole auto-apply policy turns on.
  it.each([
    [0.85, "high"],
    [0.9, "high"],
    [0.84, "medium"],
    [0.65, "medium"],
    [0.64, "low"],
    [0, "low"],
  ])("puts %s in the %s bucket", (value, bucket) => {
    const { container } = render(<ConfidenceBadge value={value} />);
    expect(container.firstElementChild?.className).toContain(`confidence--${bucket}`);
  });

  it("renders the value as a rounded percentage", () => {
    render(<ConfidenceBadge value={0.876} />);
    expect(screen.getByText("88%")).toBeInTheDocument();
  });
});

describe("SeverityBadge", () => {
  it.each(["unsafe", "review", "safe"] as const)("carries a class for %s", (severity) => {
    const { container } = render(<SeverityBadge severity={severity} />);
    expect(container.firstElementChild?.className).toContain(`severity--${severity}`);
  });
});

describe("DatasetBadge", () => {
  it("says it is waiting rather than rendering an empty chip", () => {
    render(<DatasetBadge dataset={null} />);
    expect(screen.getByText("Waiting")).toBeInTheDocument();
  });

  it("reports the preview shape once a dataset is loaded", () => {
    render(
      <DatasetBadge
        dataset={{
          file: new File(["a,b\n1,2\n"], "t.csv", { type: "text/csv" }),
          source: "upload",
          preview: { columns: ["a", "b"], rows: [{ a: "1", b: "2" }], totalPreviewRows: 1, truncated: false },
        }}
      />,
    );
    expect(screen.getByText(/1 preview rows, 2 columns/)).toBeInTheDocument();
  });
});

describe("Metric", () => {
  it("renders a label and its value", () => {
    render(<Metric label="Verifier" value="accept" />);
    expect(screen.getByText("Verifier")).toBeInTheDocument();
    expect(screen.getByText("accept")).toBeInTheDocument();
  });
});

describe("OfflineBanner", () => {
  it("says what is wrong without blaming the user's CSV", () => {
    render(<OfflineBanner />);
    expect(screen.getByRole("status")).toHaveTextContent(/you are offline/i);
    expect(screen.queryByText(/validation/i)).not.toBeInTheDocument();
  });

  it("reassures that loaded work survives, which is the user's real question mid-run", () => {
    render(<OfflineBanner />);
    expect(screen.getByRole("status")).toHaveTextContent(/still here/i);
    expect(screen.getByRole("status")).toHaveTextContent(/nothing was applied/i);
  });

  it("is polite rather than assertive, since losing the network is not an error to act on", () => {
    render(<OfflineBanner />);
    expect(screen.getByRole("status")).toHaveAttribute("aria-live", "polite");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
