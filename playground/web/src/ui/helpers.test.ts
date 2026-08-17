/**
 * Unit tests for the shared UI helpers.
 *
 * Two of these functions decide how honestly the product reports its own failures, and both
 * are currently wrong in ways the tests below state plainly rather than paper over:
 *
 *   localProblem  hardcodes the title "Dataset validation failed" and is used for network
 *                 failures and stream parse errors as well as CSV validation.
 *   isAbortError  cannot distinguish a 20-second client timeout from a user pressing Cancel,
 *                 so a timeout is rendered as nothing at all.
 *
 * The tests here pin the CURRENT behaviour of both, and name the defect in the assertion, so
 * that the later fix has to change a test deliberately rather than silently.
 */

import { describe, expect, it, vi } from "vitest";

import { ApiProblemError } from "../api";
import type { IssueGroup, ProblemDetail, Severity } from "../types";
import {
  failureKey,
  filterAndSortIssues,
  formatConstraintColumns,
  isAbortError,
  localProblem,
  problemFromUnknown,
  repairKey,
  selectionFromReviewItem,
  sleep,
  toneClass,
  toneForSeverity,
} from "./helpers";

const issue = (column: string, severity: Severity, count: number, type = "type_mismatch"): IssueGroup =>
  ({ column, severity, count, issue_type: type, rows: [], detail: null }) as unknown as IssueGroup;

describe("filterAndSortIssues", () => {
  const issues = [
    issue("beds", "safe", 9),
    issue("rating", "unsafe", 2),
    issue("state", "review", 5),
    issue("zip", "unsafe", 7, "format_violation"),
  ];

  it("orders by severity first, then by descending count", () => {
    const sorted = filterAndSortIssues(issues, "", "all", "severity");
    expect(sorted.map((entry) => entry.column)).toEqual(["zip", "rating", "state", "beds"]);
  });

  it("orders by count when asked, ignoring severity", () => {
    const sorted = filterAndSortIssues(issues, "", "all", "count");
    expect(sorted.map((entry) => entry.column)).toEqual(["beds", "zip", "state", "rating"]);
  });

  it("orders by column name when asked", () => {
    const sorted = filterAndSortIssues(issues, "", "all", "column");
    expect(sorted.map((entry) => entry.column)).toEqual(["beds", "rating", "state", "zip"]);
  });

  it("filters on the severity facet", () => {
    const sorted = filterAndSortIssues(issues, "", "unsafe", "severity");
    expect(sorted.map((entry) => entry.column)).toEqual(["zip", "rating"]);
  });

  it("matches the text filter against the column and the issue type", () => {
    expect(filterAndSortIssues(issues, "zip", "all", "severity")).toHaveLength(1);
    expect(filterAndSortIssues(issues, "format", "all", "severity")).toHaveLength(1);
  });

  it("ignores case and surrounding whitespace in the filter", () => {
    expect(filterAndSortIssues(issues, "  RATING ", "all", "severity")).toHaveLength(1);
  });

  it("returns an empty list rather than everything when nothing matches", () => {
    expect(filterAndSortIssues(issues, "nosuchcolumn", "all", "severity")).toEqual([]);
  });

  it("does not mutate the input order", () => {
    const before = issues.map((entry) => entry.column);
    filterAndSortIssues(issues, "", "all", "count");
    expect(issues.map((entry) => entry.column)).toEqual(before);
  });
});

describe("tone mapping", () => {
  it.each([
    ["running", "active"],
    ["completed", "verified"],
    ["blocked", "review"],
    ["cancelled", "review"],
    ["failed", "danger"],
    ["pending", "neutral"],
    ["something_new", "neutral"],
  ])("maps status %s to %s", (status, tone) => {
    expect(toneClass(status)).toBe(tone);
  });

  it("treats a cancelled run as review rather than danger, since the user chose it", () => {
    expect(toneClass("cancelled")).toBe("review");
    expect(toneClass("failed")).toBe("danger");
  });

  it.each([
    ["unsafe", "danger"],
    ["review", "review"],
    ["safe", "verified"],
  ] as const)("maps severity %s to %s", (severity, tone) => {
    expect(toneForSeverity(severity)).toBe(tone);
  });
});

describe("problemFromUnknown", () => {
  it("passes an API problem through untouched, preserving the server's own title", () => {
    const detail: ProblemDetail = {
      type: "about:blank",
      title: "Too Many Cells",
      status: 413,
      detail: "too wide",
      error: "too_many_cells",
    };
    expect(problemFromUnknown(new ApiProblemError(detail))).toBe(detail);
  });

  it("wraps a plain Error, keeping its message as the detail", () => {
    expect(problemFromUnknown(new Error("boom")).detail).toBe("boom");
  });

  it("copes with a thrown non-Error", () => {
    expect(problemFromUnknown("just a string").detail).toBe("The request failed.");
  });

  it("MISTITLES a network failure as a dataset validation problem", () => {
    // Documents a real defect: every locally-originated failure -- a dropped connection, a
    // truncated NDJSON line, a CSV parse error -- is reported under one hardcoded CSV title.
    const network = problemFromUnknown(new TypeError("Failed to fetch"));
    expect(network.title).toBe("Dataset validation failed");
    expect(network.detail).toBe("Failed to fetch");
  });
});

describe("localProblem", () => {
  it("produces a 400 problem+json shape the banner can render", () => {
    const detail = localProblem("Choose a CSV file.");
    expect(detail.status).toBe(400);
    expect(detail.error).toBe("frontend_validation");
    expect(detail.detail).toBe("Choose a CSV file.");
  });
});

describe("isAbortError", () => {
  it("recognises an aborted fetch", () => {
    expect(isAbortError(new DOMException("aborted", "AbortError"))).toBe(true);
  });

  it("ignores unrelated errors", () => {
    expect(isAbortError(new Error("AbortError"))).toBe(false);
    expect(isAbortError(null)).toBe(false);
  });

  it("CANNOT distinguish a client timeout from a user cancel", () => {
    // Both the 20s fetchWithTimeout guard and the Cancel button abort a controller, so both
    // arrive here as an indistinguishable AbortError. The caller treats that as "the user
    // meant it" and shows no error, which is why a timeout looks like nothing happened.
    const userCancelled = new DOMException("aborted", "AbortError");
    const timedOut = new DOMException("aborted", "AbortError");
    expect(isAbortError(userCancelled)).toBe(isAbortError(timedOut));
  });
});

describe("key builders", () => {
  it("distinguishes two fixes that differ only in the new value", () => {
    const base = { row: 1, column: "rating", old_value: "45.0" };
    expect(repairKey({ ...base, new_value: "4.5" } as never)).not.toBe(
      repairKey({ ...base, new_value: "45" } as never),
    );
  });

  it("keys a failure by row, column and issue type", () => {
    expect(failureKey({ row: 2, column: "zip", issue_type: "format_violation" } as never)).toBe(
      "2:zip:format_violation",
    );
  });

  it("renders a functional dependency as determinant to dependent", () => {
    expect(
      formatConstraintColumns({ columns: ["zip"], dependent: "city" } as never),
    ).toBe("zip -> city");
  });

  it("renders a non-dependency constraint as a plain column list", () => {
    expect(formatConstraintColumns({ columns: ["a", "b"], dependent: null } as never)).toBe("a, b");
  });
});

describe("selectionFromReviewItem", () => {
  it.each(["constraint", "failure"] as const)("preserves the %s kind", (kind) => {
    expect(selectionFromReviewItem({ kind, id: "x" } as never)).toEqual({ kind, id: "x" });
  });

  it("falls back to the receipt for any other review kind", () => {
    expect(selectionFromReviewItem({ kind: "receipt", id: "txn" } as never)).toEqual({
      kind: "receipt",
      id: "txn",
    });
  });
});

describe("sleep", () => {
  it("resolves after the requested delay", async () => {
    vi.useFakeTimers();
    try {
      let done = false;
      const pending = sleep(500).then(() => {
        done = true;
      });
      expect(done).toBe(false);
      await vi.advanceTimersByTimeAsync(500);
      await pending;
      expect(done).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
});
