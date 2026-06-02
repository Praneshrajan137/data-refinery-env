import { describe, expect, it } from "vitest";
import {
  DEFAULT_MAX_UPLOAD_BYTES,
  buildEvidenceExport,
  groupIssues,
  parseCsvPreview,
  validateCsvFile,
} from "./csv";
import type { ProfileResponse, RepairResponse } from "./types";

describe("CSV validation and preview", () => {
  it("accepts CSV files within the backend cap", () => {
    const file = new File(["id,amount\n1,100"], "sample.csv", { type: "text/csv" });

    expect(validateCsvFile(file)).toEqual({ ok: true });
  });

  it("rejects empty, non-CSV, and oversize files", () => {
    expect(validateCsvFile(new File([""], "sample.csv", { type: "text/csv" })).ok).toBe(false);
    expect(validateCsvFile(new File(["x"], "sample.txt", { type: "text/plain" })).ok).toBe(true);
    expect(
      validateCsvFile(
        new File(["x".repeat(DEFAULT_MAX_UPLOAD_BYTES + 1)], "big.csv", {
          type: "text/csv",
        }),
      ).ok,
    ).toBe(false);
  });

  it("parses a stable preview with headers and rows", () => {
    const preview = parseCsvPreview('id,amount,note\n1,100,"ok"\n2,1020,"needs review"');

    expect(preview.columns).toEqual(["id", "amount", "note"]);
    expect(preview.rows).toHaveLength(2);
    expect(preview.rows[1].amount).toBe("1020");
  });

  it("rejects header-only CSV snippets", () => {
    expect(() => parseCsvPreview("id,amount\n")).toThrow(/at least one data row/i);
  });
});

describe("result shaping", () => {
  it("groups issues by severity, count, and column", () => {
    const groups = groupIssues([
      { column: "amount", issue_type: "decimal_shift", severity: "review", row_indices: [2], count: 1 },
      { column: "state", issue_type: "fd_violation", severity: "unsafe", row_indices: [4, 5], count: 2 },
      { column: "name", issue_type: "type_mismatch", severity: "safe", row_indices: [1], count: 1 },
    ]);

    expect(groups.map((group) => group.column)).toEqual(["state", "amount", "name"]);
  });

  it("exports repair evidence as deterministic JSON", () => {
    const profile: ProfileResponse = {
      issues: [],
      meta: {
        rows: 2,
        columns: 2,
        column_names: ["id", "amount"],
        total_issues: 0,
        advanced_requested: false,
        api_version: "0.1.0",
        contract_version: "repair_contract_v2",
      },
    };
    const repair: RepairResponse = {
      fixes: [],
      txn_journal: null,
      meta: { api_version: "0.1.0", contract_version: "repair_contract_v2" },
    };

    const payload = JSON.parse(buildEvidenceExport("sample.csv", profile, repair));

    expect(payload).toMatchObject({
      product: "DataForge Playground",
      dataset_name: "sample.csv",
      dry_run: true,
      contract_version: "repair_contract_v2",
    });
  });
});
