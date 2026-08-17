/**
 * Shared e2e fixtures.
 *
 * `analyzePayload` was defined inside playground.spec.ts, so the density perf spec could not
 * reach it and hand-rolled its own response instead. That hand-rolled payload was silently
 * REJECTED by the app -- no result rendered, and the perf test failed on a missing canvas with
 * no indication that the fixture was the cause. A response shape this large needs one
 * definition; a second one is a second thing to get wrong.
 */
export const sampleCsv = [
  "provider_number,hospital_name,city,state,zip_code,phone_number,rating,mortality_rate,readmission_rate,er_wait_time",
  "PRV001,General Hospital,Springfield,IL,62701,2175550101,4.2,0.023,0.145,28",
  "PRV002,St. Mary Medical Center,Chicago,IL,60601,3125550202,3.8,0.031,0.162,35",
  "PRV001,Springfield Medical,Springfield,IL,62701,2175550303,4.5,0.019,0.138,22",
  "PRV003,Mercy Hospital,Peoria,IL,61602,3095550404,3.5,0.028,0.158,31",
  "PRV004,Northwestern Memorial,Chicago,IL,60611,3125550505,4.1,0.025,0.149,26",
  "PRV005,Rush University MC,Chicago,IL,60612,3125550606,45.0,0.022,0.141,29",
  "PRV006,Advocate Christ,Oak Lawn,IL,60453,7085550707,3.9,0.027,0.155,33",
  "PRV007,Loyola University MC,Maywood,IL,60153,7085550808,4.3,0.020,0.142,25",
  "PRV008,Presence St. Joseph,Joliet,IL,60435,8155550909,4.0,0.026,0.151,30",
  "PRV009,Edward Hospital,Naperville,IL,60540,6305551010,3.7,0.029,0.160,34",
].join("\n");
export const sourceHash = "a".repeat(64);

export function analyzePayload(accepted = false) {
  return {
    source: {
      name: "hospital_10rows.csv",
      size_bytes: sampleCsv.length,
      sha256: sourceHash,
      rows: 10,
      columns: 10,
      column_names: [
        "provider_number",
        "hospital_name",
        "city",
        "state",
        "zip_code",
        "phone_number",
        "rating",
        "mortality_rate",
        "readmission_rate",
        "er_wait_time",
      ],
    },
    schema_inference: {
      schema_version: "constraint_review_v1",
      source_sha256: sourceHash,
      row_count: 10,
      candidates: [
        {
          candidate_id: "cnd-state-fd",
          kind: "functional_dependency",
          columns: ["provider_number"],
          dependent: "hospital_name",
          inferred_type: null,
          pattern: null,
          min_value: null,
          max_value: null,
          confidence: 0.92,
          evidence: "provider_number determines hospital_name in 9/10 rows.",
          decision: accepted ? "accepted" : "pending",
          repair_supported: true,
        },
        {
          candidate_id: "cnd-amount-regex",
          kind: "regex",
          columns: ["rating"],
          dependent: null,
          inferred_type: null,
          pattern: "^\\d+$",
          min_value: null,
          max_value: null,
          confidence: 1,
          evidence: "10 non-empty values matched numeric rating values.",
          decision: "pending",
          repair_supported: false,
        },
      ],
    },
    risk_summary: {
      dataset_level: "medium",
      repair_readiness: "verified",
      severity_counts: { safe: 0, review: 1, unsafe: 0 },
      pending_repair_supported_constraints: accepted ? 0 : 1,
      reasons: [
        "1 review-level issue(s) were detected.",
        accepted
          ? "Accepted constraints were used for this dry run."
          : "1 repair-supported inferred constraint(s) remain pending.",
      ],
    },
    issues: [
      {
        column: "rating",
        issue_type: "decimal_shift",
        severity: "review",
        row_indices: [5],
        row_indices_truncated: false,
        count: 1,
      },
    ],
    // The untruncated per-cell channel the evidence surface is built from. Kept in
    // lockstep with `issues` above: the same cell, with the fields the grouping
    // destroys (confidence, actual, expected, reason).
    flagged_cells: {
      // rating is column index 4 in the hospital sample header.
      index: { column_indices: [4], rows: [5] },
      confidence_histogram: [
        {
          issue_type: "decimal_shift",
          bins: Array.from({ length: 10 }, (_, index) => ({
            from_value: index / 10,
            to_value: (index + 1) / 10,
            count: index === 8 ? 1 : 0,
          })),
          count: 1,
          distinct_values: 1,
          mode_value: 0.86,
          mode_share: 1,
        },
      ],
      cells: [
        {
          row: 5,
          column: "rating",
          issue_type: "decimal_shift",
          severity: "review",
          confidence: 0.86,
          actual: "45.0",
          expected: "4.5",
          reason: "Value 45 in column rating appears to be ~10x the typical value.",
        },
      ],
      total: 1,
      truncated: false,
      note: "All 1 flagged cells are individually listed.",
    },
    repairs: [
      {
        row: 5,
        column: "rating",
        old_value: "45.0",
        new_value: "4.5",
        detector_id: "decimal_shift",
        reason: "Value 45 in column rating appears to be ~10x the typical value.",
        confidence: 0.94,
        provenance: "deterministic",
        verifier_reason: "All proposed fixes passed the SMT verifier.",
        verification_strength: "proven",
      },
    ],
    verification: {
      safety_verdict: "allow",
      verifier_verdict: "accept",
      accepted_constraint_ids: accepted ? ["cnd-state-fd"] : [],
      failures: [
        {
          row: 2,
          column: "hospital_name",
          issue_type: "fd_violation",
          status: "attempted_not_fixed",
          reason: "No repair proposal was available for this issue.",
          attempt_count: 1,
          unsat_core: [],
        },
      ],
      abstentions: ["No repair proposal was available for this issue."],
      failure_reasons: ["No repair proposal was available for this issue."],
    },
    certificate: {
      ok: true,
      checks: [
        {
          name: "schema_recognized",
          ok: true,
          detail: "schema_version='repair_receipt_v1'",
        },
        {
          name: "data_identity",
          ok: true,
          detail: "sha256(data) matches source_sha256.",
        },
        {
          name: "auto_apply_is_proven_deterministic",
          ok: true,
          detail: "auto-applied set is proven (deterministic).",
        },
      ],
    },
    txn_journal: {
      txn_id: "txn-demo",
      created_at: "2026-05-20T12:00:00Z",
      source_name: "hospital_10rows.csv",
      source_sha256: sourceHash,
      fixes_count: 1,
      applied: false,
      events: [{ event_type: "created" }],
      note: "Playground is stateless.",
    },
    receipt: {
      schema_version: "repair_receipt_v1",
      receipt_version: "repair_receipt_v1",
      contract_version: "repair_contract_v2",
      mode: "dry_run",
      applied: false,
      reversible: true,
      source_sha256: sourceHash,
      post_sha256: null,
      txn_id: "txn-demo",
      safety_verdict: "allow",
      verifier_verdict: "accept",
      independent_verification: "not_run",
      issues_count: 1,
      fixes_count: 1,
      applied_fixes: [],
      suggested_fixes: [],
      candidate_provenance: ["heuristic"],
      root_causes: [
        {
          row: 5,
          column: "rating",
          issue_type: "decimal_shift",
          category: "decimal_shift",
          confidence: 0.94,
          reason: "Rating appears to be shifted one decimal place.",
        },
        {
          row: 2,
          column: "hospital_name",
          issue_type: "fd_violation",
          category: "fd_conflict",
          confidence: 0.9,
          reason: "FD conflict.",
        },
      ],
      candidate_repairs: [
        {
          row: 5,
          column: "rating",
          old_value: "45.0",
          new_value: "4.5",
          detector_id: "decimal_shift",
          operation: "update",
          reason: "Move decimal point one place left.",
          confidence: 0.94,
          provenance: "deterministic",
          verifier_reason: "accepted",
          verification_strength: "proven",
        },
      ],
      proof_obligations: [
        {
          obligation_id: "smt::decimal_shift::5::rating::attempt::1",
          verifier: "smt",
          status: "accepted",
          reason: "accepted",
          unsat_core: [],
        },
      ],
      accepted_constraint_ids: accepted ? ["cnd-state-fd"] : [],
      constraints_artifact_sha256: accepted ? "b".repeat(64) : null,
      patch_plan_sha256: "c".repeat(64),
      revert_command: "dataforge revert txn-demo",
      limitations: ["Dry run only; no source data was mutated."],
      reason: "Dry run completed without mutating the source file.",
    },
    apply_handoff: {
      source_name: "hospital_10rows.csv",
      dry_run_command: accepted
        ? "dataforge repair path/to/hospital_10rows.csv --constraints constraints.json --dry-run"
        : "dataforge repair path/to/hospital_10rows.csv --dry-run",
      apply_command: accepted
        ? "dataforge repair path/to/hospital_10rows.csv --constraints constraints.json --apply"
        : "dataforge repair path/to/hospital_10rows.csv --apply",
      audit_command: "dataforge audit txn-demo",
      revert_command: "dataforge revert txn-demo",
      note: "The hosted playground never mutates uploads.",
    },
    limitations: [
      "Hosted analysis is stateless and dry-run only.",
      "Inferred constraints are pending unless explicitly accepted for this run.",
    ],
    meta: {
      api_version: "0.1.0",
      contract_version: "repair_contract_v2",
    },
  };
}

