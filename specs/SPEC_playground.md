# SPEC: Playground (Hosted Demo)

> Status: Reviewed
> Owner: @Praneshrajan15
> Last updated: 2026-06-02

## 1. Purpose (2 sentences)

Provide a stateless, free-tier browser proof loop for DataForge's core promise:
upload messy CSV data, understand risk, review inferred assumptions, inspect
verified repair proposals, and leave with an auditable apply handoff. The
frontend is served from Cloudflare Workers Static Assets and the backend is
served as an API-only Hugging Face Docker Space.

## 2. Outcomes (measurable, binary pass/fail)

- [x] `GET /api/health` returns `status`, `advanced_available`, and `max_upload_bytes`.
- [x] `GET /api/health` returns `streaming_available=true` and
  `workflow_contract_version="workflow_event_v1"` for the supervision cockpit.
- [x] `GET /` returns stable API service metadata and never tries to serve a SPA.
- [x] `POST /api/analyze` on `hospital_10rows.csv` returns source facts, schema inference, categorical risk, issues, verified repairs, verification evidence, dry-run journal, receipt, apply handoff, and limitations within 5 s warm.
- [x] `POST /api/analyze/stream` returns NDJSON `workflow_event_v1` events for
  `intake`, `schema_inference`, `constraint_review`, `detectors`,
  `repair_candidates`, `safety_gate`, `smt_verifier`, `dry_run_transaction`,
  and `receipt`; the final completed receipt event includes the same
  `AnalyzeResponse` payload shape as `/api/analyze`.
- [x] `POST /api/analyze/stream` emits a failed receipt event with a public
  problem payload when the workflow stops after streaming begins.
- [x] `POST /api/analyze` keeps inferred constraints pending by default and uses only submitted accepted constraint IDs for repair semantics.
- [x] Unknown accepted constraint IDs return 400 `application/problem+json` with `error="unknown_constraint_id"`.
- [x] `POST /api/profile` and `POST /api/repair?dry_run=true` remain compatibility routes backed by the shared analyzer/serializer contracts.
- [x] API errors use RFC 9457 `application/problem+json` while preserving stable error codes as extension members.
- [x] OpenAPI snapshots under `specs/openapi/` match the generated Playground and OpenEnv schemas.
- [x] `advanced=true` is rejected with 400 when no provider key is configured and accepted when one is present.
- [x] Uploaded CSV files larger than 1 MiB are rejected with 413; valid near-limit CSVs are not rejected only because of multipart overhead.
- [x] Production CORS allows only exact origins from `DATAFORGE_PLAYGROUND_ORIGINS`; localhost is regex-allowed only when `DATAFORGE_PLAYGROUND_DEV=1`.
- [x] Rate limiting returns 429 on the 11th POST within a minute from one client.
- [x] The frontend uses relative assets plus `config.js` and never assumes HF static hosting.
- [x] No browser storage APIs or frontend API keys appear under `playground/web/`.
- [x] The frontend primary action is Analyze; evidence is organized through
  multi-page product routes for Run, Atlas, Evidence, Repairs, Receipt, and
  System.
- [x] The frontend renders the proof loop as a persistent DataForge Observatory:
  product navigation, mission bar, proof atlas, evidence dock, human review
  queue, repair comparison, and receipt handoff.
- [x] The frontend uses `/api/analyze/stream` when the health response advertises
  streaming and falls back to `/api/analyze` when streaming is unavailable.
- [x] The frontend exposes cancel/retry behavior for in-flight stream requests
  without clearing the last completed analysis.
- [x] Constraint selections are per-run memory only and are never persisted to browser storage.
- [x] The authoritative HF deploy path uses `scripts/playground/stage_space.py`, not subtree push.
- [x] The frontend imports the generated DataForge color system, passes
  `npm run colors:check`, and contains no hand-authored raw hex colors.
- [x] Light and dark playground schemes preserve WCAG 2.2 contrast gates:
  primary text 7:1, secondary text 4.5:1, and non-text affordances 3:1.

## 3. Scope

**IN**:
- API endpoints: `/`, `/api/health`, `/api/samples/{name}`, `/api/analyze`, `/api/profile`, `/api/repair`
- Stream endpoint: `/api/analyze/stream` with line-delimited
  `workflow_event_v1` events
- Temporary-directory-only dry-run transaction journaling
- Schema-inference review artifacts with pending-by-default constraints
- Categorical risk and repair readiness summaries
- Heuristic-first behavior with optional advanced mode
- Cloudflare Workers Static Assets frontend with runtime backend configuration
  via `config.js`
- Hugging Face Docker Space named `dataforge-playground`
- Hugging Face staging script and deployment runbooks
- Playground-focused tests, CI checks, and quality-gate coverage

**OUT**:
- Persistent state, sessions, accounts, or browser storage
- Apply-mode writes from the hosted playground
- Browser-side LLM calls
- Paid hosting beyond the current Workers/HF deployment
- README claims that the playground is live before deploy verification

## 4. Constraints

- Performance: warm `/api/analyze` on the 10-row sample should complete within 5 s; compatibility `/api/profile` and `/api/repair` should remain in the same envelope.
- Compatibility: Python 3.11/3.12 development, Python 3.12 Docker runtime, modern evergreen browsers.
- Safety: no endpoint may silently bypass safety or verifier failures.
- Visual quality: color tokens come from `SPEC_color_system.md`; color never
  carries critical state without adjacent text, iconography, or ARIA semantics.
  The playground uses the DataForge Aurelian Proof Intelligence surface:
  porcelain field, pearl mineral structure, cinnabar command, teal-steel
  evidence, viridian proof, brass caution, hematite failure, and muted
  ultraviolet agent presence.
  State color appears as precision instrumentation rather than large
  decorative fills.
- AUX quality: agency, uncertainty, verifier boundaries, abstentions,
  handoffs, and human-required decisions are visible without opening exported
  JSON. Motion follows `SPEC_motion_system.md`: it communicates
  causality/progress, uses real workflow events rather than fake activity, and
  respects `prefers-reduced-motion`.
- Hosting: single-worker Space runtime, `PORT` honored, UID 1000, all temporary I/O under a request-local temp directory.
- Quality gate: `make lint`, `make type`, `make test`, playground smoke tests, and regression smoke must all pass.
- Contract gate: `make backend-gate` verifies OpenAPI drift, README truth,
  MCP tests, secret scan, dependency-audit availability, and package-build
  availability in addition to lint/type/test.

## 5. Prior decisions (locked - require new spec to change)

- Cloudflare Workers Static Assets serves the frontend; Hugging Face Space
  `dataforge-playground` serves the API backend.
- The hosted playground is stateless and dry-run only.
- Inferred constraints are pending unless explicitly accepted for the current run.
- Apply and revert remain local CLI transaction workflows.
- Heuristic mode is the default; advanced mode is opt-in and backend-key-gated.
- Playground-only runtime dependencies stay out of core package runtime deps.
- The HF deploy artifact is built from a staged repo snapshot, not from subtree push.

## 6. Task breakdown (atomic sub-tasks)

### 6.1 Backend contract hardening
- Acceptance: `/` returns JSON service metadata; `/api/health` returns capability metadata; broken static-file assumptions are removed.
- Depends on: none
- Estimated complexity: M

### 6.2 Shared analyzer and repair-pipeline reuse
- Acceptance: `/api/analyze` calls `dataforge.engine.repair.run_repair_pipeline`
  with dry-run transactions, returns proof-loop evidence, and `/api/profile`
  plus `/api/repair` remain thin compatibility projections where possible.
- Depends on: 6.1
- Estimated complexity: L

### 6.3 Advanced-mode enforcement
- Acceptance: keyless advanced requests fail with 400; keyed advanced requests are accepted.
- Depends on: 6.1
- Estimated complexity: S

### 6.4 Frontend decoupling
- Acceptance: relative assets, `config.js`, request-state locking, and keyboard-complete tabs all ship.
- Depends on: 6.1
- Estimated complexity: M

### 6.5 HF staging and runbooks
- Acceptance: `scripts/playground/stage_space.py` produces a Docker-buildable Space tree and docs reference that flow.
- Depends on: 6.1
- Estimated complexity: M

### 6.6 Quality gate expansion
- Acceptance: Makefile and CI cover the shipped Week 5 Python paths and playground contract tests.
- Depends on: 6.1 to 6.5
- Estimated complexity: S

### 6.7 Problem details and OpenAPI contracts
- Acceptance: expected HTTP failures return RFC 9457 problem details with
  stable `error` extension values, and `scripts/ci/openapi_contract.py --check`
  fails on schema drift.
- Depends on: 6.1, 6.2
- Estimated complexity: S

### 6.8 Constraint review proof loop
- Acceptance: schema inference emits `constraint_review_v1` candidates in
  `/api/analyze`; candidates default to `pending`; only submitted accepted IDs
  affect repair; the receipt records accepted IDs and source hash.
- Depends on: 6.2
- Estimated complexity: M

### 6.9 Streamable supervision workflow
- Acceptance: `/api/analyze/stream` emits NDJSON `workflow_event_v1` events in
  fixed stage order, includes final `AnalyzeResponse` data on the receipt event,
  emits failed receipt events for stream-time public errors, and remains
  dry-run/stateless.
- Depends on: 6.2, 6.7
- Estimated complexity: M

### 6.10 DataForge multi-page AUX surface
- Acceptance: the React playground uses the stream when available, falls back to
  `/api/analyze`, supports direct `/playground/*` product routes, shows
  workflow stages, proof obligations, root causes, failures, accepted
  constraints, limitations, and local CLI handoff as first-class UI.
- Depends on: 6.4, 6.8, 6.9
- Estimated complexity: L

## 7. Verification

- Integration tests: `tests/integration/test_playground_smoke.py`
- Unit tests: `tests/unit/test_playground_stage_space.py`, `tests/unit/test_playground_web_contract.py`
- Regression tests: `tests/regression/test_env.py`
- Frontend visual contract: `npm run colors:check`, `npm run audit:colors`, `npm run test:unit`, `npm run test:e2e`
- Stream contract: integration tests cover event order, final payload parity,
  health metadata, stream failure events, and fallback behavior.
- CI assertions: no browser storage APIs, no frontend API keys, no raw
  hand-authored hex colors, valid HF Space front matter

## 8. Acceptance gate (ALL must be TRUE to mark SPEC complete)

- [x] All Section 2 outcomes are met.
- [x] All Section 6 tasks have "passes".
- [x] Playground smoke and contract tests are green.
- [x] No test in `tests/regression/` fails.
- [x] The HF staging script output matches Docker COPY sources.
- [x] Docs describe the authoritative deploy flows and do not overclaim a live deployment.

Evidence: `tests/integration/test_playground_smoke.py`,
`tests/unit/test_playground_stage_space.py`,
`tests/unit/test_playground_web_contract.py`,
`tests/regression/test_env.py`, `npm --prefix playground/web run colors:check`,
and `scripts/ci/openapi_contract.py --check`.

## Appendix A - Toy cases (write the FIRST failing tests from these)

### Case A.1: API root is stable
Input: `GET /`
Expected output: 200 JSON response with `service`, `status`, and `docs_url`
Reasoning: catches the broken same-origin SPA fallback and guarantees the backend is API-only.

### Case A.2: Health exposes capabilities
Input: `GET /api/health`
Expected output: `{"status":"ok","advanced_available":<bool>,"max_upload_bytes":1048576}`
Reasoning: the frontend needs this to render the advanced toggle and upload guard honestly.

### Case A.3: Advanced mode is key-gated
Input: `POST /api/analyze?advanced=true` with and without a provider key
Expected output: 400 when unkeyed, 200 when keyed
Reasoning: prevents a dead toggle in the hosted UI.

### Case A.4: Analyze returns the proof-loop payload
Input: `POST /api/analyze` with `hospital_10rows.csv`
Expected output: `source`, `schema_inference`, `risk_summary`, `issues`,
`repairs`, `verification`, `txn_journal`, `receipt`, `apply_handoff`, and
`limitations`
Reasoning: proves the hosted flow reflects the product promise without enabling browser mutation.

### Case A.4b: Compatibility repair returns a real ephemeral journal
Input: `POST /api/repair?dry_run=true` with `hospital_10rows.csv`
Expected output: `fixes` plus `txn_journal` with `txn_id`, `created_at`, `source_sha256`, and `events`
Reasoning: keeps old clients on the true transaction model while `/api/analyze` becomes primary.

### Case A.5: Rate limit boundary
Input: 11 POST requests from one client in under a minute
Expected output: first 10 accepted, 11th returns 429
Reasoning: validates the free-tier abuse guard and the single-worker contract.

### Case A.6: CORS is exact-origin in production
Input: production request from an unconfigured `workers.dev` origin
Expected output: no `Access-Control-Allow-Origin` response header
Reasoning: prevents another Cloudflare account from calling the API by virtue of sharing the same platform domain.

### Case A.7: HF staging is internally consistent
Input: `python scripts/playground/stage_space.py --output-dir <tmp>`
Expected output: staged repo contains every Docker COPY source and omits the frontend tree
Reasoning: prevents deploy docs from drifting away from the actual Docker build context.

### Case A.8: Problem detail shape
Input: `POST /api/analyze?advanced=true` without a provider key.
Expected output: 400 `application/problem+json` with `type`, `title`, `status`, `detail`, and `error="advanced_mode_unavailable"`.
Reasoning: gives clients a stable error contract without preserving ad hoc FastAPI exception wrappers.

### Case A.9: OpenAPI drift check
Input: generated Playground or OpenEnv schema differs from `specs/openapi/*.json`.
Expected output: backend gate fails and asks the contributor to regenerate snapshots intentionally.
Reasoning: treats API schema as a reviewed contract artifact.

### Case A.10: Unknown accepted constraint ID
Input: `POST /api/analyze` with `accepted_constraint_ids=["cnd-missing"]`
Expected output: 400 `application/problem+json` with `error="unknown_constraint_id"`
Reasoning: prevents stale UI state or forged IDs from silently changing repair semantics.
