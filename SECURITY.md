# Security Policy

## Reporting A Vulnerability

Please do not open a public issue for security vulnerabilities. Contact the
maintainer privately or open a draft GitHub security advisory.

Include:

1. A description of the vulnerability and attack scenario.
2. Steps to reproduce.
3. Affected version or commit hash.
4. Suggested fix, if you have one.

We aim to acknowledge reports within 48 hours and patch critical issues within
7 days.

## In Scope

- `dataforge/` CLI and library code
- `dataforge-mcp/` local MCP server source package, distributed as `dataforge_07_mcp`
- `playground/api/` backend
- safety filter and SMT verifier
- transaction hash-chain integrity, audit verification, and revert behavior
- provider-call paths that could leak user data or API keys

## Transaction Audit Boundary

New transaction logs are tamper-evident local hash chains. Audit verification
detects local JSONL payload edits, event reordering, and broken replay before a
v2 transaction is reverted. DataForge does not claim external non-repudiation
unless a deployment separately anchors the transaction head hash in a trusted
system.

## Dependency Audit Policy

Release gates run `pip-audit` for the active Python environment and `npm audit`
for the playground frontend. Findings are release blockers unless they are
explicitly triaged in `scripts/ci/backend_gate.py`.

Current scoped exception:

- **None.** Nothing is currently suppressed, so `pip-audit` runs with no
  `--ignore-vuln` argument at all.

`PIP_AUDIT_EXCEPTIONS` in `scripts/ci/backend_gate.py` is the single source of
truth for this list, and `canonical-backend-gate` fails if this section and that
list disagree in either direction — an exception that no longer exists must not
be documented as current, and a live suppression must not go undisclosed. Until
2026-08-30 this section advertised a `torch` exception (`CVE-2025-3000` /
`GHSA-rrmf-rvhw-rf47`) that had been deleted on 2026-08-28 once a fresh resolve
installed torch 2.13.0, far outside the affected 0–2.6.0 range. It carried an
expiry of 2026-07-13 that had already passed and the claim "pip-audit reports no
fixed version", which that release falsifies. Nothing in the repository read this
file, so the drift was invisible; that is what the check now prevents.

Every exception must name the advisory, state where the package is reachable
from, justify itself against the vulnerable construct rather than the package,
cite an upstream reference, and carry an expiry. An exception that never expires
is a permanently silenced check.

## Out Of Scope

- Third-party dependencies, except where DataForge configuration makes an issue
  exploitable
- Static playground frontend issues that do not expose secrets or user data
- Generated local cache or staging directories

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.x.x | Latest only |

## Disclosure

DataForge follows coordinated disclosure. Reporters are credited in the
`CHANGELOG.md` entry for the fix unless anonymity is requested.
