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

- `CVE-2025-3000` / `GHSA-rrmf-rvhw-rf47` in `torch`: optional local
  training/HF evaluation dependency only, local-JIT attack surface, and
  pip-audit reports no fixed version as of 2026-06-13. The exception is
  encoded in `scripts/ci/backend_gate.py`, expires on 2026-07-13, and must be
  removed when an audited fixed version is available.

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
