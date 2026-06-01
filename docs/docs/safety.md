# Safety

DataForge is designed for conservative repair. A repair is acceptable only when
it is explainable, narrowly scoped, and reversible.

The repository-level `THREAT_MODEL.md` is release-blocking for 0.1. It covers
the CLI, playground, MCP, optional LLM fallback, model artifacts, and
supply-chain workflow.

## Public safety guarantees

- No applied repair bypasses `SafetyFilter`.
- No applied repair bypasses `SMTVerifier`.
- No applied repair writes before a transaction journal and snapshot exist.
- Row deletion is denied in the shipped constitution.
- PII-like overwrites require explicit confirmation or are denied.

## Operational guidance

Use `--dry-run` for review and `--apply` only on files that can be modified.
For production data, copy files into a controlled workspace and keep the
transaction logs with the repaired artifact.
