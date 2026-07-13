"""Grounded, contract-bound LLM corrector.

This repairer attacks the hard half of data repair -- producing the *exact*
correct value (the Baran problem) -- for issue classes that have no
deterministic derivation (typos, free-text normalization, context-dependent
fills). It is built to be safe and honest, not impressive:

* **Grounded.** The prompt carries only real evidence from the table (the
  erroneous value, the detector's finding, sample values from the same column,
  and any functional-dependency group), never invented context.
* **Contract-bound.** Every sampled value must pass the
  :class:`~dataforge.repairers.contract.CorrectionContract` *and* the inferred
  constraint guard (the exact checks the verifier will later enforce), so the
  corrector can only ever propose values that the SMT/constitution gates would
  also accept. Anything else is filtered out.
* **Self-consistent.** ``k`` samples are drawn; the confidence is the agreement
  fraction among all samples for the chosen (contract-passing) value. Low
  agreement yields low confidence, which the calibration layer turns into a
  human-review suggestion rather than an auto-apply.
* **Calibration-aware.** When the model emits its own ``confidence`` for a value,
  it is combined with the self-consistency agreement by ``min(...)`` -- a strict
  monotonic-safety invariant: model confidence can only *lower*, never raise, the
  effective confidence. It is fed only into the calibrated abstention policy and
  can never push a fix past the SMT/constitution/conformal floor.
* **Cached.** Results are keyed by a content hash of the grounded prompt and the
  model, so repeated runs are deterministic and free; a cache hit is reported
  with ``llm_cache`` provenance.

The corrector never writes anything itself. It returns a ``ProposedFix`` with
``llm_*`` provenance, which the pipeline routes through the safety filter and
the verifier (including the inferred-constraint guard) before anything is
applied -- defense in depth.
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from dataforge.detectors.base import Issue, Schema
from dataforge.repairers.base import ProposedFix, ProvenanceLiteral, RetryContext
from dataforge.repairers.contract import CorrectionContract, build_correction_contract
from dataforge.schema_inference import infer_verification_schema
from dataforge.table import TableLike, cell_value, column_names, column_values, row_count
from dataforge.transactions.log import sha256_bytes
from dataforge.transactions.txn import CellFix
from dataforge.verifier.inferred import inferred_value_violation

if TYPE_CHECKING:
    from dataforge.agent.providers import Message

# Optional synchronous completion override: given the prompt messages, return the
# raw model text. Used to route corrector calls through a quota-tracking client
# (e.g. in the benchmark) without changing the production provider path.
CompletionFn = Callable[[list["Message"]], str]

_DEFAULT_SAMPLES = 3
_DEFAULT_TEMPERATURE = 0.4
_MAX_COLUMN_EXAMPLES = 20


async def complete(messages: list[Message], *, model: str, temperature: float) -> str:
    """Lazy provider wrapper kept patchable for tests."""
    try:
        from dataforge.agent.providers import complete as provider_complete
    except ImportError as exc:  # pragma: no cover - exercised only without extra
        raise RuntimeError(
            "The LLM corrector requires the provider extra: pip install 'dataforge[providers]'."
        ) from exc
    return await provider_complete(messages, model=model, temperature=temperature)


def _resolve_model() -> str:
    """Resolve the effective model from the active provider's env/default.

    Falls back to a stable default when the provider extra is unavailable so the
    repairer can still be constructed in offline/test contexts.
    """
    try:
        from dataforge.agent.providers import resolve_model
    except ImportError:  # pragma: no cover - exercised only without extra
        return "gemini-2.0-flash"
    resolved = resolve_model()
    return resolved or "gemini-2.0-flash"


def _parse_value(raw: str) -> str:
    """Extract a single corrected value from a model response."""
    text = raw.strip()
    if text.startswith("{") and text.endswith("}"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            for key in ("value", "corrected_value", "chosen_value", "answer"):
                if key in parsed:
                    return str(parsed[key]).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1].strip()
    return text


def _parse_confidence(raw: str) -> float | None:
    """Extract an optional model-emitted ``confidence`` (0-1), else ``None``.

    This reads untrusted model output: any missing, malformed, or out-of-range
    value yields ``None`` so the corrector falls back to self-consistency.
    """
    text = raw.strip()
    if not (text.startswith("{") and text.endswith("}")):
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    raw_conf = parsed.get("confidence")
    if isinstance(raw_conf, bool) or not isinstance(raw_conf, int | float):
        return None
    conf = float(raw_conf)
    if not 0.0 <= conf <= 1.0:
        return None
    return conf


def _parse_sample(raw: str) -> tuple[str, float | None]:
    """Parse one raw completion into (value, optional model confidence)."""
    return _parse_value(raw), _parse_confidence(raw)


class LLMCorrectorRepairer:
    """Propose exact-value corrections via a grounded, contract-bound LLM."""

    def __init__(
        self,
        *,
        cache_dir: Path | None,
        allow_llm: bool = False,
        model: str | None = None,
        samples: int = _DEFAULT_SAMPLES,
        temperature: float = _DEFAULT_TEMPERATURE,
        completion_fn: CompletionFn | None = None,
    ) -> None:
        self._cache_dir = cache_dir
        self._allow_llm = allow_llm
        self._model = model or _resolve_model()
        self._samples = max(1, samples)
        self._temperature = temperature
        self._completion_fn = completion_fn
        self._schema_cache: tuple[int, Schema] | None = None

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Return a contract-passing exact-value correction, or ``None``."""
        del retry_context
        if not self._allow_llm:
            return None
        if issue.row < 0 or issue.row >= row_count(df):
            return None
        if issue.column not in column_names(df):
            return None

        constraints = self._constraints_for(df, schema)
        contract = build_correction_contract(issue, constraints)
        if not contract.is_cell_correction:
            return None

        chosen, agreement, provenance = self._resolve(issue, df, contract, constraints)
        if chosen is None:
            return None

        old_value = cell_value(df, issue.row, issue.column)
        if chosen == old_value:
            return None
        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value=chosen,
                detector_id=issue.issue_type,
            ),
            reason=(
                f"LLM corrector (self-consistency {self._samples} samples, "
                f"agreement {agreement:.2f}): {issue.reason}"
            ),
            confidence=agreement,
            provenance=provenance,
        )

    def _constraints_for(self, df: TableLike, schema: Schema | None) -> Schema:
        """Return the authoritative schema or a cached inferred one."""
        if schema is not None:
            return schema
        cached = self._schema_cache
        if cached is not None and cached[0] == id(df):
            return cached[1]
        inferred = infer_verification_schema(df)
        self._schema_cache = (id(df), inferred)
        return inferred

    def _resolve(
        self,
        issue: Issue,
        df: TableLike,
        contract: CorrectionContract,
        constraints: Schema,
    ) -> tuple[str | None, float, ProvenanceLiteral]:
        """Sample (or replay) values, filter by contract, return the majority."""
        prompt = self._build_messages(issue, df, contract)
        cache_path = self._cache_path(issue, df, prompt)

        cached_samples = self._read_cache(cache_path)
        if cached_samples is not None:
            chosen, agreement = self._vote(cached_samples, issue, df, contract, constraints)
            return chosen, agreement, "llm_cache"

        raw_samples = [self._one_sample(prompt) for _ in range(self._samples)]
        samples = [_parse_sample(raw) for raw in raw_samples]
        values = [value for value, _ in samples]
        confidences = [confidence for _, confidence in samples]
        self._write_cache(cache_path, issue, raw_samples, values, confidences)
        chosen, agreement = self._vote(samples, issue, df, contract, constraints)
        return chosen, agreement, "llm_live"

    def _one_sample(self, prompt: list[Message]) -> str:
        """Draw one raw completion, via the injected fn or the provider layer."""
        if self._completion_fn is not None:
            return self._completion_fn(prompt)
        return asyncio.run(complete(prompt, model=self._model, temperature=self._temperature))

    def _vote(
        self,
        samples: list[tuple[str, float | None]],
        issue: Issue,
        df: TableLike,
        contract: CorrectionContract,
        constraints: Schema,
    ) -> tuple[str | None, float]:
        """Majority vote among contract- and guard-passing candidate values.

        The confidence is the self-consistency agreement fraction, optionally
        lowered (never raised) by the model's own emitted confidence for the
        chosen value -- the monotonic-safety invariant.
        """
        total = len(samples) or 1
        passing = [
            (value, confidence)
            for value, confidence in samples
            if self._candidate_ok(value, issue, df, contract, constraints)
        ]
        if not passing:
            return None, 0.0
        counts = Counter(value for value, _ in passing)
        chosen, votes = counts.most_common(1)[0]
        agreement = votes / total
        model_confidences = [
            confidence
            for value, confidence in passing
            if value == chosen and confidence is not None
        ]
        if model_confidences:
            mean_model_confidence = sum(model_confidences) / len(model_confidences)
            return chosen, min(agreement, mean_model_confidence)
        return chosen, agreement

    def _candidate_ok(
        self,
        value: str,
        issue: Issue,
        df: TableLike,
        contract: CorrectionContract,
        constraints: Schema,
    ) -> bool:
        """A candidate is acceptable only if it passes every gate the verifier will."""
        if not contract.check(value).ok:
            return False
        violation = inferred_value_violation(df, issue.row, issue.column, value, constraints)
        return violation is None

    def _build_messages(
        self,
        issue: Issue,
        df: TableLike,
        contract: CorrectionContract,
    ) -> list[Message]:
        """Construct a grounded prompt from real table evidence and the contract."""
        examples = self._column_examples(df, issue.column, issue.row)
        evidence: dict[str, object] = {
            "column": issue.column,
            "current_value": issue.actual,
            "issue_type": issue.issue_type,
            "column_examples": examples,
        }
        fd_context = self._fd_context(df, issue, contract.constraints)
        if fd_context is not None:
            evidence["functional_dependency"] = fd_context

        system = (
            "You correct a single erroneous cell in a tabular dataset. "
            "Use only the evidence provided; do not invent facts. "
            "Respond with only the corrected value and nothing else."
        )
        user = f"{contract.describe()}\n\nEvidence:\n{json.dumps(evidence, sort_keys=True)}"
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    @staticmethod
    def _column_examples(df: TableLike, column: str, row: int) -> list[str]:
        """Return a sample of distinct non-empty values from the column."""
        seen: list[str] = []
        for index, value in enumerate(column_values(df, column)):
            if index == row:
                continue
            text = str(value).strip()
            if text and text not in seen:
                seen.append(text)
            if len(seen) >= _MAX_COLUMN_EXAMPLES:
                break
        return seen

    @staticmethod
    def _fd_context(
        df: TableLike,
        issue: Issue,
        constraints: Schema,
    ) -> dict[str, object] | None:
        """Return determinant-group evidence when the column is an FD dependent."""
        columns = set(column_names(df))
        for fd in constraints.functional_dependencies:
            if fd.dependent != issue.column:
                continue
            if any(det not in columns for det in fd.determinant):
                continue
            key = {det: str(cell_value(df, issue.row, det)).strip() for det in fd.determinant}
            peers: list[str] = []
            for other in range(row_count(df)):
                if other == issue.row:
                    continue
                if all(
                    str(cell_value(df, other, det)).strip() == key[det] for det in fd.determinant
                ):
                    dependent = str(cell_value(df, other, issue.column)).strip()
                    if dependent and dependent not in peers:
                        peers.append(dependent)
            if peers:
                return {
                    "determinant": dict(key),
                    "dependent_column": issue.column,
                    "peer_values": peers,
                }
        return None

    def _cache_path(self, issue: Issue, df: TableLike, prompt: list[Message]) -> Path | None:
        """Return the content-hash cache path for this grounded prompt."""
        if self._cache_dir is None:
            return None
        payload = {
            "issue_type": issue.issue_type,
            "row": issue.row,
            "column": issue.column,
            "current_value": cell_value(df, issue.row, issue.column),
            "prompt": prompt,
            "model": self._model,
            "samples": self._samples,
        }
        digest = sha256_bytes(json.dumps(payload, sort_keys=True).encode("utf-8"))
        model_slug = self._model.replace("/", "_")
        return self._cache_dir / f"corrector_{digest}_{model_slug}.json"

    @staticmethod
    def _read_cache(cache_path: Path | None) -> list[tuple[str, float | None]] | None:
        """Return cached (value, confidence) samples, or ``None`` on miss.

        Backward-compatible: older caches carry only ``values`` (no confidences),
        which read back with ``None`` confidence and therefore fall back to pure
        self-consistency.
        """
        if cache_path is None or not cache_path.exists():
            return None
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            values = cached["values"]
        except (json.JSONDecodeError, KeyError, OSError):
            return None
        if not isinstance(values, list):
            return None
        confidences = cached.get("confidences")
        samples: list[tuple[str, float | None]] = []
        for index, value in enumerate(values):
            confidence: float | None = None
            if isinstance(confidences, list) and index < len(confidences):
                raw_conf = confidences[index]
                if (
                    isinstance(raw_conf, int | float)
                    and not isinstance(raw_conf, bool)
                    and 0.0 <= float(raw_conf) <= 1.0
                ):
                    confidence = float(raw_conf)
            samples.append((str(value), confidence))
        return samples

    def _write_cache(
        self,
        cache_path: Path | None,
        issue: Issue,
        raw_samples: list[str],
        values: list[str],
        confidences: list[float | None],
    ) -> None:
        """Persist raw samples, parsed values, and model confidences for replay."""
        if cache_path is None:
            return
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "issue_type": issue.issue_type,
                    "model": self._model,
                    "raw_samples": raw_samples,
                    "values": values,
                    "confidences": confidences,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
