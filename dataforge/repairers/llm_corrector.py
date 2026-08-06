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
* **Optionally schema-constrained** (``structured=True``). The candidate pool
  becomes a hard decode-time ``enum`` via Structured Outputs instead of a prompt
  request plus post-filter. This matters for three measured reasons:

  - Pool membership becomes a *guarantee*, so no sample is wasted being filtered
    out after it was paid for, and the ``votes / total`` agreement denominator
    stops being polluted by inadmissible samples.
  - The ``confidence`` field becomes reachable. In free-text mode both system
    prompts ask for "only the value", never JSON, so ``_parse_confidence``
    returns ``None`` on every production call and the ``min(...)`` invariant above
    never fires. Under a strict schema the field is required, so it is real.
  - The confidence grid widens from the ~3 discrete values that ``k=3`` agreement
    can take to a continuous score. ``conformal.certify_threshold`` searches only
    *observed* confidences, so a 3-point grid leaves almost nowhere to place a
    threshold that isolates a clean high-confidence slice.

  Support is per-deployment and the vendor docs disagree, so it is measured, not
  assumed: see ``eval/results/azure_capability_probe.json``. Note that reasoning
  deployments reject ``temperature``, so ``temperature`` is a no-op there and
  sample diversity must be tuned via ``k``, not temperature.
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

# Structured-mode override. Kept as a SEPARATE type rather than widening
# ``CompletionFn``: the schema must actually reach the wire, and a one-argument
# override would silently drop it, making "structured mode" a no-op through the
# benchmark adapter while still reporting structured results.
StructuredCompletionFn = Callable[[list["Message"], dict[str, object] | None], str]

_DEFAULT_SAMPLES = 3
_DEFAULT_TEMPERATURE = 0.4
_MAX_COLUMN_EXAMPLES = 20
# Candidate-pool constraint (measured lever): restricting the model to SELECT a
# value from the column's frequent values (rather than free-text) lifted measured
# correction precision from ~0.08-0.16 to 0.85 on hospital. The pool is the
# distinct values with support >= _POOL_MIN_SUPPORT, capped at _POOL_CAP by
# frequency. It is a proposal constraint only -- corrector output is still
# plausibility_only and held for review by default (never auto-applied).
_POOL_MIN_SUPPORT = 2
_POOL_CAP = 50

# The sentinel the model must emit when no candidate is clearly correct. It is a
# member of the enum so abstention is always representable -- otherwise a strict
# schema would force the model to pick a value it does not believe.
_ABSTAIN_TOKEN = "NONE"


async def complete(
    messages: list[Message],
    *,
    model: str,
    temperature: float,
    response_format: dict[str, object] | None = None,
) -> str:
    """Lazy provider wrapper kept patchable for tests."""
    try:
        from dataforge.agent.providers import complete_with_usage
    except ImportError as exc:  # pragma: no cover - exercised only without extra
        raise RuntimeError(
            "The LLM corrector requires the provider extra: pip install 'dataforge[providers]'."
        ) from exc
    result = await complete_with_usage(
        messages, model=model, temperature=temperature, response_format=response_format
    )
    return result.text


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
        structured_completion_fn: StructuredCompletionFn | None = None,
        pool_constrained: bool = False,
        structured: bool = False,
    ) -> None:
        self._cache_dir = cache_dir
        self._allow_llm = allow_llm
        self._model = model or _resolve_model()
        self._samples = max(1, samples)
        self._temperature = temperature
        self._completion_fn = completion_fn
        self._structured_completion_fn = structured_completion_fn
        self._structured = structured
        # A decode-time enum IS the pool constraint, so structured mode implies
        # it. Keeping them independent would allow a schema whose enum is the
        # whole column while the post-filter still rejected values -- two
        # disagreeing definitions of "admissible".
        self._pool_constrained = pool_constrained or structured
        self._schema_cache: tuple[int, Schema] | None = None
        self._pool_cache: dict[tuple[int, str], list[str]] = {}

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

        response_format = self._response_format(df, issue)
        raw_samples = [self._one_sample(prompt, response_format) for _ in range(self._samples)]
        samples = [_parse_sample(raw) for raw in raw_samples]
        values = [value for value, _ in samples]
        confidences = [confidence for _, confidence in samples]
        self._write_cache(cache_path, issue, raw_samples, values, confidences)
        chosen, agreement = self._vote(samples, issue, df, contract, constraints)
        return chosen, agreement, "llm_live"

    def _response_format(self, df: TableLike, issue: Issue) -> dict[str, object] | None:
        """Return a strict Structured-Outputs schema, or ``None`` in free-text mode.

        Honours every documented Structured Outputs constraint: ``strict: true``,
        ``additionalProperties: false``, every property required, and no
        unsupported type-specific keywords (notably no ``minimum``/``maximum`` on
        ``confidence`` -- it is clamped locally by ``_parse_confidence`` instead,
        which also keeps the parser authoritative over untrusted model output).
        """
        if not self._structured:
            return None
        pool = self._candidate_pool(df, issue.column)
        if not pool:
            # No closed candidate set means no enum to constrain to. Abstain from
            # the structured path rather than emit a degenerate one-value schema.
            return None
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "cell_correction",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string", "enum": [*pool, _ABSTAIN_TOKEN]},
                        "confidence": {"type": "number"},
                    },
                    "required": ["value", "confidence"],
                    "additionalProperties": False,
                },
            },
        }

    def _one_sample(self, prompt: list[Message], response_format: dict[str, object] | None) -> str:
        """Draw one raw completion, via an injected fn or the provider layer."""
        if response_format is not None and self._structured_completion_fn is not None:
            return self._structured_completion_fn(prompt, response_format)
        if self._completion_fn is not None:
            return self._completion_fn(prompt)
        if response_format is None:
            # Omit the kwarg entirely (rather than passing None) so the free-text
            # call signature is exactly what it was before structured mode
            # existed. Anything patching `complete` keeps working unchanged.
            return asyncio.run(complete(prompt, model=self._model, temperature=self._temperature))
        return asyncio.run(
            complete(
                prompt,
                model=self._model,
                temperature=self._temperature,
                response_format=response_format,
            )
        )

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
        # The abstention sentinel is never a candidate value. Under a strict enum
        # the model must answer with a member of the schema, so this is how it
        # says "no candidate is clearly correct".
        if value == _ABSTAIN_TOKEN:
            return False
        if self._pool_constrained and value not in self._candidate_pool(df, issue.column):
            return False
        if not contract.check(value).ok:
            return False
        violation = inferred_value_violation(df, issue.row, issue.column, value, constraints)
        return violation is None

    def _candidate_pool(self, df: TableLike, column: str) -> list[str]:
        """Return the column's frequent-value pool (support-graded, freq-capped).

        Cached per (df identity, column). This is the closed candidate set the
        constrained corrector must select from; the current (dirty) value is
        naturally excluded when it is rare (below the support floor).
        """
        key = (id(df), column)
        cached = self._pool_cache.get(key)
        if cached is not None:
            return cached
        counts = Counter(str(v) for v in column_values(df, column) if str(v).strip() != "")
        pool = [value for value, n in counts.most_common() if n >= _POOL_MIN_SUPPORT][:_POOL_CAP]
        self._pool_cache[key] = pool
        return pool

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

        if self._structured:
            pool = self._candidate_pool(df, issue.column)
            evidence["candidate_pool"] = pool
            # The enum already enforces membership at decode time, so the prompt
            # spends its words on the decision rule and on making abstention
            # feel legitimate rather than on restating the constraint.
            system = (
                "You correct a single erroneous cell in a tabular dataset. Use only "
                "the evidence provided; do not invent facts. Choose the value that the "
                "evidence supports, and report your confidence in it from 0 to 1. If no "
                f"candidate is clearly correct, answer {_ABSTAIN_TOKEN} -- abstaining is "
                "correct and preferred over guessing."
            )
        elif self._pool_constrained:
            pool = self._candidate_pool(df, issue.column)
            evidence["candidate_pool"] = pool
            system = (
                "You correct a single erroneous cell in a tabular dataset. Use only "
                "the evidence provided; do not invent facts. You MUST choose the "
                "corrected value EXACTLY from candidate_pool, using the other evidence "
                "as context to disambiguate. If no candidate is clearly correct, "
                "respond with exactly NONE. Respond with only the chosen value or NONE."
            )
        else:
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
            # Structured and free-text modes produce different distributions from
            # the same prompt, so they must not share a cache entry. Omitted when
            # False so every pre-existing cache file stays valid.
            **({"structured": True} if self._structured else {}),
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
