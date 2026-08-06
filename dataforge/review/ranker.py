"""Review-queue ranker: score detected cells by likelihood of being a true error.

This is a *scorer*, not a repairer. Given cells the deterministic detectors have
already flagged, it asks a grounded yes/no question - "is this flagged cell's
value actually erroneous?" - and returns a score in [0, 1] (the fraction of
``samples`` self-consistency votes that said "yes"). Callers rank the review
queue by that score so a human reviews likely-true errors first.

Design (mirrors ``dataforge.repairers.llm_corrector`` deliberately):
* **Never writes, never proposes a value, never auto-applies.** It only produces
  a ranking score. The verified apply-gate and the correction floor are untouched
  by construction - there is no path from a score to a mutation.
* **Grounded.** The prompt carries only the flagged cell and its own row; no
  invented context.
* **Self-consistent.** ``samples`` votes; score = fraction saying "yes". ``k=1``
  is the confirmed cheap yes/no; ``k>1`` gives a finer, more stable ranking.
* **Cached.** Keyed by a content hash of the prompt + model, so re-runs are free
  and deterministic (``llm_cache`` provenance on a hit).
* **Injectable.** A ``completion_fn`` routes calls through a quota-tracking bench
  client without touching the production provider path, exactly like the
  corrector - so it is fully testable offline.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from dataforge.table import TableLike, cell_value, column_names, row_count
from dataforge.transactions.log import sha256_bytes

if TYPE_CHECKING:
    from dataforge.agent.providers import Message

CompletionFn = Callable[[list["Message"]], str]
#: Per-cell detector findings the ranker may be shown. Keys are ``(row, column)``.
DetectorEvidence = Mapping[tuple[int, str], Mapping[str, object]]

_DEFAULT_SAMPLES = 1
_DEFAULT_TEMPERATURE = 0.0

_SYSTEM_PROMPT = (
    "You are a data-quality auditor. A specific cell in a table row has been "
    "flagged as possibly erroneous. Using the whole row as context, decide "
    "whether the flagged cell's value is actually erroneous. Respond with ONLY "
    "'yes' (erroneous) or 'no' (fine). No prose."
)

# Used ONLY when detector evidence is supplied. Keeping the evidence-free prompt
# byte-identical preserves every existing cache entry and leaves the previously
# measured behaviour untouched, so the two paths can be compared honestly.
_SYSTEM_PROMPT_WITH_EVIDENCE = (
    "You are a data-quality auditor. A specific cell in a table row has been "
    "flagged as possibly erroneous by automated detectors, whose findings are "
    "included. The detectors have high recall but low precision, so treat their "
    "findings as evidence to weigh, not as ground truth. Using the whole row and "
    "those findings, decide whether the flagged cell's value is actually "
    "erroneous. Respond with ONLY 'yes' (erroneous) or 'no' (fine). No prose."
)


def _resolve_model() -> str:
    """Resolve the active provider's model for the cache key, robust offline."""
    try:
        from dataforge.agent.providers import get_provider_name, resolve_model

        return resolve_model(get_provider_name()) or "review-ranker"
    except Exception:  # noqa: BLE001 - cache-key only; never fail ranking on this
        return "review-ranker"


class CellScore(NamedTuple):
    """A ranked review-queue candidate."""

    row: int
    column: str
    score: float
    provenance: str  # "llm_live" | "llm_cache"


class ReviewRanker:
    """Score flagged cells for review-queue ordering. Never mutates anything."""

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        model: str | None = None,
        samples: int = _DEFAULT_SAMPLES,
        temperature: float = _DEFAULT_TEMPERATURE,
        completion_fn: CompletionFn | None = None,
    ) -> None:
        self._cache_dir = cache_dir
        self._model = model or _resolve_model()
        self._samples = max(1, samples)
        self._temperature = temperature
        self._completion_fn = completion_fn

    def rank(
        self,
        cells: Sequence[tuple[int, str]],
        df: TableLike,
        evidence: DetectorEvidence | None = None,
    ) -> list[CellScore]:
        """Score each ``(row, column)`` and return them highest-score first.

        Ties preserve input order (stable sort), so a caller may pre-order
        ``cells`` by a free baseline (e.g. detector confidence) to break ties.

        Args:
            cells: The flagged cells to score.
            df: The table the cells belong to.
            evidence: Optional per-cell detector findings (``issue_type``, ``severity``,
                ``confidence``, ``reason``, ``expected``). When omitted the prompt is
                byte-identical to the original evidence-free one, so existing caches and
                previously measured results remain valid.

                **MEASURED HARMFUL -- do not enable by default.** The hypothesis behind
                this parameter was that the ranker was handicapped by re-deriving a
                judgement the detectors had already made. Measurement refuted it
                (``eval/results/ranker_arms_cross_dataset.json``, gpt-5-mini, 300 cells per
                dataset, paired): supplying evidence moved ROC-AUC by +0.033 on hospital
                (CI [-0.014, +0.083], no detectable change) and **-0.401 on rayyan**
                (0.656 -> 0.256, CI [-0.472, -0.329]) -- below chance, with precision in the
                top decile collapsing from 0.567 to 0.067.

                The cause is anchoring: the model inherits the detectors' false positives
                instead of checking them. rayyan's detectors are only 33.7% precise, so
                trusting them is worse than ignoring them. A prompt that explicitly warns
                the findings are low-precision did **not** prevent this.

                The deeper reason matters for the trust story: the verifier's value comes
                from being *independent* of the detector. Feeding it the detector's opinion
                converts an independent check into an amplifier of the detector's errors.
                Retained only so the negative result stays reproducible.
        """
        scored = [
            self._score(row, column, df, (evidence or {}).get((row, column)))
            for row, column in cells
        ]
        return sorted(scored, key=lambda cs: cs.score, reverse=True)

    def _score(
        self,
        row: int,
        column: str,
        df: TableLike,
        cell_evidence: Mapping[str, object] | None = None,
    ) -> CellScore:
        if row < 0 or row >= row_count(df) or column not in column_names(df):
            return CellScore(row, column, 0.0, "invalid")
        prompt = self._build_messages(row, column, df, cell_evidence)
        cache_path = self._cache_path(row, column, df, prompt)
        cached = self._read_cache(cache_path)
        if cached is not None:
            return CellScore(row, column, _vote(cached), "llm_cache")
        raw_samples = [self._one_sample(prompt) for _ in range(self._samples)]
        self._write_cache(cache_path, raw_samples)
        return CellScore(row, column, _vote(raw_samples), "llm_live")

    def _one_sample(self, prompt: list[Message]) -> str:
        if self._completion_fn is not None:
            return self._completion_fn(prompt)
        from dataforge.agent.providers import complete

        return asyncio.run(complete(prompt, model=self._model, temperature=self._temperature))

    def _build_messages(
        self,
        row: int,
        column: str,
        df: TableLike,
        cell_evidence: Mapping[str, object] | None = None,
    ) -> list[Message]:
        values = {col: str(cell_value(df, row, col)) for col in column_names(df)}
        evidence: dict[str, object] = {
            "flagged_column": column,
            "flagged_value": values.get(column, ""),
            "row": values,
        }
        system = _SYSTEM_PROMPT
        if cell_evidence:
            evidence["detector_findings"] = {
                key: str(value) for key, value in sorted(cell_evidence.items())
            }
            system = _SYSTEM_PROMPT_WITH_EVIDENCE
        return [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": json.dumps(evidence, sort_keys=True, separators=(",", ":")),
            },
        ]

    def _cache_path(
        self, row: int, column: str, df: TableLike, prompt: list[Message]
    ) -> Path | None:
        if self._cache_dir is None:
            return None
        payload = {
            "row": row,
            "column": column,
            "current_value": cell_value(df, row, column),
            "prompt": prompt,
            "model": self._model,
            "samples": self._samples,
        }
        digest = sha256_bytes(json.dumps(payload, sort_keys=True).encode("utf-8"))
        model_slug = self._model.replace("/", "_")
        return self._cache_dir / f"review_ranker_{digest}_{model_slug}.json"

    @staticmethod
    def _read_cache(cache_path: Path | None) -> list[str] | None:
        if cache_path is None or not cache_path.exists():
            return None
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            raw = cached["raw_samples"]
        except (json.JSONDecodeError, KeyError, OSError):
            return None
        if not isinstance(raw, list):
            return None
        return [str(item) for item in raw]

    def _write_cache(self, cache_path: Path | None, raw_samples: list[str]) -> None:
        if cache_path is None:
            return
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps({"raw_samples": raw_samples}, sort_keys=True),
            encoding="utf-8",
        )


def _vote(raw_samples: Sequence[str]) -> float:
    """Fraction of raw samples that judged the cell erroneous ("yes")."""
    if not raw_samples:
        return 0.0
    yes = sum(1 for raw in raw_samples if str(raw).strip().lower().startswith("y"))
    return round(yes / len(raw_samples), 4)
