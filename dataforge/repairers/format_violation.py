"""Repairer for format-violation issues via safe, invertible canonicalization.

This repairer only proposes a fix when it can canonicalize a minority-format
value to the column's dominant format through a *safe, well-understood*
transform: date reformatting (when the parse is unambiguous), leading-zero
padding of fixed-width numeric codes, or whitespace/case normalization. When
the correct canonical value cannot be derived unambiguously, it abstains
(returns ``None``) - the issue stays detection-only rather than risking a wrong
fix. Every proposal still passes the SMT verifier and safety constitution.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime

from dataforge.detectors.base import Issue, Schema
from dataforge.detectors.format_violation import value_shape
from dataforge.repairers.base import ProposedFix, RetryContext
from dataforge.table import TableLike, cell_value, column_values
from dataforge.transactions.txn import CellFix

# Candidate date formats, ordered. ISO first.
_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%m-%d-%Y",
    "%d-%m-%Y",
    "%Y.%m.%d",
    "%d.%m.%Y",
)


class FormatViolationRepairer:
    """Canonicalize minority-format values to the column's dominant format."""

    def propose(
        self,
        issue: Issue,
        df: TableLike,
        schema: Schema | None,
        retry_context: RetryContext | None = None,
    ) -> ProposedFix | None:
        """Propose a canonicalization fix, or abstain when ambiguous."""
        del retry_context
        if issue.issue_type != "format_violation":
            return None

        old_value = cell_value(df, issue.row, issue.column)
        if old_value.strip() == "":
            return None

        dominant_shape, dominant_examples = self._dominant_profile(df, issue.column)
        if dominant_shape is None:
            return None

        new_value = self._canonicalize(old_value, dominant_shape, dominant_examples)
        if new_value is None or new_value == old_value:
            return None

        return ProposedFix(
            fix=CellFix(
                row=issue.row,
                column=issue.column,
                old_value=old_value,
                new_value=new_value,
                detector_id="format_violation",
                operation="update",
            ),
            reason=(
                f"Canonicalized '{old_value}' to dominant column format -> '{new_value}'."
            ),
            confidence=issue.confidence,
            provenance="deterministic",
        )

    @staticmethod
    def _dominant_profile(df: TableLike, column: str) -> tuple[str | None, list[str]]:
        """Return the dominant shape and example values carrying that shape."""
        values = [str(v).strip() for v in column_values(df, column) if str(v).strip()]
        if not values:
            return None, []
        shapes = Counter(value_shape(v) for v in values)
        dominant_shape, _ = shapes.most_common(1)[0]
        examples = [v for v in values if value_shape(v) == dominant_shape]
        return dominant_shape, examples

    def _canonicalize(
        self, value: str, dominant_shape: str, dominant_examples: list[str]
    ) -> str | None:
        """Return the canonicalized value, or None when it cannot be derived safely."""
        # 1. Whitespace/case normalization that lands on the dominant shape.
        trimmed = value.strip()
        if value_shape(trimmed) == dominant_shape and trimmed != value:
            return trimmed

        # 2. Date reformat: dominant format is a date, value parses unambiguously.
        target_fmt = self._dominant_date_format(dominant_examples)
        if target_fmt is not None:
            reformatted = self._reformat_date(value, target_fmt)
            if reformatted is not None:
                return reformatted

        # 3. Leading-zero padding of fixed-width numeric codes.
        if set(dominant_shape) == {"9"} and trimmed.isdigit():
            target_len = len(dominant_examples[0]) if dominant_examples else len(dominant_shape)
            if all(len(ex) == target_len for ex in dominant_examples) and len(trimmed) < target_len:
                return trimmed.zfill(target_len)

        return None

    @staticmethod
    def _dominant_date_format(examples: list[str]) -> str | None:
        """Infer the single date format that parses all dominant examples."""
        for fmt in _DATE_FORMATS:
            if all(_parses(example, fmt) for example in examples[:25]):
                return fmt
        return None

    @staticmethod
    def _reformat_date(value: str, target_fmt: str) -> str | None:
        """Parse value with non-target formats; reformat only if unambiguous."""
        candidate = value.strip()
        parsed: set[str] = set()
        for fmt in _DATE_FORMATS:
            dt = _try_parse(candidate, fmt)
            if dt is not None:
                parsed.add(dt.strftime(target_fmt))
        # Unambiguous only when every successful parse yields the same target.
        if len(parsed) == 1:
            result = next(iter(parsed))
            return result if result != candidate else None
        return None


def _parses(value: str, fmt: str) -> bool:
    """Return whether value parses under fmt as a valid date."""
    return _try_parse(value, fmt) is not None


def _try_parse(value: str, fmt: str) -> datetime | None:
    """Try to parse a date; return the datetime or None."""
    try:
        return datetime.strptime(value.strip(), fmt)
    except (ValueError, TypeError):
        return None
