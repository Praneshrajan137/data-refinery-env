"""Detector for categorical normalization variants (case/whitespace/punctuation).

Categorical columns often contain the same logical value written several ways:
``"New York"`` / ``"new york"`` / ``"NEW  YORK"``. This detector clusters values
by a normalization key and, within a cluster that has a clear dominant exact
form, flags the minority variants as ``categorical_normalization`` with the
dominant form as the expected value.

Precision guards: only categorical columns are considered (low distinct
cardinality with real repetition), and a variant is only flagged when its
cluster has a strict-majority canonical form. Free-text and high-cardinality
columns are skipped.

The detector is pure: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

from dataforge.detectors.base import Issue, Schema, Severity
from dataforge.table import TableLike, column_names, column_values

_MIN_VALUES = 8
# A column is categorical when distinct normalized keys are a small fraction of
# its non-empty values (i.e. values repeat).
_MAX_DISTINCT_RATIO = 0.6
_WS = re.compile(r"\s+")


def normalization_key(value: str) -> str:
    """Return the case/whitespace/punctuation-insensitive key for a value."""
    lowered = value.strip().lower()
    collapsed = _WS.sub(" ", lowered)
    return "".join(ch for ch in collapsed if ch.isalnum() or ch == " ").strip()


class CategoricalNormalizationDetector:
    """Flags minority spelling/format variants of a categorical value.

    Example:
        >>> import pandas as pd
        >>> detector = CategoricalNormalizationDetector()
        >>> col = ["NY", "NY", "ny", "NY", "CA", "CA", "CA", "NY", "ca", "NY"]
        >>> issues = detector.detect(pd.DataFrame({"state": col}))
        >>> sorted(i.row for i in issues)
        [2, 8]
    """

    def detect(self, df: TableLike, schema: Schema | None = None) -> list[Issue]:
        """Detect categorical-normalization issues across categorical columns."""
        issues: list[Issue] = []
        for col_name in column_names(df):
            issues.extend(self._check_column(df, str(col_name)))
        return issues

    def _check_column(self, df: TableLike, col_name: str) -> list[Issue]:
        """Flag minority variants within near-duplicate clusters of one column."""
        entries = [
            (i, str(v).strip()) for i, v in enumerate(column_values(df, col_name)) if str(v).strip()
        ]
        if len(entries) < _MIN_VALUES:
            return []

        keys = {value: normalization_key(value) for _, value in entries}
        distinct_keys = {k for k in keys.values() if k}
        if not distinct_keys or len(distinct_keys) / len(entries) > _MAX_DISTINCT_RATIO:
            return []  # unique-ID-like or free-text column

        # Group exact forms and row indices per normalization key.
        forms_by_key: dict[str, Counter[str]] = defaultdict(Counter)
        rows_by_key_form: dict[tuple[str, str], list[int]] = defaultdict(list)
        for row_idx, value in entries:
            key = keys[value]
            if not key:
                continue
            forms_by_key[key][value] += 1
            rows_by_key_form[(key, value)].append(row_idx)

        issues: list[Issue] = []
        for key, form_counts in forms_by_key.items():
            if len(form_counts) < 2:
                continue  # no variants in this cluster
            ranked = form_counts.most_common()
            canonical, top = ranked[0]
            second = ranked[1][1]
            if top <= second:
                continue  # no strict-majority canonical; ambiguous, skip
            cluster_total = sum(form_counts.values())
            confidence = round(min(0.95, 0.5 + top / cluster_total / 2), 2)
            for form, _count in ranked[1:]:
                for row_idx in rows_by_key_form[(key, form)]:
                    issues.append(
                        Issue(
                            row=row_idx,
                            column=col_name,
                            issue_type="categorical_normalization",
                            severity=Severity.REVIEW,
                            confidence=confidence,
                            expected=canonical,
                            actual=form,
                            reason=(
                                f"Value '{form}' is a normalization variant of the dominant "
                                f"form '{canonical}' in column '{col_name}'."
                            ),
                        )
                    )
        return issues
