"""Detector for functional-dependency violations in tabular data.

Given a declared functional dependency X -> Y (where X is a set of
determinant columns and Y is a dependent column), this detector groups
rows by X and flags any group where Y takes more than one distinct value.

Week 1 scope: declared FDs only (from the schema YAML).  Automatic FD
mining is deferred to a later milestone.

The detector is **pure**: no LLM calls, no I/O, no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from dataforge.detectors.base import Issue, Schema, Severity

if TYPE_CHECKING:
    pass


class FDViolationDetector:
    """Detects rows that violate declared functional dependencies.

    For each FD ``determinant -> dependent`` in the schema, groups the
    DataFrame by the determinant columns and checks that each group has
    exactly one unique value in the dependent column.  All rows in a
    violating group are flagged.

    Requires a ``Schema`` with ``functional_dependencies`` to do anything;
    returns an empty list if no schema or no FDs are provided.

    Example:
        >>> import pandas as pd
        >>> from dataforge.detectors.base import FunctionalDependency, Schema
        >>> detector = FDViolationDetector()
        >>> df = pd.DataFrame({
        ...     "zip": ["10001", "10001", "90210"],
        ...     "city": ["NY", "Manhattan", "LA"],
        ... })
        >>> schema = Schema(functional_dependencies=[
        ...     FunctionalDependency(determinant=["zip"], dependent="city"),
        ... ])
        >>> issues = detector.detect(df, schema)
        >>> len(issues)
        2
    """

    def detect(self, df: pd.DataFrame, schema: Schema | None = None) -> list[Issue]:
        """Detect FD-violation issues in the DataFrame.

        Args:
            df: The input DataFrame to analyze.
            schema: Schema containing declared functional dependencies.
                If None or no FDs declared, returns an empty list.

        Returns:
            A list of Issue objects for rows violating declared FDs.
        """
        if schema is None or not schema.functional_dependencies:
            return []

        issues: list[Issue] = []

        for fd in schema.functional_dependencies:
            fd_issues = self._check_fd(df, fd.determinant, fd.dependent)
            issues.extend(fd_issues)

        return issues

    def _check_fd(
        self,
        df: pd.DataFrame,
        determinant: tuple[str, ...],
        dependent: str,
    ) -> list[Issue]:
        """Check a single functional dependency X -> Y.

        Args:
            df: The DataFrame to check.
            determinant: List of determinant column names (X).
            dependent: The dependent column name (Y).

        Returns:
            Issues for all rows in groups that violate the FD.
        """
        determinant_columns = list(determinant)

        # Verify all columns exist in the DataFrame.
        all_cols = [*determinant_columns, dependent]
        for col in all_cols:
            if col not in df.columns:
                return []

        # Drop rows with null values in determinant columns.
        subset = df[all_cols].copy()
        mask = subset[determinant_columns].notna().all(axis=1)
        subset = subset[mask]

        if subset.empty:
            return []

        # Group by determinant and find groups with multiple distinct
        # dependent values.
        issues: list[Issue] = []

        grouped = subset.groupby(determinant_columns, sort=False)
        for group_key, group_df in grouped:
            unique_deps = group_df[dependent].dropna().unique()
            if len(unique_deps) <= 1:
                continue

            # All rows in this group are part of the violation.
            det_desc = self._format_determinant(determinant, group_key)
            unique_str = ", ".join(repr(str(v)) for v in unique_deps)

            for idx in group_df.index:
                actual_val = str(group_df.at[idx, dependent])
                reason = (
                    f"Functional dependency {determinant} -> {dependent} "
                    f"violated: {det_desc} maps to multiple values: "
                    f"{{{unique_str}}}"
                )
                issues.append(
                    Issue(
                        row=int(idx),
                        column=dependent,
                        issue_type="fd_violation",
                        severity=Severity.UNSAFE,
                        confidence=0.95,
                        actual=actual_val,
                        reason=reason,
                    )
                )

        return issues

    @staticmethod
    def _format_determinant(determinant: tuple[str, ...], group_key: object) -> str:
        """Format the determinant key for human-readable output.

        Args:
            determinant: List of determinant column names.
            group_key: The group key (scalar or tuple).

        Returns:
            A formatted string like ``zip_code='10001'``.
        """
        if len(determinant) == 1:
            return f"{determinant[0]}='{group_key}'"

        # Composite key: group_key is a tuple.
        if isinstance(group_key, tuple):
            parts = [f"{col}='{val}'" for col, val in zip(determinant, group_key, strict=True)]
            return ", ".join(parts)

        return f"{determinant}='{group_key}'"
