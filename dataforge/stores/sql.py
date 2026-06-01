"""SQL rendering helpers for DataForge table-store adapters."""

from __future__ import annotations

import re

_SAFE_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*|\.\"[^\"]+\")*$")
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ensure_safe_relation(relation: str) -> str:
    """Return a validated relation identifier or raise ``ValueError``."""
    if not _SAFE_RELATION_RE.fullmatch(relation):
        raise ValueError(f"Unsafe relation identifier: {relation}")
    return relation


def quote_identifier(identifier: str) -> str:
    """Return a double-quoted SQL identifier after strict validation."""
    if not _SAFE_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"Unsafe column identifier: {identifier}")
    return f'"{identifier}"'


def sql_literal(value: object) -> str:
    """Render a SQL string literal for generated patch statements."""
    return "'" + str(value).replace("'", "''") + "'"
