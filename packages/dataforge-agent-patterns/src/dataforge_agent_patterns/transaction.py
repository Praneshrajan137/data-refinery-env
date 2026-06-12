"""Reversible transaction decorator for side-effecting calls."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import ParamSpec, TypeVar

__all__ = ["ReversibleTransaction", "TransactionRecord"]

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class TransactionRecord:
    """Recorded reversible transaction.

    Args:
        name: Transaction name.
        result: Function result.
    """

    name: str
    result: object


class ReversibleTransaction:
    """Decorate side-effect calls that return an undo callback.

    Wrapped functions must return `(result, undo)`, where `undo` is a callable
    that reverses the side effect.

    Example:
        >>> state = []
        >>> tx = ReversibleTransaction()
        >>> @tx.wrap("append")
        ... def append_item(value: str):
        ...     state.append(value)
        ...     return value, lambda: state.pop()
        >>> append_item("x")
        'x'
        >>> tx.rollback_last()
        >>> state
        []
    """

    def __init__(self) -> None:
        self._undo_stack: list[Callable[[], None]] = []
        self._records: list[TransactionRecord] = []

    @property
    def records(self) -> tuple[TransactionRecord, ...]:
        """Return committed transaction records."""
        return tuple(self._records)

    def wrap(
        self, name: str
    ) -> Callable[[Callable[P, tuple[R, Callable[[], None]]]], Callable[P, R]]:
        """Return a decorator that records the undo callback.

        Args:
            name: Transaction name.

        Returns:
            Decorator for functions returning `(result, undo)`.
        """

        def decorator(function: Callable[P, tuple[R, Callable[[], None]]]) -> Callable[P, R]:
            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
                result, undo = function(*args, **kwargs)
                self._undo_stack.append(undo)
                self._records.append(TransactionRecord(name=name, result=result))
                return result

            return wrapped

        return decorator

    def rollback_last(self) -> None:
        """Rollback the most recent transaction.

        Raises:
            IndexError: If no transaction is available.
        """
        undo = self._undo_stack.pop()
        self._records.pop()
        undo()

    def rollback_all(self) -> None:
        """Rollback all transactions in reverse order."""
        while self._undo_stack:
            self.rollback_last()
