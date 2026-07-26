"""Review-queue ranking: score detected cells for human-review ordering.

This subpackage never mutates data or proposes values; it only ranks flagged
cells so a human reviews likely-true errors first. See ``ranker.ReviewRanker``.
"""

from dataforge.review.ranker import CellScore, ReviewRanker

__all__ = ["CellScore", "ReviewRanker"]
