# Copyright (c) 2026 Data Quality Environment Project
# SPDX-License-Identifier: MIT

"""Data Quality Environment — server package.

Exports the ``DataQualityEnvironment`` class for use by ``app.py``
and external consumers.  The old scaffold echo environment has been
superseded by the real implementation in ``data_quality_environment.py``.
"""

from .data_quality_environment import DataQualityEnvironment

__all__ = ["DataQualityEnvironment"]
