"""CI wrapper for the full original DataForge vision external gate."""

from __future__ import annotations

import sys

from dataforge.release.full_vision import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
