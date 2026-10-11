"""Run the MarketPulse mock-data CLI with ``python -m marketpulse_mock``."""

from __future__ import annotations

import sys

from .cli import main


if __name__ == "__main__":
    sys.exit(main())
