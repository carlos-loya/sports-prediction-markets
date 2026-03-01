"""Entry script for the real-time edge detection system.

Usage:
    uv run python scripts/run_realtime.py
    # or
    make run-rt
"""

from __future__ import annotations

import asyncio

from sports_pipeline.realtime.app import main

if __name__ == "__main__":
    asyncio.run(main())
