#!/usr/bin/env python
"""Download and extract Jon Becker's prediction-market-analysis dataset.

Downloads ~36GB compressed archive from R2, decompresses with zstandard,
and extracts via tarfile. Idempotent: skips if data already present.

Usage:
    uv run python scripts/download_becker.py [--output data/becker]
"""

from __future__ import annotations

import argparse
import sys
import tarfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

BECKER_URL = "https://s3.jbecker.dev/data.tar.zst"
DEFAULT_OUTPUT = "data/becker"

# Expected directories after extraction
EXPECTED_DIRS = [
    "kalshi/trades",
    "kalshi/markets",
]


def _check_existing(output_dir: Path) -> bool:
    """Return True if the expected directory structure already exists."""
    for subdir in EXPECTED_DIRS:
        if not (output_dir / subdir).is_dir():
            return False
    return True


def _download_and_extract(url: str, output_dir: Path) -> None:
    """Stream download, decompress zstd, and extract tar in one pass."""
    import requests
    import zstandard as zstd

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {url} ...")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    dctx = zstd.ZstdDecompressor()

    downloaded = 0
    with dctx.stream_reader(resp.raw) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            for member in tar:
                # Strip leading directory (e.g., "data/") so we control output root
                parts = Path(member.name).parts
                if len(parts) > 1:
                    member.name = str(Path(*parts[1:]))
                else:
                    continue

                tar.extract(member, path=output_dir, filter="data")

                downloaded += member.size
                if total > 0:
                    pct = min(downloaded / total * 100, 100)
                    gb = downloaded / 1e9
                    print(f"\r  Extracted: {gb:.1f} GB ({pct:.0f}%)", end="", flush=True)
                else:
                    print(f"\r  Extracted: {downloaded / 1e9:.1f} GB", end="", flush=True)

    print()  # newline after progress


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Becker prediction market dataset")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--url",
        default=BECKER_URL,
        help="Override download URL",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if data exists",
    )
    args = parser.parse_args()

    output_dir = Path(args.output)

    if not args.force and _check_existing(output_dir):
        print(f"Dataset already present at {output_dir}. Use --force to re-download.")
        return

    _download_and_extract(args.url, output_dir)

    # Verify
    if _check_existing(output_dir):
        print(f"Dataset extracted successfully to {output_dir}")
        for subdir in EXPECTED_DIRS:
            count = len(list((output_dir / subdir).glob("*.parquet")))
            print(f"  {subdir}: {count} parquet files")
    else:
        print("WARNING: Expected directory structure not found after extraction.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
