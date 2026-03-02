#!/usr/bin/env python
"""Run historical backtest against Becker's prediction market dataset.

Orchestrates: Becker views → trade replay → metrics → calibration → report.

Usage:
    uv run python scripts/run_becker_backtest.py [--sport nba] [--max-markets 100]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sports_pipeline.utils.logging import get_logger, setup_logging

log = get_logger(__name__)

# Map CLI sport names to Kalshi ticker prefixes
SPORT_PREFIX_MAP = {
    "nba": "KXNBA",
    "nfl": "KXNFL",
    "mlb": "KXMLB",
    "nhl": "KXNHL",
    "soccer": "KXSOC",
    "mma": "KXMMA",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest against Becker dataset")
    parser.add_argument(
        "--sport",
        choices=list(SPORT_PREFIX_MAP.keys()),
        default=None,
        help="Filter to a specific sport",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Maximum number of markets to replay",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date filter (ISO format)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date filter (ISO format)",
    )
    parser.add_argument(
        "--becker-path",
        default=None,
        help="Override Becker data path",
    )
    args = parser.parse_args()

    setup_logging()

    from sports_pipeline.backtesting.calibration import (
        edge_calibration,
        generate_calibration_report,
        model_uncertainty,
        optimal_thresholds,
    )
    from sports_pipeline.backtesting.metrics import calculate_metrics
    from sports_pipeline.backtesting.replayer import TradeStreamReplayer
    from sports_pipeline.config import PROJECT_ROOT, get_settings
    from sports_pipeline.loaders.becker_views import create_becker_views
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader

    settings = get_settings()
    becker_path = args.becker_path or str(PROJECT_ROOT / settings.storage.becker_data_path)

    if not Path(becker_path).is_dir():
        print(f"Becker data not found at {becker_path}")
        print("Run: make download-becker")
        sys.exit(1)

    log.info("becker_backtest_started", becker_path=becker_path)

    # 1. Create views
    loader = DuckDBLoader()
    create_becker_views(loader, becker_path)

    # 2. Run replay
    sport_prefix = SPORT_PREFIX_MAP.get(args.sport) if args.sport else None
    replayer = TradeStreamReplayer(config=settings.realtime, loader=loader)
    results = replayer.replay(
        sport_prefix=sport_prefix,
        max_markets=args.max_markets,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if results.empty:
        print("No results. Check that settled markets exist in the dataset.")
        sys.exit(0)

    # 3. Compute metrics
    metrics = calculate_metrics(results)
    print("\n--- Performance Metrics ---")
    for key, value in metrics.items():
        print(f"  {key}: {value}")

    # 4. Calibration analysis
    edge_cal = edge_calibration(results)
    uncertainty = model_uncertainty(results)
    thresholds = optimal_thresholds(results)

    report = generate_calibration_report(results, edge_cal, uncertainty, thresholds)
    print(f"\n{report}")

    # 5. Save results to DuckDB
    loader.load_dataframe(results, "becker_backtest_results", mode="replace")
    log.info("becker_backtest_complete", total_trades=len(results))


if __name__ == "__main__":
    main()
