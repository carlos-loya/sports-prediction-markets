#!/usr/bin/env python
"""Run backtests on historical edge detection performance."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sports_pipeline.utils.logging import setup_logging, get_logger

log = get_logger(__name__)

if __name__ == "__main__":
    setup_logging()
    log.info("backtest_started")

    from sports_pipeline.backtesting.simulator import BacktestSimulator
    from sports_pipeline.backtesting.metrics import calculate_metrics
    from sports_pipeline.backtesting.reports import generate_report

    simulator = BacktestSimulator()
    results = simulator.run()
    metrics = calculate_metrics(results)
    report = generate_report(metrics)

    print(report)
    log.info("backtest_complete")
