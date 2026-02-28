"""Backtesting performance metrics."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def calculate_metrics(results: pd.DataFrame) -> dict[str, Any]:
    """Calculate comprehensive backtest metrics from simulation results.

    Args:
        results: DataFrame from BacktestSimulator.run()

    Returns:
        Dict of performance metrics.
    """
    if results.empty:
        return {"error": "No results to analyze"}

    pnl = results["pnl"]
    bankroll = results["bankroll"]

    total_trades = len(results)
    winning_trades = int((pnl > 0).sum())
    losing_trades = int((pnl < 0).sum())

    initial = bankroll.iloc[0] - pnl.iloc[0]  # Initial bankroll before first trade
    final = bankroll.iloc[-1]
    total_pnl = float(pnl.sum())
    roi = total_pnl / initial if initial > 0 else 0

    # Win rate
    hit_rate = winning_trades / total_trades if total_trades > 0 else 0

    # Average P&L
    avg_win = float(pnl[pnl > 0].mean()) if winning_trades > 0 else 0
    avg_loss = float(pnl[pnl < 0].mean()) if losing_trades > 0 else 0

    # Max drawdown
    peak = bankroll.expanding().max()
    drawdown = (bankroll - peak) / peak
    max_drawdown = float(drawdown.min())

    # Sharpe-like ratio (annualized)
    daily_returns = pnl / bankroll.shift(1).fillna(initial)
    if daily_returns.std() > 0:
        sharpe = float(
            daily_returns.mean() / daily_returns.std() * np.sqrt(252)
        )
    else:
        sharpe = 0

    # Profit factor
    gross_profit = float(pnl[pnl > 0].sum()) if winning_trades > 0 else 0
    gross_loss = float(abs(pnl[pnl < 0].sum())) if losing_trades > 0 else 1
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    metrics = {
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "hit_rate": round(hit_rate, 3),
        "total_pnl": round(total_pnl, 2),
        "roi": round(roi, 4),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "max_drawdown": round(max_drawdown, 4),
        "sharpe_ratio": round(sharpe, 2),
        "profit_factor": round(profit_factor, 2),
        "initial_bankroll": round(initial, 2),
        "final_bankroll": round(final, 2),
    }

    log.info("backtest_metrics", **metrics)
    return metrics
