"""Backtest report generation."""

from __future__ import annotations

from typing import Any


def generate_report(metrics: dict[str, Any]) -> str:
    """Generate a human-readable backtest report.

    Args:
        metrics: Dict from calculate_metrics()

    Returns:
        Formatted report string.
    """
    if "error" in metrics:
        return f"Backtest Error: {metrics['error']}"

    lines = [
        "=" * 60,
        "BACKTEST PERFORMANCE REPORT",
        "=" * 60,
        "",
        f"Total Trades:      {metrics.get('total_trades', 0)}",
        f"Winning Trades:    {metrics.get('winning_trades', 0)}",
        f"Losing Trades:     {metrics.get('losing_trades', 0)}",
        f"Hit Rate:          {metrics.get('hit_rate', 0):.1%}",
        "",
        f"Initial Bankroll:  ${metrics.get('initial_bankroll', 0):,.2f}",
        f"Final Bankroll:    ${metrics.get('final_bankroll', 0):,.2f}",
        f"Total P&L:         ${metrics.get('total_pnl', 0):+,.2f}",
        f"ROI:               {metrics.get('roi', 0):.2%}",
        "",
        f"Avg Win:           ${metrics.get('avg_win', 0):,.2f}",
        f"Avg Loss:          ${metrics.get('avg_loss', 0):,.2f}",
        f"Profit Factor:     {metrics.get('profit_factor', 0):.2f}",
        "",
        f"Max Drawdown:      {metrics.get('max_drawdown', 0):.2%}",
        f"Sharpe Ratio:      {metrics.get('sharpe_ratio', 0):.2f}",
        "",
        "=" * 60,
    ]

    return "\n".join(lines)
