"""Backtest simulator for historical edge detection performance."""

from __future__ import annotations

from typing import Any

import pandas as pd

from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class BacktestSimulator:
    """Simulate historical edge detection and P&L."""

    def __init__(
        self,
        initial_bankroll: float = 10000.0,
        loader: DuckDBLoader | None = None,
    ) -> None:
        self.initial_bankroll = initial_bankroll
        self.loader = loader or DuckDBLoader()

    def run(self, edges: list[dict[str, Any]] | None = None) -> pd.DataFrame:
        """Run backtest simulation.

        Args:
            edges: List of edge signal dicts with 'actual_outcome' filled in.
                  If None, loads from DuckDB resolved edges.

        Returns:
            DataFrame with per-signal P&L results.
        """
        log.info("running_backtest")

        if edges is None:
            try:
                df = self.loader.query("""
                    SELECT * FROM gold.edge_signals
                    WHERE resolved = TRUE AND actual_outcome IS NOT NULL
                    ORDER BY timestamp
                """)
                edges = df.to_dict("records")
            except Exception:
                log.warning("no_resolved_edges")
                return pd.DataFrame()

        if not edges:
            return pd.DataFrame()

        bankroll = self.initial_bankroll
        results = []

        for edge in edges:
            kelly = edge.get("kelly_fraction", 0)
            bet_amount = bankroll * kelly
            side = edge.get("suggested_side", "YES")
            outcome = edge.get("actual_outcome")
            market_price = edge.get("kalshi_implied_prob", 0.5)

            if outcome is None or bet_amount <= 0:
                continue

            # Calculate P&L
            if side == "YES":
                if outcome:
                    pnl = bet_amount * (1.0 / market_price - 1)
                else:
                    pnl = -bet_amount
            else:
                no_price = 1 - market_price
                if not outcome:
                    pnl = bet_amount * (1.0 / no_price - 1) if no_price > 0 else 0
                else:
                    pnl = -bet_amount

            bankroll += pnl

            results.append({
                "timestamp": edge.get("timestamp"),
                "ticker": edge.get("kalshi_ticker"),
                "sport": edge.get("sport"),
                "market_type": edge.get("market_type"),
                "model_name": edge.get("model_name"),
                "edge": edge.get("edge"),
                "kelly_fraction": kelly,
                "bet_amount": round(bet_amount, 2),
                "pnl": round(pnl, 2),
                "bankroll": round(bankroll, 2),
                "won": pnl > 0,
            })

        result_df = pd.DataFrame(results)
        log.info("backtest_complete", trades=len(results), final_bankroll=round(bankroll, 2))
        return result_df
