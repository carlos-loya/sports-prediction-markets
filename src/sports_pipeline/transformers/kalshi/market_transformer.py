"""Kalshi market data transformer (Bronze -> Silver)."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer


class KalshiMarketTransformer(BaseTransformer):
    """Transform bronze Kalshi market data to silver layer."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_kalshi_markets", rows=len(df))

        silver = pd.DataFrame({
            "snapshot_timestamp": pd.to_datetime(df["snapshot_timestamp"]),
            "ticker": df["ticker"],
            "event_ticker": df["event_ticker"],
            "title": df["title"],
            "sport": None,  # Filled by entity matcher
            "market_type": None,  # Filled by entity matcher
            "matched_entity_id": None,
            "matched_entity_name": None,
            "status": df["status"],
            "yes_price": pd.to_numeric(df["yes_price"], errors="coerce").fillna(0.0),
            "no_price": pd.to_numeric(df["no_price"], errors="coerce").fillna(0.0),
            "volume": pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int),
            "open_interest": pd.to_numeric(
                df["open_interest"], errors="coerce"
            ).fillna(0).astype(int),
            "close_time": pd.to_datetime(df.get("close_time"), errors="coerce"),
        })

        # Derive implied probability (YES price = implied probability)
        silver["implied_probability"] = silver["yes_price"]

        # Derive mid price and spread from bid/ask if available
        yes_bid = pd.to_numeric(df.get("yes_bid"), errors="coerce")
        yes_ask = pd.to_numeric(df.get("yes_ask"), errors="coerce")
        silver["mid_price"] = ((yes_bid + yes_ask) / 2).where(yes_bid.notna() & yes_ask.notna())
        silver["spread"] = (yes_ask - yes_bid).where(yes_bid.notna() & yes_ask.notna())

        self.log.info("transformed_kalshi_markets", rows=len(silver))
        return silver
