"""Kalshi historical/candlestick data extractor."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.kalshi.client import KalshiClient


class KalshiHistoricalExtractor(BaseExtractor):
    """Extract historical price data from Kalshi for backtesting."""

    def __init__(self, client: KalshiClient | None = None) -> None:
        super().__init__()
        self.client = client or KalshiClient()

    def extract(self, ticker: str, series_ticker: str, period_interval: int = 60) -> pd.DataFrame:
        """Extract candlestick data for a market.

        Args:
            ticker: Market ticker
            series_ticker: Series ticker for the market
            period_interval: Candlestick period in minutes (default 60)

        Returns:
            DataFrame of candlestick data.
        """
        self.log.info("extracting_candlesticks", ticker=ticker, period=period_interval)
        raw = self.client.get_market_candlesticks(
            ticker=ticker,
            series_ticker=series_ticker,
            period_interval=period_interval,
        )

        if not raw:
            self.log.warning("no_candlesticks_found", ticker=ticker)
            return pd.DataFrame()

        records = []
        for candle in raw:
            if isinstance(candle, dict):
                c = candle
            elif hasattr(candle, "to_dict"):
                c = candle.to_dict()
            else:
                c = vars(candle)
            records.append({
                "ticker": ticker,
                "timestamp": c.get("end_period_ts") or c.get("timestamp"),
                "open_price": float(c.get("open", 0)) / 100,
                "high_price": float(c.get("high", 0)) / 100,
                "low_price": float(c.get("low", 0)) / 100,
                "close_price": float(c.get("close", 0)) / 100,
                "volume": int(c.get("volume", 0)),
            })

        df = pd.DataFrame(records)
        self.log.info("extracted_candlesticks", ticker=ticker, count=len(df))
        return df
