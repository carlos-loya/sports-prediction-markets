"""Kalshi market data extractor."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.kalshi.client import KalshiClient
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SPORTS_CATEGORIES = {"Sports", "NBA", "Soccer", "NFL", "MLB", "NHL"}


class KalshiMarketExtractor(BaseExtractor):
    """Extract active sports markets from Kalshi."""

    def __init__(self, client: KalshiClient | None = None) -> None:
        super().__init__()
        self.client = client or KalshiClient()

    def extract(self, status: str = "active") -> pd.DataFrame:
        """Extract all active sports markets.

        Paginates through the Kalshi API to get all sports markets.

        Returns:
            DataFrame of bronze-level market data.
        """
        self.log.info("extracting_kalshi_markets", status=status)
        all_markets: list[dict[str, Any]] = []
        cursor = None

        while True:
            result = self.client.get_markets(cursor=cursor, status=status, limit=200)
            markets = result["markets"]

            for market in markets:
                market_dict = self._market_to_dict(market)
                if market_dict and self._is_sports_market(market_dict):
                    all_markets.append(market_dict)

            cursor = result.get("cursor")
            if not cursor or not markets:
                break

        if not all_markets:
            self.log.warning("no_sports_markets_found")
            return pd.DataFrame()

        df = pd.DataFrame(all_markets)
        self.log.info("extracted_kalshi_markets", count=len(df))
        return df

    def extract_orderbooks(self, tickers: list[str]) -> pd.DataFrame:
        """Extract order books for given market tickers."""
        self.log.info("extracting_orderbooks", count=len(tickers))
        records = []

        for ticker in tickers:
            try:
                ob = self.client.get_market_orderbook(ticker)
                records.append({
                    "snapshot_timestamp": datetime.utcnow(),
                    "ticker": ticker,
                    "yes_bids": getattr(ob, "yes", []) if hasattr(ob, "yes") else [],
                    "yes_asks": getattr(ob, "no", []) if hasattr(ob, "no") else [],
                })
            except Exception:
                self.log.warning("orderbook_fetch_failed", ticker=ticker, exc_info=True)

        return pd.DataFrame(records) if records else pd.DataFrame()

    def _market_to_dict(self, market: Any) -> dict[str, Any] | None:
        """Convert SDK market object to dict."""
        try:
            if isinstance(market, dict):
                m = market
            else:
                m = market.to_dict() if hasattr(market, "to_dict") else vars(market)

            return {
                "snapshot_timestamp": datetime.utcnow(),
                "ticker": m.get("ticker", ""),
                "event_ticker": m.get("event_ticker", ""),
                "title": m.get("title", ""),
                "category": m.get("category", ""),
                "sub_category": m.get("sub_title", "") or m.get("subtitle", ""),
                "status": m.get("status", ""),
                "yes_price": float(m.get("yes_bid", 0) or 0) / 100 if m.get("yes_bid") else 0.0,
                "no_price": float(m.get("no_bid", 0) or 0) / 100 if m.get("no_bid") else 0.0,
                "yes_bid": float(m.get("yes_bid", 0) or 0) / 100 if m.get("yes_bid") else None,
                "yes_ask": float(m.get("yes_ask", 0) or 0) / 100 if m.get("yes_ask") else None,
                "no_bid": float(m.get("no_bid", 0) or 0) / 100 if m.get("no_bid") else None,
                "no_ask": float(m.get("no_ask", 0) or 0) / 100 if m.get("no_ask") else None,
                "volume": int(m.get("volume", 0) or 0),
                "open_interest": int(m.get("open_interest", 0) or 0),
                "close_time": m.get("close_time") or m.get("expiration_time"),
                "result": m.get("result"),
            }
        except Exception:
            self.log.warning("market_parse_failed", exc_info=True)
            return None

    @staticmethod
    def _is_sports_market(market_dict: dict[str, Any]) -> bool:
        """Check if a market is sports-related based on category or ticker."""
        category = (market_dict.get("category") or "").strip()
        ticker = market_dict.get("ticker", "")

        if category in SPORTS_CATEGORIES:
            return True

        sports_prefixes = ("KXNBA", "KXSOC", "KXNFL", "KXMLB", "KXNHL")
        return any(ticker.startswith(prefix) for prefix in sports_prefixes)
