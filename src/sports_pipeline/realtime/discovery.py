"""Market discovery service for real-time trading.

Queries the Kalshi REST API for active sports markets, filters by entropy
(price range), and feeds discovered tickers to the WebSocket client.
Runs periodically to pick up new markets.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from sports_pipeline.realtime.config import EntropyConfig, TargetSeriesConfig
from sports_pipeline.realtime.execution.kalshi_rest import AsyncKalshiClient
from sports_pipeline.realtime.websocket.client import KalshiWebSocketClient
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class DiscoveredMarket:
    """Metadata for a discovered tradeable market."""

    ticker: str
    title: str
    yes_price: float
    series_ticker: str


class MarketDiscoveryService:
    """Discovers tradeable sports markets from Kalshi REST API.

    Paginates through configured series tickers, applies an entropy filter
    (YES price within a useful range), and returns market metadata.
    """

    def __init__(
        self,
        client: AsyncKalshiClient,
        target_series: TargetSeriesConfig,
        entropy: EntropyConfig,
    ) -> None:
        self._client = client
        self._series_tickers = target_series.series_tickers
        self._min_price = entropy.min_price
        self._max_price = entropy.max_price

    async def discover(self) -> list[DiscoveredMarket]:
        """Query all target series and return filtered markets."""
        all_markets: list[DiscoveredMarket] = []
        for series in self._series_tickers:
            markets = await self._discover_series(series)
            all_markets.extend(markets)
        log.info(
            "discovery_complete",
            series_count=len(self._series_tickers),
            markets_found=len(all_markets),
        )
        return all_markets

    async def _discover_series(self, series_ticker: str) -> list[DiscoveredMarket]:
        """Paginate through a single series and filter by entropy."""
        markets: list[DiscoveredMarket] = []
        cursor: str | None = None

        while True:
            resp = await self._client.list_markets(
                series_ticker=series_ticker,
                status="active",
                cursor=cursor,
                limit=100,
            )
            for m in resp.get("markets", []):
                yes_price = m.get("yes_price", 0)
                # Kalshi returns price in cents (1-99); normalize to [0,1]
                if isinstance(yes_price, (int, float)) and yes_price > 1:
                    yes_price = yes_price / 100.0

                if self._min_price <= yes_price <= self._max_price:
                    markets.append(
                        DiscoveredMarket(
                            ticker=m["ticker"],
                            title=m.get("title", ""),
                            yes_price=yes_price,
                            series_ticker=series_ticker,
                        )
                    )

            cursor = resp.get("cursor")
            if not cursor:
                break

        log.info(
            "series_discovered",
            series=series_ticker,
            found=len(markets),
        )
        return markets

    async def run_loop(
        self,
        ws_client: KalshiWebSocketClient,
        interval: int = 300,
    ) -> None:
        """Periodically discover markets and update WS subscriptions.

        Args:
            ws_client: WebSocket client to update with new tickers.
            interval: Seconds between discovery runs.
        """
        while True:
            try:
                markets = await self.discover()
                tickers = [m.ticker for m in markets]
                if tickers:
                    await ws_client.update_subscriptions(tickers)
                    log.info("subscriptions_updated", count=len(tickers))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("discovery_loop_error")
            await asyncio.sleep(interval)
