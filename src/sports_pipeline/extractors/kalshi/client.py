"""Kalshi API client using the kalshi-python SDK."""

from __future__ import annotations

from typing import Any

from kalshi_python.api import EventsApi, MarketsApi
from kalshi_python.api_client import ApiClient
from kalshi_python.configuration import Configuration
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from sports_pipeline.config import get_settings
from sports_pipeline.constants import KALSHI_BASE_URL
from sports_pipeline.utils.logging import get_logger
from sports_pipeline.utils.rate_limiter import TokenBucketRateLimiter

log = get_logger(__name__)


class KalshiClient:
    """Wrapper around the Kalshi API SDK with rate limiting."""

    def __init__(self) -> None:
        settings = get_settings()
        self._limiter = TokenBucketRateLimiter(
            rate=settings.rate_limits.kalshi.reads_per_second, per=1.0
        )

        config = Configuration()
        config.host = KALSHI_BASE_URL

        self._api_client = ApiClient(configuration=config)

        if settings.kalshi_api_key_id and settings.kalshi_private_key_path:
            self._authenticate(settings.kalshi_api_key_id, settings.kalshi_private_key_path)

        self._market_api = MarketsApi(self._api_client)
        self._events_api = EventsApi(self._api_client)

    def _authenticate(self, api_key_id: str, private_key_path: str) -> None:
        """Set up API key authentication."""
        self._api_client.configuration.api_key["bearer"] = api_key_id
        self._api_client.configuration.api_key_prefix["bearer"] = "Bearer"
        log.info("kalshi_authenticated", api_key_id=api_key_id[:8] + "...")

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_markets(
        self,
        cursor: str | None = None,
        limit: int = 200,
        status: str = "active",
        series_ticker: str | None = None,
        event_ticker: str | None = None,
    ) -> dict[str, Any]:
        """Fetch markets from Kalshi API.

        Returns dict with 'markets' list and 'cursor' for pagination.
        """
        self._limiter.acquire()
        log.debug("fetching_markets", status=status, limit=limit)

        kwargs: dict[str, Any] = {"limit": limit, "status": status}
        if cursor:
            kwargs["cursor"] = cursor
        if series_ticker:
            kwargs["series_ticker"] = series_ticker
        if event_ticker:
            kwargs["event_ticker"] = event_ticker

        response = self._market_api.get_markets(**kwargs)
        return {"markets": response.markets or [], "cursor": response.cursor}

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_market(self, ticker: str) -> dict[str, Any]:
        """Fetch a single market by ticker."""
        self._limiter.acquire()
        log.debug("fetching_market", ticker=ticker)
        response = self._market_api.get_market(ticker)
        return response.market

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_market_orderbook(self, ticker: str) -> dict[str, Any]:
        """Fetch order book for a market."""
        self._limiter.acquire()
        log.debug("fetching_orderbook", ticker=ticker)
        response = self._market_api.get_market_orderbook(ticker)
        return response.orderbook

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_market_candlesticks(
        self,
        ticker: str,
        series_ticker: str,
        period_interval: int = 60,
    ) -> list[dict[str, Any]]:
        """Fetch candlestick/historical price data for a market."""
        self._limiter.acquire()
        log.debug("fetching_candlesticks", ticker=ticker)
        response = self._market_api.get_market_candlesticks(
            series_ticker=series_ticker,
            ticker=ticker,
            period_interval=period_interval,
        )
        return response.candlesticks or []

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_events(
        self,
        cursor: str | None = None,
        limit: int = 200,
        status: str = "active",
        series_ticker: str | None = None,
    ) -> dict[str, Any]:
        """Fetch events from Kalshi API."""
        self._limiter.acquire()
        kwargs: dict[str, Any] = {"limit": limit, "status": status}
        if cursor:
            kwargs["cursor"] = cursor
        if series_ticker:
            kwargs["series_ticker"] = series_ticker
        response = self._events_api.get_events(**kwargs)
        return {"events": response.events or [], "cursor": response.cursor}
