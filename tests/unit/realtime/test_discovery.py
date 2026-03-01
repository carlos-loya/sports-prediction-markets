"""Tests for MarketDiscoveryService."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from sports_pipeline.realtime.config import EntropyConfig, TargetSeriesConfig
from sports_pipeline.realtime.discovery import MarketDiscoveryService


def _make_service(
    series: list[str] | None = None,
    min_price: float = 0.30,
    max_price: float = 0.70,
) -> tuple[MarketDiscoveryService, AsyncMock]:
    client = AsyncMock()
    target = TargetSeriesConfig(series_tickers=series or ["KXNBA"])
    entropy = EntropyConfig(min_price=min_price, max_price=max_price)
    svc = MarketDiscoveryService(client=client, target_series=target, entropy=entropy)
    return svc, client


class TestMarketDiscoveryService:
    @pytest.mark.asyncio
    async def test_discover_filters_by_entropy(self) -> None:
        svc, client = _make_service()
        client.list_markets.return_value = {
            "markets": [
                {"ticker": "T1", "title": "M1", "yes_price": 50},  # 0.50 → pass
                {"ticker": "T2", "title": "M2", "yes_price": 10},  # 0.10 → reject
                {"ticker": "T3", "title": "M3", "yes_price": 95},  # 0.95 → reject
                {"ticker": "T4", "title": "M4", "yes_price": 45},  # 0.45 → pass
            ],
            "cursor": "",
        }

        result = await svc.discover()
        assert len(result) == 2
        assert result[0].ticker == "T1"
        assert result[1].ticker == "T4"

    @pytest.mark.asyncio
    async def test_discover_handles_float_prices(self) -> None:
        svc, client = _make_service()
        client.list_markets.return_value = {
            "markets": [
                {"ticker": "T1", "title": "M1", "yes_price": 0.50},  # already [0,1]
            ],
            "cursor": "",
        }

        result = await svc.discover()
        assert len(result) == 1
        assert result[0].yes_price == 0.50

    @pytest.mark.asyncio
    async def test_discover_paginates(self) -> None:
        svc, client = _make_service()
        client.list_markets.side_effect = [
            {
                "markets": [
                    {"ticker": "T1", "title": "M1", "yes_price": 50},
                ],
                "cursor": "page2",
            },
            {
                "markets": [
                    {"ticker": "T2", "title": "M2", "yes_price": 40},
                ],
                "cursor": "",
            },
        ]

        result = await svc.discover()
        assert len(result) == 2
        assert client.list_markets.call_count == 2

    @pytest.mark.asyncio
    async def test_discover_multiple_series(self) -> None:
        svc, client = _make_service(series=["KXNBA", "KXNFL"])
        client.list_markets.return_value = {
            "markets": [
                {"ticker": "T1", "title": "M1", "yes_price": 50},
            ],
            "cursor": "",
        }

        result = await svc.discover()
        assert len(result) == 2  # one per series
        assert result[0].series_ticker == "KXNBA"
        assert result[1].series_ticker == "KXNFL"

    @pytest.mark.asyncio
    async def test_discover_empty_response(self) -> None:
        svc, client = _make_service()
        client.list_markets.return_value = {"markets": [], "cursor": ""}

        result = await svc.discover()
        assert result == []

    @pytest.mark.asyncio
    async def test_discovered_market_fields(self) -> None:
        svc, client = _make_service()
        client.list_markets.return_value = {
            "markets": [
                {"ticker": "NBA-T1", "title": "Will the Lakers beat the Celtics?", "yes_price": 55},
            ],
            "cursor": "",
        }

        result = await svc.discover()
        assert len(result) == 1
        m = result[0]
        assert m.ticker == "NBA-T1"
        assert m.title == "Will the Lakers beat the Celtics?"
        assert m.yes_price == pytest.approx(0.55)
        assert m.series_ticker == "KXNBA"

    @pytest.mark.asyncio
    async def test_run_loop_updates_subscriptions(self) -> None:
        svc, client = _make_service()
        client.list_markets.return_value = {
            "markets": [
                {"ticker": "T1", "title": "M1", "yes_price": 50},
            ],
            "cursor": "",
        }
        ws_client = AsyncMock()

        # Run one iteration then cancel
        async def run_once() -> None:
            task = asyncio.ensure_future(svc.run_loop(ws_client, interval=3600))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        import asyncio

        await run_once()
        ws_client.update_subscriptions.assert_called_once_with(["T1"])
