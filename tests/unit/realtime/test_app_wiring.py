"""Tests for app.py wiring: edge→order bridge and handler logic."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from sports_pipeline.realtime.app import edge_to_order_request
from sports_pipeline.realtime.events import EdgeEvent, OrderRequestEvent
from sports_pipeline.realtime.execution.order_manager import OrderManager


def _make_edge(
    ticker: str = "T1",
    model_prob: float = 0.65,
    market_prob: float = 0.50,
    raw_edge: float = 0.15,
    tradable_edge: float = 0.07,
    kelly_fraction: float = 0.10,
    suggested_side: str = "yes",
    confidence: str = "high",
    rejected: bool = False,
    reject_reason: str = "",
) -> EdgeEvent:
    return EdgeEvent(
        ticker=ticker,
        model_prob=model_prob,
        market_prob=market_prob,
        raw_edge=raw_edge,
        tradable_edge=tradable_edge,
        kelly_fraction=kelly_fraction,
        suggested_side=suggested_side,
        confidence=confidence,
        rejected=rejected,
        reject_reason=reject_reason,
    )


class TestEdgeToOrderRequest:
    def test_basic_yes_order(self) -> None:
        edge = _make_edge()
        order = edge_to_order_request(edge, bankroll=10000.0)

        assert order is not None
        assert order.ticker == "T1"
        assert order.side == "yes"
        assert order.action == "buy"
        assert order.price == 50.0  # market_prob * 100
        assert order.count == 20  # floor(0.10 * 10000 / 50)
        assert order.source == "edge_processor"

    def test_no_side_order(self) -> None:
        edge = _make_edge(
            suggested_side="no",
            market_prob=0.60,
            kelly_fraction=0.08,
        )
        order = edge_to_order_request(edge, bankroll=10000.0)

        assert order is not None
        assert order.side == "no"
        assert order.price == 40.0  # (1 - 0.60) * 100

    def test_rejected_edge_returns_none(self) -> None:
        edge = _make_edge(rejected=True, reject_reason="no_model")
        order = edge_to_order_request(edge, bankroll=10000.0)
        assert order is None

    def test_zero_kelly_returns_none(self) -> None:
        edge = _make_edge(kelly_fraction=0.0)
        order = edge_to_order_request(edge, bankroll=10000.0)
        assert order is None

    def test_tiny_kelly_returns_none(self) -> None:
        """When kelly * bankroll / price < 1, returns None."""
        edge = _make_edge(kelly_fraction=0.001, market_prob=0.99)
        order = edge_to_order_request(edge, bankroll=100.0)
        # 0.001 * 100 / 99 = 0.001 → floor = 0
        assert order is None

    def test_price_clamped(self) -> None:
        # Edge case: market_prob = 0.005 → price_cents = 1 (clamped)
        edge = _make_edge(market_prob=0.005, kelly_fraction=0.10)
        order = edge_to_order_request(edge, bankroll=10000.0)

        assert order is not None
        assert order.price >= 1
        assert order.price <= 99

    def test_count_floor(self) -> None:
        edge = _make_edge(kelly_fraction=0.07, market_prob=0.50)
        order = edge_to_order_request(edge, bankroll=10000.0)

        # floor(0.07 * 10000 / 50) = floor(14.0) = 14
        assert order is not None
        assert order.count == 14


class TestOrderManagerPaperMode:
    @pytest.mark.asyncio
    async def test_paper_mode_logs_and_returns_id(self) -> None:
        client = AsyncMock()
        om = OrderManager(
            client=client,
            risk_config=MagicMock(
                max_position_per_market=100,
                max_total_exposure=5000.0,
                daily_loss_limit=500.0,
                gtd_expiry_minutes=10,
            ),
            paper_mode=True,
        )
        event = OrderRequestEvent(
            ticker="T1",
            side="yes",
            action="buy",
            price=50.0,
            count=10,
            source="edge_processor",
        )

        order_id = await om.handle_order_request(event)

        assert order_id is not None
        assert order_id.startswith("paper-")
        # REST client should NOT have been called
        client.place_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_paper_mode_tracks_placement_count(self) -> None:
        client = AsyncMock()
        om = OrderManager(
            client=client,
            risk_config=MagicMock(
                max_position_per_market=100,
                max_total_exposure=5000.0,
                daily_loss_limit=500.0,
                gtd_expiry_minutes=10,
            ),
            paper_mode=True,
        )
        event = OrderRequestEvent(
            ticker="T1",
            side="yes",
            action="buy",
            price=50.0,
            count=10,
            source="edge_processor",
        )

        await om.handle_order_request(event)
        await om.handle_order_request(event)

        assert om._orders_placed == 2

    @pytest.mark.asyncio
    async def test_live_mode_calls_rest(self) -> None:
        client = AsyncMock()
        client.place_order.return_value = {
            "order": {"order_id": "live-123"}
        }
        om = OrderManager(
            client=client,
            risk_config=MagicMock(
                max_position_per_market=100,
                max_total_exposure=5000.0,
                daily_loss_limit=500.0,
                gtd_expiry_minutes=10,
            ),
            paper_mode=False,
        )
        event = OrderRequestEvent(
            ticker="T1",
            side="yes",
            action="buy",
            price=50.0,
            count=10,
            source="edge_processor",
        )

        order_id = await om.handle_order_request(event)

        assert order_id == "live-123"
        client.place_order.assert_called_once()
