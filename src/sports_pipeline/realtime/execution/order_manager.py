"""Order manager: placement, tracking, cancellation.

Consumes order requests, performs pre-trade risk checks, places orders
via the async REST client, and tracks active orders and positions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sports_pipeline.realtime.config import RiskConfig
from sports_pipeline.realtime.events import FillEvent, OrderRequestEvent, RiskAlertEvent
from sports_pipeline.realtime.execution.kalshi_rest import AsyncKalshiClient
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class ActiveOrder:
    """An order we've placed that hasn't been fully filled or cancelled."""

    order_id: str
    ticker: str
    side: str
    action: str
    price: float
    count: int
    remaining: int
    placed_at: float = field(default_factory=time.monotonic)


@dataclass
class Position:
    """Current position in a market."""

    ticker: str
    yes_count: int = 0
    no_count: int = 0
    cost_basis: float = 0.0
    realized_pnl: float = 0.0

    @property
    def net_exposure(self) -> float:
        """Net dollar exposure (simplified)."""
        return abs(self.yes_count - self.no_count)


class OrderManager:
    """Manages order lifecycle: placement, fill tracking, cancellation."""

    def __init__(
        self,
        client: AsyncKalshiClient,
        risk_config: RiskConfig,
    ) -> None:
        self._client = client
        self._risk_config = risk_config
        self._active_orders: dict[str, ActiveOrder] = {}
        self._positions: dict[str, Position] = {}
        self._daily_pnl: float = 0.0
        self._orders_placed: int = 0
        self._orders_filled: int = 0
        self._orders_cancelled: int = 0

    @property
    def active_order_count(self) -> int:
        return len(self._active_orders)

    @property
    def positions(self) -> dict[str, Position]:
        return self._positions.copy()

    @property
    def daily_pnl(self) -> float:
        return self._daily_pnl

    async def handle_order_request(self, event: OrderRequestEvent) -> str | None:
        """Process an order request. Returns order_id if placed, None if rejected.

        Performs pre-trade risk checks before placement.
        """
        # Pre-trade risk check
        rejection = self._pre_trade_check(event)
        if rejection:
            log.warning(
                "order_rejected",
                ticker=event.ticker,
                reason=rejection,
            )
            return None

        # Compute GTD expiry
        expiry_ts = None
        if event.expiration_ts:
            expiry_ts = int(event.expiration_ts.timestamp())
        elif self._risk_config.gtd_expiry_minutes > 0:
            expiry = datetime.utcnow() + timedelta(
                minutes=self._risk_config.gtd_expiry_minutes
            )
            expiry_ts = int(expiry.timestamp())

        try:
            result = await self._client.place_order(
                ticker=event.ticker,
                side=event.side,
                action=event.action,
                count=event.count,
                price=int(event.price),
                expiration_ts=expiry_ts,
            )
            order_id = result.get("order", {}).get("order_id", "")
            if order_id:
                self._active_orders[order_id] = ActiveOrder(
                    order_id=order_id,
                    ticker=event.ticker,
                    side=event.side,
                    action=event.action,
                    price=event.price,
                    count=event.count,
                    remaining=event.count,
                )
                self._orders_placed += 1
                log.info(
                    "order_placed",
                    order_id=order_id,
                    ticker=event.ticker,
                    side=event.side,
                    count=event.count,
                    price=event.price,
                )
            return order_id
        except Exception:
            log.exception("order_placement_failed", ticker=event.ticker)
            return None

    def handle_fill(self, event: FillEvent) -> None:
        """Update state on fill event."""
        # Update active order
        order = self._active_orders.get(event.order_id)
        if order:
            order.remaining = event.remaining_count
            if event.remaining_count == 0:
                del self._active_orders[event.order_id]
                self._orders_filled += 1

        # Update position
        pos = self._positions.setdefault(
            event.ticker, Position(ticker=event.ticker)
        )
        if event.side == "yes":
            if event.action == "buy":
                pos.yes_count += event.count
                pos.cost_basis += event.count * event.price
            else:
                pos.yes_count -= event.count
                total = pos.yes_count + event.count
                avg_cost = pos.cost_basis / max(1, total)
                pos.realized_pnl += event.count * (event.price - avg_cost)
        else:
            if event.action == "buy":
                pos.no_count += event.count
            else:
                pos.no_count -= event.count

        log.info(
            "fill_processed",
            order_id=event.order_id,
            ticker=event.ticker,
            side=event.side,
            count=event.count,
        )

    async def handle_risk_alert(self, event: RiskAlertEvent) -> None:
        """Handle a risk alert by cancelling orders."""
        if event.action == "cancel_ticker" and event.ticker:
            await self.cancel_orders_for_ticker(event.ticker)
        elif event.action in ("cancel_all", "shutdown"):
            await self.cancel_all_orders()

    async def cancel_orders_for_ticker(self, ticker: str) -> int:
        """Cancel all active orders for a specific ticker."""
        cancelled = 0
        for order_id, order in list(self._active_orders.items()):
            if order.ticker == ticker:
                try:
                    await self._client.cancel_order(order_id)
                    del self._active_orders[order_id]
                    cancelled += 1
                    self._orders_cancelled += 1
                except Exception:
                    log.exception("cancel_failed", order_id=order_id)
        return cancelled

    async def cancel_all_orders(self) -> int:
        """Cancel all active orders."""
        try:
            await self._client.cancel_all_orders()
            count = len(self._active_orders)
            self._orders_cancelled += count
            self._active_orders.clear()
            log.info("all_orders_cancelled", count=count)
            return count
        except Exception:
            log.exception("cancel_all_failed")
            return 0

    def _pre_trade_check(self, event: OrderRequestEvent) -> str | None:
        """Run pre-trade risk checks. Returns rejection reason or None."""
        # Position limit check
        pos = self._positions.get(event.ticker, Position(ticker=event.ticker))
        current_pos = pos.yes_count if event.side == "yes" else pos.no_count
        if current_pos + event.count > self._risk_config.max_position_per_market:
            return "position_limit"

        # Total exposure check
        total_exposure = sum(p.net_exposure for p in self._positions.values())
        if total_exposure + event.count > self._risk_config.max_total_exposure:
            return "exposure_limit"

        # Daily loss check
        if self._daily_pnl < -self._risk_config.daily_loss_limit:
            return "daily_loss_limit"

        return None
