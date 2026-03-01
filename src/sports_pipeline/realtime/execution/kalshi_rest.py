"""Async Kalshi REST client for order management.

Uses aiohttp for non-blocking HTTP requests to the Kalshi trading API.
Handles order placement, cancellation, and position queries.
"""

from __future__ import annotations

from typing import Any

import aiohttp

from sports_pipeline.constants import KALSHI_BASE_URL
from sports_pipeline.realtime.websocket.auth import load_private_key
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class AsyncKalshiClient:
    """Async REST client for Kalshi order operations."""

    def __init__(
        self,
        api_key_id: str,
        private_key_path: str,
        base_url: str = KALSHI_BASE_URL,
    ) -> None:
        self._api_key_id = api_key_id
        self._private_key_path = private_key_path
        self._base_url = base_url
        self._session: aiohttp.ClientSession | None = None
        self._private_key = None

    async def start(self) -> None:
        if self._private_key_path:
            self._private_key = load_private_key(self._private_key_path)
        self._session = aiohttp.ClientSession()
        log.info("kalshi_rest_started")

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _auth_headers(self) -> dict[str, str]:
        """Generate authentication headers."""
        if not self._private_key or not self._api_key_id:
            return {}
        return {
            "Authorization": f"Bearer {self._api_key_id}",
            "Content-Type": "application/json",
        }

    async def place_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price: int,
        expiration_ts: int | None = None,
        order_type: str = "limit",
    ) -> dict[str, Any]:
        """Place a limit order.

        Args:
            ticker: Market ticker.
            side: "yes" or "no".
            action: "buy" or "sell".
            count: Number of contracts.
            price: Price in cents (1-99).
            expiration_ts: Unix timestamp for GTD expiry (optional).
            order_type: "limit" or "market".

        Returns:
            Order response dict with order_id.
        """
        payload: dict[str, Any] = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": order_type,
        }
        if order_type == "limit":
            payload["yes_price"] = price
        if expiration_ts:
            payload["expiration_ts"] = expiration_ts

        return await self._post("/orders", payload)

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        """Cancel a specific order."""
        return await self._delete(f"/orders/{order_id}")

    async def cancel_all_orders(self) -> dict[str, Any]:
        """Cancel all open orders."""
        return await self._delete("/orders")

    async def get_positions(self) -> dict[str, Any]:
        """Get current positions."""
        return await self._get("/portfolio/positions")

    async def get_balance(self) -> dict[str, Any]:
        """Get account balance."""
        return await self._get("/portfolio/balance")

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        """Get order book snapshot for REST-based resync."""
        return await self._get(f"/orderbook/{ticker}")

    async def list_markets(
        self,
        series_ticker: str | None = None,
        status: str = "active",
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """List markets with optional filters and pagination.

        Args:
            series_ticker: Filter by series (e.g. "KXNBA").
            status: Market status filter ("active", "closed", etc.).
            cursor: Pagination cursor from previous response.
            limit: Max results per page (1-200).

        Returns:
            Dict with "markets" list and "cursor" for next page.
        """
        params: dict[str, str] = {"limit": str(limit), "status": status}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if cursor:
            params["cursor"] = cursor
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return await self._get(f"/markets?{query}")

    async def _get(self, path: str) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError("Client not started")
        url = f"{self._base_url}{path}"
        async with self._session.get(url, headers=self._auth_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _post(self, path: str, payload: dict) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError("Client not started")
        url = f"{self._base_url}{path}"
        async with self._session.post(
            url, json=payload, headers=self._auth_headers()
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _delete(self, path: str) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError("Client not started")
        url = f"{self._base_url}{path}"
        async with self._session.delete(
            url, headers=self._auth_headers()
        ) as resp:
            resp.raise_for_status()
            return await resp.json()
