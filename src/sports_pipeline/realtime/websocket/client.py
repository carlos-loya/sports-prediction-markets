"""Kalshi WebSocket client with auto-reconnect and Kafka integration.

Connects to the Kalshi WS API, authenticates via RSA, subscribes to
channels, parses messages into typed events, and produces them to Kafka.
"""

from __future__ import annotations

import asyncio
import json
from itertools import count

import websockets
from websockets.asyncio.client import ClientConnection

from sports_pipeline.constants import (
    TOPIC_BOOK,
    TOPIC_FILLS,
    TOPIC_LIFECYCLE,
    TOPIC_TICKS,
    TOPIC_TRADES,
)
from sports_pipeline.realtime.config import WebSocketConfig
from sports_pipeline.realtime.events import (
    BookLevel,
    BookSnapshotEvent,
    FillEvent,
    LifecycleEvent,
    TickEvent,
    TradeEvent,
)
from sports_pipeline.realtime.kafka.producer import KafkaEventProducer
from sports_pipeline.realtime.websocket.auth import load_private_key, sign_ws_auth
from sports_pipeline.realtime.websocket.messages import (
    FillMsg,
    MarketLifecycleMsg,
    OrderBookDeltaMsg,
    TickerMsg,
    TradeMsg,
    WSMessage,
    parse_channel_message,
    ts_to_datetime,
)
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

# Map WS channel names to Kafka topics
CHANNEL_TOPIC_MAP: dict[str, str] = {
    "ticker": TOPIC_TICKS,
    "trade": TOPIC_TRADES,
    "orderbook_delta": TOPIC_BOOK,
    "fill": TOPIC_FILLS,
    "market_lifecycle_v2": TOPIC_LIFECYCLE,
}


class KalshiWebSocketClient:
    """Manages connection to the Kalshi WebSocket API.

    Handles authentication, subscription, reconnection, message parsing,
    and forwarding to Kafka.
    """

    def __init__(
        self,
        config: WebSocketConfig,
        api_key_id: str,
        private_key_path: str,
        producer: KafkaEventProducer,
        tickers: list[str] | None = None,
    ) -> None:
        self._config = config
        self._api_key_id = api_key_id
        self._private_key_path = private_key_path
        self._producer = producer
        self._tickers = tickers or []
        self._ws: ClientConnection | None = None
        self._running = False
        self._msg_id = count(1)
        self._reconnect_delay = config.reconnect_delay_initial

    async def start(self) -> None:
        """Connect and start consuming messages. Reconnects on failure."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_consume()
            except (
                websockets.ConnectionClosed,
                websockets.InvalidStatusCode,
                OSError,
            ) as exc:
                if not self._running:
                    break
                log.warning(
                    "ws_disconnected",
                    error=str(exc),
                    reconnect_delay=self._reconnect_delay,
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._config.reconnect_delay_max,
                )
            except asyncio.CancelledError:
                log.info("ws_client_cancelled")
                break

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def update_subscriptions(self, tickers: list[str]) -> None:
        """Update the list of tickers to subscribe to."""
        new_tickers = [t for t in tickers if t not in self._tickers]
        self._tickers = tickers
        if self._ws and new_tickers:
            for channel in self._config.channels:
                await self._subscribe(channel, new_tickers)

    async def _connect_and_consume(self) -> None:
        """Single connection lifecycle: connect → auth → subscribe → consume."""
        log.info("ws_connecting", url=self._config.url)
        async with websockets.connect(
            self._config.url,
            ping_interval=self._config.ping_interval,
        ) as ws:
            self._ws = ws
            self._reconnect_delay = self._config.reconnect_delay_initial

            await self._authenticate()
            await self._subscribe_all()
            log.info("ws_connected", tickers=len(self._tickers))

            async for raw_msg in ws:
                if not self._running:
                    break
                await self._handle_message(raw_msg)

    async def _authenticate(self) -> None:
        """Send login command with RSA signature."""
        if not self._api_key_id or not self._private_key_path:
            log.warning("ws_auth_skipped", reason="no credentials")
            return
        private_key = load_private_key(self._private_key_path)
        auth_payload = sign_ws_auth(private_key, self._api_key_id)
        cmd = {
            "id": next(self._msg_id),
            "cmd": "login",
            **auth_payload,
        }
        await self._ws.send(json.dumps(cmd))
        # Wait for login response
        resp = await self._ws.recv()
        parsed = json.loads(resp)
        if parsed.get("type") == "error":
            raise RuntimeError(f"WS auth failed: {parsed}")
        log.info("ws_authenticated")

    async def _subscribe_all(self) -> None:
        """Subscribe to all configured channels for all tickers."""
        if not self._tickers:
            return
        for channel in self._config.channels:
            await self._subscribe(channel, self._tickers)

    async def _subscribe(self, channel: str, tickers: list[str]) -> None:
        """Subscribe to a channel for a list of tickers."""
        cmd = {
            "id": next(self._msg_id),
            "cmd": "subscribe",
            "params": {
                "channels": [channel],
                "market_tickers": tickers,
            },
        }
        await self._ws.send(json.dumps(cmd))
        log.debug("ws_subscribed", channel=channel, tickers=len(tickers))

    async def _handle_message(self, raw: str | bytes) -> None:
        """Parse a raw WS message and produce to Kafka."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("ws_invalid_json", raw=str(raw)[:200])
            return

        msg_type = data.get("type", "")

        # Skip control messages (ack, error, heartbeat)
        if msg_type in ("ack", "error", "heartbeat", ""):
            if msg_type == "error":
                log.warning("ws_error", data=data)
            return

        topic = CHANNEL_TOPIC_MAP.get(msg_type)
        if not topic:
            log.debug("ws_unknown_type", type=msg_type)
            return

        try:
            ws_msg = WSMessage(type=msg_type, msg=data.get("msg", data))
            channel_msg = parse_channel_message(msg_type, ws_msg.msg)
            event = self._to_event(msg_type, channel_msg)
            if event:
                ticker = getattr(channel_msg, "market_ticker", "")
                await self._producer.send(topic, event, key=ticker)
        except Exception:
            log.exception("ws_message_processing_error", type=msg_type)

    def _to_event(
        self, channel: str, msg: object
    ) -> TickEvent | TradeEvent | BookSnapshotEvent | FillEvent | LifecycleEvent | None:
        """Convert a parsed channel message to an internal event."""
        if isinstance(msg, TickerMsg):
            return TickEvent(
                ticker=msg.market_ticker,
                yes_price=msg.yes_price,
                no_price=msg.no_price,
                yes_bid=msg.yes_bid,
                yes_ask=msg.yes_ask,
                volume=msg.volume,
                open_interest=msg.open_interest,
                timestamp=ts_to_datetime(msg.ts),
            )
        elif isinstance(msg, TradeMsg):
            return TradeEvent(
                ticker=msg.market_ticker,
                price=msg.yes_price,
                count=msg.count,
                taker_side=msg.taker_side,
                trade_id=msg.trade_id,
                timestamp=ts_to_datetime(msg.ts),
            )
        elif isinstance(msg, OrderBookDeltaMsg):
            # Convert deltas to a snapshot-like event
            # Full orderbook sync will be implemented in Phase 2
            return BookSnapshotEvent(
                ticker=msg.market_ticker,
                yes_bids=[BookLevel(price=lv.price, quantity=lv.delta) for lv in msg.yes],
                yes_asks=[BookLevel(price=lv.price, quantity=lv.delta) for lv in msg.no],
                timestamp=ts_to_datetime(msg.ts),
            )
        elif isinstance(msg, FillMsg):
            return FillEvent(
                order_id=msg.order_id,
                ticker=msg.market_ticker,
                side=msg.side,
                action=msg.action,
                price=msg.yes_price,
                count=msg.count,
                remaining_count=msg.remaining_count,
                timestamp=ts_to_datetime(msg.ts),
            )
        elif isinstance(msg, MarketLifecycleMsg):
            return LifecycleEvent(
                ticker=msg.market_ticker,
                status=msg.status,
                result=msg.result,
                timestamp=ts_to_datetime(msg.ts),
            )
        return None
