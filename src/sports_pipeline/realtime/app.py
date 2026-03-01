"""Main entrypoint for the real-time edge detection and trading system.

Wires together all components end-to-end:
  Market Discovery → WS Client → Kafka → Edge Detection → Logging/Alerts/Orders

Consumer groups:
  - rt-edge:      kalshi.ticks  → EdgeProcessor → produce EdgeEvent
  - rt-trade-log: edges         → TradeLogger → periodic DuckDB flush
  - rt-alerts:    edges         → TelegramBot (non-rejected, high confidence)
  - rt-orders:    edges         → OrderManager (non-rejected → paper/live order)

Background loops:
  - Market discovery (configurable interval, default 5 min)
  - ModelCache refresh (configurable interval, default 5 min)
  - TradeLogger flush (every 30s)
"""

from __future__ import annotations

import asyncio
import math
import signal
from pathlib import Path

from sports_pipeline.config import get_settings
from sports_pipeline.constants import TOPIC_EDGES, TOPIC_TICKS
from sports_pipeline.realtime.alerts.telegram_bot import TelegramBot
from sports_pipeline.realtime.discovery import DiscoveredMarket, MarketDiscoveryService
from sports_pipeline.realtime.events import BaseEvent, EdgeEvent, OrderRequestEvent, TickEvent
from sports_pipeline.realtime.execution.kalshi_rest import AsyncKalshiClient
from sports_pipeline.realtime.execution.order_manager import OrderManager
from sports_pipeline.realtime.kafka.consumer import KafkaEventConsumer
from sports_pipeline.realtime.kafka.producer import KafkaEventProducer
from sports_pipeline.realtime.kafka.topics import TOPIC_CONFIGS
from sports_pipeline.realtime.logging.trade_logger import TradeLogger
from sports_pipeline.realtime.model_loader import ModelCacheLoader
from sports_pipeline.realtime.processors.edge_processor import EdgeProcessor, ModelCache
from sports_pipeline.realtime.websocket.client import KalshiWebSocketClient
from sports_pipeline.utils.logging import get_logger, setup_logging

log = get_logger(__name__)

FLUSH_INTERVAL_SECONDS = 30


async def create_topics(bootstrap_servers: str) -> None:
    """Create Kafka topics if they don't exist."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        new_topics = []
        for name, config in TOPIC_CONFIGS.items():
            if name not in existing:
                new_topics.append(
                    NewTopic(
                        name=config.name,
                        num_partitions=config.num_partitions,
                        replication_factor=config.replication_factor,
                    )
                )
        if new_topics:
            await admin.create_topics(new_topics)
            log.info("kafka_topics_created", count=len(new_topics))
        else:
            log.info("kafka_topics_exist", count=len(existing))
    finally:
        await admin.close()


def edge_to_order_request(
    edge: EdgeEvent,
    bankroll: float,
) -> OrderRequestEvent | None:
    """Convert a non-rejected EdgeEvent to an OrderRequestEvent.

    Returns None if the edge is rejected or kelly sizing yields 0 contracts.
    """
    if edge.rejected or edge.kelly_fraction <= 0:
        return None

    # Price in cents for the suggested side
    if edge.suggested_side == "yes":
        price_cents = round(edge.market_prob * 100)
    else:
        price_cents = round((1.0 - edge.market_prob) * 100)

    price_cents = max(1, min(99, price_cents))

    # Size: floor(kelly_fraction × bankroll / price_cents)
    count = int(math.floor(edge.kelly_fraction * bankroll / price_cents))
    if count < 1:
        return None

    return OrderRequestEvent(
        ticker=edge.ticker,
        side=edge.suggested_side,
        action="buy",
        price=float(price_cents),
        count=count,
        source="edge_processor",
    )


async def main() -> None:
    """Main async entrypoint: wire all components and run end-to-end."""
    setup_logging("INFO")
    settings = get_settings()
    rt = settings.realtime

    log.info(
        "rt_starting",
        kafka=rt.kafka.bootstrap_servers,
        ws_url=rt.websocket.url,
        paper_mode=rt.paper_mode,
    )

    # -- Create Kafka topics --
    await create_topics(rt.kafka.bootstrap_servers)

    # -- Shared state --
    model_cache = ModelCache(refresh_interval=rt.model_cache_refresh_seconds)
    discovered_markets: list[DiscoveredMarket] = []

    # -- Kafka producer --
    producer = KafkaEventProducer(rt.kafka)
    await producer.start()

    # -- REST client --
    rest_client = AsyncKalshiClient(
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
    )
    await rest_client.start()

    # -- Core processors --
    edge_processor = EdgeProcessor(config=rt, model_cache=model_cache)
    trade_logger = TradeLogger()
    telegram_bot = TelegramBot(rt.telegram)
    await telegram_bot.start()

    order_manager = OrderManager(
        client=rest_client,
        risk_config=rt.risk,
        paper_mode=rt.paper_mode,
    )

    # -- Discovery + model loader --
    discovery_service = MarketDiscoveryService(
        client=rest_client,
        target_series=rt.target_series,
        entropy=rt.entropy,
    )
    db_path = Path(settings.storage.duckdb_path)
    model_loader = ModelCacheLoader(db_path=db_path)

    # -- WebSocket client --
    ws_client = KalshiWebSocketClient(
        config=rt.websocket,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        producer=producer,
        tickers=[],
    )

    # -- Handler functions --
    async def handle_tick(event: BaseEvent) -> None:
        """Tick → EdgeProcessor → produce EdgeEvent to Kafka."""
        if not isinstance(event, TickEvent):
            return
        edge_event = edge_processor.evaluate(event)
        await producer.send(TOPIC_EDGES, edge_event, key=edge_event.ticker)

    async def handle_edge_for_log(event: BaseEvent) -> None:
        """EdgeEvent → TradeLogger buffer."""
        if not isinstance(event, EdgeEvent):
            return
        trade_logger.log(event)

    async def handle_edge_for_alert(event: BaseEvent) -> None:
        """EdgeEvent → Telegram alert (non-rejected, high confidence only)."""
        if not isinstance(event, EdgeEvent):
            return
        if event.rejected:
            return
        if event.confidence == "high":
            await telegram_bot.send_edge_alert(event)

    async def handle_edge_for_order(event: BaseEvent) -> None:
        """EdgeEvent → OrderRequestEvent → OrderManager."""
        if not isinstance(event, EdgeEvent):
            return
        if event.rejected:
            return
        order_req = edge_to_order_request(event, bankroll=rt.bankroll)
        if order_req:
            await order_manager.handle_order_request(order_req)

    # -- Consumers --
    edge_consumer = KafkaEventConsumer(
        config=rt.kafka,
        topics=[TOPIC_TICKS],
        group_id="rt-edge",
        handler=handle_tick,
    )
    await edge_consumer.start()

    log_consumer = KafkaEventConsumer(
        config=rt.kafka,
        topics=[TOPIC_EDGES],
        group_id="rt-trade-log",
        handler=handle_edge_for_log,
    )
    await log_consumer.start()

    alert_consumer = KafkaEventConsumer(
        config=rt.kafka,
        topics=[TOPIC_EDGES],
        group_id="rt-alerts",
        handler=handle_edge_for_alert,
    )
    await alert_consumer.start()

    order_consumer = KafkaEventConsumer(
        config=rt.kafka,
        topics=[TOPIC_EDGES],
        group_id="rt-orders",
        handler=handle_edge_for_order,
    )
    await order_consumer.start()

    # -- Background loops --
    async def discovery_loop() -> None:
        """Periodically discover markets and update WS subscriptions."""
        nonlocal discovered_markets
        while True:
            try:
                discovered_markets = await discovery_service.discover()
                tickers = [m.ticker for m in discovered_markets]
                if tickers:
                    await ws_client.update_subscriptions(tickers)
                    log.info("subscriptions_updated", count=len(tickers))
                # Also refresh model cache after new discovery
                model_loader.load(discovered_markets, model_cache)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("discovery_loop_error")
            await asyncio.sleep(rt.discovery_interval_seconds)

    async def model_refresh_loop() -> None:
        """Periodically refresh model cache from DuckDB."""
        while True:
            await asyncio.sleep(rt.model_cache_refresh_seconds)
            try:
                if discovered_markets:
                    model_loader.load(discovered_markets, model_cache)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("model_refresh_error")

    async def trade_log_flush_loop() -> None:
        """Periodically flush trade log buffer to DuckDB."""
        conn = None
        if db_path.exists():
            try:
                from sports_pipeline.loaders.duckdb_loader import DuckDBLoader

                loader = DuckDBLoader(db_path=db_path)
                conn = loader.get_connection()
            except Exception:
                log.warning("trade_log_db_unavailable", exc_info=True)

        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            try:
                if trade_logger.pending > 0:
                    trade_logger.flush(conn=conn)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("trade_log_flush_error")

    # -- Graceful shutdown --
    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        log.info("rt_shutdown_signal")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # -- Launch all tasks --
    tasks = [
        asyncio.create_task(ws_client.start(), name="ws_client"),
        asyncio.create_task(edge_consumer.consume(), name="edge_consumer"),
        asyncio.create_task(log_consumer.consume(), name="log_consumer"),
        asyncio.create_task(alert_consumer.consume(), name="alert_consumer"),
        asyncio.create_task(order_consumer.consume(), name="order_consumer"),
        asyncio.create_task(discovery_loop(), name="discovery_loop"),
        asyncio.create_task(model_refresh_loop(), name="model_refresh_loop"),
        asyncio.create_task(trade_log_flush_loop(), name="trade_log_flush_loop"),
    ]

    log.info(
        "rt_running",
        tasks=len(tasks),
        paper_mode=rt.paper_mode,
        bankroll=rt.bankroll,
    )

    # Wait for shutdown signal
    await shutdown_event.wait()
    log.info("rt_shutting_down")

    # Cancel tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Final flush
    try:
        if trade_logger.pending > 0:
            trade_logger.flush()
            log.info("trade_log_final_flush", pending=trade_logger.pending)
    except Exception:
        log.exception("trade_log_final_flush_error")

    # Cleanup
    await ws_client.stop()
    await edge_consumer.stop()
    await log_consumer.stop()
    await alert_consumer.stop()
    await order_consumer.stop()
    await producer.stop()
    await telegram_bot.stop()
    await rest_client.stop()

    log.info(
        "rt_stopped",
        edges_evaluated=edge_processor.stats["evaluated"],
        edges_traded=edge_processor.stats["traded"],
        edges_rejected=edge_processor.stats["rejected"],
        trade_log_total=trade_logger.total_logged,
    )
