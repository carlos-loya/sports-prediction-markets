"""Main entrypoint for the real-time edge detection system.

Launches the WebSocket client, Kafka producer, and a logging consumer.
Phase 1 focuses on establishing the data flow: WS → Kafka → log.
"""

from __future__ import annotations

import asyncio
import signal

from sports_pipeline.config import get_settings
from sports_pipeline.constants import (
    TOPIC_FILLS,
    TOPIC_LIFECYCLE,
    TOPIC_TICKS,
    TOPIC_TRADES,
)
from sports_pipeline.realtime.events import BaseEvent
from sports_pipeline.realtime.kafka.consumer import KafkaEventConsumer
from sports_pipeline.realtime.kafka.producer import KafkaEventProducer
from sports_pipeline.realtime.kafka.topics import TOPIC_CONFIGS
from sports_pipeline.realtime.websocket.client import KalshiWebSocketClient
from sports_pipeline.utils.logging import get_logger, setup_logging

log = get_logger(__name__)


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


async def log_event_handler(event: BaseEvent) -> None:
    """Simple handler that logs events for Phase 1 verification."""
    log.info(
        "rt_event",
        event_type=event.event_type.value,
        timestamp=event.timestamp.isoformat(),
        **{
            k: v
            for k, v in event.model_dump().items()
            if k not in ("event_type", "timestamp")
        },
    )


async def main() -> None:
    """Main async entrypoint: start WS client + Kafka producer + log consumer."""
    setup_logging("INFO")
    settings = get_settings()
    rt_config = settings.realtime

    log.info(
        "rt_starting",
        kafka=rt_config.kafka.bootstrap_servers,
        ws_url=rt_config.websocket.url,
    )

    # Create Kafka topics
    await create_topics(rt_config.kafka.bootstrap_servers)

    # Start Kafka producer
    producer = KafkaEventProducer(rt_config.kafka)
    await producer.start()

    # Start logging consumer (consumes all market data topics)
    log_topics = [TOPIC_TICKS, TOPIC_TRADES, TOPIC_FILLS, TOPIC_LIFECYCLE]
    log_consumer = KafkaEventConsumer(
        config=rt_config.kafka,
        topics=log_topics,
        group_id="rt-logger",
        handler=log_event_handler,
    )
    await log_consumer.start()

    # Start WebSocket client
    ws_client = KalshiWebSocketClient(
        config=rt_config.websocket,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        producer=producer,
        tickers=[],  # Will be populated by market discovery (Phase 2+)
    )

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        log.info("rt_shutdown_signal")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Run all tasks
    tasks = [
        asyncio.create_task(ws_client.start(), name="ws_client"),
        asyncio.create_task(log_consumer.consume(), name="log_consumer"),
    ]

    log.info("rt_running", tasks=len(tasks))

    # Wait for shutdown signal
    await shutdown_event.wait()
    log.info("rt_shutting_down")

    # Cancel tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Cleanup
    await ws_client.stop()
    await log_consumer.stop()
    await producer.stop()

    log.info("rt_stopped")
