"""Kafka event consumer with Pydantic deserialization."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from aiokafka import AIOKafkaConsumer

from sports_pipeline.realtime.config import KafkaConfig
from sports_pipeline.realtime.events import BaseEvent, deserialize_event
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

EventHandler = Callable[[BaseEvent], Coroutine[Any, Any, None]]


class KafkaEventConsumer:
    """Consumes typed events from Kafka topics and dispatches to handlers."""

    def __init__(
        self,
        config: KafkaConfig,
        topics: list[str],
        group_id: str | None = None,
        handler: EventHandler | None = None,
    ) -> None:
        self._config = config
        self._topics = topics
        self._group_id = group_id or config.group_id
        self._handler = handler
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda v: v,  # we deserialize manually
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        log.info(
            "kafka_consumer_started",
            topics=self._topics,
            group_id=self._group_id,
        )

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
            log.info("kafka_consumer_stopped")

    async def consume(self) -> None:
        """Consume messages and dispatch to handler. Runs until stop() is called."""
        if not self._consumer or not self._handler:
            raise RuntimeError("Consumer not started or no handler set")
        self._running = True
        try:
            async for msg in self._consumer:
                if not self._running:
                    break
                try:
                    event = deserialize_event(msg.value)
                    await self._handler(event)
                except Exception:
                    log.exception(
                        "event_processing_error",
                        topic=msg.topic,
                        offset=msg.offset,
                    )
        except asyncio.CancelledError:
            log.info("consumer_cancelled", topics=self._topics)

    async def __aenter__(self) -> KafkaEventConsumer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
