"""Kafka event producer with Pydantic serialization."""

from __future__ import annotations

from aiokafka import AIOKafkaProducer

from sports_pipeline.realtime.config import KafkaConfig
from sports_pipeline.realtime.events import BaseEvent
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class KafkaEventProducer:
    """Produces typed events to Kafka topics."""

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            client_id=self._config.client_id,
            value_serializer=lambda v: v,  # we serialize manually
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        log.info("kafka_producer_started", servers=self._config.bootstrap_servers)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
            log.info("kafka_producer_stopped")

    async def send(self, topic: str, event: BaseEvent, key: str | None = None) -> None:
        """Serialize and send an event to a Kafka topic."""
        if not self._producer:
            raise RuntimeError("Producer not started")
        await self._producer.send_and_wait(
            topic=topic,
            value=event.to_json(),
            key=key,
        )

    async def __aenter__(self) -> KafkaEventProducer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
