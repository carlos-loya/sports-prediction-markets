"""Tests for Kafka producer and consumer wrappers (mocked)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sports_pipeline.realtime.config import KafkaConfig
from sports_pipeline.realtime.events import TickEvent, TradeEvent, deserialize_event
from sports_pipeline.realtime.kafka.consumer import KafkaEventConsumer
from sports_pipeline.realtime.kafka.producer import KafkaEventProducer
from sports_pipeline.realtime.kafka.topics import ALL_TOPICS, TOPIC_CONFIGS


class TestTopicConfigs:
    def test_all_topics_defined(self):
        assert len(ALL_TOPICS) == 9

    def test_retention_hours(self):
        from sports_pipeline.constants import TOPIC_TICKS, TOPIC_TRADES

        assert TOPIC_CONFIGS[TOPIC_TICKS].retention_hours == 24
        assert TOPIC_CONFIGS[TOPIC_TRADES].retention_hours == 168  # 7 days


@pytest.mark.asyncio
class TestKafkaEventProducer:
    async def test_send_serializes_and_produces(self):
        config = KafkaConfig()
        producer = KafkaEventProducer(config)

        mock_aiokafka = AsyncMock()
        mock_aiokafka.send_and_wait = AsyncMock()
        producer._producer = mock_aiokafka

        event = TickEvent(
            ticker="T1",
            yes_price=0.65,
            no_price=0.35,
        )
        await producer.send("kalshi.ticks", event, key="T1")

        mock_aiokafka.send_and_wait.assert_called_once()
        call_kwargs = mock_aiokafka.send_and_wait.call_args
        assert call_kwargs.kwargs["topic"] == "kalshi.ticks"
        assert call_kwargs.kwargs["key"] == "T1"

        # Verify the value is valid JSON that deserializes back
        value = call_kwargs.kwargs["value"]
        restored = deserialize_event(value)
        assert isinstance(restored, TickEvent)
        assert restored.ticker == "T1"

    async def test_send_raises_if_not_started(self):
        config = KafkaConfig()
        producer = KafkaEventProducer(config)

        with pytest.raises(RuntimeError, match="not started"):
            await producer.send("topic", TickEvent(ticker="T1", yes_price=0.5, no_price=0.5))

    async def test_context_manager(self):
        config = KafkaConfig()
        producer = KafkaEventProducer(config)

        with patch.object(producer, "start", new_callable=AsyncMock) as mock_start, patch.object(
            producer, "stop", new_callable=AsyncMock
        ) as mock_stop:
            async with producer as p:
                assert p is producer
            mock_start.assert_called_once()
            mock_stop.assert_called_once()


@pytest.mark.asyncio
class TestKafkaEventConsumer:
    async def test_context_manager(self):
        config = KafkaConfig()
        consumer = KafkaEventConsumer(config, topics=["test"], handler=AsyncMock())

        with patch.object(consumer, "start", new_callable=AsyncMock) as mock_start, patch.object(
            consumer, "stop", new_callable=AsyncMock
        ) as mock_stop:
            async with consumer as c:
                assert c is consumer
            mock_start.assert_called_once()
            mock_stop.assert_called_once()

    async def test_consume_raises_without_handler(self):
        config = KafkaConfig()
        consumer = KafkaEventConsumer(config, topics=["test"])
        consumer._consumer = AsyncMock()

        with pytest.raises(RuntimeError, match="not started"):
            await consumer.consume()

    async def test_consume_dispatches_events(self):
        config = KafkaConfig()
        handler = AsyncMock()
        consumer = KafkaEventConsumer(config, topics=["test"], handler=handler)

        event = TradeEvent(ticker="T1", price=0.5, count=1, taker_side="yes")
        mock_msg = MagicMock()
        mock_msg.value = event.to_json()
        mock_msg.topic = "test"
        mock_msg.offset = 0

        # Create a mock async iterator that yields one message then stops
        async def mock_iter():
            yield mock_msg

        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = lambda self: mock_iter()
        consumer._consumer = mock_consumer
        consumer._running = True

        # Stop after first message
        async def stop_after_one(evt):
            consumer._running = False

        handler.side_effect = stop_after_one

        await consumer.consume()
        handler.assert_called_once()
        dispatched = handler.call_args[0][0]
        assert isinstance(dispatched, TradeEvent)
        assert dispatched.ticker == "T1"
