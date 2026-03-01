"""Kafka topic definitions and configuration."""

from __future__ import annotations

from dataclasses import dataclass

from sports_pipeline.constants import (
    TOPIC_BOOK,
    TOPIC_EDGES,
    TOPIC_FILLS,
    TOPIC_LIFECYCLE,
    TOPIC_ORDERS,
    TOPIC_RISK,
    TOPIC_SYSTEM,
    TOPIC_TICKS,
    TOPIC_TRADES,
)


@dataclass(frozen=True)
class TopicConfig:
    """Configuration for a Kafka topic."""

    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    retention_ms: int = 86_400_000  # 24h default

    @property
    def retention_hours(self) -> int:
        return self.retention_ms // 3_600_000


# Topic definitions with retention policies
TOPIC_CONFIGS: dict[str, TopicConfig] = {
    TOPIC_TICKS: TopicConfig(name=TOPIC_TICKS, retention_ms=86_400_000),  # 24h
    TOPIC_TRADES: TopicConfig(name=TOPIC_TRADES, retention_ms=604_800_000),  # 7d
    TOPIC_BOOK: TopicConfig(name=TOPIC_BOOK, retention_ms=3_600_000),  # 1h
    TOPIC_FILLS: TopicConfig(name=TOPIC_FILLS, retention_ms=2_592_000_000),  # 30d
    TOPIC_LIFECYCLE: TopicConfig(name=TOPIC_LIFECYCLE, retention_ms=604_800_000),  # 7d
    TOPIC_EDGES: TopicConfig(name=TOPIC_EDGES, retention_ms=2_592_000_000),  # 30d
    TOPIC_ORDERS: TopicConfig(name=TOPIC_ORDERS, retention_ms=2_592_000_000),  # 30d
    TOPIC_RISK: TopicConfig(name=TOPIC_RISK, retention_ms=2_592_000_000),  # 30d
    TOPIC_SYSTEM: TopicConfig(name=TOPIC_SYSTEM, retention_ms=86_400_000),  # 1d
}

ALL_TOPICS: list[str] = list(TOPIC_CONFIGS.keys())
