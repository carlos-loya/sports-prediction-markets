"""Trade logger that records every edge evaluation to DuckDB.

Logs ALL evaluated markets (traded + rejected with reason) to enable
calibration analysis and understanding of the full decision distribution.
Uses buffered writes to reduce DuckDB I/O.
"""

from __future__ import annotations

import uuid
from collections import deque

from sports_pipeline.realtime.events import EdgeEvent
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class TradeLogEntry:
    """A single entry in the trade log."""

    __slots__ = (
        "log_id",
        "timestamp",
        "ticker",
        "model_prob",
        "market_prob",
        "raw_edge",
        "tradable_edge",
        "kelly_fraction",
        "suggested_side",
        "confidence",
        "model_name",
        "rejected",
        "reject_reason",
    )

    def __init__(self, event: EdgeEvent) -> None:
        self.log_id = str(uuid.uuid4())
        self.timestamp = event.timestamp
        self.ticker = event.ticker
        self.model_prob = event.model_prob
        self.market_prob = event.market_prob
        self.raw_edge = event.raw_edge
        self.tradable_edge = event.tradable_edge
        self.kelly_fraction = event.kelly_fraction
        self.suggested_side = event.suggested_side
        self.confidence = event.confidence
        self.model_name = event.model_name
        self.rejected = event.rejected
        self.reject_reason = event.reject_reason

    def to_tuple(self) -> tuple:
        return (
            self.log_id,
            self.timestamp,
            self.ticker,
            self.model_prob,
            self.market_prob,
            self.raw_edge,
            self.tradable_edge,
            self.kelly_fraction,
            self.suggested_side,
            self.confidence,
            self.model_name,
            self.rejected,
            self.reject_reason,
        )


class TradeLogger:
    """Buffered trade logger that flushes to DuckDB periodically."""

    INSERT_SQL = """
        INSERT INTO gold.trade_log (
            log_id, timestamp, ticker, model_prob, market_prob,
            raw_edge, tradable_edge, kelly_fraction, suggested_side,
            confidence, model_name, rejected, reject_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def __init__(self, buffer_size: int = 100) -> None:
        self._buffer: deque[TradeLogEntry] = deque()
        self._buffer_size = buffer_size
        self._total_logged: int = 0
        self._total_flushed: int = 0

    @property
    def pending(self) -> int:
        return len(self._buffer)

    @property
    def total_logged(self) -> int:
        return self._total_logged

    def log(self, event: EdgeEvent) -> None:
        """Add an edge event to the buffer."""
        self._buffer.append(TradeLogEntry(event))
        self._total_logged += 1

    def should_flush(self) -> bool:
        return len(self._buffer) >= self._buffer_size

    def flush(self, conn: object | None = None) -> list[tuple]:
        """Flush buffer and return entries as tuples.

        If a DuckDB connection is provided, writes directly.
        Otherwise returns tuples for external handling.
        """
        if not self._buffer:
            return []

        entries = list(self._buffer)
        tuples = [e.to_tuple() for e in entries]

        if conn is not None:
            conn.executemany(self.INSERT_SQL, tuples)
            log.info("trade_log_flushed", count=len(tuples))

        self._total_flushed += len(tuples)
        self._buffer.clear()
        return tuples
