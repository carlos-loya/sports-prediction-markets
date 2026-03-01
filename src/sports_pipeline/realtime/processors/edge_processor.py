"""Real-time edge detection processor.

Consumes tick events from Kafka, computes edges between model probabilities
and market prices, applies entropy and fee filters, sizes with Kelly,
and produces edge events for downstream consumption.

Every evaluated market is logged (traded or rejected with reason).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from sports_pipeline.realtime.config import RealtimeConfig
from sports_pipeline.realtime.events import EdgeEvent, TickEvent
from sports_pipeline.realtime.processors.entropy_filter import EntropyFilter
from sports_pipeline.realtime.sizing.empirical_kelly import empirical_kelly
from sports_pipeline.realtime.sizing.fee_model import compute_tradable_edge, is_tradable
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class ModelCacheEntry:
    """Cached model probability for a market."""

    ticker: str
    model_prob: float
    model_uncertainty: float  # std dev across sub-models
    model_name: str
    updated_at: float = field(default_factory=time.monotonic)


class ModelCache:
    """Bridges batch model outputs to real-time processing.

    Loads model probabilities from DuckDB and caches them in memory.
    Refreshes periodically (default every 5 minutes).
    """

    def __init__(self, refresh_interval: int = 300) -> None:
        self._cache: dict[str, ModelCacheEntry] = {}
        self._refresh_interval = refresh_interval
        self._last_refresh: float = 0.0

    def get(self, ticker: str) -> ModelCacheEntry | None:
        return self._cache.get(ticker)

    def put(self, entry: ModelCacheEntry) -> None:
        self._cache[entry.ticker] = entry

    def needs_refresh(self) -> bool:
        return (time.monotonic() - self._last_refresh) > self._refresh_interval

    def mark_refreshed(self) -> None:
        self._last_refresh = time.monotonic()

    @property
    def size(self) -> int:
        return len(self._cache)

    def refresh_from_dict(self, entries: dict[str, ModelCacheEntry]) -> None:
        """Bulk update cache from a dictionary of entries."""
        self._cache.update(entries)
        self.mark_refreshed()


class EdgeProcessor:
    """Real-time edge detection from tick events.

    Flow: tick → model cache lookup → entropy check → raw edge →
          fee adjustment → Kelly sizing → produce EdgeEvent
    """

    def __init__(self, config: RealtimeConfig, model_cache: ModelCache) -> None:
        self._config = config
        self._model_cache = model_cache
        self._entropy_filter = EntropyFilter(config.entropy)
        self._stats = {"evaluated": 0, "traded": 0, "rejected": 0}

    @property
    def stats(self) -> dict[str, int]:
        return self._stats.copy()

    def evaluate(self, tick: TickEvent) -> EdgeEvent:
        """Evaluate a tick for edge and return an EdgeEvent.

        Always returns an EdgeEvent — rejected edges have `rejected=True`
        with a `reject_reason` explaining why.
        """
        self._stats["evaluated"] += 1

        # 1. Check model cache
        model = self._model_cache.get(tick.ticker)
        if model is None:
            return self._reject(tick, 0.0, 0.0, "no_model")

        market_prob = tick.yes_price

        # 2. Entropy filter
        if not self._entropy_filter.evaluate(tick.ticker, market_prob):
            return self._reject(
                tick, model.model_prob, market_prob, "entropy_filter"
            )

        # 3. Compute raw edge
        raw_edge = model.model_prob - market_prob

        # Determine side
        if raw_edge > 0:
            suggested_side = "yes"
        elif raw_edge < 0:
            suggested_side = "no"
            raw_edge = abs(raw_edge)
        else:
            return self._reject(tick, model.model_prob, market_prob, "zero_edge")

        is_yes = suggested_side == "yes"
        model_prob_for_kelly = model.model_prob if is_yes else (1.0 - model.model_prob)
        market_price_for_kelly = tick.yes_price if is_yes else (1.0 - tick.yes_price)

        # 4. Fee adjustment
        tradable_edge = compute_tradable_edge(raw_edge, self._config.fees)
        if not is_tradable(raw_edge, self._config.fees):
            return self._reject(
                tick,
                model.model_prob,
                tick.yes_price,
                "below_min_edge",
                raw_edge=raw_edge,
                tradable_edge=tradable_edge,
                suggested_side=suggested_side,
            )

        # 5. Kelly sizing
        kelly_f = empirical_kelly(
            model_prob=model_prob_for_kelly,
            market_price=market_price_for_kelly,
            model_uncertainty=model.model_uncertainty,
            config=self._config.kelly,
        )

        # 6. Confidence level
        confidence = self._classify_confidence(raw_edge)

        self._stats["traded"] += 1
        return EdgeEvent(
            ticker=tick.ticker,
            model_prob=model.model_prob,
            market_prob=tick.yes_price,
            raw_edge=raw_edge,
            tradable_edge=tradable_edge,
            kelly_fraction=kelly_f,
            suggested_side=suggested_side,
            confidence=confidence,
            model_name=model.model_name,
            rejected=False,
        )

    def _classify_confidence(self, edge: float) -> str:
        if edge >= 0.10:
            return "high"
        elif edge >= 0.07:
            return "medium"
        return "low"

    def _reject(
        self,
        tick: TickEvent,
        model_prob: float,
        market_prob: float,
        reason: str,
        raw_edge: float = 0.0,
        tradable_edge: float = 0.0,
        suggested_side: str = "",
    ) -> EdgeEvent:
        self._stats["rejected"] += 1
        return EdgeEvent(
            ticker=tick.ticker,
            model_prob=model_prob,
            market_prob=market_prob,
            raw_edge=raw_edge,
            tradable_edge=tradable_edge,
            kelly_fraction=0.0,
            suggested_side=suggested_side or "yes",
            confidence="low",
            rejected=True,
            reject_reason=reason,
        )
