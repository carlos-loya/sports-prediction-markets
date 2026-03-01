"""Local order book synchronization from snapshots + deltas.

Maintains a local copy of the order book per market, applying deltas
from the WebSocket and detecting sequence gaps that require a resync.

Full implementation in Phase 2 (feat/vpin-entropy).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class OrderBookLevel:
    price: float
    quantity: int


@dataclass
class LocalOrderBook:
    """Local order book state for a single market."""

    ticker: str
    yes_bids: dict[float, int] = field(default_factory=dict)
    yes_asks: dict[float, int] = field(default_factory=dict)
    last_seq: int = 0

    @property
    def best_bid(self) -> float | None:
        if not self.yes_bids:
            return None
        return max(p for p, q in self.yes_bids.items() if q > 0)

    @property
    def best_ask(self) -> float | None:
        if not self.yes_asks:
            return None
        return min(p for p, q in self.yes_asks.items() if q > 0)

    @property
    def mid_price(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2

    @property
    def spread(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return ask - bid

    def apply_delta(self, price: float, delta: int, side: str) -> None:
        """Apply a single delta to the book."""
        book = self.yes_bids if side == "bid" else self.yes_asks
        current = book.get(price, 0)
        new_qty = current + delta
        if new_qty <= 0:
            book.pop(price, None)
        else:
            book[price] = new_qty

    def reset(self) -> None:
        self.yes_bids.clear()
        self.yes_asks.clear()
        self.last_seq = 0


class OrderBookManager:
    """Manages local order books for multiple markets."""

    def __init__(self) -> None:
        self._books: dict[str, LocalOrderBook] = defaultdict(
            lambda: LocalOrderBook(ticker="")
        )

    def get_book(self, ticker: str) -> LocalOrderBook:
        if ticker not in self._books:
            self._books[ticker] = LocalOrderBook(ticker=ticker)
        return self._books[ticker]

    def remove_book(self, ticker: str) -> None:
        self._books.pop(ticker, None)
