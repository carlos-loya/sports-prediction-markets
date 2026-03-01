"""Risk manager: position limits, exposure caps, daily loss tracking."""

from __future__ import annotations

from dataclasses import dataclass, field

from sports_pipeline.realtime.config import RiskConfig
from sports_pipeline.realtime.events import RiskAlertEvent, RiskLevel
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class RiskState:
    """Current risk state across all markets."""

    total_exposure: float = 0.0
    daily_pnl: float = 0.0
    positions: dict[str, int] = field(default_factory=dict)
    level: RiskLevel = RiskLevel.NORMAL


class RiskManager:
    """Monitors risk metrics and emits alerts when thresholds are breached."""

    def __init__(self, config: RiskConfig) -> None:
        self._config = config
        self._state = RiskState()

    @property
    def state(self) -> RiskState:
        return self._state

    @property
    def level(self) -> RiskLevel:
        return self._state.level

    def update_position(self, ticker: str, net_position: int) -> None:
        """Update the tracked position for a market."""
        self._state.positions[ticker] = net_position
        self._state.total_exposure = sum(
            abs(p) for p in self._state.positions.values()
        )

    def update_pnl(self, pnl_delta: float) -> None:
        """Update daily P&L."""
        self._state.daily_pnl += pnl_delta

    def check(self) -> RiskAlertEvent | None:
        """Evaluate current risk state and return alert if thresholds breached."""
        # Check position limits per market
        for ticker, pos in self._state.positions.items():
            if abs(pos) > self._config.max_position_per_market:
                self._state.level = RiskLevel.ELEVATED
                return RiskAlertEvent(
                    level=RiskLevel.ELEVATED,
                    reason=f"Position limit exceeded: {ticker} = {pos}",
                    ticker=ticker,
                    action="cancel_ticker",
                )

        # Check total exposure
        if self._state.total_exposure > self._config.max_total_exposure:
            self._state.level = RiskLevel.CRITICAL
            return RiskAlertEvent(
                level=RiskLevel.CRITICAL,
                reason=f"Total exposure {self._state.total_exposure:.0f}"
                f" > {self._config.max_total_exposure:.0f}",
                action="cancel_all",
            )

        # Check daily loss
        if self._state.daily_pnl < -self._config.daily_loss_limit:
            self._state.level = RiskLevel.CRITICAL
            return RiskAlertEvent(
                level=RiskLevel.CRITICAL,
                reason=f"Daily loss {self._state.daily_pnl:.2f}"
                f" > {self._config.daily_loss_limit:.2f}",
                action="cancel_all",
            )

        # Check emergency loss
        if self._state.daily_pnl < -self._config.emergency_loss_limit:
            self._state.level = RiskLevel.EMERGENCY
            return RiskAlertEvent(
                level=RiskLevel.EMERGENCY,
                reason=f"Emergency loss {self._state.daily_pnl:.2f}",
                action="shutdown",
            )

        self._state.level = RiskLevel.NORMAL
        return None

    def reset_daily(self) -> None:
        """Reset daily counters (call at start of each trading day)."""
        self._state.daily_pnl = 0.0
        self._state.level = RiskLevel.NORMAL
