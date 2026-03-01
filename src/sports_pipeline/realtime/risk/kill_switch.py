"""Three-layer kill switch for the real-time trading system.

Layer 1 (Passive): All orders placed as GTD with 10-min expiry.
                    Auto-cancel on crash — no code needed, handled by exchange.

Layer 2 (Active):  cancelAll() on VPIN > threshold, position limit breach,
                    or repeated errors.

Layer 3 (Emergency): Full shutdown on daily loss > emergency limit,
                     WS disconnect, or anomalous behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from sports_pipeline.realtime.config import RiskConfig
from sports_pipeline.realtime.events import RiskAlertEvent, RiskLevel
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class KillSwitchState(StrEnum):
    ACTIVE = "active"
    TRIGGERED_L2 = "triggered_l2"
    TRIGGERED_L3 = "triggered_l3"


@dataclass
class KillSwitch:
    """Three-layer circuit breaker for the trading system."""

    config: RiskConfig
    state: KillSwitchState = KillSwitchState.ACTIVE
    error_count: int = 0
    max_errors: int = 5
    _triggered_reasons: list[str] = field(default_factory=list)

    @property
    def is_active(self) -> bool:
        return self.state == KillSwitchState.ACTIVE

    @property
    def triggered_reasons(self) -> list[str]:
        return self._triggered_reasons.copy()

    def on_vpin_update(self, vpin: float) -> RiskAlertEvent | None:
        """Check VPIN threshold (Layer 2)."""
        if vpin >= 0.6:
            return self._trigger_l2(f"VPIN critical: {vpin:.3f}")
        return None

    def on_error(self, error: str) -> RiskAlertEvent | None:
        """Track errors and trigger if too many (Layer 2)."""
        self.error_count += 1
        if self.error_count >= self.max_errors:
            return self._trigger_l2(
                f"Too many errors ({self.error_count}): {error}"
            )
        return None

    def on_ws_disconnect(self) -> RiskAlertEvent:
        """WebSocket disconnection triggers emergency (Layer 3)."""
        return self._trigger_l3("WebSocket disconnected")

    def on_daily_loss(self, daily_pnl: float) -> RiskAlertEvent | None:
        """Check daily loss against emergency limit (Layer 3)."""
        if daily_pnl < -self.config.emergency_loss_limit:
            return self._trigger_l3(
                f"Emergency loss limit: ${daily_pnl:.2f}"
            )
        if daily_pnl < -self.config.daily_loss_limit:
            return self._trigger_l2(
                f"Daily loss limit: ${daily_pnl:.2f}"
            )
        return None

    def reset(self) -> None:
        """Reset kill switch to active state."""
        self.state = KillSwitchState.ACTIVE
        self.error_count = 0
        self._triggered_reasons.clear()
        log.info("kill_switch_reset")

    def _trigger_l2(self, reason: str) -> RiskAlertEvent:
        """Trigger Layer 2: cancel all orders."""
        self.state = KillSwitchState.TRIGGERED_L2
        self._triggered_reasons.append(reason)
        log.warning("kill_switch_l2", reason=reason)
        return RiskAlertEvent(
            level=RiskLevel.CRITICAL,
            reason=reason,
            action="cancel_all",
        )

    def _trigger_l3(self, reason: str) -> RiskAlertEvent:
        """Trigger Layer 3: full shutdown."""
        self.state = KillSwitchState.TRIGGERED_L3
        self._triggered_reasons.append(reason)
        log.error("kill_switch_l3", reason=reason)
        return RiskAlertEvent(
            level=RiskLevel.EMERGENCY,
            reason=reason,
            action="shutdown",
        )
