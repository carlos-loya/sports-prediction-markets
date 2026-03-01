"""Telegram alert dispatcher for real-time notifications.

Sends edge alerts, risk alerts, and fill notifications via the Telegram
Bot API using aiohttp (no heavy bot framework).
"""

from __future__ import annotations

import aiohttp

from sports_pipeline.realtime.config import TelegramConfig
from sports_pipeline.realtime.events import EdgeEvent, FillEvent, RiskAlertEvent
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

TELEGRAM_API = "https://api.telegram.org"


class TelegramBot:
    """Async Telegram alert sender."""

    def __init__(self, config: TelegramConfig) -> None:
        self._config = config
        self._session: aiohttp.ClientSession | None = None

    @property
    def enabled(self) -> bool:
        return (
            self._config.enabled
            and bool(self._config.bot_token)
            and bool(self._config.chat_id)
        )

    async def start(self) -> None:
        if self.enabled:
            self._session = aiohttp.ClientSession()
            log.info("telegram_bot_started")

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def send_message(self, text: str) -> bool:
        """Send a text message to the configured chat."""
        if not self.enabled or not self._session:
            return False
        url = f"{TELEGRAM_API}/bot{self._config.bot_token}/sendMessage"
        payload = {
            "chat_id": self._config.chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("telegram_send_failed", status=resp.status, body=body)
                    return False
                return True
        except Exception:
            log.exception("telegram_send_error")
            return False

    async def send_edge_alert(self, event: EdgeEvent) -> bool:
        """Format and send an edge detection alert."""
        emoji = {"high": "\u2757", "medium": "\u26a0\ufe0f", "low": "\u2139\ufe0f"}.get(
            event.confidence, ""
        )
        text = (
            f"{emoji} <b>Edge Detected</b>\n"
            f"Ticker: <code>{event.ticker}</code>\n"
            f"Side: {event.suggested_side.upper()}\n"
            f"Model: {event.model_prob:.1%} vs Market: {event.market_prob:.1%}\n"
            f"Edge: {event.raw_edge:.1%} (tradable: {event.tradable_edge:.1%})\n"
            f"Kelly: {event.kelly_fraction:.1%}\n"
            f"Confidence: {event.confidence}"
        )
        return await self.send_message(text)

    async def send_risk_alert(self, event: RiskAlertEvent) -> bool:
        """Format and send a risk management alert."""
        text = (
            f"\U0001f6a8 <b>Risk Alert [{event.level.value.upper()}]</b>\n"
            f"Reason: {event.reason}\n"
            f"Action: {event.action}"
        )
        if event.ticker:
            text += f"\nTicker: <code>{event.ticker}</code>"
        return await self.send_message(text)

    async def send_fill_notification(self, event: FillEvent) -> bool:
        """Format and send a fill notification."""
        text = (
            f"\u2705 <b>Fill</b>\n"
            f"Ticker: <code>{event.ticker}</code>\n"
            f"{event.action.upper()} {event.count}x "
            f"{event.side.upper()} @ {event.price:.0f}\u00a2\n"
            f"Remaining: {event.remaining_count}"
        )
        return await self.send_message(text)
