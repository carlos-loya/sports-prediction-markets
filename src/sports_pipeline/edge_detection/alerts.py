"""Alert dispatch for detected edges."""

from __future__ import annotations

from typing import Any

import requests

from sports_pipeline.config import get_settings
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class AlertDispatcher:
    """Dispatch edge alerts to various channels."""

    def __init__(self) -> None:
        settings = get_settings()
        self.slack_webhook_url = settings.slack_webhook_url

    def dispatch(self, edges: list[dict[str, Any]]) -> None:
        """Send alerts for detected edges."""
        if not edges:
            return

        # Always log to console
        self._log_edges(edges)

        # Optional Slack notification
        if self.slack_webhook_url:
            self._send_slack(edges)

    def _log_edges(self, edges: list[dict[str, Any]]) -> None:
        """Log edge signals to structured logging."""
        for edge in edges:
            log.info(
                "edge_alert",
                ticker=edge.get("kalshi_ticker"),
                edge=edge.get("edge"),
                confidence=edge.get("confidence"),
                side=edge.get("suggested_side"),
                kelly=edge.get("kelly_fraction"),
                model=edge.get("model_name"),
            )

    def _send_slack(self, edges: list[dict[str, Any]]) -> None:
        """Send edge alerts to Slack webhook."""
        blocks = []
        for edge in edges[:10]:  # Limit to 10 alerts
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{edge.get('kalshi_ticker', '?')}* | "
                        f"Edge: `{edge.get('edge', 0):+.1%}` | "
                        f"Side: `{edge.get('suggested_side', '?')}` | "
                        f"Kelly: `{edge.get('kelly_fraction', 0):.1%}` | "
                        f"Confidence: `{edge.get('confidence', '?')}`\n"
                        f"_{edge.get('reasoning', '')}_"
                    ),
                },
            })

        payload = {
            "text": f"Edge Detection: {len(edges)} signal(s) found",
            "blocks": blocks,
        }

        try:
            resp = requests.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
            log.info("slack_alert_sent", count=len(edges))
        except Exception:
            log.warning("slack_alert_failed", exc_info=True)
